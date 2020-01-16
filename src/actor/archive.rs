use super::chunk;
use crate::actor::chunk::{ChunkActor, GetChunk, GetChunkResult};
use crate::actor::{GetDump, GetDumpResult, UpdateChunkCommand, UpdateCommand};
use crate::prelude::EntityId;
use actix::{Actor, Addr, Arbiter, AsyncContext, Context, Handler, Message};
use futures::stream::iter;
use futures::StreamExt;
use log::*;
use std::future::Future;

use std::ops::RangeInclusive;
use std::pin::Pin;


const ARBITERS: usize = 8;



use std::mem::{replace};


use std::sync::Arc;

const MAX_CHUNK_SIZE: usize = 22 * 1024 * 1024;

pub struct ArchiveActor {
    initialization_in_progress: bool,
    closed_actors: Vec<(EntityRange, Addr<chunk::ChunkActor>)>,
    open_actor: Addr<chunk::ChunkActor>,
    last_id_to_open_actor: Option<EntityId>,
    arbeiters: Vec<Arbiter>,
}

impl ArchiveActor {
    pub fn new() -> ArchiveActor {
        let arbeiters = (0..ARBITERS).map(|_| Arbiter::new()).collect::<Vec<_>>();

        let closed_actors = vec![];
        let new_id = closed_actors.len();
        let open_actor = chunk::ChunkActor::start_in_arbiter(&arbeiters[0], move |_| {
            chunk::ChunkActor::new(new_id as i32)
        });
        let last_id_to_open_actor: Option<EntityId> = None;

        ArchiveActor {
            initialization_in_progress: true,
            arbeiters,
            closed_actors,
            open_actor,
            last_id_to_open_actor,
        }
    }

    fn find_closed(&self, id: EntityId) -> Option<Addr<ChunkActor>> {
        let target_actor = self
            .closed_actors
            .iter()
            .find(|(range, _actor)| range.inner.contains(&id))
            .map(|(_, a)| a.clone());
        target_actor
    }

    fn close_current_open_actor(&mut self) {
        let new_id = self.closed_actors.len() + 1;

        let arbeiter_index = new_id % self.arbeiters.len();

        let new_open_actor =
            chunk::ChunkActor::start_in_arbiter(&self.arbeiters[arbeiter_index], move |_| {
                chunk::ChunkActor::new(new_id as i32)
            });

        let old_open_actor = replace(&mut self.open_actor, new_open_actor);

        if self.initialization_in_progress {
            info!("Sending command to persist the chunk");
            old_open_actor.do_send(chunk::Persist);
        }

        let top_id = self
            .last_id_to_open_actor
            .take()
            .expect("Closed an empty open actor");
        let new_range = self
            .closed_actors
            .last()
            .map(|(last_range, _)| last_range.next_adjacent(top_id))
            .unwrap_or_else(|| EntityRange::from_start(top_id));

        self.closed_actors.push((new_range, old_open_actor))
    }
}

impl Actor for ArchiveActor {
    type Context = Context<Self>;
    fn started(&mut self, _ctx: &mut Self::Context) {
        info!("ArchiveActor started!")
    }
}

impl Handler<GetDump> for ArchiveActor {
    type Result = GetDumpResult;

    fn handle(&mut self, _msg: GetDump, _ctx: &mut Self::Context) -> Self::Result {
        let thread1 = std::thread::current();
        let thread = thread1.name().unwrap_or("<unknown>").to_owned();
        debug!("thread={} Get dump", thread);

        let mut children: Vec<Addr<_>> =
            self.closed_actors.iter().map(|(_, c)| c.clone()).collect();

        children.push(self.open_actor.clone());

        let stream = iter(children)
            .map(|c| c.send(GetChunk))
            .buffered(ARBITERS)
            .map(|r| {
                let b: GetChunkResult = r.expect("Actor communication issue");
                b
            })
            .filter_map(|b: GetChunkResult| {
                async {
                    let b = b.expect("Failed to get chunk");
                    if b.len() == 0 {
                        None
                    } else {
                        Some(b)
                    }
                }
            });
        Ok(Box::pin(stream))
    }
}

impl Handler<UpdateCommand> for ArchiveActor {
    type Result = Result<Pin<Box<dyn Future<Output = ()> + Send + Sync>>, ()>;

    fn handle(&mut self, item: UpdateCommand, ctx: &mut Self::Context) -> Self::Result {
        let thread1 = std::thread::current();
        let thread = thread1.name().unwrap_or("<unknown>");
        debug!(
            "thread={} UpdateCommand[ArchiveActor]: entity_id={}",
            thread, item.id
        );

        let (is_open_actor, child) = {
            let target_actor = self.find_closed(item.id);

            let actor_tuple = match target_actor {
                Some(actor) => (false, actor),
                None => (true, self.open_actor.clone()),
            };

            actor_tuple.clone()
        };

        // TODO: Close and reopen open actor. Don't forget to check ID of the chunk actor
        // TODO: Figure out ARBEITER scheduling

        let addr = ctx.address();

        if is_open_actor {
            self.last_id_to_open_actor = Some(item.id);
        }

        let UpdateCommand { id, revision, data } = item;

        let result = async move {
            let result = child.send(UpdateChunkCommand { id, revision, data });

            let size = result
                .await
                .expect("Communication with child result failed")
                .expect("Child failed");

            if size > MAX_CHUNK_SIZE && is_open_actor {
                debug!("Schedule the open actor closing. chunk_size={}", size);
                addr.do_send(CloseOpenActor { addr: child });

                if size > MAX_CHUNK_SIZE * 2 {
                    panic!("Size of a chunk is way too big: {}", size)
                }
            }
        };
        Ok(Box::pin(result))
    }
}

impl Handler<CloseOpenActor> for ArchiveActor {
    type Result = Arc<()>;

    fn handle(&mut self, msg: CloseOpenActor, _ctx: &mut Self::Context) -> Self::Result {
        if self.open_actor != msg.addr {
            return Arc::new(());
        }

        self.close_current_open_actor();

        Arc::new(())
    }
}

impl Handler<InitializationFinished> for ArchiveActor {
    type Result = Arc<()>;

    fn handle(&mut self, _msg: InitializationFinished, _ctx: &mut Self::Context) -> Self::Result {
        self.initialization_in_progress = false;

        info!("Initialization finished. Sending command to persist the chunk of an open actor");
        self.open_actor.do_send(chunk::Persist);

        Arc::new(())
    }
}

struct CloseOpenActor {
    addr: Addr<ChunkActor>,
}

impl Message for CloseOpenActor {
    type Result = Arc<()>;
}

#[derive(Debug)]
struct EntityRange {
    inner: RangeInclusive<EntityId>,
}

impl EntityRange {
    fn from_start(up_to: EntityId) -> EntityRange {
        let start = up_to.ty().id(1);
        EntityRange {
            inner: RangeInclusive::new(start, up_to),
        }
    }

    fn next_adjacent(&self, up_to: EntityId) -> EntityRange {
        EntityRange {
            inner: RangeInclusive::new(self.inner.end().next(), up_to),
        }
    }
}

pub struct InitializationFinished;

impl Message for InitializationFinished {
    type Result = Arc<()>;
}
