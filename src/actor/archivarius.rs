use super::volume;
use crate::actor::volume::{GetChunk, GetChunkResult, VolumeActor};
use crate::actor::{GetDump, GetDumpResult, UpdateChunkCommand, UpdateCommand};
use crate::prelude::*;
use actix::{Actor, Addr, Arbiter, AsyncContext, Context, Handler, Message};
use futures::stream::iter;
use futures::StreamExt;
use log::*;
use serde::{Deserialize, Serialize};
use std::future::Future;

use std::ops::RangeInclusive;
use std::pin::Pin;

const ARBITERS: usize = 8;

use std::mem::replace;

use std::path::Path;
use std::sync::{Arc, RwLock};

const MAX_CHUNK_SIZE: usize = 22 * 1024 * 1024;

pub struct ArchivariusActor {
    store: ArchivariusStore<String>,
    ty: EntityType,
    initialized: bool,
    closed_actors: Vec<(EntityRange, Addr<VolumeActor>)>,
    open_actor: Addr<VolumeActor>,
    last_id_to_open_actor: Option<EntityId>,
    arbiters: Vec<Arbiter>,
}

impl ArchivariusActor {
    pub fn new(ty: EntityType) -> ArchivariusActor {
        let store =
            ArchivariusStore::new(format!("/tmp/wd-rt-dumps/{:?}/archivarius.json", ty));

        let state = store.load().unwrap_or_default();

        Self::new_initialized(ty, store, state)
    }

    fn new_initialized(
        ty: EntityType,
        store: ArchivariusStore<String>,
        state: StoredState,
    ) -> ArchivariusActor {
        let arbiters = (0..ARBITERS).map(|_| Arbiter::new()).collect::<Vec<_>>();

        let closed_actors = state
            .closed
            .iter()
            .enumerate()
            .map(|(id, er)| {
                let ar_idx = id % arbiters.len();
                let vol = VolumeActor::start_in_arbiter(&arbiters[ar_idx], move |_| {
                    volume::VolumeActor::new_closed(ty, id as i32)
                });

                (er.clone(), vol)
            })
            .collect::<Vec<_>>();
        let new_id = closed_actors.len();
        let ar_idx = new_id % arbiters.len();
        let last_id_to_open_actor: Option<EntityId> = state.open_volume_last_entity_id;
        let initialized = state.initialized;

        let open_actor = VolumeActor::start_in_arbiter(&arbiters[ar_idx], move |_| {
            if initialized {
                VolumeActor::new_closed(ty, new_id as i32)
            } else {
                VolumeActor::new_open(ty, new_id as i32)
            }
        });

        ArchivariusActor {
            store,
            ty,
            initialized,
            arbiters,
            closed_actors,
            open_actor,
            last_id_to_open_actor,
        }
    }

    fn state(&self) -> StoredState {
        let closed = self.closed_actors.iter().map(|(r, _)| r.clone()).collect();

        StoredState {
            closed,
            open_volume_last_entity_id: self.last_id_to_open_actor,
            initialized: self.initialized,
        }
    }

    fn initialization_in_progress(&self) -> bool {
        !self.initialized
    }

    fn find_closed(&self, id: EntityId) -> Option<Addr<VolumeActor>> {
        let target_actor = self
            .closed_actors
            .iter()
            .find(|(range, _actor)| range.inner.contains(&id))
            .map(|(_, a)| a.clone());
        target_actor
    }

    fn close_current_open_actor(&mut self) {
        let new_id = self.closed_actors.len() + 1;

        let arbeiter_index = new_id % self.arbiters.len();

        let ty = self.ty;
        let new_open_actor =
            VolumeActor::start_in_arbiter(&self.arbiters[arbeiter_index], move |_| {
                VolumeActor::new_open(ty, new_id as i32)
            });

        let old_open_actor = replace(&mut self.open_actor, new_open_actor);

        if self.initialization_in_progress() {
            info!("Sending command to persist the chunk");
            old_open_actor.do_send(volume::Persist);
            // TODO: Must await Persist response to sore Archivarius state
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

        self.closed_actors.push((new_range, old_open_actor));

        self.store.save(self.state());
    }
}

impl Actor for ArchivariusActor {
    type Context = Context<Self>;
    fn started(&mut self, _ctx: &mut Self::Context) {
        info!("ArchiveActor started!")
    }
}

impl Handler<GetDump> for ArchivariusActor {
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

impl Handler<UpdateCommand> for ArchivariusActor {
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

impl Handler<CloseOpenActor> for ArchivariusActor {
    type Result = Arc<()>;

    fn handle(&mut self, msg: CloseOpenActor, _ctx: &mut Self::Context) -> Self::Result {
        if self.open_actor != msg.addr {
            return Arc::new(());
        }

        self.close_current_open_actor();

        Arc::new(())
    }
}

impl Handler<InitializationFinished> for ArchivariusActor {
    type Result = Arc<()>;

    fn handle(&mut self, _msg: InitializationFinished, _ctx: &mut Self::Context) -> Self::Result {
        self.initialized = true;

        info!("Initialization finished. Sending command to persist the chunk of an open actor");
        self.open_actor.do_send(volume::Persist);

        Arc::new(())
    }
}

struct CloseOpenActor {
    addr: Addr<VolumeActor>,
}

impl Message for CloseOpenActor {
    type Result = Arc<()>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

struct ArchivariusStore<P: AsRef<Path>> {
    path: RwLock<P>,
}

impl<P: AsRef<Path>> ArchivariusStore<P> {
    fn new(path: P) -> ArchivariusStore<P> {
        use std::fs::create_dir_all;
        create_dir_all(path.as_ref().parent().unwrap()).unwrap();
        ArchivariusStore {
            path: RwLock::new(path),
        }
    }

    fn load(&self) -> Option<StoredState> {
        use serde_json::*;
        use std::fs::read;
        let path = self.path.read().unwrap();

        read(path.as_ref())
            .ok()
            .map(|r| from_slice::<StoredState>(&r).unwrap())
    }

    fn save(&mut self, state: StoredState) {
        use serde_json::to_string;
        use std::fs::write;

        let contents = to_string(&state).unwrap();

        let path = self.path.write().unwrap();

        write(path.as_ref(), contents).unwrap();
    }
}

#[derive(Serialize, Deserialize)]
struct StoredState {
    closed: Vec<EntityRange>,
    open_volume_last_entity_id: Option<EntityId>,
    initialized: bool, //TODO Should be Option<last event Id> from event stream
}

impl Default for StoredState {
    fn default() -> Self {
        StoredState {
            closed: vec![],
            open_volume_last_entity_id: None,
            initialized: false,
        }
    }
}
