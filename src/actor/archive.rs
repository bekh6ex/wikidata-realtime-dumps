use super::chunk;
use crate::actor::chunk::{GetChunk, GetChunkResult};
use crate::actor::{GetDump, GetDumpResult, UpdateChunkCommand, UpdateCommand};
use actix::{Actor, Addr, Arbiter, Context, Handler};
use futures::stream::iter;
use futures::StreamExt;
use log::*;
use std::future::Future;
use std::pin::Pin;

const SUBACTORS: usize = 17;
const ARBITERS: usize = 8;

pub struct ArchiveActor {
    children: Vec<Addr<chunk::ChunkActor>>,
}

impl ArchiveActor {
    pub fn new() -> ArchiveActor {
        let arbiters: Vec<_> = (0..ARBITERS).map(|_| Arbiter::new()).collect();

        ArchiveActor {
            children: (0..SUBACTORS)
                .map(|i| {
                    let index = i as usize % arbiters.len();
                    let arb = arbiters.get(index).unwrap();
                    chunk::ChunkActor::start_in_arbiter(&arb, move |_| {
                        chunk::ChunkActor::new(i as i32)
                    })
                })
                .collect(),
        }
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

        let stream = iter(self.children.clone())
            .map(|c| c.send(GetChunk))
            .buffer_unordered(ARBITERS)
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

    fn handle(&mut self, item: UpdateCommand, _ctx: &mut Self::Context) -> Self::Result {
        let thread1 = std::thread::current();
        let thread = thread1.name().unwrap_or("<unknown>");
        debug!(
            "thread={} UpdateCommand[ArchiveActor]: entity_id={}",
            thread, item.id
        );
        let child_index = item.id.n() % SUBACTORS as u32;
        let child = self.children.get(child_index as usize).unwrap().clone();

        let result = async move {
            let UpdateCommand { id, revision, data } = item;
            child
                .send(UpdateChunkCommand { id, revision, data })
                .await
                .expect("Communication with child result failed")
                .expect("Child failed");
        };
        Ok(Box::pin(result))
    }
}
