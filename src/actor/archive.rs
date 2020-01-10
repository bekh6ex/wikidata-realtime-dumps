use super::chunk;
use crate::actor::chunk::GetChunk;
use crate::actor::{GetDump, GetDumpResult, UpdateCommand};
use actix::{Actor, Addr, Context, Handler};
use actix_web::web::Bytes;
use futures::stream::iter;
use futures::StreamExt;
use log::*;
use std::future::Future;
use std::pin::Pin;

pub struct ArchiveActor {
    children: Vec<Addr<chunk::ChunkActor>>,
}

impl ArchiveActor {
    pub fn new() -> ArchiveActor {
        ArchiveActor {
            children: (0..1000)
                .map(|i| chunk::ChunkActor::new(i).start())
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
        let stream = iter(self.children.clone())
            .map(|c| c.send(GetChunk))
            .buffer_unordered(6)
            .map(|r| {
                let b = r.expect("response").expect("Bytes");
                b
            })
            .filter_map(|b: Bytes| {
                async {
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
        println!("Archive UpdateCommand: {}", item.id);
        let child_index = item.id % 1000;
        let child = self.children.get(child_index as usize).unwrap();
        use futures::future::FutureExt;
        Ok(Box::pin(child.send(item).map(|_| ())))
    }
}
