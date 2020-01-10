use std::future::Future;
use std::pin::Pin;
use actix::{Handler, Actor, Context, Message};
use actix_web::web::Bytes;
use crate::actor::UpdateCommand;
use log::*;


impl Handler<UpdateCommand> for ChunkActor {
    type Result = Result<Pin<Box<dyn Future<Output = ()> + Send + Sync>>, ()>;

    fn handle(&mut self, msg: UpdateCommand, _ctx: &mut Self::Context) -> Self::Result {
        info!("UpdateCommand({}): {}", self.i, msg.id);
        self.data.push_str(&msg.data);
        self.data.push_str("\n");
        Ok(Box::pin(futures::future::ready(())))
    }
}

pub struct ChunkActor {
    i: i32,
    data: String,
}

impl ChunkActor {
    pub fn new(i: i32) -> ChunkActor {
        ChunkActor {
            i,
            data: "".to_owned(),
        }
    }
}

impl Actor for ChunkActor {
    type Context = Context<Self>;
}

pub struct GetChunk;

type GetChunkResult = Result<Bytes, ()>;

impl Message for GetChunk {
    type Result = GetChunkResult;
}

impl Handler<GetChunk> for ChunkActor {
    type Result = GetChunkResult;

    fn handle(&mut self, _msg: GetChunk, _ctx: &mut Self::Context) -> Self::Result {
        let to_send = Bytes::from(self.data.clone());
        if self.data.len() > 0 {
            info!(
                "GetChunk : {}, len={}, clone_len={}",
                self.i,
                self.data.len(),
                self.data.clone().len()
            );
        }
        Ok(to_send)
    }
}
