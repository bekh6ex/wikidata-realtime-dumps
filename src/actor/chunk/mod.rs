use super::SerializedEntity;
use crate::actor::UpdateChunkCommand;
use actix::{Actor, Context, Handler, Message};
use actix_web::web::Bytes;

use log::*;

use crate::prelude::{EntityId, EntityType};
use chunk_storage::{GzChunkStorage, GzippedData};

use std::collections::BTreeMap;

mod chunk_storage;

pub struct ChunkActor {
    i: i32,
    storage: GzChunkStorage<String>,
}

impl ChunkActor {
    pub fn new(i: i32) -> ChunkActor {
        ChunkActor {
            i,
            storage: GzChunkStorage::new(
                EntityType::Property,
                format!("/tmp/wd-rt-dumps/chunk/{}.gz", i),
            ),
        }
    }

    fn load(&self) -> GzippedData {
        self.storage.load()
    }
}

impl Handler<UpdateChunkCommand> for ChunkActor {
    type Result = Result<usize, ()>;

    fn handle(&mut self, msg: UpdateChunkCommand, _ctx: &mut Self::Context) -> Self::Result {
        let thread = {
            let thread1 = std::thread::current();
            thread1.name().unwrap_or("<unknown>").to_owned()
        };

        debug!(
            "thread={} UpdateCommand[actor_id={}]: entity_id={}",
            thread, self.i, msg.id
        );

        let UpdateChunkCommand { id, revision, data } = msg;
        let new = SerializedEntity { id, revision, data };

        let new_raw_size = self.storage
            .change(move |mut entities: BTreeMap<EntityId, SerializedEntity>| {
                if entities.contains_key(&id) {
                    entities.remove(&id);
                    // TODO: Check revision
                    entities.insert(id, new);
                } else {
                    entities.insert(id, new);
                }

                entities
            });

        Ok(new_raw_size)
    }
}

pub(super) struct GetChunk;

pub type GetChunkResult = Result<Bytes, ()>;

impl Message for GetChunk {
    type Result = GetChunkResult;
}

impl Handler<GetChunk> for ChunkActor {
    type Result = GetChunkResult;

    fn handle(&mut self, _msg: GetChunk, _ctx: &mut Self::Context) -> Self::Result {
        let thread1 = std::thread::current();
        let thread = thread1.name().unwrap_or("<unknown>").to_owned();
        debug!("thread={} Get chunk: i={}", thread, self.i);
        let res = self.load().to_bytes();
        Ok(res)
    }
}

impl Actor for ChunkActor {
    type Context = Context<Self>;
}
