use super::SerializedEntity;
use crate::actor::UpdateChunkCommand;
use actix::{Actor, Context, Handler, Message};
use actix_web::web::Bytes;

use log::*;

use crate::prelude::{EntityId, EntityType};
use chunk_storage::GzippedData;

use std::collections::BTreeMap;

mod chunk_storage;

use crate::actor::chunk::chunk_storage::ClosableStorage;
use chunk_storage::ChunkStorage;

pub struct ChunkActor {
    i: i32,
    storage: Option<ClosableStorage<String>>,
}

impl ChunkActor {
    pub fn new(i: i32) -> ChunkActor {
        ChunkActor {
            i,
            storage: Some(ClosableStorage::new_open(
                EntityType::Property,
                format!("/tmp/wd-rt-dumps/chunk/{}.gz", i),
            )),
        }
    }

    fn load(&self) -> GzippedData {
        self.storage.as_ref().unwrap().load()
    }

    fn close_storage(&mut self) {
        let storage = self.storage.take().unwrap();
        self.storage = Some(storage.close());
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

        let new_raw_size = self.storage.as_mut().unwrap().change(
            move |entities: &mut BTreeMap<EntityId, SerializedEntity>| {
                if entities.contains_key(&id) {
                    entities.remove(&id);
                    // TODO: Check revision
                    entities.insert(id, new);
                } else {
                    entities.insert(id, new);
                }
            },
        );

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
        self.close_storage();
        Ok(res)
    }
}

impl Actor for ChunkActor {
    type Context = Context<Self>;
}
