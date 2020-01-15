use crate::actor::UpdateChunkCommand;
use actix::{Actor, Context, Handler, Message};
use actix_web::web::Bytes;
use super::SerializedEntity;

use log::*;

use std::io;
use serde_json;
use serde;
use crate::prelude::{EntityType, RevisionId, EntityId};
use std::collections::BTreeMap;
use crate::actor::chunk_storage::{GzippedData, GzChunkStorage};

pub struct ChunkActor {
    i: i32,
    storage: GzChunkStorage<String>
}

impl ChunkActor {
    pub fn new(i: i32) -> ChunkActor {
        ChunkActor {
            i,
            storage: GzChunkStorage::new(EntityType::Property, format!("/tmp/wd-rt-dumps/chunk/{}.gz", i))
        }
    }

    fn load(&self) -> GzippedData {
        self.storage.load()
    }

    fn store(&self, data: GzippedData) {
       self.storage.store(data)
    }
}

impl Handler<UpdateChunkCommand> for ChunkActor {
    type Result = Result<(), ()>;

    fn handle(&mut self, msg: UpdateChunkCommand, _ctx: &mut Self::Context) -> Self::Result {
        let thread = {
            let thread1 = std::thread::current();
            thread1.name().unwrap_or("<unknown>").to_owned()
        };

        debug!(
            "thread={} UpdateCommand[actor_id={}]: entity_id={}",
            thread, self.i, msg.id
        );

        let UpdateChunkCommand {
            id,
            revision,
            data,
        } = msg;
        let new = SerializedEntity{id,
            revision,
            data
        };

        let gzipped_data = self.load();
        let index = self.i;

        let res = {

            let data = gzipped_data.change(EntityType::Property, move |mut entities: BTreeMap<EntityId, SerializedEntity>| {
                if entities.contains_key(&id) {
                    entities.remove(&id);
                    // TODO: Check revision
                    entities.insert(id, new);
                } else {
                    entities.insert(id, new);
                }

                entities
            });

            let thread1 = {
                let thread1 = std::thread::current();
                thread1.name().unwrap_or("<unknown>").to_owned()
            };

            debug!("thread={} Will store, i={}", thread1, index);
            let r = self.store(data);
            debug!("Done storing");
            r
        };

        Ok(res)
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
