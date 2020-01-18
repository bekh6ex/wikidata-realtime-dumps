use super::SerializedEntity;
use crate::actor::UpdateChunkCommand;
use actix::{Actor, Context, Handler, Message, MessageResult};
use actix_web::web::Bytes;

use log::*;

use crate::prelude::{EntityId, EntityType};
use storage::GzippedData;

use std::collections::BTreeMap;

mod storage;

use crate::actor::volume::storage::ClosableStorage;
use std::sync::Arc;
use storage::VolumeStorage;

pub struct VolumeActor {
    i: i32,
    storage: Option<ClosableStorage<String>>,
}

impl VolumeActor {
    pub fn in_memory(ty: EntityType, i: i32) -> VolumeActor {
        VolumeActor {
            i,
            storage: Some(ClosableStorage::new_open(
                ty,
                format!("/tmp/wd-rt-dumps/{:?}/{}.gz", ty, i),
            )),
        }
    }

    pub fn persistent(ty: EntityType, i: i32) -> Self {
        VolumeActor {
            i,
            storage: Some(ClosableStorage::new_closed(
                ty,
                format!("/tmp/wd-rt-dumps/{:?}/{}.gz", ty, i),
            )),
        }
    }

    fn load(&self) -> GzippedData {
        self.storage.as_ref().unwrap().load()
    }

    fn close_storage(&mut self) {
        info!("Actor {} closing storage.", self.i);
        let storage = self.storage.take().unwrap();
        self.storage = Some(storage.close());
    }
}

impl Handler<UpdateChunkCommand> for VolumeActor {
    type Result = Result<usize, ()>;

    fn handle(&mut self, msg: UpdateChunkCommand, _ctx: &mut Self::Context) -> Self::Result {
        let thread = {
            let thread1 = std::thread::current();
            thread1.name().unwrap_or("<unknown>").to_owned()
        };

        debug!(
            "thread={} UpdateCommand[actor_id={}]: entity_id={}",
            thread, self.i, msg.entity.id
        );

        let new = msg.entity;

        let new_raw_size = self.storage.as_mut().unwrap().change(
            move |entities: &mut BTreeMap<EntityId, SerializedEntity>| {
                if entities.contains_key(&new.id) {
                    entities.remove(&new.id);
                    // TODO: Check revision
                    entities.insert(new.id, new);
                } else {
                    entities.insert(new.id, new);
                }
            },
        );

        Ok(new_raw_size)
    }
}

pub(super) struct GetChunk;

impl Message for GetChunk {
    type Result = Bytes;
}

impl Handler<GetChunk> for VolumeActor {
    type Result = MessageResult<GetChunk>;

    fn handle(&mut self, _msg: GetChunk, _ctx: &mut Self::Context) -> Self::Result {
        let thread1 = std::thread::current();
        let thread = thread1.name().unwrap_or("<unknown>").to_owned();
        debug!("thread={} Get chunk: i={}", thread, self.i);
        let res = self.load().to_bytes();
        MessageResult(res)
    }
}

impl Actor for VolumeActor {
    type Context = Context<Self>;
}

pub struct Persist;

impl Message for Persist {
    type Result = Arc<()>;
}

impl Handler<Persist> for VolumeActor {
    type Result = Arc<()>;

    fn handle(&mut self, _msg: Persist, _ctx: &mut Self::Context) -> Self::Result {
        self.close_storage();
        Arc::new(())
    }
}
