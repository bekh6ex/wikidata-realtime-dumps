use crate::archive::UpdateChunkCommand;
use actix::{Actor, Context, Handler, Message, MessageResult};
use bytes::Bytes;

use log::*;

use crate::prelude::{EntityId, EntityType, SerializedEntity};
use storage::GzippedData;

use std::collections::BTreeMap;

mod keeper;
mod storage;

use self::storage::Volume;
use storage::VolumeStorage;

pub struct VolumeKeeper {
    i: i32,
    storage: Option<Volume>,
}

impl VolumeKeeper {
    pub fn in_memory(ty: EntityType, i: i32) -> VolumeKeeper {
        VolumeKeeper {
            i,
            storage: Some(Volume::new_open(
                ty,
                format!("/tmp/wd-rt-dumps/{:?}/{}.gz", ty, i),
            )),
        }
    }

    pub fn persistent(ty: EntityType, i: i32) -> Self {
        VolumeKeeper {
            i,
            storage: Some(Volume::new_closed(
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

impl Handler<UpdateChunkCommand> for VolumeKeeper {
    type Result = MessageResult<UpdateChunkCommand>;

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

        MessageResult(new_raw_size)
    }
}

pub(super) struct GetChunk;

impl Message for GetChunk {
    type Result = Bytes;
}

impl Handler<GetChunk> for VolumeKeeper {
    type Result = MessageResult<GetChunk>;

    fn handle(&mut self, _msg: GetChunk, _ctx: &mut Self::Context) -> Self::Result {
        let thread1 = std::thread::current();
        let thread = thread1.name().unwrap_or("<unknown>").to_owned();
        debug!("thread={} Get chunk: i={}", thread, self.i);
        let res = self.load().into_bytes();
        MessageResult(res)
    }
}

impl Actor for VolumeKeeper {
    type Context = Context<Self>;
}

pub struct Persist;

impl Message for Persist {
    type Result = ();
}

impl Handler<Persist> for VolumeKeeper {
    type Result = MessageResult<Persist>;

    fn handle(&mut self, _msg: Persist, _ctx: &mut Self::Context) -> Self::Result {
        self.close_storage();
        MessageResult(())
    }
}
