use crate::archive::{UpdateChunkCommand, Archivarius};
use actix::{Actor, Context, Handler, Message, MessageResult, AsyncContext, SpawnHandle, Addr};
use bytes::Bytes;

use log::*;

use crate::prelude::{EntityId, EntityType, SerializedEntity};
use storage::GzippedData;

use std::collections::{BTreeMap, BTreeSet};

mod storage;

use self::storage::Volume;
use storage::VolumeStorage;
use std::borrow::BorrowMut;
use std::time::Duration;

pub struct VolumeKeeper {
    i: i32,
    storage: Option<Volume>,
    command_buffer: Vec<UpdateChunkCommand>,
    write_down_reminder: Option<SpawnHandle>,
//    master: Addr<Archivarius>,
}

impl VolumeKeeper {
    pub fn in_memory(ty: EntityType, i: i32) -> VolumeKeeper {
        VolumeKeeper {
            i,
            storage: Some(Volume::new_open(
                ty,
                format!("/tmp/wd-rt-dumps/{:?}/{}.gz", ty, i),
            )),
            command_buffer: vec![],
            write_down_reminder: None,
        }
    }

    pub fn persistent(ty: EntityType, i: i32) -> Self {
        VolumeKeeper {
            i,
            storage: Some(Volume::new_closed(
                ty,
                format!("/tmp/wd-rt-dumps/{:?}/{}.gz", ty, i),
            )),
            command_buffer: vec![],
            write_down_reminder: None,
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

    fn storage(&mut self) -> &mut Volume {
        self.storage.as_mut().unwrap()
    }

    fn apply_changes(&mut self, commands: Vec<UpdateChunkCommand>) -> usize {
        self.storage().change(move |entities: &mut BTreeMap<EntityId, SerializedEntity>| {
            for msg in commands {
                let new = msg.entity;

                if entities.contains_key(&new.id) {
                    entities.remove(&new.id);
                    // TODO: Check revision
                    entities.insert(new.id, new);
                } else {
                    entities.insert(new.id, new);
                }
            }
        })
    }

    fn remind_to_write_down(&mut self, ctx: &mut Context<Self>) {
        match self.write_down_reminder.take() {
            Some(handle) => {ctx.cancel_future(handle);},
            None => ()
        };

        let handle = ctx.notify_later(WriteDown, Duration::from_secs(10));
        self.write_down_reminder.replace(handle);
    }
}

impl Handler<UpdateChunkCommand> for VolumeKeeper {
    type Result = MessageResult<UpdateChunkCommand>;

    fn handle(&mut self, msg: UpdateChunkCommand, ctx: &mut Self::Context) -> Self::Result {
        debug!(
            "UpdateCommand[actor_id={}]: entity_id={}",
            self.i, msg.entity.id
        );

//        self.command_buffer.push(msg.clone());

        let new_raw_size = self.apply_changes(vec![msg]);

        self.remind_to_write_down(ctx);

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
        debug!("Get chunk: i={}", self.i);
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

#[derive(Message)]
#[rtype(result = "()")]
struct WriteDown;

impl Handler<WriteDown> for VolumeKeeper {
    type Result = ();

    fn handle(&mut self, msg: WriteDown, ctx: &mut Self::Context) -> Self::Result {
        info!("VolumeKeeper({}): Writing down the results", self.i);
        if self.command_buffer.is_empty() {
            info!("VolumeKeeper({}): Nothing to write down", self.i);

            return;
        }
        let buffer = core::mem::replace(&mut self.command_buffer, vec![]);

        let ids: BTreeSet<EntityId> = buffer.iter().map(|uc| uc.entity.id).collect();

        self.apply_changes(buffer);

//        self.master.send()
    }
}
