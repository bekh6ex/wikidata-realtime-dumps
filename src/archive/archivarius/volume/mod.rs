use crate::archive::{UpdateChunkCommand};
use actix::{Actor, AsyncContext, Context, Handler, Message, MessageResult, SpawnHandle};
use bytes::Bytes;

use log::*;

use crate::prelude::{EntityId, EntityType, SerializedEntity};
use storage::GzippedData;

use std::collections::{BTreeMap, BTreeSet};

mod storage;

use self::storage::Volume;

use std::time::Duration;
use storage::VolumeStorage;

const MAX_CHUNK_SIZE: usize = 22 * 1024 * 1024;

pub struct VolumeKeeper {
    i: i32,
    storage: Option<Volume>,
    command_buffer: Vec<UpdateChunkCommand>,
    write_down_reminder: Option<SpawnHandle>,
    //    range_start: EntityId,
    //    range_end: Option<EntityId>,
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
        self.storage()
            .change(move |entities: &mut BTreeMap<EntityId, SerializedEntity>| {
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

    //    fn in_the_range(&self, id: EntityId) -> bool {
    //        match self.range_end {
    //            None => id >= self.range_start,
    //            Some(range_end) => id >= self.range_start && id <= range_end,
    //        }
    //    }

    fn finish_the_volume() {}

    fn remind_to_write_down(&mut self, ctx: &mut Context<Self>) {
        match self.write_down_reminder.take() {
            Some(handle) => {
                ctx.cancel_future(handle);
            }
            None => (),
        };

        let handle = ctx.notify_later(WriteDown, Duration::from_secs(10));
        self.write_down_reminder.replace(handle);
    }
}

impl Handler<UpdateChunkCommand> for VolumeKeeper {
    type Result = MessageResult<UpdateChunkCommand>;

    fn handle(&mut self, msg: UpdateChunkCommand, ctx: &mut Self::Context) -> Self::Result {
        //        if !self.in_the_range(msg.entity.id) {
        //            self.master.do_send(Redeliver(msg))
        //        }
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
#[derive(Message)]
#[rtype(result = "Bytes")]
pub(super) struct GetChunk;

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

#[derive(Message)]
#[rtype(result = "()")]
pub struct Persist;

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

    fn handle(&mut self, _msg: WriteDown, _ctx: &mut Self::Context) -> Self::Result {
        info!("VolumeKeeper({}): Writing down the results", self.i);
        if self.command_buffer.is_empty() {
            info!("VolumeKeeper({}): Nothing to write down", self.i);

            return;
        }
        let buffer = core::mem::replace(&mut self.command_buffer, vec![]);

        let _ids: BTreeSet<EntityId> = buffer.iter().map(|uc| uc.entity.id).collect();

        self.apply_changes(buffer);

        //        self.master.send(report)
    }
}
