use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use actix::{Actor, AsyncContext, Context, Handler, Message, MessageResult, SpawnHandle, Addr};
use bytes::Bytes;
use log::*;

use storage::GzippedData;
use storage::VolumeStorage;

use crate::archive::{UpdateChunkCommand, Archivarius};
use crate::prelude::{EntityId, EntityType, SerializedEntity};

use self::storage::Volume;

mod storage;

pub struct VolumeKeeper {
    i: i32,
    config: VolumeKeeperConfig,
    storage: Option<Volume>,
    command_buffer: Vec<UpdateChunkCommand>,
    write_down_reminder: Option<SpawnHandle>,
    range_start: EntityId,
    range_end: Option<EntityId>,
    master: Addr<Archivarius>,
    volume_size: Option<usize>,
}

impl VolumeKeeper {
    pub(super) fn in_memory(root_path: String, config: VolumeKeeperConfig, master: Addr<Archivarius>, ty: EntityType, i: i32, from: EntityId, to: Option<EntityId>) -> VolumeKeeper {
        let to = to.map(|to| {
            assert!(to > from);
            to
        });
        VolumeKeeper {
            master,
            i,
            config,
            storage: Some(Volume::new_open(
                ty,
                format!("{}/{}.gz", root_path, i),
            )),
            command_buffer: vec![],
            write_down_reminder: None,
            range_start: from,
            range_end: to,
            volume_size: None,
        }
    }

    pub(super) fn persistent(root_path: String, config: VolumeKeeperConfig, master: Addr<Archivarius>, ty: EntityType, i: i32, from: EntityId, to: Option<EntityId>) -> Self {
        let to = to.map(|to| {
            assert!(to > from);
            to
        });

        VolumeKeeper {
            master,
            i,
            config,
            storage: Some(Volume::new_closed(
                ty,
                format!("{}/{}.gz", root_path, i),
            )),
            command_buffer: vec![],
            write_down_reminder: None,
            range_start: from,
            range_end: to,
            volume_size: None,
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
        let size = self.storage()
            .change(move |entities: &mut BTreeMap<EntityId, SerializedEntity>| {
                for msg in commands {
                    if entities.contains_key(&msg.entity_id()) {
                        let current_revision = entities.get(&msg.entity_id()).unwrap().revision;
                        if current_revision > msg.revision() {
                            debug!(
                                "Current revision is newer than one in command. {} {:?} {:?}",
                                msg.message_type(),
                                msg.entity_id(),
                                msg.revision()
                            );
                        } else {
                            match msg {
                                UpdateChunkCommand::Update { entity } => {
                                    entities.insert(entity.id, entity);
                                }
                                UpdateChunkCommand::Delete { id, revision: _ } => {
                                    entities.remove(&id);
                                }
                            }
                        }
                    } else {
                        match msg {
                            UpdateChunkCommand::Update { entity } => {
                                entities.insert(entity.id, entity);
                            }
                            UpdateChunkCommand::Delete { id, revision } => warn!(
                                "Expect to delete entity, but there is none. {:?} {:?}",
                                id, revision
                            ),
                        }
                    }
                }
            });
        self.volume_size = Some(size);

        size
    }

    fn in_the_range(&self, id: EntityId) -> bool {
        match self.range_end {
            None => id >= self.range_start,
            Some(range_end) => id >= self.range_start && id <= range_end,
        }
    }

    #[allow(dead_code)]
    fn finish_the_volume() {}

    fn remind_to_write_down(&mut self, ctx: &mut Context<Self>) {
        match self.write_down_reminder.take() {
            Some(handle) => {
                ctx.cancel_future(handle);
            }
            None => (),
        };

        let handle = ctx.notify_later(WriteDown, self.config.write_down_delay.clone());
        self.write_down_reminder.replace(handle);
    }
}

#[derive(Debug, Clone)]
pub struct VolumeKeeperConfig {
    /// Size of volume in bytes after which VolumeKeeper should finish the volume
    pub max_volume_size: usize,
    /// Delay between receiving the last message and persisting the state
    pub write_down_delay: Duration,
}

impl Default for VolumeKeeperConfig {
    fn default() -> Self {
        VolumeKeeperConfig {
            max_volume_size: 44 * 1024 * 1024,
            write_down_delay: Duration::from_secs(2),
        }
    }
}

impl Handler<UpdateChunkCommand> for VolumeKeeper {
    type Result = MessageResult<UpdateChunkCommand>;

    fn handle(&mut self, msg: UpdateChunkCommand, ctx: &mut Self::Context) -> Self::Result {
        if !self.in_the_range(msg.entity_id()) {
//            self.master.do_send(Redeliver(msg))
        }
        debug!(
            "UpdateCommand[actor_id={}]: entity_id={}",
            self.i,
            msg.entity_id()
        );

        self.command_buffer.push(msg.clone());

        let new_raw_size = if self.command_buffer.len() >= 20 {
            let buffer = core::mem::replace(&mut self.command_buffer, vec![]);
            self.apply_changes(buffer)
        } else {
            match self.volume_size {
                None => {
                    let buffer = core::mem::replace(&mut self.command_buffer, vec![]);
                    self.apply_changes(buffer)
                },
                Some(size) => {
                    size
                },
            }
        };


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
pub enum Persist {
    JustPersist,
    Close{ range_end: EntityId }
}

impl Handler<Persist> for VolumeKeeper {
    type Result = MessageResult<Persist>;

    fn handle(&mut self, msg: Persist, _ctx: &mut Self::Context) -> Self::Result {
        self.close_storage();
        match msg {
            Persist::JustPersist => {
                // Already done
            },
            Persist::Close { range_end } => {
                assert!(self.range_end.is_none());
                self.range_end = Some(range_end)
            },
        }
        MessageResult(())
    }
}

#[derive(Message)]
#[rtype(result = "()")]
struct WriteDown;

impl Handler<WriteDown> for VolumeKeeper {
    type Result = ();

    fn handle(&mut self, _msg: WriteDown, _ctx: &mut Self::Context) -> Self::Result {
        debug!("VolumeKeeper({}): Writing down the results", self.i);
        if self.command_buffer.is_empty() {
            debug!("VolumeKeeper({}): Nothing to write down", self.i);

            return;
        }
        let buffer = core::mem::replace(&mut self.command_buffer, vec![]);

        let _ids: BTreeSet<EntityId> = buffer.iter().map(|uc| uc.entity_id()).collect();

        self.apply_changes(buffer);

        //        self.master.send(report)
    }
}
