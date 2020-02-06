use std::mem::replace;
use std::ops::RangeInclusive;
use std::path::Path;
use std::sync::{RwLock, RwLockReadGuard};

use actix::{Actor, Addr, AsyncContext, Context, Handler, Message, MessageResult};
use bytes::Bytes;
use futures::*;
use futures::future::*;
use futures::stream::iter;
use log::*;
use serde::{Deserialize, Serialize};

use crate::archive::{GetDump, GetDumpResult, UnitFuture, UpdateChunkCommand, UpdateCommand};
use crate::archive::arbiter_pool::ArbiterPool;
use crate::events::EventId;
use crate::prelude::*;

use self::volume::{GetChunk, VolumeKeeper};

mod tracker;
mod volume;

const ARBITERS: usize = 8;

const MAX_CHUNK_SIZE: usize = 44 * 1024 * 1024;
//const MAX_CHUNK_SIZE: usize = 5 * 1024 * 1024;

pub(crate) struct Archivarius {
    store: ArchivariusStore,
    ty: EntityType,
    everything_is_persisted: bool,
    finished_volumes: Vec<(EntityRange, Addr<VolumeKeeper>)>,
    open_volume: Addr<VolumeKeeper>,
    last_id_to_open_actor: Option<EntityId>,
    arbiters: ArbiterPool,
    last_processed_event_id: Option<EventId>,
    updates_received: u32,
}

impl Archivarius {
    pub fn new(root_path: &str, ty: EntityType, arbiters: ArbiterPool, self_address: Addr<Archivarius>) -> Archivarius {
        let store = ArchivariusStore::new(root_path, ty);

        let state = store.load().unwrap_or_default();

        Self::new_initialized(ty, store, state, arbiters, self_address)
    }

    fn new_initialized(
        ty: EntityType,
        store: ArchivariusStore,
        state: StoredState,
        arbiters: ArbiterPool,
        self_address: Addr<Archivarius>,
    ) -> Archivarius {
        let volume_root_path = store.volume_root_path();
        let closed_actors = state
            .closed
            .iter()
            .enumerate()
            .map(|(id, er)| {
                let volume_root_path = volume_root_path.clone();
                let range = er.clone();
                let self_address = self_address.clone();
                let vol = VolumeKeeper::start_in_arbiter(arbiters.next().as_ref(), move |_| {
                    volume::VolumeKeeper::persistent(volume_root_path, self_address, ty, id as i32, *range.inner.start(), Some(*range.inner.end()))
                });

                (er.clone(), vol)
            })
            .collect::<Vec<_>>();
        let new_id = closed_actors.len();
        let last_id_to_open_actor: Option<EntityId> = state.open_volume_last_entity_id;
        let initialized = state.initialized;
        let start_id_for_new_volume = last_id_to_open_actor.unwrap_or(ty.id(1));

        let open_actor = VolumeKeeper::start_in_arbiter(arbiters.next().as_ref(), move |_| {
            if initialized {
                VolumeKeeper::persistent(volume_root_path.clone(),self_address, ty, new_id as i32, start_id_for_new_volume, None)
            } else {
                VolumeKeeper::in_memory(volume_root_path.clone(),self_address, ty, new_id as i32,start_id_for_new_volume, None)
            }
        });

        info!(
            "Starting Archivarius for {:?} with last_processed_event_id = {:?}",
            ty, state.last_processed_event_id
        );

        Archivarius {
            store,
            ty,
            everything_is_persisted: initialized,
            arbiters,
            finished_volumes: closed_actors,
            open_volume: open_actor,
            last_id_to_open_actor,
            last_processed_event_id: state.last_processed_event_id,
            updates_received: 0,
        }
    }

    fn state(&self) -> StoredState {
        let closed = self.finished_volumes.iter().map(|(r, _)| r.clone()).collect();

        StoredState {
            closed,
            open_volume_last_entity_id: self.last_id_to_open_actor,
            initialized: self.everything_is_persisted,
            last_processed_event_id: self.last_processed_event_id.clone(),
        }
    }

    fn initialization_in_progress(&self) -> bool {
        !self.everything_is_persisted
    }

    fn find_volume_in_finished(&self, id: EntityId) -> Option<Addr<VolumeKeeper>> {
        self.finished_volumes
            .iter()
            .find(|(range, _actor)| range.inner.contains(&id))
            .map(|(_, a)| a.clone())
    }

    fn find_volume(&self, id: EntityId) -> (bool, Addr<VolumeKeeper>) {
        let target_actor = self.find_volume_in_finished(id);

        match target_actor {
            Some(actor) => (false, actor),
            None => (true, self.open_volume.clone()),
        }
    }

    fn maybe_close_the_open_volume(self_addr: &Addr<Self>, child: Addr<VolumeKeeper>, size: usize) {
        if size > MAX_CHUNK_SIZE {
            debug!("Schedule the open actor closing. chunk_size={}", size);
            self_addr.do_send(CloseOpenActor { addr: child });

            if size > MAX_CHUNK_SIZE * 2 {
                warn!("Size of a chunk is way too big: {}", size)
            } else if size > MAX_CHUNK_SIZE * 10 {
                panic!("Size of a chunk is way too big: {}", size)
            }
        } else {
            debug!("Not closing volume yet. Size is: {}", size)
        }
    }

    fn close_current_open_actor(&mut self, self_addr: Addr<Self>) {
        let volume_root_path = self.store.volume_root_path();

        let new_id = self.finished_volumes.len() + 1;

        let ty = self.ty;
        let initializing = self.initialization_in_progress();

        let last_id_to_open_volume = self
            .last_id_to_open_actor
            .expect("Closed an empty open actor");

        let new_range = self
            .finished_volumes
            .last()
            .map(|(last_range, _)| last_range.next_adjacent(last_id_to_open_volume))
            .unwrap_or_else(|| EntityRange::from_start(last_id_to_open_volume));

        let range_start_for_new_volume = new_range.inner.end().next();

        let new_open_actor =
            VolumeKeeper::start_in_arbiter(self.arbiters.next().as_ref(), move |_| {
                if initializing {
                    VolumeKeeper::in_memory(volume_root_path.clone(), self_addr,ty, new_id as i32, range_start_for_new_volume, None)
                } else {
                    VolumeKeeper::persistent(volume_root_path.clone(),self_addr, ty, new_id as i32,range_start_for_new_volume, None)
                }
            });

        let old_open_actor = replace(&mut self.open_volume, new_open_actor);

        if self.initialization_in_progress() {
            info!("Sending command to persist the chunk");
            old_open_actor.do_send(volume::Persist::Close {range_end: last_id_to_open_volume });
            // TODO: Must await Persist response to sore Archivarius state
        }

        self.finished_volumes.push((new_range, old_open_actor));

        self.save_state();
    }

    fn save_state(&mut self) {
        debug!("Saving state");
        self.store.save(self.state());
    }

    fn update_last_processed_event_id(&mut self, init_even_id: EventId) {
        info!(
            "Updating Archivarius for {:?} with last_processed_event_id = {:?}",
            self.ty, init_even_id
        );
        match &self.last_processed_event_id {
            None => {
                self.last_processed_event_id = Some(init_even_id);
            }
            Some(last_event_id) => {
                // Being conservative: if the init stream is started from event that is later
                // in time than we've seen (in case of app restart), ignore it.
                // Otherwise replace ours.
                if last_event_id >= &init_even_id {
                    self.last_processed_event_id = Some(init_even_id);
                }
            }
        }

        info!(
            "Archivarius for {:?}: current last_processed_event_id = {:?}",
            self.ty, self.last_processed_event_id
        );

        self.save_state();
    }
}

impl Actor for Archivarius {
    type Context = Context<Self>;
    fn started(&mut self, _ctx: &mut Self::Context) {
        info!("ArchiveActor started!")
    }
}

impl Handler<GetDump> for Archivarius {
    type Result = MessageResult<GetDump>;

    fn handle(&mut self, _msg: GetDump, _ctx: &mut Self::Context) -> Self::Result {
        let thread1 = std::thread::current();
        let thread = thread1.name().unwrap_or("<unknown>").to_owned();
        debug!("thread={} Get dump", thread);

        let mut children: Vec<Addr<_>> =
            self.finished_volumes.iter().map(|(_, c)| c.clone()).collect();

        children.push(self.open_volume.clone());

        let stream = iter(children)
            .map(|c| c.send(GetChunk))
            .buffered(ARBITERS)
            .map(|r| r.expect("Actor communication issue"))
            .filter_map(|b: Bytes| {
                async {
                    if b.is_empty() {
                        None
                    } else {
                        Some(b)
                    }
                }
            });

        let stream: GetDumpResult = Box::pin(stream);

        MessageResult(stream)
    }
}

impl Handler<UpdateCommand> for Archivarius {
    type Result = MessageResult<UpdateCommand>;

    fn handle(&mut self, msg: UpdateCommand, ctx: &mut Self::Context) -> Self::Result {
        let self_addr = ctx.address();
        self.updates_received += 1;

        if self.updates_received % 50 == 0 {
            self.save_state();
        }

        debug!("UpdateCommand[ArchiveActor]: entity_id={}", msg.entity_id());
        let id = msg.entity_id();

        let (is_open_actor, child) = self.find_volume(id);

        // TODO: Figure out ARBEITER scheduling

        if is_open_actor {
            self.last_id_to_open_actor = Some(id);
        }

        //        let UpdateCommand { entity, event_id } = msg;

        let result: UnitFuture = match msg {
            UpdateCommand::UpdateCommand { entity, event_id } => {
                Box::pin(async move {
                    let result = child.send(UpdateChunkCommand::Update { entity });

                    let size = result.await.expect("Communication with child failed");

                    // TODO: This is incorrect as soon as it is asynchronous.
                    self_addr.do_send(UpdateLastEventEventId(event_id));

                    if is_open_actor {
                        Self::maybe_close_the_open_volume(&self_addr, child, size);
                    }
                })
            }
            UpdateCommand::DeleteCommand {
                event_id,
                id,
                revision,
            } => {
                Box::pin(async move {
                    let result = child.send(UpdateChunkCommand::Delete { id, revision });

                    let _ = result.await.expect("Communication with child failed");

                    // TODO: This is incorrect as soon as it is asynchronous.
                    self_addr.do_send(UpdateLastEventEventId(event_id));
                })
            }
        };

        MessageResult(result)
    }
}

struct CloseOpenActor {
    addr: Addr<VolumeKeeper>,
}

impl Message for CloseOpenActor {
    type Result = ();
}

impl Handler<CloseOpenActor> for Archivarius {
    type Result = MessageResult<CloseOpenActor>;

    fn handle(&mut self, msg: CloseOpenActor, ctx: &mut Self::Context) -> Self::Result {
        if self.open_volume != msg.addr {
            return MessageResult(());
        }

        self.close_current_open_actor(ctx.address());

        MessageResult(())
    }
}

struct UpdateLastEventEventId(EventId);

impl Message for UpdateLastEventEventId {
    type Result = ();
}

impl Handler<UpdateLastEventEventId> for Archivarius {
    type Result = MessageResult<UpdateLastEventEventId>;

    fn handle(
        &mut self,
        UpdateLastEventEventId(event_id): UpdateLastEventEventId,
        _ctx: &mut Self::Context,
    ) -> Self::Result {
        self.last_processed_event_id = Some(event_id);

        MessageResult(())
    }
}

pub struct QueryState;

pub struct QueryStateResponse {
    pub initialized_up_to: Option<EntityId>,
    pub last_event_id: Option<EventId>,
}

impl Message for QueryState {
    type Result = QueryStateResponse;
}

impl Handler<QueryState> for Archivarius {
    type Result = MessageResult<QueryState>;

    fn handle(&mut self, _msg: QueryState, _ctx: &mut Self::Context) -> Self::Result {
        let initialized_up_to = if self.everything_is_persisted {
            self.last_id_to_open_actor
        } else {
            self.finished_volumes.last().map(|(r, _)| *r.inner.end())
        };

        MessageResult(QueryStateResponse {
            initialized_up_to,
            last_event_id: self.last_processed_event_id.clone(),
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EntityRange {
    inner: RangeInclusive<EntityId>,
}

impl EntityRange {
    fn from_start(up_to: EntityId) -> EntityRange {
        let start = up_to.ty().id(1);
        EntityRange {
            inner: RangeInclusive::new(start, up_to),
        }
    }

    fn next_adjacent(&self, up_to: EntityId) -> EntityRange {
        EntityRange {
            inner: RangeInclusive::new(self.inner.end().next(), up_to),
        }
    }
}

#[derive(Message)]
#[rtype(result = "UnitFuture")]
pub enum Initialization {
    Start(EventId),
    UpdateEntity(SerializedEntity),
    Finished,
}

impl Handler<Initialization> for Archivarius {
    type Result = MessageResult<Initialization>;

    fn handle(&mut self, msg: Initialization, ctx: &mut Self::Context) -> Self::Result {
        match msg {
            Initialization::Start(init_even_id) => {
                self.update_last_processed_event_id(init_even_id);

                MessageResult(Box::pin(ready(())) as UnitFuture)
            }
            Initialization::UpdateEntity(entity) => {
                let self_addr = ctx.address();

                debug!(
                    "Initialization::UpdateEntity[ArchiveActor]: entity_id={}",
                    entity.id
                );
                let id = entity.id;

                let (is_open_actor, child) = self.find_volume(id);

                if is_open_actor {
                    self.last_id_to_open_actor = Some(id);
                }

                let result: UnitFuture = Box::pin(async move {
                    let result = child.send(UpdateChunkCommand::Update { entity });
                    let size = result.await.expect("Communication with child failed");

                    if is_open_actor {
                        Self::maybe_close_the_open_volume(&self_addr, child, size);
                    }
                });
                MessageResult(result)
            }
            Initialization::Finished => {
                self.everything_is_persisted = true;

                info!("Initialization finished. Sending command to persist the chunk of an open actor");

                let result = self.open_volume.send(volume::Persist::JustPersist);

                MessageResult(Box::pin(result.map(|r| r.unwrap())) as UnitFuture)
            }
        }
    }
}


struct ArchivariusStore {
    path: RwLock<String>,
}

impl ArchivariusStore {
    fn new<P: AsRef<Path>>(root_path: P, ty: EntityType) -> Self {
        use std::fs::create_dir_all;
        let root_path = root_path.as_ref().display();
        let path = format!("{}/{:?}/archivarius.json", root_path, ty);
        let path_ref: &Path = path.as_ref();
        create_dir_all(path_ref.parent().unwrap()).unwrap();
        ArchivariusStore {
            path: RwLock::new(path),
        }
    }

    fn load(&self) -> Option<StoredState> {
        use serde_json::*;
        use std::fs::read;
        let path: RwLockReadGuard<String> = self.path.read().unwrap();

        let path_ref: &Path = path.as_ref();
        read(path_ref)
            .ok()
            .map(|r| from_slice::<StoredState>(&r).unwrap())
    }

    fn save(&mut self, state: StoredState) {
        use std::fs::write;

        let contents = serde_json::to_string_pretty(&state).unwrap();

        let path = self.path.write().unwrap();

        let path: &Path = path.as_ref();
        write(path, contents).unwrap();
    }

    fn volume_root_path(&self) -> String {
        let path: RwLockReadGuard<String> = self.path.read().unwrap();
        let path_ref: &Path = path.as_ref();

        path_ref.parent().unwrap().display().to_string()
    }
}

#[allow(dead_code)]
struct VolumeKeeperReport {
    keeper_id: i32,
    work: Vec<(EventId, EntityId)>,
}

#[derive(Serialize, Deserialize)]
struct StoredState {
    closed: Vec<EntityRange>,
    open_volume_last_entity_id: Option<EntityId>,
    last_processed_event_id: Option<EventId>,
    initialized: bool,
}

impl Default for StoredState {
    fn default() -> Self {
        StoredState {
            closed: vec![],
            open_volume_last_entity_id: None,
            last_processed_event_id: None,
            initialized: false,
        }
    }
}
