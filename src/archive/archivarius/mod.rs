use std::mem::replace;
use std::ops::RangeInclusive;
use std::path::Path;
use std::sync::{Arc, RwLock};

use actix::{Actor, Addr, AsyncContext, Context, Handler, Message, MessageResult};
use bytes::Bytes;
use futures::future::*;
use futures::stream::iter;
use futures::*;
use log::*;
use serde::{Deserialize, Serialize};

use self::volume::{GetChunk, VolumeKeeper};
use crate::archive::{GetDump, GetDumpResult, UnitFuture, UpdateChunkCommand, UpdateCommand};
use crate::events::EventId;
use crate::prelude::*;

use crate::archive::arbiter_pool::ArbiterPool;

mod tracker;
mod volume;

const ARBITERS: usize = 8;

const MAX_CHUNK_SIZE: usize = 44 * 1024 * 1024;
//const MAX_CHUNK_SIZE: usize = 5 * 1024 * 1024;

pub(crate) struct Archivarius {
    store: ArchivariusStore<String>,
    ty: EntityType,
    everything_is_persisted: bool,
    closed_actors: Vec<(EntityRange, Addr<VolumeKeeper>)>,
    open_actor: Addr<VolumeKeeper>,
    last_id_to_open_actor: Option<EntityId>,
    arbiters: ArbiterPool,
    last_processed_event_id: Option<EventId>,
    updates_received: u32,
    // initialized_up_to - last EntityId of persisted actor
}

impl Archivarius {
    pub fn new(ty: EntityType, arbiters: ArbiterPool) -> Archivarius {
        let store = ArchivariusStore::new(format!("wd-rt-dumps/{:?}/archivarius.json", ty));

        let state = store.load().unwrap_or_default();

        Self::new_initialized(ty, store, state, arbiters)
    }

    fn new_initialized(
        ty: EntityType,
        store: ArchivariusStore<String>,
        state: StoredState,
        arbiters: ArbiterPool,
    ) -> Archivarius {
        let closed_actors = state
            .closed
            .iter()
            .enumerate()
            .map(|(id, er)| {
                let vol = VolumeKeeper::start_in_arbiter(arbiters.next().as_ref(), move |_| {
                    volume::VolumeKeeper::persistent(ty, id as i32)
                });

                (er.clone(), vol)
            })
            .collect::<Vec<_>>();
        let new_id = closed_actors.len();
        let last_id_to_open_actor: Option<EntityId> = state.open_volume_last_entity_id;
        let initialized = state.initialized;

        let open_actor = VolumeKeeper::start_in_arbiter(arbiters.next().as_ref(), move |_| {
            if initialized {
                VolumeKeeper::persistent(ty, new_id as i32)
            } else {
                VolumeKeeper::in_memory(ty, new_id as i32)
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
            closed_actors,
            open_actor,
            last_id_to_open_actor,
            last_processed_event_id: state.last_processed_event_id,
            updates_received: 0,
        }
    }

    fn state(&self) -> StoredState {
        let closed = self.closed_actors.iter().map(|(r, _)| r.clone()).collect();

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

    fn find_closed(&self, id: EntityId) -> Option<Addr<VolumeKeeper>> {
        self.closed_actors
            .iter()
            .find(|(range, _actor)| range.inner.contains(&id))
            .map(|(_, a)| a.clone())
    }

    fn find_volume(&self, id: EntityId) -> (bool, Addr<VolumeKeeper>) {
        let target_actor = self.find_closed(id);

        match target_actor {
            Some(actor) => (false, actor),
            None => (true, self.open_actor.clone()),
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

    fn close_current_open_actor(&mut self) {
        let new_id = self.closed_actors.len() + 1;

        let ty = self.ty;
        let initializing = self.initialization_in_progress();

        let new_open_actor =
            VolumeKeeper::start_in_arbiter(self.arbiters.next().as_ref(), move |_| {
                if initializing {
                    VolumeKeeper::in_memory(ty, new_id as i32)
                } else {
                    VolumeKeeper::persistent(ty, new_id as i32)
                }
            });

        let old_open_actor = replace(&mut self.open_actor, new_open_actor);

        if self.initialization_in_progress() {
            info!("Sending command to persist the chunk");
            old_open_actor.do_send(volume::Persist);
            // TODO: Must await Persist response to sore Archivarius state
        }

        let top_id = self
            .last_id_to_open_actor
            .expect("Closed an empty open actor");
        let new_range = self
            .closed_actors
            .last()
            .map(|(last_range, _)| last_range.next_adjacent(top_id))
            .unwrap_or_else(|| EntityRange::from_start(top_id));

        self.closed_actors.push((new_range, old_open_actor));

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
            self.closed_actors.iter().map(|(_, c)| c.clone()).collect();

        children.push(self.open_actor.clone());

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

impl Handler<InitializationFinished> for Archivarius {
    type Result = Arc<()>;

    fn handle(&mut self, _msg: InitializationFinished, _ctx: &mut Self::Context) -> Self::Result {
        self.everything_is_persisted = true;

        info!("Initialization finished. Sending command to persist the chunk of an open actor");
        self.open_actor.do_send(volume::Persist);

        Arc::new(())
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

    fn handle(&mut self, msg: CloseOpenActor, _ctx: &mut Self::Context) -> Self::Result {
        if self.open_actor != msg.addr {
            return MessageResult(());
        }

        self.close_current_open_actor();

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
            self.closed_actors.last().map(|(r, _)| *r.inner.end())
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
                // Mark as initialized???
                MessageResult(Box::pin(ready(())) as UnitFuture)
            }
        }
    }
}

pub struct InitializationFinished;

impl Message for InitializationFinished {
    type Result = Arc<()>;
}

struct ArchivariusStore<P: AsRef<Path>> {
    path: RwLock<P>,
}

impl<P: AsRef<Path>> ArchivariusStore<P> {
    fn new(path: P) -> ArchivariusStore<P> {
        use std::fs::create_dir_all;
        create_dir_all(path.as_ref().parent().unwrap()).unwrap();
        ArchivariusStore {
            path: RwLock::new(path),
        }
    }

    fn load(&self) -> Option<StoredState> {
        use serde_json::*;
        use std::fs::read;
        let path = self.path.read().unwrap();

        read(path.as_ref())
            .ok()
            .map(|r| from_slice::<StoredState>(&r).unwrap())
    }

    fn save(&mut self, state: StoredState) {
        use serde_json::to_string;
        use std::fs::write;

        let contents = to_string(&state).unwrap();

        let path = self.path.write().unwrap();

        write(path.as_ref(), contents).unwrap();
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
    initialized: bool, //TODO Should be Option<last event Id> from event stream
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
