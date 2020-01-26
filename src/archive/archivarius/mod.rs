use std::mem::replace;
use std::ops::RangeInclusive;
use std::path::Path;
use std::sync::{Arc, RwLock};

use actix::{Actor, Addr, AsyncContext, Context, Handler, Message, MessageResult};
use bytes::Bytes;
use futures::stream::iter;
use futures::StreamExt;
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

const MAX_CHUNK_SIZE: usize = 22 * 1024 * 1024;
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
    // initialized_up_to - last EntityId of persisted actor
}

impl Archivarius {
    pub fn new(ty: EntityType, arbiters: ArbiterPool) -> Archivarius {
        let store = ArchivariusStore::new(format!("/tmp/wd-rt-dumps/{:?}/archivarius.json", ty));

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

        Archivarius {
            store,
            ty,
            everything_is_persisted: initialized,
            arbiters,
            closed_actors,
            open_actor,
            last_id_to_open_actor,
            last_processed_event_id: state.last_processed_event_id,
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

        self.store.save(self.state());
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

        debug!("UpdateCommand[ArchiveActor]: entity_id={}", msg.entity.id);
        let id = msg.entity.id;

        let (is_open_actor, child) = self.find_volume(id);

        // TODO: Figure out ARBEITER scheduling

        if is_open_actor {
            self.last_id_to_open_actor = Some(id);
        }

        let UpdateCommand { entity, event_id } = msg;

        let result: UnitFuture = Box::pin(async move {
            let result = child.send(UpdateChunkCommand { entity });

            let size = result.await.expect("Communication with child failed");

            // TODO: This is incorrect as soon as it is asynchronous.
            self_addr.do_send(UpdateLastEventEventId(event_id));

            if is_open_actor {
                Self::maybe_close_the_open_volume(&self_addr, child, size);
            }
        });
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

pub struct StartInitialization;

pub struct StartInitializationResponse {
    pub initialized_up_to: Option<EntityId>,
    pub last_event_id: Option<EventId>,
}

impl Message for StartInitialization {
    type Result = StartInitializationResponse;
}

impl Handler<StartInitialization> for Archivarius {
    type Result = MessageResult<StartInitialization>;

    fn handle(&mut self, _msg: StartInitialization, _ctx: &mut Self::Context) -> Self::Result {
        let initialized_up_to = if self.everything_is_persisted {
            self.last_id_to_open_actor
        } else {
            self.closed_actors.last().map(|(r, _)| *r.inner.end())
        };

        MessageResult(StartInitializationResponse {
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
