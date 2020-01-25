use self::archivarius::Archivarius;
use super::prelude::*;
use crate::archive::arbiter_pool::ArbiterPool;
use crate::events::EventId;
use actix::prelude::*;
use actix::{Addr, Message};
use bytes::Bytes;
use num_cpus;
use std::future::Future;
use std::iter::FromIterator;
use std::num::NonZeroUsize;
use std::pin::Pin;

use self::archivarius::volume;
use std::collections::BTreeMap;
use std::sync::Arc;

pub type ArchivariusMap = Arc<BTreeMap<EntityType, Addr<Archivarius>>>;

pub mod arbiter_pool;
pub mod archivarius;

pub struct GetDump;

pub type GetDumpResult = Pin<Box<dyn Stream<Item = Bytes> + Send + Sync>>;

impl Message for GetDump {
    type Result = GetDumpResult;
}

// TODO: Split to UpdateFromEventCommand and Initialize(Entity)Command
#[derive(Debug)]
pub struct UpdateCommand {
    pub event_id: EventId,
    pub entity: SerializedEntity,
}

impl Message for UpdateCommand {
    type Result = UnitFuture;
}

pub type UnitFuture = Pin<Box<dyn Future<Output = ()> + Send + Sync>>;

pub struct UpdateChunkCommand {
    pub entity: SerializedEntity,
}

impl Message for UpdateChunkCommand {
    type Result = usize;
}

pub(super) fn start(types: Vec<EntityType>) -> ArchivariusMap {
    let cpu_number = num_cpus::get();

    // Multiplying cores by X, because Volume actors a synchronous and block the thread.
    // This way we can utilize cpu for 100% percent.
    let actor_number = NonZeroUsize::new(cpu_number * 2).unwrap();
    let arbiter_pool = ArbiterPool::new(actor_number);

    let tuples = types.iter().map(move |ty| {
        let act = Archivarius::new(*ty, arbiter_pool.clone()).start();
        (*ty, act)
    });

    let map: BTreeMap<EntityType, Addr<Archivarius>> = BTreeMap::from_iter(tuples);
    Arc::new(map)
}
