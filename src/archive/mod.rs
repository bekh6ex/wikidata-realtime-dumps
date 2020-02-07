use self::arbiter_pool::ArbiterPool;
use super::prelude::*;
use crate::events::EventId;
use actix::prelude::*;
use actix::{Addr, Message};
use bytes::Bytes;
use num_cpus;
use std::future::Future;
use std::iter::FromIterator;
use std::num::NonZeroUsize;
use std::pin::Pin;

use std::collections::BTreeMap;
use std::sync::Arc;
use self::archivarius::VolumeKeeperConfig;

pub(crate) use self::archivarius::{Archivarius, Initialization, QueryState};

pub(crate) type ArchivariusMap = Arc<BTreeMap<EntityType, Addr<Archivarius>>>;

mod arbiter_pool;
mod archivarius;

#[cfg(test)]
mod test;

pub struct GetDump;

pub type GetDumpResult = Pin<Box<dyn Stream<Item = Bytes> + Send + Sync>>;

impl Message for GetDump {
    type Result = GetDumpResult;
}

// TODO: Split to UpdateFromEventCommand and Initialize(Entity)Command
#[derive(Debug)]
pub enum UpdateCommand {
    UpdateCommand {
        event_id: EventId,
        entity: SerializedEntity,
    },
    DeleteCommand {
        event_id: EventId,
        id: EntityId,
        revision: RevisionId,
    },
}

impl UpdateCommand {
    pub fn entity_id(&self) -> EntityId {
        match self {
            UpdateCommand::UpdateCommand { entity, .. } => entity.id,
            UpdateCommand::DeleteCommand { id, .. } => *id,
        }
    }
    pub fn event_id(&self) -> &EventId {
        match self {
            UpdateCommand::UpdateCommand { event_id, .. } => event_id,
            UpdateCommand::DeleteCommand { event_id, .. } => event_id,
        }
    }
}

impl Message for UpdateCommand {
    type Result = UnitFuture;
}

pub type UnitFuture = Pin<Box<dyn Future<Output = ()> + Send + Sync>>;

#[derive(Clone, Debug)]
enum UpdateChunkCommand {
    Update { entity: SerializedEntity },
    Delete { id: EntityId, revision: RevisionId },
}

impl UpdateChunkCommand {
    fn entity_id(&self) -> EntityId {
        match self {
            UpdateChunkCommand::Update { entity, .. } => entity.id,
            UpdateChunkCommand::Delete { id, .. } => *id,
        }
    }

    fn revision(&self) -> RevisionId {
        match self {
            UpdateChunkCommand::Update { entity, .. } => entity.revision,
            UpdateChunkCommand::Delete { revision, .. } => *revision,
        }
    }

    fn message_type(&self) -> &str {
        match self {
            UpdateChunkCommand::Update { .. } => "Update",
            UpdateChunkCommand::Delete { .. } => "Delete",
        }
    }
}

impl Message for UpdateChunkCommand {
    type Result = usize;
}

pub(super) fn start(types: Vec<EntityType>) -> ArchivariusMap {
    let cpu_number = num_cpus::get();

    // Multiplying cores by X, because Volume actors are synchronous and block the thread.
    // This way we can utilize cpu for 100% percent.
    let actor_number = NonZeroUsize::new(cpu_number * 3).unwrap();
    let arbiter_pool = ArbiterPool::new(actor_number);

    let tuples = types.iter().map(move |ty| {
        let arbiter = Arbiter::new();
        let ty = *ty;
        let arbiter_pool = arbiter_pool.clone();
        let act = Archivarius::start_in_arbiter(&arbiter, move |ctx| {
            Archivarius::new(
                "wd-rt-dumps",
                ty,
                VolumeKeeperConfig::default(),
                arbiter_pool.clone(),
                ctx.address()
            )
        });
        (ty, act)
    });

    let map: BTreeMap<EntityType, Addr<Archivarius>> = BTreeMap::from_iter(tuples);
    Arc::new(map)
}
