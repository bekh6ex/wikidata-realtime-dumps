use super::prelude::*;
use crate::events::EventId;
use crate::stream_ext::Sequential;
use actix::prelude::Stream;
use actix::Message;
use bytes::Bytes;
use std::future::Future;
use std::pin::Pin;

use self::archivarius::volume;


pub mod archivarius;
pub mod arbiter_pool;

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

#[derive(Debug, PartialEq)]
pub struct SerializedEntity {
    pub id: EntityId,
    pub revision: RevisionId,
    pub data: String,
}

impl Sequential for SerializedEntity {
    type Marker = EntityId;

    fn seq_marker(&self) -> Self::Marker {
        self.id
    }
}

impl Message for UpdateChunkCommand {
    type Result = usize;
}
