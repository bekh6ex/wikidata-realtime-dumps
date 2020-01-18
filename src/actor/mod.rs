use super::prelude::*;
use crate::events::EventId;
use actix::prelude::Stream;
use actix::Message;
use actix_web::web::Bytes;
use std::future::Future;
use std::pin::Pin;

pub mod archivarius;
pub mod volume;

pub struct GetDump;

pub type GetDumpResult = Result<Pin<Box<dyn Stream<Item = Bytes> + Send + Sync>>, ()>;

impl Message for GetDump {
    type Result = GetDumpResult;
}

// TODO: Split to UpdateFromEventCommand and Initialize(Entity)Command
#[derive(Debug)]
pub struct UpdateCommand {
    pub event_id: Option<EventId>,
    pub entity: SerializedEntity,
}

impl Message for UpdateCommand {
    type Result = Result<Pin<Box<dyn Future<Output = ()> + Send + Sync>>, ()>;
}

pub struct UpdateChunkCommand {
    pub entity: SerializedEntity,
}

#[derive(Debug)]
pub struct SerializedEntity {
    pub id: EntityId,
    pub revision: RevisionId,
    pub data: String,
}

impl Message for UpdateChunkCommand {
    type Result = usize;
}
