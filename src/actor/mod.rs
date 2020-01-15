use super::prelude::*;
use actix::prelude::Stream;
use actix::Message;
use actix_web::web::Bytes;
use std::future::Future;
use std::pin::Pin;

pub mod archive;
pub mod chunk;

pub struct GetDump;

pub type GetDumpResult = Result<Pin<Box<dyn Stream<Item = Bytes> + Send + Sync>>, ()>;

impl Message for GetDump {
    type Result = GetDumpResult;
}

#[derive(Debug)]
pub struct UpdateCommand {
    pub id: EntityId,
    pub revision: RevisionId,
    pub data: String,
}

impl Message for UpdateCommand {
    type Result = Result<Pin<Box<dyn Future<Output = ()> + Send + Sync>>, ()>;
}

pub struct UpdateChunkCommand {
    pub id: EntityId,
    pub revision: RevisionId,
    pub data: String,
}

pub struct SerializedEntity {
    pub id: EntityId,
    pub revision: RevisionId,
    pub data: String,
}

impl Message for UpdateChunkCommand {
    type Result = Result<(), ()>;
}
