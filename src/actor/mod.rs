use std::pin::Pin;
use actix_web::web::Bytes;
use actix::prelude::Stream;
use actix::Message;
use std::future::Future;
use super::prelude::*;

pub mod archive;
pub mod chunk;

pub struct GetDump;

pub type GetDumpResult = Result<Pin<Box<dyn Stream<Item = Bytes> + Send + Sync>>, ()>;

impl Message for GetDump {
    type Result = GetDumpResult;
}


#[derive(Debug)]
pub struct UpdateCommand {
    pub entity_type: EntityType,
    pub id: u64,
    pub revision: u64,
    pub data: String,
}
impl Message for UpdateCommand {
    type Result = Result<Pin<Box<dyn Future<Output = ()> + Send + Sync>>, ()>;
}
