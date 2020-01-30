#![forbid(unsafe_code)]
#![type_length_limit = "1321428"]
#![warn(unused_extern_crates)]

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;

use actix::prelude::*;
use futures::stream;
use futures::{self, StreamExt};
use log::*;

use crate::archive::UpdateCommand;
use crate::archive::{Archivarius, QueryState};
use crate::events::{get_current_event_id, update_command_stream, EventId};
use crate::prelude::EntityType;

use self::archive::{start, ArchivariusMap};

mod archive;
mod events;
mod get_entity;
mod http_client;
mod init;
mod prelude;
mod stream_ext;
mod warp_server;

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    init_logger();

    info!("Starting...");

    // TODO: Lock storage file

    let types = vec![EntityType::Item];
    //    let types = vec![EntityType::Lexeme, EntityType::Property, EntityType::Item];

    let map: ArchivariusMap = start(types);

    let ws = warp_server::start(&map);

    let update_streams = get_streams(map.clone()).await;

    let futures: Vec<Pin<Box<dyn Future<Output = ()>>>> = vec![Box::pin(ws), update_streams];

    let _ = futures::future::join_all(futures).await;
    Ok(())
}

async fn get_streams(
    map: Arc<BTreeMap<EntityType, Addr<Archivarius>>>,
) -> Pin<Box<dyn Future<Output = ()>>> {
    fn to_vec(
        map: Arc<BTreeMap<EntityType, Addr<Archivarius>>>,
    ) -> Vec<(EntityType, Addr<Archivarius>)> {
        map.iter()
            .map(|(ty, ad)| (*ty, ad.clone()))
            .collect::<Vec<_>>()
    }

    let map = to_vec(map);
    let res = stream::iter(map).for_each_concurrent(None, |(entity_type, archive_actor)| {
        async move {
            let entity_type = entity_type;
            let initial_event_id = initialize(entity_type, archive_actor.clone()).await;

            let update_stream = update_command_stream(entity_type, initial_event_id).await;

            let send_forward = move |e: UpdateCommand| {
                let result = archive_actor.send(e);
                async_std::task::spawn(async move {
                    result.await.expect("Actor communication failed").await;
                    trace!("Got command result.");
                })
            };

            update_stream.for_each(send_forward).await
        }
    });

    Box::pin(res)
}

async fn initialize(ty: EntityType, actor: Addr<Archivarius>) -> EventId {
    let response = actor.send(QueryState).await.expect("Failed commun");

    let init_stream = init::init(ty, response.initialized_up_to).await;

    let init_stream = init_stream.for_each({
        let actor = actor.clone();
        move |e| {
            let actor = actor.clone();
            async move {
                actor
                    .send(e)
                    .await
                    .expect("Failed to initialize actor")
                    .await;
            }
        }
    });

    init_stream.await;

    let response = actor.send(QueryState).await.expect("Failed commun");

    response
        .last_event_id
        .expect("Archivarius does not seem to be initialized")
}

fn init_logger() {
    log4rs::init_file("log4rs.yaml", Default::default()).unwrap();
}
