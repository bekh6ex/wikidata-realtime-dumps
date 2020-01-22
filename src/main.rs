#![type_length_limit = "12557260"]
#![forbid(unsafe_code)]
#![warn(unused_extern_crates)]

use std::collections::BTreeMap;
use std::sync::Arc;

use actix::prelude::*;
use futures::future::ready;
use futures::stream::{self, once};
use futures::{self, StreamExt};
use log::*;

use crate::actor::archivarius::{ArchivariusActor, InitializationFinished, StartInitialization};
use crate::actor::UpdateCommand;
use crate::events::{get_current_event_id, update_command_stream, EventId};
use crate::prelude::EntityType;
use std::iter::FromIterator;
use std::pin::Pin;

mod actor;
mod events;
mod get_entity;
mod init;
mod prelude;
mod stream_ext;
mod warp_server;

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    init_logger();

    info!("Starting...");

    // TODO: Lock storage file

    let types = vec![EntityType::Lexeme, EntityType::Property, EntityType::Item];

    let tuples = types.iter().map(|ty| {
        let act = ArchivariusActor::new(*ty).start();
        (*ty, act)
    });

    let map: BTreeMap<EntityType, Addr<ArchivariusActor>> = BTreeMap::from_iter(tuples);
    let map = Arc::new(map);

    let ws = warp_server::start(&map);

    let update_streams = get_streams(map.clone()).await;

    let futures: Vec<Pin<Box<dyn Future<Output = ()>>>> = vec![Box::pin(ws), update_streams];

    let _ = futures::future::join_all(futures).await;
    Ok(())
}

async fn get_streams(
    map: Arc<BTreeMap<EntityType, Addr<ArchivariusActor>>>,
) -> Pin<Box<dyn Future<Output = ()>>> {
    fn to_vec(
        map: Arc<BTreeMap<EntityType, Addr<ArchivariusActor>>>,
    ) -> Vec<(EntityType, Addr<ArchivariusActor>)> {
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

async fn initialize(ty: EntityType, actor: Addr<ArchivariusActor>) -> EventId {
    let current = get_current_event_id();

    let response = actor
        .send(StartInitialization)
        .await
        .expect("Failed commun");

    let initial_event_id: EventId = response.last_event_id.unwrap_or(current.await);

    let init_stream = init::init(ty, response.initialized_up_to, initial_event_id.clone()).await;

    let init_finished_stream = once(ready(None));

    let send_forward = |e: UpdateCommand| {
        let result = actor.send(e);
        async_std::task::spawn(async move {
            result.await.expect("Actor communication failed").await;
            debug!("Got update result");
        })
    };

    let init_stream = init_stream
        .map(Option::Some)
        .chain(init_finished_stream)
        .for_each(|e| {
            async {
                match e {
                    None => {
                        actor.do_send(InitializationFinished);
                    }
                    Some(event) => {
                        send_forward(event).await;
                    }
                }
            }
        });

    init_stream.await;

    initial_event_id
}

fn init_logger() {
    log4rs::init_file("log4rs.yaml", Default::default()).unwrap();
}
