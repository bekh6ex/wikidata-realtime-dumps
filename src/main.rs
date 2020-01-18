use std::collections::BTreeMap;
use std::sync::Arc;

use actix::prelude::*;
use futures::future::ready;
use futures::stream::once;
use futures::{self, StreamExt};
use log::*;

use crate::actor::archivarius::{ArchivariusActor, InitializationFinished, StartInitialization};
use crate::actor::UpdateCommand;
use crate::events::{get_current_event_id, get_update_stream, EventId};
use crate::prelude::EntityType;

mod actor;
mod events;
mod get_entity;
mod init;
mod prelude;
mod warp_server;

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    init_logger();

    info!("Starting...");

    // TODO: Lock storage file

    let entity_type = EntityType::Property;

    let archive_actor = ArchivariusActor::new(entity_type).start();

    let mut map = BTreeMap::new();
    map.insert(entity_type, archive_actor.clone());
    let map = Arc::new(map);

    let initial_event_id = initialize(entity_type, archive_actor.clone()).await;

    let update_stream = get_update_stream(entity_type, initial_event_id).await;

    let send_forward = |e: UpdateCommand| {
        let archive_actor_for_stream = archive_actor.clone();
        async_std::task::spawn(async move {
            let result = archive_actor_for_stream
                .send(e)
                .await
                .expect("Actor communication failed")
                .await;
            trace!("Got command result {:?}.", result);
        })
    };

    let update_stream = update_stream.for_each(send_forward);

    let ws = warp_server::start(&map);

    let _ = futures::future::join(update_stream, ws).await;
    Ok(())
}

async fn initialize(ty: EntityType, actor: Addr<ArchivariusActor>) -> EventId {
    let current = get_current_event_id();

    let response = actor
        .send(StartInitialization)
        .await
        .expect("Failed commun");

    let initial_event_id: EventId = response.last_event_id.unwrap_or(current.await);

    let init_stream = init::init(ty, response.initialized_up_to).await;

    let init_finished_stream = once(ready(None));

    let send_forward = |e: UpdateCommand| {
        let archive_actor_for_stream = actor.clone();
        async_std::task::spawn(async move {
            let result = archive_actor_for_stream
                .send(e)
                .await
                .expect("Actor communication failed")
                .await;
            // TODO: Should get last event id here
            debug!("Got update result {:?} ", result);
            ()
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

