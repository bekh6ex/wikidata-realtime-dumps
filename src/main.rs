use actix::prelude::*;

use log::*;

use futures::{self, StreamExt};

use crate::actor::archivarius::{ArchivariusActor, InitializationFinished};
use crate::actor::UpdateCommand;
use crate::events::{get_current_event_id, get_update_stream, EventId};
use crate::prelude::EntityType;
use futures::future::ready;
use futures::stream::once;

mod actor;
mod events;
mod get_entity;
mod init;
pub mod prelude;
mod server;

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var(
        "RUST_LOG",
        "wikidata_realtime_dumps=info,actix_server=info,actix_web=info",
    );
    env_logger::init();

    info!("Starting...");

    // TODO Fix data race: If entity gets deleted while initialization is happening there might be a race
    let entity_type = EntityType::Property;

    let archive_actor = ArchivariusActor::new(entity_type).start();

    let initial_event_id = initialize(entity_type, archive_actor.clone()).await;

    let update_stream = get_update_stream(entity_type, initial_event_id).await;

    let send_forward = |e: UpdateCommand| {
        let archive_actor_for_stream = archive_actor.clone();
        async_std::task::spawn(async move {
            let result = archive_actor_for_stream
                .send(e)
                .await
                .expect("Actor communication failed")
                .expect("ArchiveActor have failed")
                .await;
            // TODO: Should get last event id here
            debug!("Got update result {:?} ", result);
            ()
        })
    };

    let update_stream = update_stream.for_each(send_forward);

    let server_started = server::start(archive_actor.clone());

    let _ = futures::future::join(update_stream, server_started).await;
    Ok(())
}

async fn initialize(ty: EntityType, actor: Addr<ArchivariusActor>) -> EventId {
    let initial_event_id = get_current_event_id().await;

    let init_stream = init::init(ty).await;

    let init_finished_stream = once(ready(None));

    let send_forward = |e: UpdateCommand| {
        let archive_actor_for_stream = actor.clone();
        async_std::task::spawn(async move {
            let result = archive_actor_for_stream
                .send(e)
                .await
                .expect("Actor communication failed")
                .expect("ArchiveActor have failed")
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
