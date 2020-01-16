use actix::prelude::*;

use log::*;

use futures::{self, StreamExt};

use crate::actor::archive::{ArchiveActor, InitializationFinished};
use crate::actor::UpdateCommand;
use crate::events::get_update_stream;
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

    let init_stream = init::init(EntityType::Property).await;

    // TODO: Update stream last-event-id should be read and stream can only be started after init is done
    let update_stream = get_update_stream().await;

    let archive_actor = ArchiveActor::new().start();

    let archive_actor_for_stream = archive_actor.clone();
    let archive_actor_for_stream2 = archive_actor.clone();

    let init_finished_stream = once(ready(None));

    let send_forward = |e: UpdateCommand| {
        let archive_actor_for_stream = archive_actor_for_stream.clone();
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
    let init_stream = init_stream
        .map(Option::Some)
        .chain(init_finished_stream)
        .for_each(|e| {
            async {
                match e {
                    None => {
                        archive_actor_for_stream2.do_send(InitializationFinished);
                    }
                    Some(event) => {
                        send_forward(event).await;
                    }
                }
            }
        });

    let server_started = server::start(archive_actor);

    let _ = futures::future::join3(update_stream, init_stream, server_started).await;
    Ok(())
}
