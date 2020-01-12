use actix::prelude::*;

use log::*;

use futures::{self, StreamExt};

use crate::actor::archive::ArchiveActor;
use crate::actor::UpdateCommand;
use crate::events::get_update_stream;
use crate::prelude::EntityType;

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
        "wikidata_realtime_dumps=debug,actix_server=info,actix_web=info",
    );
    env_logger::init();

    info!("Starting...");

    // TODO Fix data race: If entity gets deleted while initialization is happening there might be a race

    let init_stream = init::init(EntityType::Property).await;

    let update_stream = get_update_stream().await;

    let archive_actor = ArchiveActor::new().start();

    let archive_actor_for_stream = archive_actor.clone();

    let send_forward = |e: UpdateCommand| {
        async {
            let result = archive_actor_for_stream
                .send(e)
                .await
                .expect("Actor communication failed")
                .expect("ArchiveActor have failed")
                .await;
            // TODO: Should get last event id here
            debug!("Got update result {:?} ", result);
            ()
        }
    };

    let update_stream = update_stream.for_each(send_forward);
    let init_stream = init_stream.for_each(send_forward);

    let server_started = server::start(archive_actor);

    let _ = futures::future::join3(update_stream, init_stream, server_started).await;
    Ok(())
}
