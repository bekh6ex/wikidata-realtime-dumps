use actix::prelude::*;

use log::*;

use futures::{self, StreamExt};

use crate::actor::archive::ArchiveActor;
use crate::events::get_update_stream;

mod actor;
mod events;
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

    let update_stream = get_update_stream().await;

    let archive_actor = ArchiveActor::new().start();

    let archive_actor_for_stream = archive_actor.clone();

    let stream = update_stream.for_each(|e| {
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
    });

    let server_started = server::start(archive_actor);

    let _ = futures::future::join(stream, server_started).await;
    Ok(())
}
