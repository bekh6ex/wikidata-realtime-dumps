use actix::prelude::*;

use actix_web::{middleware, web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use bytes::Bytes;
use futures::stream::*;
use log::*;
use std::fmt::Debug;
use std::pin::Pin;

use actix_web::client::{Client, ClientBuilder, Connector};
use actix_web::error::PayloadError;
use futures::io::ErrorKind;

use futures::{self, Stream, StreamExt, TryStreamExt};
use sse_codec::{decode_stream, Event};
use std::io::Error;

use std::time::Duration;
use prelude::*;
use actor::archive;

use actor::{GetDump, GetDumpResult};
use crate::events::get_update_stream;
use crate::actor::archive::ArchiveActor;

mod server;
mod actor;
mod events;
pub mod prelude;


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
            let _result = archive_actor_for_stream.send(e).await;
            ()
        }
    });

    let server_started = server::start(archive_actor);

    let _ = futures::future::join(stream, server_started).await;
    Ok(())
}

