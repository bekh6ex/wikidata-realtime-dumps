// # Cargo.toml
//
//[package]
// edition = "2018"
//
//[dependencies]
//actix="0.9.0"
//actix-web="2.0.0-rc"
//actix-rt = "1.0.0"
//env_logger="0.7.1"
//futures = "0.3.1"
//bytes = "0.5.3"

use actix::prelude::*;
use actix_web::body::*;
use actix_web::guard::Get;
use actix_web::http::StatusCode;
use actix_web::{get, middleware, web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use bytes::Bytes;
use futures::prelude::*;
use futures::stream::*;
use log::error;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;

use actix_web::client::{Client, ClientBuilder, Connector};
use actix_web::dev::Service;
use actix_web::error::PayloadError;
use futures::io::{AsyncReadExt, ErrorKind};
use futures::sink::drain;
use futures::stream::{self};
use futures::{
    self, AsyncBufRead, AsyncRead, Future, FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt,
};
use sse_codec::{decode_stream, Event};
use std::io::Error;
use std::thread::sleep;
use std::time::Duration;

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "actix_server=info,actix_web=info");
    env_logger::init();

    let update_stream = get_update_stream().await;

    let archive_actor = ArchiveActor::new().start();

    let server = HttpServer::new(move || {
        App::new()
            .data(archive_actor.clone())
            .wrap(middleware::Compress::default())
            .wrap(middleware::Logger::default())
            .service(web::resource("/").to(handle_request))
    })
    .bind("127.0.0.1:8080")?
    .workers(1)
    .start();

    server.await
}

async fn handle_request(req: HttpRequest, ar: web::Data<Addr<ArchiveActor>>) -> impl Responder {
    println!("REQ: {:?}", req);
    let mut result = ar.send(GetDump).await.expect("asd").expect("jj");
    HttpResponse::Ok().streaming(result.map(|b| Ok(b) as Result<Bytes, ()>))
}

struct ArchiveActor {
    children: Vec<Addr<ChunkActor>>,
}

impl ArchiveActor {
    fn new() -> ArchiveActor {
        ArchiveActor {
            children: (1..100).map(|i| ChunkActor::new(i).start()).collect(),
        }
    }
}

impl Actor for ArchiveActor {
    type Context = Context<Self>;
}

type GetDumpResult = Result<Pin<Box<dyn Stream<Item = Bytes> + Send + Sync>>, ()>;

struct GetDump;

impl Message for GetDump {
    type Result = GetDumpResult;
}

impl Handler<GetDump> for ArchiveActor {
    type Result = GetDumpResult;

    fn handle(&mut self, msg: GetDump, ctx: &mut Self::Context) -> Self::Result {
        let stream = iter(self.children.clone())
            .map(|c| c.send(GetChunk))
            .buffer_unordered(3)
            .map(|r| r.expect("response").expect("Bytes"));
        Ok(Box::pin(stream))
    }
}

struct ChunkActor {
    i: i32,
}

impl ChunkActor {
    fn new(i: i32) -> ChunkActor {
        ChunkActor { i }
    }
}

impl Actor for ChunkActor {
    type Context = Context<Self>;
}

struct GetChunk;

type GetChunkResult = Result<Bytes, ()>;

impl Message for GetChunk {
    type Result = GetChunkResult;
}

impl Handler<GetChunk> for ChunkActor {
    type Result = GetChunkResult;

    fn handle(&mut self, msg: GetChunk, ctx: &mut Self::Context) -> Self::Result {
        Ok(Bytes::from(format!("{}\n", self.i)))
    }
}

async fn get_update_stream() -> impl Stream<Item = UpdateCommand> {
    let client = create_client();

    fn create_client() -> Client {
        ClientBuilder::new()
            .timeout(Duration::from_secs(600))
            .connector(Connector::new().timeout(Duration::from_secs(600)).finish())
            .finish()
    }

    // Create request builder, configure request and send
    let mut response = client
        .get("https://stream.wikimedia.org/v2/stream/recentchange")
        .header("User-Agent", "Actix-web")
        .timeout(Duration::from_secs(600))
        .send()
        .await
        .expect("response");

    // server http response
    println!("Response: {:?}", response);
    let async_read = response
        .into_stream()
        .map(|x| {
            //                    println!("{:?}", x);
            x
        })
        .map_err(|e: PayloadError| Error::new(ErrorKind::Other, format!("{}", e)))
        .into_async_read();

    decode_stream(async_read)
        .filter_map(|event| {
            async move {
                use serde_json::{self, Result};

                match event {
                    Ok(Event::Message { data, .. }) => {
                        let data1: Result<EventData> = serde_json::from_str(&data);
                        match data1 {
                            Ok(result) => Some(result),
                            Err(e) => {
                                error!("{}", e);
                                None
                            }
                        }
                    }
                    _ => None,
                }
            }
        })
        .filter(|e| futures::future::ready(e.wiki == "wikidatawiki" && e.namespace == 0))
        .take(10)
        .then(|event_data| {
            async {
                let EventData { title: id, .. } = event_data;
                let mut result = create_client()
                    .get(format!(
                        "https://www.wikidata.org/wiki/Special:EntityData/{}.json",
                        id
                    ))
                    .send()
                    .await
                    .unwrap();
                let body = result.body().await.unwrap();

                let unser = serde_json::from_slice::<WikidataResponse>(body.as_ref()).unwrap();
                let value = unser.entities.get(&id).unwrap();
                let data = serde_json::to_string(value).unwrap();

                let revision = value
                    .as_object()
                    .unwrap()
                    .get("lastrevid")
                    .unwrap()
                    .as_u64()
                    .unwrap();

                let id = id[1..].parse().unwrap();

                UpdateCommand {
                    entity_type: EntityType::Item,
                    revision,
                    id,
                    data,
                }
            }
        })
}

#[cfg(test)]
mod tests {
    use crate::{EntityType, EventData, UpdateCommand, WikidataResponse};
    use actix_web::client::{Client, ClientBuilder, Connector};
    use actix_web::dev::Service;
    use actix_web::error::PayloadError;
    use bytes::Bytes;
    use futures::io::{AsyncReadExt, ErrorKind};
    use futures::sink::drain;
    use futures::stream::{self};
    use futures::{
        self, AsyncBufRead, AsyncRead, Future, FutureExt, Stream, StreamExt, TryFutureExt,
        TryStreamExt,
    };
    use log::error;
    use sse_codec::{decode_stream, Event};
    use std::io::Error;
    use std::thread::sleep;
    use std::time::Duration;
    //    use std::error::Error;

    #[actix_rt::test]
    async fn asd() {
        let client = ClientBuilder::new()
            .timeout(Duration::from_secs(600))
            .connector(Connector::new().timeout(Duration::from_secs(600)).finish())
            .finish();

        // Create request builder, configure request and send
        let mut response = client
            .get("https://stream.wikimedia.org/v2/stream/recentchange")
            .header("User-Agent", "Actix-web")
            .timeout(Duration::from_secs(600))
            .send()
            .await
            .expect("response");

        // server http response
        println!("Response: {:?}", response);
        let async_read = into_ar(
            response
                .into_stream()
                .map(|x| {
                    //                    println!("{:?}", x);
                    x
                })
                .map_err(|e: PayloadError| Error::new(ErrorKind::Other, format!("{}", e))),
        );

        ds(async_read)
            .filter_map(|event| {
                async move {
                    use serde_json::{self, Result};

                    match event {
                        Ok(Event::Message { data, .. }) => {
                            let data1: Result<EventData> = serde_json::from_str(&data);
                            match data1 {
                                Ok(result) => Some(result),
                                Err(e) => {
                                    error!("{}", e);
                                    None
                                }
                            }
                        }
                        _ => None,
                    }
                }
            })
            .filter(|e| futures::future::ready(e.wiki == "wikidatawiki" && e.namespace == 0))
            .take(10)
            .then(|event_data| {
                async {
                    let EventData { title: id, .. } = event_data;
                    let mut result = client
                        .get(format!(
                            "https://www.wikidata.org/wiki/Special:EntityData/{}.json",
                            id
                        ))
                        .send()
                        .await
                        .unwrap();
                    let body = result.body().await.unwrap();

                    let unser = serde_json::from_slice::<WikidataResponse>(body.as_ref()).unwrap();
                    let value = unser.entities.get(&id).unwrap();
                    let data = serde_json::to_string(value).unwrap();

                    let revision = value
                        .as_object()
                        .unwrap()
                        .get("lastrevid")
                        .unwrap()
                        .as_u64()
                        .unwrap();

                    let id = id[1..].parse().unwrap();

                    UpdateCommand {
                        entity_type: EntityType::Item,
                        revision,
                        id,
                        data,
                    }
                }
            })
            .for_each(|e| {
                println!("{:?}", e);
                futures::future::ready(())
            })
            .await;
    }

    fn ds(
        async_read: impl AsyncRead + Unpin,
    ) -> impl Stream<Item = Result<Event, sse_codec::Error>> {
        decode_stream(async_read)
    }

    fn into_ar(
        mut response: impl Stream<Item = Result<Bytes, std::io::Error>> + Unpin,
    ) -> impl AsyncRead {
        response.into_async_read()
    }
}

use serde::Deserialize;

#[derive(Deserialize, Debug)]
struct EventData {
    wiki: String,
    title: String,
    namespace: u64,
    revision: RevisionData,
}

#[derive(Deserialize, Debug)]
struct RevisionData {
    new: u64,
}

#[derive(Deserialize, Debug)]
struct WikidataResponse {
    entities: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug)]
struct UpdateCommand {
    entity_type: EntityType,
    id: u64,
    revision: u64,
    data: String,
}
#[derive(Debug)]
enum EntityType {
    Item,
    Property,
}
