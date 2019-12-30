use actix::prelude::*;

use actix_web::{middleware, web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use bytes::Bytes;
use futures::stream::*;
use log::{debug, error, info, trace};
use std::fmt::Debug;
use std::pin::Pin;

use actix_web::client::{Client, ClientBuilder, Connector};
use actix_web::error::PayloadError;
use futures::io::ErrorKind;

use futures::{self, Stream, StreamExt, TryStreamExt};
use sse_codec::{decode_stream, Event};
use std::io::Error;

use std::time::Duration;

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
            let result = archive_actor_for_stream.send(e).await;
            ()
        }
    });

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

    let _ = futures::future::join(stream, server).await;
    Ok(())
}

async fn handle_request(req: HttpRequest, ar: web::Data<Addr<ArchiveActor>>) -> impl Responder {
    println!("REQ: {:?}", req);
    let result = ar.send(GetDump).await.expect("asd").expect("jj");
    HttpResponse::Ok().streaming(result.map(|b| Ok(b) as Result<Bytes, ()>))
}

struct ArchiveActor {
    children: Vec<Addr<ChunkActor>>,
}

impl ArchiveActor {
    fn new() -> ArchiveActor {
        ArchiveActor {
            children: (0..1000).map(|i| ChunkActor::new(i).start()).collect(),
        }
    }
}

impl Actor for ArchiveActor {
    type Context = Context<Self>;
    fn started(&mut self, ctx: &mut Self::Context) {
        info!("ArchiveActor started!")
    }
}

type GetDumpResult = Result<Pin<Box<dyn Stream<Item = Bytes> + Send + Sync>>, ()>;

struct GetDump;

impl Message for GetDump {
    type Result = GetDumpResult;
}

impl Handler<GetDump> for ArchiveActor {
    type Result = GetDumpResult;

    fn handle(&mut self, _msg: GetDump, _ctx: &mut Self::Context) -> Self::Result {
        let stream = iter(self.children.clone())
            .map(|c| c.send(GetChunk))
            .buffer_unordered(3)
            .map(|r| {
                let b = r.expect("response").expect("Bytes");
                debug!("Got chunk: {:?}", b);
                b
            });
        Ok(Box::pin(stream))
    }
}

impl Handler<UpdateCommand> for ArchiveActor {
    type Result = Result<Pin<Box<dyn Future<Output = ()> + Send + Sync>>, ()>;

    fn handle(&mut self, item: UpdateCommand, ctx: &mut Self::Context) -> Self::Result {
        println!("Archive UpdateCommand: {}", item.id);
        let child_index = item.id % 1000;
        let child = self.children.get(child_index as usize).unwrap();
        use futures::future::FutureExt;
        Ok(Box::pin(child.send(item).map(|_| ())))
    }
}

impl Handler<UpdateCommand> for ChunkActor {
    type Result = Result<Pin<Box<dyn Future<Output = ()> + Send + Sync>>, ()>;

    fn handle(&mut self, msg: UpdateCommand, ctx: &mut Self::Context) -> Self::Result {
        info!("UpdateCommand({}): {}", self.i, msg.id);
        self.data = self.data.clone() + &msg.data + "\n";
        Ok(Box::pin(futures::future::ready(())))
    }
}

struct ChunkActor {
    i: i32,
    data: String,
}

impl ChunkActor {
    fn new(i: i32) -> ChunkActor {
        ChunkActor {
            i,
            data: "".to_owned(),
        }
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

    fn handle(&mut self, _msg: GetChunk, _ctx: &mut Self::Context) -> Self::Result {
        let to_send = Bytes::from(self.data.clone());
        if self.data.len() > 0 {
            info!(
                "GetChunk : {}, len={}, clone_len={}",
                self.i,
                self.data.len(),
                self.data.clone().len()
            );
            debug!("{:?}", to_send);
        }
        Ok(to_send)
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

    let response = client
        .get("https://stream.wikimedia.org/v2/stream/recentchange")
        .header("User-Agent", "Actix-web")
        .timeout(Duration::from_secs(600))
        .send()
        .await
        .expect("response");

    info!("Stream started");
    trace!("Got response from stream API: {:?}", response);
    let async_read = response
        .into_stream()
        .map_err(|e: PayloadError| Error::new(ErrorKind::Other, format!("{}", e)))
        .into_async_read();

    decode_stream(async_read)
        .filter_map(|event| {
            async move {
                use serde_json::Result;

                match event {
                    Ok(Event::Message { data, .. }) => {
                        let data1: Result<EventData> = serde_json::from_str(&data);
                        match data1 {
                            Ok(result) => Some(result),
                            Err(e) => {
                                error!("{:?}", e);
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
}

impl Message for UpdateCommand {
    type Result = Result<Pin<Box<dyn Future<Output = ()> + Send + Sync>>, ()>;
}
