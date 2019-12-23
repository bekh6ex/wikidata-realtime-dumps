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

use actix_web::{get, middleware, web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use actix::prelude::*;
use actix_web::body::*;
use futures::stream::*;
use futures::prelude::*;
use bytes::Bytes;
use actix_web::http::StatusCode;
use std::sync::Arc;
use std::pin::Pin;
use std::fmt::Debug;
use actix_web::guard::Get;
use std::iter::Map;


#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "actix_server=info,actix_web=info");
    env_logger::init();


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
    children: Vec<Addr<ChunkActor>>
}

impl ArchiveActor {
    fn new() -> ArchiveActor {
        ArchiveActor {
            children: (1..100).map(|i| ChunkActor::new(i).start()).collect()
        }
    }
}

impl Actor for ArchiveActor {
    type Context = Context<Self>;
}

type GetDumpResult = Result<Pin<Box<dyn Stream<Item=Bytes> + Send + Sync>>, ()>;

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
    i: i32
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
