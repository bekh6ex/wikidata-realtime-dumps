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


#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    std::env::set_var("RUST_LOG", "actix_server=info,actix_web=info");
    env_logger::init();


    let archive_actor = (ArchiveActor {}).start();

    let server = HttpServer::new(|| {
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
    let mut result = ar.send(GetDump).await.expect("asd");
    HttpResponse::Ok().streaming(result.map(|b| Ok(b) as Result<Bytes, ()>))
}


struct ArchiveActor {}

impl Actor for ArchiveActor {
    type Context = Context<Self>;
}

type GetDumpResult = Arc<BoxStream<'static, Bytes>>;

struct GetDump;

impl Message for GetDump {
    type Result = GetDumpResult;
}

impl Handler<GetDump> for ArchiveActor {
    type Result = GetDumpResult;

    fn handle(&mut self, msg: GetDump, ctx: &mut Self::Context) -> Self::Result {
        let stream = iter(1..10).map(|i| Bytes::from("D".to_owned()));
        Arc::new(Box::pin(stream))
    }
}
