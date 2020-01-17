use crate::actor::archivarius::ArchivariusActor;
use crate::actor::GetDump;
use actix::prelude::*;
use actix_web::web::Bytes;
use actix_web::{middleware, web, App, HttpRequest, HttpResponse, HttpServer, Responder};
use futures::StreamExt;

pub async fn start(archive_actor: Addr<ArchivariusActor>) -> std::io::Result<()> {
    HttpServer::new(move || {
        App::new()
            .data(archive_actor.clone())
            .wrap(middleware::Compress::default())
            .wrap(middleware::Logger::default())
            .service(web::resource("/").to(handle_request))
    })
    .bind("127.0.0.1:8080")?
    .workers(3)
    .run()
    .await
}

async fn handle_request(_req: HttpRequest, ar: web::Data<Addr<ArchivariusActor>>) -> impl Responder {
    // TODO: As long as we return chunks in order we can make it possible to return only certain
    //       requested ranges of entities

    let result = ar.send(GetDump).await.expect("asd").expect("jj");
    HttpResponse::Ok().streaming(result.map(|b| Ok(b) as Result<Bytes, ()>))
}
