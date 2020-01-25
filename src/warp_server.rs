use std::collections::BTreeMap;
use std::convert::Infallible;
use std::net::SocketAddrV4;
use std::sync::Arc;

use actix::Addr;
use hyper::body::Bytes;
use hyper::{Body, StatusCode};
use warp::reply::Response;
use warp::*;

use crate::archive::archivarius::Archivarius;
use crate::archive::GetDump;
use crate::prelude::EntityType;

pub(super) async fn start(ar: &Arc<BTreeMap<EntityType, Addr<Archivarius>>>) {
    let hello = get_dump_route(ar);

    let ws = warp::serve(hello).run("127.0.0.1:8080".parse::<SocketAddrV4>().unwrap());
    ws.await
}

fn get_dump_route(
    ar: &Arc<BTreeMap<EntityType, Addr<Archivarius>>>,
) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
    warp::path!("dumps" / String)
        .and(warp::get())
        .and(with_actor(ar))
        .and_then(get_dump_handler)
}

fn with_actor(
    ar: &Arc<BTreeMap<EntityType, Addr<Archivarius>>>,
) -> impl Filter<
    Extract = (Arc<BTreeMap<EntityType, Addr<Archivarius>>>,),
    Error = std::convert::Infallible,
> + Clone {
    let ar = ar.clone();
    warp::any().map(move || ar.clone())
}

const ENTITY_NAMES: [(&str, EntityType); 3] = [
    ("properties", EntityType::Property),
    ("items", EntityType::Item),
    ("lexemes", EntityType::Lexeme),
];

async fn get_dump_handler(
    mut ty: String,
    map: Arc<BTreeMap<EntityType, Addr<Archivarius>>>,
) -> Result<impl warp::Reply, Infallible> {
    use futures::StreamExt;
    // TODO: As long as we return chunks in order we can make it possible to return only certain
    //       requested ranges of entities
    let suffix = ".jsonl.gz";

    if !ty.ends_with(suffix) {
        return Ok(not_found());
    }

    ty.split_off(ty.len() - suffix.len());

    let ty: Option<&EntityType> = ENTITY_NAMES
        .iter()
        .find(|(prefix, _)| **prefix == ty)
        .map(|(_, entity_type)| entity_type);

    if let Some(ty) = ty {
        if let Some(ar) = map.get(ty) {
            let result = ar
                .send(GetDump)
                .await
                .expect("Actor communication problem")
                .map(|b| Ok(b) as Result<Bytes, Infallible>);

            return Ok(Response::new(Body::wrap_stream(result)));
        }
    }

    Ok(not_found())
}

fn not_found() -> Response {
    let mut r404 = Response::new(Body::from("Not found"));
    *r404.status_mut() = StatusCode::NOT_FOUND;

    r404
}
