use std::collections::BTreeMap;
use std::sync::Arc;

use actix::prelude::*;
use futures::future::ready;
use futures::stream::once;
use futures::{self, StreamExt};
use log::*;

use crate::actor::archivarius::{ArchivariusActor, InitializationFinished, StartInitialization};
use crate::actor::UpdateCommand;
use crate::events::{get_current_event_id, get_update_stream, EventId};
use crate::prelude::EntityType;

mod actor;
mod events;
mod get_entity;
mod init;
mod prelude;

#[actix_rt::main]
async fn main() -> std::io::Result<()> {
    init_logger();

    info!("Starting...");

    // TODO: Lock storage file

    let entity_type = EntityType::Property;

    let archive_actor = ArchivariusActor::new(entity_type).start();

    let mut map = BTreeMap::new();
    map.insert(entity_type, archive_actor.clone());
    let map = Arc::new(map);

    let initial_event_id = initialize(entity_type, archive_actor.clone()).await;

    let update_stream = get_update_stream(entity_type, initial_event_id).await;

    let send_forward = |e: UpdateCommand| {
        let archive_actor_for_stream = archive_actor.clone();
        async_std::task::spawn(async move {
            let result = archive_actor_for_stream
                .send(e)
                .await
                .expect("Actor communication failed")
                .await;
            trace!("Got command result {:?}.", result);
        })
    };

    let update_stream = update_stream.for_each(send_forward);

    let ws = warp_server::start(&map);

    let _ = futures::future::join(update_stream, ws).await;
    Ok(())
}

async fn initialize(ty: EntityType, actor: Addr<ArchivariusActor>) -> EventId {
    let current = get_current_event_id();

    let response = actor
        .send(StartInitialization)
        .await
        .expect("Failed commun");

    let initial_event_id: EventId = response.last_event_id.unwrap_or(current.await);

    let init_stream = init::init(ty, response.initialized_up_to).await;

    let init_finished_stream = once(ready(None));

    let send_forward = |e: UpdateCommand| {
        let archive_actor_for_stream = actor.clone();
        async_std::task::spawn(async move {
            let result = archive_actor_for_stream
                .send(e)
                .await
                .expect("Actor communication failed")
                .await;
            // TODO: Should get last event id here
            debug!("Got update result {:?} ", result);
            ()
        })
    };

    let init_stream = init_stream
        .map(Option::Some)
        .chain(init_finished_stream)
        .for_each(|e| {
            async {
                match e {
                    None => {
                        actor.do_send(InitializationFinished);
                    }
                    Some(event) => {
                        send_forward(event).await;
                    }
                }
            }
        });

    init_stream.await;

    initial_event_id
}

fn init_logger() {
    log4rs::init_file("log4rs.yaml", Default::default()).unwrap();
}

mod warp_server {
    use std::collections::BTreeMap;
    use std::convert::Infallible;
    use std::net::SocketAddrV4;
    use std::sync::Arc;

    use actix::Addr;
    use hyper::body::Bytes;
    use hyper::{Body, StatusCode};
    use warp::reply::Response;
    use warp::*;

    use crate::actor::archivarius::ArchivariusActor;
    use crate::actor::GetDump;
    use crate::prelude::EntityType;

    pub async fn start(ar: &Arc<BTreeMap<EntityType, Addr<ArchivariusActor>>>) {
        let hello = get_dump_route(ar);

        let ws = warp::serve(hello).run("127.0.0.1:8080".parse::<SocketAddrV4>().unwrap());
        ws.await
    }

    fn get_dump_route(
        ar: &Arc<BTreeMap<EntityType, Addr<ArchivariusActor>>>,
    ) -> impl Filter<Extract = impl warp::Reply, Error = warp::Rejection> + Clone {
        warp::path!("dumps" / String)
            .and(warp::get())
            .and(with_actor(ar))
            .and_then(get_dump_handler)
    }

    fn with_actor(
        ar: &Arc<BTreeMap<EntityType, Addr<ArchivariusActor>>>,
    ) -> impl Filter<
        Extract = (Arc<BTreeMap<EntityType, Addr<ArchivariusActor>>>,),
        Error = std::convert::Infallible,
    > + Clone {
        let ar = ar.clone();
        warp::any().map(move || ar.clone())
    }

    const ENTITY_NAMES: [(&'static str, EntityType); 3] = [
        ("properties", EntityType::Property),
        ("items", EntityType::Item),
        ("lexemes", EntityType::Lexeme),
    ];

    async fn get_dump_handler(
        mut ty: String,
        map: Arc<BTreeMap<EntityType, Addr<ArchivariusActor>>>,
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
            .into_iter()
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
}
