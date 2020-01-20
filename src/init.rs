use crate::actor::UpdateCommand;
use crate::get_entity::{get_entity, GetEntityResult};
use crate::prelude::*;
use actix_web::client::{Client, ClientBuilder, Connector};
use actix_web::http::StatusCode;
use actix_web::web::Bytes;
use futures::future::ready;
use futures::stream::{iter, StreamExt};
use futures::Stream;
use log::*;
use serde::Deserialize;

use crate::events::EventId;
use std::sync::Arc;
use std::time::Duration;

pub async fn init(
    ty: EntityType,
    start_id: Option<EntityId>,
    event_id: EventId,
) -> impl Stream<Item = UpdateCommand> {
    let latest_id = get_latest_entity_id(ty).await;
    let safety_offset = 100;

    let min = start_id.map(|i| i.n()).unwrap_or(1);
    let max = latest_id.n() + safety_offset;

    const MAX_CLIENTS: u32 = 10;
    let client_pool = Arc::new((0..MAX_CLIENTS).map(|_| {
        create_client()
    }).collect::<Vec<_>>());

    debug!("Creating init stream for {:?}", ty);

    iter(min..=max)
        .map(move |n| ty.id(n))
        .map(move |id| {
            if id.n() == min {
                info!("Init stream for {:?} started from {:?}", ty, start_id);
            }
            if id.n() % 1000 == 0 {
                info!("Initializing entity {}", id);
            }
            if id.n() == max {
                info!("Initializing the last entity of type {:?}: {}", ty, id);
            }
            id
        })
        .enumerate()
        .then( move |(index, id)| {
            let pool_index = index % client_pool.len();
            let client = Arc::new(client_pool[pool_index].clone());
            async move {
                // To not make a lot of requests in the same time
                let timeout = index % 50;
                async_std::task::sleep(Duration::from_millis(timeout as u64)).await;
                get_entity(client, id)
            }
        })
        .buffered(100)
        .filter_map(move |e: Option<GetEntityResult>| {
            let event_id = event_id.clone();
            ready(e.map(move |e| UpdateCommand {
                event_id: Some(event_id),
                entity: e.into_serialized_entity(),
            }))
        })
}

async fn get_latest_entity_id(ty: EntityType) -> EntityId {
    let client = create_client();

    let url = format!("https://www.wikidata.org/w/api.php?action=query&format=json&list=recentchanges&rcnamespace={}&rctype=new&rclimit=1", ty.namespace().n());

    let mut response = client
        .get(url)
        .header("User-Agent", "Actix-web")
        .timeout(Duration::from_secs(600))
        .send()
        .await
        .map_err(|e| panic!("Failed to get RC response: type={:?}, error={:?}", ty, e))
        .unwrap();

    if response.status() != StatusCode::OK {
        panic!(
            "Got unexpected status code: type={:?}, status_code={:?}",
            ty,
            response.status()
        )
    }

    let body: Bytes = response
        .body()
        .await
        .map_err(|e| {
            panic!(
                "Failed to get body of RC response: type={:?}, error={:?}",
                ty, e
            )
        })
        .unwrap();

    let unser: QueryResponse = serde_json::from_slice::<QueryResponse>(body.as_ref())
        .unwrap_or_else(|_| {
            panic!(
                "Invalid response format: {:?}\n{:?}",
                &ty,
                std::str::from_utf8(body.as_ref())
            )
        });

    let title = &unser
        .query
        .recentchanges
        .get(0)
        .expect("No changes present")
        .title;

    let id = ty.parse_from_title(title).expect("Failed to parse ID");
    info!("Got latest ID '{}'", id);
    id
}

fn create_client() -> Client {
    ClientBuilder::new()
        .timeout(Duration::from_secs(30))
        .disable_redirects()
        .connector(
            Connector::new()
                .timeout(Duration::from_secs(30))
                .conn_lifetime(Duration::from_secs(5 * 60))
                .finish(),
        )
        .finish()
}

#[derive(Deserialize)]
struct QueryResponse {
    query: QueryMap,
}

#[derive(Deserialize)]
struct QueryMap {
    recentchanges: Vec<ChangeDescription>,
}

#[derive(Deserialize)]
struct ChangeDescription {
    title: String,
}
