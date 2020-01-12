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

use std::sync::Arc;
use std::time::Duration;

pub async fn init(ty: EntityType) -> impl Stream<Item = UpdateCommand> {
    let latest_id = get_latest_entity_id(ty).await;
    let safety_offset = 100;

    let max = latest_id.n() + safety_offset;

    let client = Arc::new(create_client());

    iter(1..max)
        .map(move |n| ty.id(n))
        .map(move |id| get_entity(client.clone(), id))
        .buffer_unordered(16)
        .filter_map(|e: Option<GetEntityResult>| ready(e.map(|e| e.into())))
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

    let unser: QueryResponse =
        serde_json::from_slice::<QueryResponse>(body.as_ref()).expect(&format!(
            "Invalid response format: {:?}\n{:?}",
            &ty,
            std::str::from_utf8(body.as_ref())
        ));

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
