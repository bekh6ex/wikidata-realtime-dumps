use super::prelude::*;
use crate::actor::UpdateCommand;
use crate::get_entity::get_entity;
use actix::prelude::Stream;
use actix_web::client::{Client, ClientBuilder, Connector, PayloadError};
use futures::future::ready;
use futures::{StreamExt, TryStreamExt};
use log::*;
use serde::Deserialize;

use sse_codec::{decode_stream, Event};
use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::time::Duration;

pub async fn get_update_stream() -> impl Stream<Item = UpdateCommand> {
    let client = Arc::new(create_client());
    let client_for_entities = client.clone();

    fn create_client() -> Client {
        ClientBuilder::new()
            .timeout(Duration::from_secs(30))
            .connector(Connector::new().timeout(Duration::from_secs(30)).finish())
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
        .map(|c| {
            trace!("Stream Body Chunk: {:?}", c);
            c
        })
        .map_err(|e| {
            error!("Stream error: {:?}", e);
            e
        })
        .map_err(|e: PayloadError| Error::new(ErrorKind::Other, format!("{:?}", e)))
        .into_async_read();

    decode_stream(async_read)
        .take_while(|decoding_result| {
            if decoding_result.is_err() {
                error!("Error after decoding: {:?}", decoding_result)
            }
            ready(decoding_result.is_ok())
        })
        .filter_map(|event| {
            async move {
                use serde_json::Result;

                match event {
                    Ok(Event::Message { data, .. }) => {
                        let data: Result<EventData> = serde_json::from_str(&data);
                        match data {
                            Ok(result) => Some(result),
                            Err(e) => {
                                error!("{:?}", e);
                                None
                            }
                        }
                    }
                    x => {
                        trace!("Something: {:?}", x);
                        None
                    }
                }
            }
        })
        .filter(|e| {
            ready(e.wiki == "wikidatawiki" && e.namespace == EntityType::Property.namespace().n())
        })
        .filter_map(move |event_data| {
            let client = client_for_entities.clone();
            async {
                let EventData { title, .. } = event_data;
                let id = EntityType::Property.parse_from_title(&title).unwrap();

                let client = client;

                let entity_result = get_entity(client, id).await?;
                Some(entity_result.into())
            }
        })
}

#[derive(Deserialize, Debug)]
struct EventData {
    wiki: String,
    title: String,
    #[serde(rename(deserialize = "type"))]
    event_type: String,
    namespace: u32,
    revision: Option<RevisionData>,
}

#[derive(Deserialize, Debug)]
struct RevisionData {
    new: u64,
}

#[derive(Deserialize, Debug)]
struct WikidataResponse {
    entities: serde_json::Map<String, serde_json::Value>,
}
