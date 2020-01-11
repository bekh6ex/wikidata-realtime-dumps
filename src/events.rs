use super::prelude::*;
use crate::actor::UpdateCommand;
use actix::prelude::Stream;
use actix_web::client::{Client, ClientBuilder, Connector, PayloadError};
use actix_web::web::Bytes;
use futures::future::ready;
use futures::{StreamExt, TryStreamExt};
use log::*;
use serde::Deserialize;
use serde_json::Value;
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
        .filter(|e| ready(e.wiki == "wikidatawiki" && e.namespace == 0))
        .filter_map(move |event_data| {
            let client = client_for_entities.clone();
            async {
                let EventData { title: id, .. } = event_data;

                let client = client;
                // TODO Send Accept header
                let req = client.get(format!(
                    "https://www.wikidata.org/wiki/Special:EntityData/{}.json",
                    id
                ));

                let mut result = req
                    .send()
                    .await
                    .expect(&format!("Didn't get the response: {}", &id));

                // TODO: Check for 200 status
                let body: Bytes = result
                    .body()
                    .limit(8 * 1024 * 1024)
                    .await
                    .expect("Entity response body");

                let unser =
                    serde_json::from_slice::<WikidataResponse>(body.as_ref()).expect(&format!(
                        "Invalid response format: {}\n{:?}",
                        &id,
                        std::str::from_utf8(body.as_ref())
                    ));
                // Entity might be a redirect to another one which will be automatically resolved.
                // The response will then contains some other entity which should be ignored.
                let value = unser.entities.get(&id)?;

                let data = serde_json::to_string(value)
                    .expect(&format!("Serialize {} entity back failed O_o", id));

                let revision = extract_revision_id(&id, value);

                let id = id[1..].parse().expect(&format!("Entity {} ", id));

                Some(UpdateCommand {
                    entity_type: EntityType::Item,
                    revision,
                    id,
                    data,
                })
            }
        })
}

fn extract_revision_id(id: &String, value: &Value) -> u64 {
    value
        .as_object()
        .expect(&format!("Entity {} representation was not an object", id))
        .get("lastrevid")
        .expect(&format!("Entity {} does not contain revision ID", id))
        .as_u64()
        .expect(&format!("Entity {} revision ID is not a u64", id))
}

#[derive(Deserialize, Debug)]
struct EventData {
    wiki: String,
    title: String,
    namespace: u64,
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
