use super::prelude::*;
use crate::actor::UpdateCommand;
use actix::prelude::Stream;
use actix_web::client::{Client, ClientBuilder, Connector, PayloadError};
use actix_web::web::Bytes;
use futures::{StreamExt, TryStreamExt};
use log::*;
use serde::Deserialize;
use sse_codec::{decode_stream, Event};
use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::time::Duration;

pub async fn get_update_stream() -> impl Stream<Item = UpdateCommand> {
    let client = Arc::new(create_client());
    let client_copy = client.clone();

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
            debug!("Stream Body Chunk: {:?}", c);
            c
        })
        .map_err(|e| {
            error!("Stream error: {:?}", e);
            e
        })
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
                    Err(e) => {
                        error!("Error after decoding: {:?}", e);
                        None
                    }
                    x => {
                        debug!("Something: {:?}", x);
                        None
                    }
                }
            }
        })
        .filter(|e| futures::future::ready(e.wiki == "wikidatawiki" && e.namespace == 0))
        .map(move |ed| (ed, client_copy.clone()))
        .then(|(event_data, client)| {
            async {
                let EventData { title: id, .. } = event_data;
                let client = client;

                let req = client.get(format!(
                    "https://www.wikidata.org/wiki/Special:EntityData/{}.json",
                    id
                ));

                let future_response = req.send();

                let mut result = future_response.await.unwrap();

                let body: Bytes = result
                    .body()
                    .limit(8 * 1024 * 1024)
                    .await
                    .expect("Entity response body");

                let unser = serde_json::from_slice::<WikidataResponse>(body.as_ref()).unwrap();
                // Entity might be a redirect to another one which will be automatically resolved.
                // The response will then contains some other entity which should be ignored.
                let value = unser.entities.get(&id)?;

                let data = serde_json::to_string(value)
                    .expect(&format!("Serialize {} entity back failed O_o", id));

                let revision = value
                    .as_object()
                    .expect(&format!("Entity {} representation was not an object", id))
                    .get("lastrevid")
                    .expect(&format!("Entity {} does not contain revision ID", id))
                    .as_u64()
                    .expect(&format!("Entity {} revision ID is not a u64", id));

                let id = id[1..].parse().expect(&format!("Entity {} ", id));

                Some(UpdateCommand {
                    entity_type: EntityType::Item,
                    revision,
                    id,
                    data,
                })
            }
        })
        .filter_map(|i| async { i })
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
