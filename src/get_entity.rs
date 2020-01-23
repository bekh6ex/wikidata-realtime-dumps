use crate::actor::SerializedEntity;
use crate::prelude::*;

use log::*;
use rand;
use rand::Rng;
use serde::Deserialize;
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use hyper::client::connect::dns::GaiResolver;
use hyper::client::HttpConnector;

use hyper_rustls::HttpsConnector;

use hyper::{Body, Client as HyperClient};

type Client = HyperClient<HttpsConnector<HttpConnector<GaiResolver>>, Body>;

pub async fn get_entity(client: Arc<Client>, id: EntityId) -> Option<GetEntityResult> {
    with_retries(client, id, 1)
        .await
        .expect("Failed to get entity")
}

const INITIAL_TIMEOUT: u64 = 50;
const MAX_TIMEOUT: u64 = 5_000;
static TIMEOUT: AtomicU64 = AtomicU64::new(INITIAL_TIMEOUT);
const TIMEOUT_INCR: f32 = 1.1;
const TIMEOUT_REDUCE: f32 = 0.999;
const MAX_TRIES: u8 = 50;

fn with_retries(
    client: Arc<Client>,
    id: EntityId,
    try_number: u8,
) -> Pin<Box<dyn Future<Output = Result<Option<GetEntityResult>, Error>>>> {
    Box::pin(async move {
        debug!("Getting an entity {}. timeout={:?}", id, TIMEOUT);

        let r = get_entity_internal(client.clone(), id).await;
        let fuzzy = rand::thread_rng().gen_range(0.9f32, 1.1f32);

        use Error::*;
        match r {
            Ok(result) => {
                change_timeout(TIMEOUT_REDUCE);
                Ok(result)
            }
            Err(TooManyRequests) => {
                let timeout = (change_timeout(TIMEOUT_INCR) as f32 * fuzzy) as u64;
                info!("Too many requests. Waiting {:?}ms and retrying", timeout);
                async_std::task::sleep(Duration::from_millis(timeout)).await;
                with_retries(client, id, try_number).await
            }
            Err(err) => {
                warn!(
                    "Get entity failed. try={} timeout={:?} {:?}",
                    try_number, TIMEOUT, err
                );
                if try_number >= MAX_TRIES {
                    Err(err)
                } else {
                    let timeout = (change_timeout(TIMEOUT_INCR) as f32 * fuzzy) as u64;
                    async_std::task::sleep(Duration::from_millis(timeout)).await;
                    with_retries(client, id, try_number + 1).await
                }
            }
        }
    })
}

fn change_timeout(factor: f32) -> u64 {
    let mut initial_timeout = TIMEOUT.load(Ordering::Relaxed);
    if initial_timeout < INITIAL_TIMEOUT && factor > 1.0 {
        initial_timeout = INITIAL_TIMEOUT;
    }
    let timeout: f32 = initial_timeout as f32 * factor;
    let mut timeout = timeout.round() as u64;
    if timeout == initial_timeout && timeout != 0 {
        timeout -= 1;
    }

    timeout = timeout.min(MAX_TIMEOUT);

    TIMEOUT.store(timeout, Ordering::Relaxed);
    timeout as u64
}

async fn get_entity_internal(
    client: Arc<Client>,
    id: EntityId,
) -> Result<Option<GetEntityResult>, Error> {
    let url = format!(
        "https://www.wikidata.org/wiki/Special:EntityData/{}.json",
        id
    );
    let response = get_json::<SpecialEntityResponse>(&client, url).await?;
    if response.is_none() {
        return Ok(None);
    }
    let response = response.unwrap();

    // Entity might be a redirect to another one which will be automatically resolved.
    // The response will then contains some other entity which should be ignored.
    let value = response.extract(id);
    if value.is_none() {
        return Ok(None);
    }
    let value = value.unwrap();

    let data = serde_json::to_string(&value)
        .unwrap_or_else(|_| panic!("Serialize {} entity back failed O_o", id));

    let revision = RevisionId(extract_revision_id(id, &value));

    Ok(Some(GetEntityResult { id, revision, data }))
}

use crate::http_client::{get_json, Error};
fn extract_revision_id(id: EntityId, value: &Value) -> u64 {
    value
        .as_object()
        .unwrap_or_else(|| panic!("Entity {} representation was not an object", id))
        .get("lastrevid")
        .unwrap_or_else(|| panic!("Entity {} does not contain revision ID", id))
        .as_u64()
        .unwrap_or_else(|| panic!("Entity {} revision ID is not a u64", id))
}

pub struct GetEntityResult {
    id: EntityId,
    revision: RevisionId,
    data: String,
}

impl GetEntityResult {
    pub fn into_serialized_entity(self) -> SerializedEntity {
        let GetEntityResult { id, revision, data } = self;
        SerializedEntity { id, revision, data }
    }
}

#[derive(Deserialize, Debug)]
struct SpecialEntityResponse {
    entities: serde_json::Map<String, serde_json::Value>,
}

impl SpecialEntityResponse {
    fn extract(mut self, id: EntityId) -> Option<serde_json::Value> {
        self.entities.remove(&id.to_string())
    }
}
