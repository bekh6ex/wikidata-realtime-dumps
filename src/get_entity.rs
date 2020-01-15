use crate::actor::UpdateCommand;
use crate::prelude::*;
use actix_web::client::{Client, SendRequestError};
use actix_web::error::PayloadError;
use actix_web::http::StatusCode;
use actix_web::web::Bytes;
use log::*;
use serde::Deserialize;
use serde_json::Value;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

pub async fn get_entity(client: Arc<Client>, id: EntityId) -> Option<GetEntityResult> {
    with_retries(client, id, 1)
        .await
        .expect("Failed to get entity")
}

const INITIAL_TIMEOUT: u64 = 50;
static TIMEOUT: AtomicU64 = AtomicU64::new(INITIAL_TIMEOUT);
const TIMEOUT_INCR: f32 = 1.3;
const TIMEOUT_REDUCE: f32 = 0.9;

fn with_retries(
    client: Arc<Client>,
    id: EntityId,
    try_number: u8,
) -> Pin<Box<dyn Future<Output = Result<Option<GetEntityResult>, Error>>>> {
    Box::pin(async move {
        const MAX_TRIES: u8 = 5;

        debug!("Getting an entity {}. timeout={:?}", id, TIMEOUT);

        let r = get_entity_internal(client.clone(), id).await;

        use actix_web::client::SendRequestError::*;
        use Error::*;
        match r {
            Ok(result) => {
                change_timeout(TIMEOUT_REDUCE);
                Ok(result)
            }
            Err(GetResponse(H2(err))) => {
                let timeout = change_timeout(TIMEOUT_INCR);
                info!(
                    "Got connection error {:?}. Waiting {:?} ms and retrying",
                    err, TIMEOUT
                );
                async_std::task::sleep(Duration::from_millis(timeout)).await;
                with_retries(client, id, try_number).await
            }
            Err(err) => {
                warn!("Get entity failed. try={} {:?}", try_number, err);
                if try_number >= MAX_TRIES {
                    Err(err)
                } else {
                    let timeout = change_timeout(TIMEOUT_INCR);
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
        timeout = timeout - 1;
    }
    TIMEOUT.store(timeout, Ordering::Relaxed);
    timeout as u64
}

async fn get_entity_internal(
    client: Arc<Client>,
    id: EntityId,
) -> Result<Option<GetEntityResult>, Error> {
    let req = client.get(format!(
        "https://www.wikidata.org/wiki/Special:EntityData/{}.json",
        id
    ));

    debug!("Sending get entity request. id={}", id);
    let mut response = req.send().await.map_err(|e| Error::GetResponse(e))?;

    debug!(
        "Got entity response. status={} id={}",
        response.status(),
        id
    );

    if response.status() == StatusCode::NOT_FOUND {
        return Ok(None);
    }

    let body: Bytes = response
        .body()
        .limit(8 * 1024 * 1024)
        .await
        .map_err(|e| Error::GetResponseBody(e))?;

    let response = serde_json::from_slice::<WikidataResponse>(body.as_ref()).map_err(|e| {
        Error::ResponseFormat {
            cause: e,
            body: std::str::from_utf8(body.as_ref()).unwrap().to_owned(),
        }
    })?;

    // Entity might be a redirect to another one which will be automatically resolved.
    // The response will then contains some other entity which should be ignored.
    let value = response.entities.get(&id.to_string());
    if value.is_none() {
        return Ok(None);
    }

    let value = value.unwrap();

    let data =
        serde_json::to_string(value).expect(&format!("Serialize {} entity back failed O_o", id));

    let revision = RevisionId(extract_revision_id(id, value));

    Ok(Some(GetEntityResult { id, revision, data }))
}

#[derive(Debug)]
pub enum Error {
    GetResponse(SendRequestError),
    GetResponseBody(PayloadError),
    ResponseFormat {
        cause: serde_json::Error,
        body: String,
    },
}

fn extract_revision_id(id: EntityId, value: &Value) -> u64 {
    value
        .as_object()
        .expect(&format!("Entity {} representation was not an object", id))
        .get("lastrevid")
        .expect(&format!("Entity {} does not contain revision ID", id))
        .as_u64()
        .expect(&format!("Entity {} revision ID is not a u64", id))
}

pub struct GetEntityResult {
    id: EntityId,
    revision: RevisionId,
    data: String,
}

impl Into<UpdateCommand> for GetEntityResult {
    fn into(self) -> UpdateCommand {
        let GetEntityResult { id, revision, data } = self;
        UpdateCommand { id, revision, data }
    }
}

#[derive(Deserialize, Debug)]
struct WikidataResponse {
    entities: serde_json::Map<String, serde_json::Value>,
}
