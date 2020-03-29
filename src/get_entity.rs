use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::*;
use futures_backoff::Strategy;
use log::*;
use serde::Deserialize;
use serde_json::Value;
use stream_throttle::{ThrottlePool, ThrottleRate};

use crate::http_client::{create_client, get_json, HClient, Error};
use crate::prelude::*;
use isahc::prelude::*;

#[derive(Clone)]
pub struct GetEntityClient {
    client_pool: Vec<Arc<HttpClient>>,
    pool_index: Arc<AtomicUsize>,
    rate_pool: ThrottlePool,
}

impl GetEntityClient {
    pub fn new(pool_size: NonZeroUsize, rate: ThrottleRate) -> Self {
        let client_pool: Vec<_> = (0..pool_size.get())
            .map(|_| Arc::new(create_client()))
            .collect();
        GetEntityClient {
            client_pool: client_pool,
            pool_index: Arc::new(AtomicUsize::new(0)),
            rate_pool: ThrottlePool::new(rate),
        }
    }

    pub fn get_entity(
        &self,
        id: EntityId,
        revision_id: Option<RevisionId>,
    ) -> impl Future<Output = Option<SerializedEntity>> {
        let this = self.clone();

        let get_this_entity = move || {
            this.clone().get_entity_internal(id, revision_id).map(
                move |r: Result<Option<GetEntityResult>, Error>| {
                    r.map_err(move |e| {
                        match &e {
                            Error::TooManyRequests => info!("Too many requests"),
                            Error::GetResponse(e) => info!("Response error: {:?}", e),
                            Error::ResponseFormat { cause, body } => {
                                warn!("Wrong response format: {:?}. Body: {}", cause, body)
                            }
                            Error::BadRequest => panic!(
                                "Got BadRequest. Should have been handled already. {:?} {:?}",
                                id, revision_id
                            ),
                        }
                        e
                    })
                },
            )
        };

        async move {
            let strategy = Strategy::exponential(Duration::from_millis(5))
                .with_jitter(true)
                .with_max_delay(Duration::from_secs(5))
                .with_max_retries(5)
                .retry(get_this_entity);

            strategy
                .map(|r| {
                    r.expect("Failed to get an entity")
                        .map(GetEntityResult::into_serialized_entity)
                })
                .await
        }
    }

    fn client(&self) -> &HttpClient {
        let index = self.pool_index.fetch_add(1, Ordering::Relaxed);
        &self.client_pool[index % self.client_pool.len()]
    }

    async fn get_entity_internal(
        self,
        id: EntityId,
        rev: Option<RevisionId>,
    ) -> Result<Option<GetEntityResult>, Error> {
        self.rate_pool.queue().await;

        let url = if let Some(RevisionId(rev)) = rev {
            format!(
                "https://www.wikidata.org/wiki/Special:EntityData/{}.json?revision={}",
                id, rev
            )
        } else {
            format!(
                "https://www.wikidata.org/wiki/Special:EntityData/{}.json",
                id
            )
        };

        let response = get_json::<SpecialEntityResponse>(self.client(), url).await;
        let response = match response {
            Ok(None) => {
                return Ok(None);
            }
            Err(Error::BadRequest) if rev.is_some() => {
                // Some weired behaviour of Special:EntityData
                return Ok(None);
            }
            Err(err) => {
                return Err(err);
            }
            Ok(Some(response)) => response,
        };

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
}

impl Default for GetEntityClient {
    fn default() -> Self {
        const MAX_CLIENTS: usize = 300;
        let rate = ThrottleRate::new(300, Duration::from_millis(1000));
        let client_pool = GetEntityClient::new(NonZeroUsize::new(MAX_CLIENTS).unwrap(), rate);
        client_pool
    }
}

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

#[cfg(test)]
mod test {
    use std::num::NonZeroUsize;
    use std::sync::Arc;
    use std::time::Duration;

    use futures::stream::*;
    use log::*;
    use stream_throttle::ThrottleRate;

    use crate::get_entity::GetEntityClient;
    use crate::prelude::EntityType;

    #[actix_rt::test]
    #[test]
    async fn test_rate() {
        init_logger();
        info!("Starting");

        let client = Arc::new(GetEntityClient::new(
            NonZeroUsize::new(30).unwrap(),
            ThrottleRate::new(500, Duration::from_secs(1)),
        ));

        iter(1..1)
            .map(|i| EntityType::Item.id(i))
            .map(move |id| {
                let client = client.clone();
                info!("Get entity {:?}", id);
                client.get_entity(id, None)
            })
            .buffered(50)
            .enumerate()
            .for_each(|(i, _e)| {
                async move {
                    if i % 1 == 0 {
                        info!("Got {}", i)
                    }
                }
            })
            .await;
    }

    fn init_logger() {
        use log4rs::append::console::ConsoleAppender;
        use log4rs::config::{Appender, Root};

        let stdout: ConsoleAppender = ConsoleAppender::builder().build();
        let log_config = log4rs::config::Config::builder()
            .appender(Appender::builder().build("stdout", Box::new(stdout)))
            .build(Root::builder().appender("stdout").build(LevelFilter::Info))
            .unwrap();
        log4rs::init_config(log_config).unwrap();
    }
}
