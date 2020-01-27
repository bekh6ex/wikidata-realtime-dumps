use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
use std::num::NonZeroUsize;

use futures::*;
use hyper::{Body, Client as HyperClient};
use hyper::client::connect::dns::GaiResolver;
use hyper::client::HttpConnector;
use hyper_rustls::HttpsConnector;
use log::*;
use rand;
use rand::Rng;
use serde::Deserialize;
use serde_json::Value;
use stream_throttle::{ThrottlePool, ThrottleRate};

use crate::http_client::{create_client, Error, get_json};
use crate::prelude::*;

type Client = HyperClient<HttpsConnector<HttpConnector<GaiResolver>>, Body>;

#[derive(Clone)]
pub struct GetEntityClient {
    client_pool: Vec<Arc<Client>>,
    pool_index: Arc<AtomicUsize>,
    rate_pool: ThrottlePool,
}

impl GetEntityClient {
    pub fn new(pool_size: NonZeroUsize, rate: ThrottleRate) -> Self {
        let client_pool: Vec<_> = (0..pool_size.get()).map(|_| Arc::new(create_client())).collect();
        GetEntityClient {
            client_pool: client_pool,
            pool_index: Arc::new(AtomicUsize::new(0)),
            rate_pool: ThrottlePool::new(rate),
        }
    }

    pub fn get_entity(&self, id: EntityId) -> impl Future<Output = Option<SerializedEntity>> {
        let eventual_response = self.clone().with_retries(id, 1);
        eventual_response.map(|r| {
            r.expect("Failed to get an entity").map(GetEntityResult::into_serialized_entity)
        })
    }

    fn client(&self) -> &Client {
        let index = self.pool_index.fetch_add(1, Ordering::Relaxed);
        &self.client_pool[index % self.client_pool.len()]
    }

    fn with_retries(
        self,
        id: EntityId,
        try_number: u8,
    ) -> Pin<Box<dyn Future<Output=Result<Option<GetEntityResult>, Error>>>> {
        Box::pin(async move {
            debug!("Getting an entity {}. timeout={:?}", id, TIMEOUT);

            let r = self.clone().get_entity_internal(id).await;
            let fuzzy = rand::thread_rng().gen_range(0.9f32, 1.1f32);

            use Error::*;
            match r {
                Ok(result) => {
                    change_timeout(TIMEOUT_REDUCE);
                    Ok(result)
                }
                Err(Throttled) => {
                    let timeout = TIMEOUT.load(Ordering::Relaxed);
                    info!("Throttled. Waiting {:?}ms and retrying", timeout);
                    async_std::task::sleep(Duration::from_millis(timeout)).await;
                    self.with_retries(id, try_number).await
                }
                Err(TooManyRequests) => {
                    let timeout = (change_timeout(TIMEOUT_INCR) as f32 * fuzzy) as u64;
                    info!("Too many requests. Waiting {:?}ms and retrying", timeout);
                    async_std::task::sleep(Duration::from_millis(timeout)).await;
                    self.with_retries(id, try_number).await
                }
                Err(err) => {
                    info!(
                        "Get entity failed. try={} timeout={:?} {:?}",
                        try_number, TIMEOUT, err
                    );
                    if try_number >= MAX_TRIES {
                        Err(err)
                    } else {
                        let timeout = (change_timeout(TIMEOUT_INCR) as f32 * fuzzy) as u64;
                        async_std::task::sleep(Duration::from_millis(timeout)).await;
                        self.with_retries(id, try_number + 1).await
                    }
                }
            }
        })
    }


    async fn get_entity_internal(
        self,
        id: EntityId,
    ) -> Result<Option<GetEntityResult>, Error> {
        futures::compat::Compat01As03::new(self.rate_pool.queue())
            .map_err(|_| Error::Throttled)
            .await?;

        let url = format!(
            "https://www.wikidata.org/wiki/Special:EntityData/{}.json",
            id
        );
        println!("Get entity {:?};", id);


        let response = get_json::<SpecialEntityResponse>(self.client(), url).await?;
        if response.is_none() {
            return Ok(None);
        }
        let response = response.unwrap();
        println!("Get entity response {:?};", id);


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
        const MAX_CLIENTS: usize = 2;
        let rate = ThrottleRate::new(50, Duration::from_secs(1));
        let client_pool = GetEntityClient::new(NonZeroUsize::new(MAX_CLIENTS).unwrap(), rate);
        client_pool
    }
}

const INITIAL_TIMEOUT: u64 = 1;
const MAX_TIMEOUT: u64 = 5_000;
static TIMEOUT: AtomicU64 = AtomicU64::new(INITIAL_TIMEOUT);
const TIMEOUT_INCR: f32 = 1.1;
const TIMEOUT_REDUCE: f32 = 0.999;
const MAX_TRIES: u8 = 50;

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

    use futures::*;
    use futures::stream::*;
    use stream_throttle::ThrottleRate;

    use crate::get_entity::GetEntityClient;
    use crate::prelude::{EntityId, EntityType};

    #[actix_rt::test]
    async fn test_rate() {
        init_logger();

        let client = Arc::new(GetEntityClient::new(NonZeroUsize::new(4).unwrap(), ThrottleRate::new(2000, Duration::from_secs(1))));

        iter(1..1)
            .map(|i| EntityType::Item.id(i))
            .map(move |id| {
                let client = client.clone();
                async move {
                    client.get_entity(id).await
                }
            })
            .buffered(2000)
            .for_each_concurrent(100, |e| async {
                async_std::task::spawn(async move {
                    e.map(|e| println!("Got {:?}", e.id));
                }).await;
                //do nothing
            }).await;
    }

    fn init_logger() {
        use log::LevelFilter;
        use log4rs::append::console::ConsoleAppender;
        use log4rs::config::{Appender, Root};
        use log4rs::encode::json::JsonEncoder;

        let stdout: ConsoleAppender = ConsoleAppender::builder().build();
        let log_config = log4rs::config::Config::builder()
            .appender(Appender::builder().build("stdout", Box::new(stdout)))
            .build(Root::builder().appender("stdout").build(LevelFilter::Info))
            .unwrap();
        log4rs::init_config(log_config).unwrap();
    }
}
