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

mod dumps;

pub async fn init(
    ty: EntityType,
    start_id: Option<EntityId>,
    event_id: EventId,
) -> impl Stream<Item = UpdateCommand> {
    let latest_id = get_latest_entity_id(ty).await;
    let safety_offset = 100;

    let min = start_id.map(|i| i.n()).unwrap_or(1);
    let max = latest_id.n() + safety_offset;

    const MAX_CLIENTS: u32 = 1;
    let client_pool = Arc::new(
        (0..MAX_CLIENTS)
            .map(|_| create_client())
            .collect::<Vec<_>>(),
    );

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
        .then(move |(index, id)| {
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
                event_id: event_id,
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

#[cfg(test)]
mod test {

    use futures::StreamExt;
    use std::io::{Bytes, Error, ErrorKind};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use futures::future::ready;
    use futures::stream::once;
    use futures::*;
    use log::*;
    use serde::{Deserialize, Serialize};

    use actix_rt;
    use async_std::prelude::*;
    use std::collections::BTreeSet;
    use std::sync::atomic::{AtomicU64, Ordering};
    use hyper::{Body, Client, Request};


    //    #[actix_rt::test]
    //    #[test]
    async fn test1() {

        let client = Client::builder().build::<_, hyper::Body>(hyper_rustls::HttpsConnector::new());

        let req = Request::builder()
            .method("GET")
            .header("Accept-Encoding", "deflate")
            .uri("https://dumps.wikimedia.org/other/wikibase/wikidatawiki/latest-all.json.bz2");

        let resp = client
            .request(req.body(Body::empty()).unwrap())
            .await
            .unwrap();

        let body1 = resp.into_body();
        //
        let stream = body1.map_err(|e| std::io::Error::from(std::io::ErrorKind::Other));
        //
        use async_compression::stream::BzDecoder;
        let stream = BzDecoder::new(stream);

        //        let stream = stream.map(|x| {
        //           x.map(|i|i.as_bytes())
        //        }).into_async_read();

        use futures_codec::{Framed, FramedRead, LinesCodec};
        let inner = stream.into_async_read();
        let stream = FramedRead::new(inner, LinesCodec {});

        use serde::Deserialize;
        #[derive(Deserialize)]
        struct Entity {
            id: String,
        }

        //        let prev_id_inner = &mut "".to_owned();
        //        let prev_id = AtomicPtr::new(prev_id_inner);

        let pr = AtomicU64::new(0);
        let diff = AtomicU64::new(0);
        let idx = AtomicU64::new(1);

        let set = Mutex::new(BTreeSet::new());

        stream
            .skip(1)
            .map(Result::unwrap)
            .map(|s| {
                let len = s.len();

                let x1 = serde_json::from_str::<Entity>(&s[0..(len - 2)]).unwrap();
                let id = x1.id[1..].parse::<u64>().unwrap();
                id
            })
            .filter_map(move |id: u64| {
                let mut set = set.lock().unwrap();
                set.insert(id);

                let res = if set.len() < 100 {
                    None
                } else {
                    let el: u64 = set.iter().next().unwrap().clone();
                    set.take(&el)
                };

                ready(res)
            })
            .enumerate()
            .for_each(|(counter, new)| {
                if counter % 100_000 == 0 {
                    println!("Got to index {}. ID = {}", counter, new);
                }

                let old = pr.load(Ordering::Relaxed);

                if old >= new {
                    let old_diff = diff.load(Ordering::Relaxed);
                    let new_diff = old - new;
                    let max_diff = new_diff.max(old_diff);
                    diff.store(max_diff, Ordering::Relaxed);
                    let idx = idx.fetch_add(1, Ordering::Relaxed);

                    println!(
                        "{} - Old: {}, new: {}, diff = {}, max_diff = {}",
                        idx, old, new, new_diff, max_diff
                    );
                } else {
                    pr.swap(new, Ordering::Relaxed);
                }
                async { () }
            })
            .await;

        ()

        //        curl https://dumps.wikimedia.org/other/wikibase/wikidatawiki/latest-all.json.gz | gzip -d | jq -nc --stream 'inputs | select(length==2) | select( .[0][1] == "id") | [ .[1]] ' | less
    }
}
