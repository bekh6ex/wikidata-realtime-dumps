use futures::future::{ready, FutureExt};
use futures::stream::{iter, once, StreamExt};
use futures::Stream;
use log::*;
use serde::Deserialize;

use crate::archive::Initialization;
use crate::events::{get_current_event_id, EventId};
use crate::get_entity::GetEntityClient;
use crate::http_client::{create_client, get_json};
use crate::init::dumps::get_dump_stream;
use crate::prelude::*;
use crate::stream_ext::join_streams::JoinStreams;

mod dumps;

#[derive(Clone, Debug)]
pub struct DumpConfig {
    pub url: String,
    pub event_stream_start: EventId,
    pub ty: EntityType,
    pub archive_format: ArchiveFormat,
    pub dump_format: DumpFormat,
}

#[derive(Clone, Copy, Debug)]
pub enum ArchiveFormat {
    Bzip2,
    Gzip,
}

#[derive(Clone, Copy, Debug)]
pub enum DumpFormat {
    WikidataJsonArray,
    SortedJsonLines,
}

pub async fn init(
    client: GetEntityClient,
    ty: EntityType,
    start_id: Option<EntityId>,
    dump_config: Option<DumpConfig>,
) -> impl Stream<Item = Initialization> {
    let end_id = get_latest_entity_id(ty).await;

    let safety_offset = 100;

    let end_id = ty.id(end_id.n() + safety_offset);

    init_inner(client, ty, start_id, end_id, dump_config).await
}

pub async fn init_inner(
    client: GetEntityClient,
    ty: EntityType,
    start_id: Option<EntityId>,
    end_id: EntityId,
    dump_config: Option<DumpConfig>,
) -> impl Stream<Item = Initialization> {
    let init_end_stream = once(ready(Initialization::Finished));

    let min = start_id.map(|i| i.n()).unwrap_or(1);
    let max = end_id.n();

    debug!("Creating init stream for {:?}", ty);

    let maybe_dump_pair = {
        match dump_config {
            None => None,
            Some(dump_config) => {
                assert_eq!(ty, dump_config.ty);
                let id_stream = id_stream(min, max, ty);
                let dump_stream = get_dump_stream(dump_config.clone())
                    .await
                    .enumerate()
                    .filter_map(move |(i, e)| {
                        let process = e.id.n() >= min;
                        if !process && i % 100_000 == 0 {
                            info!("Filtered {} entities from dump up to {:?}", i, e.id);
                        }
                        ready(if process { Some(e) } else { None })
                    });
                let client = client.clone();

                let joined = JoinStreams::new(id_stream, dump_stream, move |id: EntityId| {
                    client.get_entity(id, None)
                });

                Some((dump_config.event_stream_start, joined))
            }
        }
    };

    let raw_pair = {
        let current = get_current_event_id().await;
        let client = client.clone();
        let id_stream = id_stream(min, max, ty);
        let entity_stream = id_stream.map(move |id| client.get_entity(id, None).left_future());

        (current, entity_stream)
    };

    let (event_id, final_stream) = if max - min >= 1_000_000 && maybe_dump_pair.is_some() {
        let (event_id, stream) = maybe_dump_pair.unwrap();
        (event_id, stream.right_stream())
    } else {
        let (event_id, stream) = raw_pair;
        (event_id, stream.left_stream())
    };

    let update_stream = final_stream
        .buffered(150)
        .filter_map(|se: Option<SerializedEntity>| {
            ready(se.map(move |se| Initialization::UpdateEntity(se)))
        });

    let init_start_stream = once(ready(Initialization::Start(event_id)));

    init_start_stream
        .chain(update_stream)
        .chain(init_end_stream)
}

fn id_stream(min: u32, max: u32, ty: EntityType) -> impl Stream<Item = EntityId> {
    iter(min..=max).map(move |n| ty.id(n)).map(move |id| {
        if id.n() == min {
            info!("Init stream for {:?} started from {:?}", ty, min);
        }
        if id.n() % 100 == 0 {
            info!("Initializing entity {}", id);
        }
        if id.n() == max {
            info!("Initializing the last entity of type {:?}: {}", ty, id);
        }
        id
    })
}

async fn get_latest_entity_id(ty: EntityType) -> EntityId {
    let client = create_client();

    let url = format!("https://www.wikidata.org/w/api.php?action=query&format=json&list=recentchanges&rcnamespace={}&rctype=new&rclimit=1", ty.namespace().n());

    let unser = get_json::<QueryResponse>(&client, url)
        .await
        .expect("Request failed")
        .expect("Unexpected API response");

    let id = unser.extract_last_id(ty);
    info!("Got latest ID '{}'", id);
    id
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

impl QueryResponse {
    fn extract_last_id(self, ty: EntityType) -> EntityId {
        let title = &self
            .query
            .recentchanges
            .get(0)
            .expect("No changes present")
            .title;

        let id = ty.parse_from_title(title).expect("Failed to parse ID");
        id
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::collections::{BTreeSet, VecDeque};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Mutex;

    use futures::future::ready;
    use futures::StreamExt;
    use futures::*;
    use hyper::{Body, Client, Request};

    #[actix_rt::test]
    #[test]
    async fn test_init_stream_order() {
        let ty = EntityType::Property;
        let id = ty.id(3038);

        let client = GetEntityClient::default();

        let stream = init_inner(client, ty, Some(id), id, None).await;

        let mut messages = stream.collect::<VecDeque<_>>().await;

        assert_eq!(messages.len(), 3);
        let start = messages.pop_front().unwrap();
        let finished = messages.pop_back().unwrap();
        let update = messages.pop_front().unwrap();

        match start {
            Initialization::Start(_) => {}
            _ => panic!("Not Start"),
        }
        match finished {
            Initialization::Finished => {}
            _ => panic!("Not Finished"),
        }
        match update {
            Initialization::UpdateEntity(entity) => assert_eq!(entity.id, id),
            _ => panic!("Not Finished"),
        }
    }

    //    #[actix_rt::test]
    //    #[test]
    #[allow(dead_code)]
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

        let stream = body1.map_err(|_e| std::io::Error::from(std::io::ErrorKind::Other));

        use async_compression::stream::BzDecoder;
        let stream = BzDecoder::new(stream);

        use futures_codec::{FramedRead, LinesCodec};
        let inner = stream.into_async_read();
        let stream = FramedRead::new(inner, LinesCodec {});

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
