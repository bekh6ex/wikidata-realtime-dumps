use std::cmp::Ordering;
use std::pin::Pin;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::future::ready;
use futures::stream::once;
use futures::{Future, FutureExt, Stream, StreamExt};
use log::*;
use serde::{Deserialize, Serialize};
use sse_codec::Event;

use sorted_stream::BufferedSortedStream;
use sorted_stream::Sequential;

use crate::archive::UpdateCommand;
use crate::events::event_stream::response_to_stream;
use crate::get_entity::GetEntityClient;

use super::prelude::*;

use isahc::*;
use futures_backoff::Strategy;
use isahc::Error;


mod event_stream;

const WIKIDATA: &str = "wikidatawiki";

///
/// Event rate analysis results:
///   Period:
///     from: 2020-01-12T11:28:53Z (1578828533)
///     to: 2020-01-12T16:24:02Z (1578846242)
///
///     duration: 17709 seconds
///
///   Size: 537M (563159040 bytes)
///   Data rate: 31 kB/s (109 MB/h; 2,6 GB/day)
///
///   Number of events: 464473
///   Event rate: 26 events/s
///
///   Number of Wikidata events: 149360
///      Items: 149187
///      Properties: 81
///      Lexeme: 92
///   Wikidata event rate: 8,5 events/s
///
///   Commands to analyze:
///       Total number of events: `cat event-stream | grep -F 'data: {' | wc -l`
///       Number of Wikidata events:
///       Items: `cat event-stream | grep -F '"wiki":"wikidatawiki"' | grep -F '"uri":"https://www.wikidata.org/wiki/Q' | grep -F '"namespace":0' | wc -l`
///       Properties: `cat event-stream | grep -F '"wiki":"wikidatawiki"' | grep -F '"uri":"https://www.wikidata.org/wiki/Property:P' | grep -F '"namespace":120' | wc -l`
///       Lexemes: `cat event-stream | grep -F '"wiki":"wikidatawiki"' | grep -F '"uri":"https://www.wikidata.org/wiki/Lexeme:L' | grep -F '"namespace":146' | wc -l`
///

// data: {"$schema":"/mediawiki/recentchange/1.0.0","meta":{"uri":"https://www.wikidata.org/wiki/Q26212910","request_id":"XjbM6gpAED4AAB3XDaAAAAAJ","id":"c36d0a88-6bee-4d40-b5c9-18e35d2e0678","dt":"2020-02-02T13:21:46Z","domain":"www.wikidata.org","stream":"mediawiki.recentchange","topic":"eqiad.mediawiki.recentchange","partition":0,"offset":2146136449},"id":1146589785,"type":"edit","namespace":0,"title":"Q26212910","comment":"/* wbmergeitems-to:0||Q1216649 */ [[MediaWiki:Gadget-Merge.js|merge.js]]  duplicate entry","timestamp":1580649706,"user":"Timár Péter","bot":false,"minor":false,"patrolled":false,"length":{"old":4197,"new":159},"revision":{"old":824457921,"new":1108532037},"server_url":"https://www.wikidata.org","server_name":"www.wikidata.org","server_script_path":"/w","wiki":"wikidatawiki","parsedcomment":"‎<span dir=\"auto\"><span class=\"autocomment\">Elem összevonása ide: Q1216649: </span></span> <a href=\"/wiki/MediaWiki:Gadget-Merge.js\" title=\"MediaWiki:Gadget-Merge.js\">merge.js</a>  duplicate entry"}
//
//data: {"$schema":"/mediawiki/recentchange/1.0.0","meta":{"uri":"https://www.wikidata.org/wiki/Q1216649","request_id":"XjbM6gpAED4AAB3XDaAAAAAJ","id":"92118634-a7b9-4509-9a9c-765cce0d1d89","dt":"2020-02-02T13:21:46Z","domain":"www.wikidata.org","stream":"mediawiki.recentchange","topic":"eqiad.mediawiki.recentchange","partition":0,"offset":2146136451},"id":1146589786,"type":"edit","namespace":0,"title":"Q1216649","comment":"/* wbmergeitems-from:0||Q26212910 */ [[MediaWiki:Gadget-Merge.js|merge.js]]  duplicate entry","timestamp":1580649706,"user":"Timár Péter","bot":false,"minor":false,"patrolled":false,"length":{"old":1771,"new":5328},"revision":{"old":812858579,"new":1108532043},"server_url":"https://www.wikidata.org","server_name":"www.wikidata.org","server_script_path":"/w","wiki":"wikidatawiki","parsedcomment":"‎<span dir=\"auto\"><span class=\"autocomment\">Elem összevonása innen: Q26212910: </span></span> <a href=\"/wiki/MediaWiki:Gadget-Merge.js\" title=\"MediaWiki:Gadget-Merge.js\">merge.js</a>  duplicate entry"}
//
//data: {"$schema":"/mediawiki/recentchange/1.0.0","meta":{"uri":"https://www.wikidata.org/wiki/Q26212910","request_id":"XjbM6gpAED4AAB3XDaAAAAAJ","id":"89a15a25-e60c-4b91-b985-05d232d23613","dt":"2020-02-02T13:21:46Z","domain":"www.wikidata.org","stream":"mediawiki.recentchange","topic":"eqiad.mediawiki.recentchange","partition":0,"offset":2146136453},"id":1146589787,"type":"edit","namespace":0,"title":"Q26212910","comment":"/* wbcreateredirect:0||Q26212910|Q1216649 */","timestamp":1580649706,"user":"Timár Péter","bot":false,"minor":false,"patrolled":false,"length":{"old":159,"new":65},"revision":{"old":1108532037,"new":1108532044},"server_url":"https://www.wikidata.org","server_name":"www.wikidata.org","server_script_path":"/w","wiki":"wikidatawiki","parsedcomment":"‎<span dir=\"auto\"><span class=\"autocomment\">Átirányítás ide: Q1216649</span></span>"}
pub async fn get_current_event_id() -> EventId {
    // TODO Should be not current but the one before that.

    get_top_event_id(None).await
}



pub fn create_client() -> HttpClient {
    use isahc::config::{RedirectPolicy, VersionNegotiation};
    use isahc::prelude::*;
    use std::time::Duration;

    let client = HttpClient::builder()
        .timeout(Duration::from_secs(30))
        .connect_timeout(Duration::from_secs(10))
        .redirect_policy(RedirectPolicy::None)
        .version_negotiation(VersionNegotiation::http2())
        .build()
        .unwrap();

    client
}

async fn open_new_sse_stream(event_id: Option<String>) -> impl Stream<Item = Event> {

    let send_request = || {
        let event_id = event_id.clone();
        async move {
            let client = create_client();

            let mut req = Request::builder()
                .method("GET")
                .uri("https://stream.wikimedia.org/v2/stream/recentchange");
            if event_id.is_some() {
                req = req.header("last-event-id", event_id.clone().unwrap());
            }
            let req = req.body(AsyncBody::empty()).unwrap();

            trace!("Sending request: {:?}", req);

            client.send_async(req).await.map_err(|e| {
                info!("Error during SSE stream opening: `{}`", e);
                e
            })
        }
    };

    let response = Strategy::exponential(Duration::from_millis(5))
        .with_jitter(true)
        .with_max_delay(Duration::from_secs(30))
        .with_max_retries(50)
        .retry(send_request)
        .await
        .expect(&format!("Failed to open SSE stream"));

    response_to_stream(response, event_id)
}

pub async fn update_command_stream(
    client: GetEntityClient,
    ty: EntityType,
    event_id: EventId,
) -> impl Stream<Item = UpdateCommand> {

    let event_id = event_id.rewind(Duration::from_secs(20)); // Due to the current VolumeKeeper batching implementation

    get_wikidata_event_stream(Some(event_id), ty)
        .await
        .filter_map(move |event: ProperEvent| {
            let client = client.clone();
            async move {
                event.into_command(client, ty)
                // TODO Add logging
            }
        })
        .buffered(200)
        .enumerate()
        .map(move |(i, e): (usize, UpdateCommand)| {
            if i % 10usize == 0 {
                let timestamp = e.event_id().timestamp_ms();
                let current_timestamp = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards")
                    .as_millis() as u64;
                let lag = Duration::from_millis(current_timestamp - timestamp);

                info!(
                    "Walked {} events for {:?}. Current event timestamp = {}, lag = {:?}",
                    i + 1,
                    ty,
                    e.event_id().timestamp_ms(),
                    lag
                );
            }
            e
        })
}

async fn id_stream(from: Option<EventId>) -> impl Stream<Item = EventId> {
    open_new_sse_stream_with_retries(from.map(|id| id.to_json_string()))
        .await
        .filter_map(|e: Event| {
            let option: Option<EventId> = match e {
                Event::Message { id, .. } => id.map(EventId::new),
                _ => None,
            };
            ready(option)
        })
}

async fn get_top_event_id(from: Option<EventId>) -> EventId {
    let id_stream = id_stream(from).await;
    let (id, _) = id_stream.into_future().await;

    id.unwrap()
}

async fn open_new_sse_stream_with_retries(event_id: Option<String>) -> impl Stream<Item = Event> {
    let stream = continuous_stream::ContinuousStream::new(
        move |id| {
            let id = id.or_else(|| event_id.clone());

            // Rewind EventId couple seconds back. Event stream has a bit random order of events,
            // so to get all of them we should go back a little.
            let id = id.map(|i| {
                EventId::new(i)
                    .rewind(Duration::from_secs(2))
                    .to_json_string()
            });
            once(open_new_sse_stream(id)).flatten()
        },
        10_000,
    );

    stream
}

async fn get_wikidata_event_stream(
    event_id: Option<EventId>,
    ty: EntityType,
) -> impl Stream<Item = ProperEvent> {
    let stream = open_new_sse_stream_with_retries(event_id.map(|i| i.to_json_string())).await;

    let stream = stream.map(|ch: Event| {
        // let s = ch.as_slice();

        match ch {
            Event::Message { id, data, .. } => SortableEvent {
                id: EventId::new(id.unwrap().clone() ),
                data: data.clone(),
            },
            _ => panic!(),
        }
    });
    let sorted_stream = BufferedSortedStream::new(stream.fuse(), 30);

    sorted_stream.filter_map(move |event: SortableEvent| {
        let hint: EventDataHint = serde_json::from_str(&event.data).unwrap();
        let result = if hint.wiki == WIKIDATA && hint.namespace == ty.namespace().n() {
            let data: EventData = serde_json::from_str(&event.data).unwrap();
            Some(ProperEvent {
                id: event.id.clone(),
                data,
            })
        } else {
            None
        };

        ready(result)
    })
}

struct SortableEvent {
    id: EventId,
    data: String,
}

#[derive(Debug, Deserialize)]
struct EventDataHint {
    wiki: String,
    namespace: i64, // Has to be signed because `-1` is namespace for Special pages
}

struct ProperEvent {
    id: EventId,
    data: EventData,
}

impl ProperEvent {
    fn into_command(
        self,
        client: GetEntityClient,
        ty: EntityType,
    ) -> Option<Pin<Box<dyn Future<Output = UpdateCommand>>>> {
        self.data.to_command(client, ty, self.id)
    }
}

impl Sequential for SortableEvent {
    type Marker = EventId;

    fn seq_marker(&self) -> Self::Marker {
        self.id.clone()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventId {
    parts: Vec<SerializedEventIdPart>,
}

impl EventId {
    pub fn new(inner: String) -> Self {
        let parts = serde_json::from_str::<Vec<SerializedEventIdPart>>(&inner)
            .unwrap_or_else(|e| panic!("Unexpected EventId format: '{}. {}'", inner, e));
        EventId { parts }
    }

    #[cfg(test)]
    pub(crate) fn test(timestamp: u64) -> Self {
        EventId {
           parts: vec![
               SerializedEventIdPart {
                   topic: "".to_owned(),
                   partition: 0,
                   timestamp: Some(timestamp),
                   offset: None,
               },
               SerializedEventIdPart {
                   topic: "".to_owned(),
                   partition: 0,
                   timestamp: None,
                   offset: Some(-1),
               }
           ]
        }
    }

    fn to_json_string(&self) -> String {
        serde_json::to_string(&self.parts).unwrap()
    }

    pub fn timestamp_ms(&self) -> u64 {
        let part = &self.parts[self.timestamp_part_pos()];
        part.timestamp.unwrap()
    }

    fn timestamp_part_pos(&self) -> usize {
        self.parts
            .iter()
            .position(|p| p.timestamp.is_some())
            .unwrap_or_else(|| {
                panic!(
                    "EventId serialization does not contain timestamp: '{:?}'",
                    self.parts
                )
            })
    }

    fn rewind(&self, dur: Duration) -> Self {
        let ms = dur.as_millis() as u64;
        let new_timestamp = self.timestamp_ms() - ms;
        let mut new = self.clone();
        let pos = new.timestamp_part_pos();
        new.parts[pos].timestamp = Some(new_timestamp);
        new
    }
}

impl PartialOrd for EventId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.timestamp_ms().partial_cmp(&other.timestamp_ms())
    }
}

impl Ord for EventId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.timestamp_ms().cmp(&other.timestamp_ms())
    }
}

impl PartialEq for EventId {
    fn eq(&self, other: &Self) -> bool {
        self.timestamp_ms().eq(&other.timestamp_ms())
    }
}
impl Eq for EventId {}

///
/// [
///  {
///    "topic": "codfw.mediawiki.recentchange",
///    "partition": 0,
///    "offset": -1
///  },
///  {
///    "topic": "eqiad.mediawiki.recentchange",
///    "partition": 0,
///    "timestamp": 1579465255001
///  }
///]
#[derive(Serialize, Deserialize, Debug, Clone)]
struct SerializedEventIdPart {
    topic: String,
    partition: i8,
    // TODO: Test it. It is crucial. Otherwise header is not processed correctly
    #[serde(skip_serializing_if = "Option::is_none")]
    timestamp: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    offset: Option<i8>,
}

mod continuous_stream;

#[derive(Deserialize, Debug, PartialEq)]
#[serde(tag = "type")]
enum EventData {
    #[serde(rename(deserialize = "edit"))]
    Edit {
        title: String,
        revision: RevisionData,
        comment: String,
    },
    #[serde(rename(deserialize = "new"))]
    New {
        title: String,
        revision: RevisionData,
    },
    #[serde(rename(deserialize = "log"))]
    Log { title: String },
}

impl EventData {
    fn to_command(
        &self,
        client: GetEntityClient,
        ty: EntityType,
        event_id: EventId,
    ) -> Option<Pin<Box<dyn Future<Output = UpdateCommand>>>> {
        match self {
            EventData::Edit {
                title,
                revision,
                comment,
            } => {
                // TODO Handle delete, which is edit with "comment":"/* wbcreateredirect:0||Q26212910|Q1216649 */"
                let revision_id = RevisionId(revision.new);
                let id = ty.parse_from_title(title).unwrap();
                let seems_to_be_a_redirect = comment.contains("wbcreateredirect:");

                let serialized_entity_fut = client.get_entity(id, Some(revision_id));
                let command_fut =
                    serialized_entity_fut.map(move |o: Option<SerializedEntity>| match o {
                        None if seems_to_be_a_redirect => UpdateCommand::DeleteCommand {
                            event_id,
                            id,
                            revision: revision_id,
                        },
                        Some(serialized_entity) => UpdateCommand::UpdateCommand {
                            event_id,
                            entity: serialized_entity,
                        },
                        None => {
                            error!("Not found: {:?} {:?}", id, revision_id);
                            // Strange, but seems to be the case for
                            // EntityId { ty: Item, id: 16443659 } RevisionId(1101501328)
                            UpdateCommand::DeleteCommand {
                                event_id,
                                id,
                                revision: revision_id,
                            }
                        }
                    });

                Some(Box::pin(command_fut))
            }
            EventData::New {
                title, revision, ..
            } => {
                let revision_id = RevisionId(revision.new);
                let id = ty.parse_from_title(title).unwrap();

                let serialized_entity_fut = client.get_entity(id, Some(revision_id));
                let command_fut = serialized_entity_fut.map(move |o: Option<SerializedEntity>| {
                    let serialized_entity = o.unwrap_or_else(|| {
                        error!(
                            "Should always present. Not found: {:?} {:?}",
                            id, revision_id
                        );
                        // TODO: Fix this hack. Should be none
                        let data = format!(r#"{{"id":"{}", "lastrevid":{}}}"#, id, revision_id.0);
                        SerializedEntity{
                            id,
                            revision: revision_id,
                            data,
                        }
                    });
                    UpdateCommand::UpdateCommand {
                        event_id,
                        entity: serialized_entity,
                    }
                });

                Some(Box::pin(command_fut))
            }
            EventData::Log { .. } => None,
        }
    }
}

#[derive(Deserialize, Debug, PartialEq)]
struct RevisionData {
    new: u64,
}

#[derive(Deserialize, Debug)]
struct WikidataResponse {
    entities: serde_json::Map<String, serde_json::Value>,
}

#[cfg(test)]
mod test {
    use std::time::Duration;

    use actix_rt;
    use futures::StreamExt;
    use serde_json;

    use crate::events::{get_wikidata_event_stream, EventId};

    use super::get_top_event_id;
    use super::*;

    //    #[actix_rt::test]
    #[allow(dead_code)]
    async fn always_get_the_same_event_by_same_id() {
        // Does not work. No guarantee that the event data will be the same every time.
        let initial_id = get_top_event_id(None).await.rewind(Duration::from_secs(1));

        println!("Starting {}", initial_id.to_json_string());
        let data1 = get_top_event_data(initial_id.clone()).await;
        let data2 = get_top_event_data(initial_id.clone()).await;

        assert_eq!(data1, data2);
    }

    async fn get_top_event_data(from: EventId) -> EventData {
        let stream = get_wikidata_event_stream(Some(from), EntityType::Item).await;
        let event = stream.take(1).collect::<Vec<_>>().await.remove(0);
        event.data
    }

    #[actix_rt::test]
    async fn smoketest_get_proper_event_stream() {
        let event_id = EventId::new(
            r#"[{
                        "topic": "codfw.mediawiki.recentchange",
                        "partition": 0,
                        "offset": -1
                      },
                      {
                        "topic": "eqiad.mediawiki.recentchange",
                        "partition": 0,
                        "timestamp": 1579465255001
                      }]"#
            .to_owned(),
        );

        let stream = get_wikidata_event_stream(Some(event_id), EntityType::Item).await;
        let _event = stream.take(1).collect::<Vec<_>>().await.remove(0);
    }

    #[test]
    fn test_event_data_edit_deserialization() {
        let event_data = r#"{"$schema":"/mediawiki/recentchange/1.0.0","meta":{"uri":"https://www.wikidata.org/wiki/Q26212910","request_id":"XjbM6gpAED4AAB3XDaAAAAAJ","id":"89a15a25-e60c-4b91-b985-05d232d23613","dt":"2020-02-02T13:21:46Z","domain":"www.wikidata.org","stream":"mediawiki.recentchange","topic":"eqiad.mediawiki.recentchange","partition":0,"offset":2146136453},"id":1146589787,"type":"edit","namespace":0,"title":"Q26212910","comment":"/* wbcreateredirect:0||Q26212910|Q1216649 */","timestamp":1580649706,"user":"Timár Péter","bot":false,"minor":false,"patrolled":false,"length":{"old":159,"new":65},"revision":{"old":1108532037,"new":1108532044},"server_url":"https://www.wikidata.org","server_name":"www.wikidata.org","server_script_path":"/w","wiki":"wikidatawiki","parsedcomment":"‎<span dir=\"auto\"><span class=\"autocomment\">Átirányítás ide: Q1216649</span></span>"}"#;

        let result = serde_json::from_str::<EventData>(event_data).unwrap();

        match result {
            EventData::Edit {
                title, revision, ..
            } => {
                assert_eq!(title, "Q26212910");
                assert_eq!(revision.new, 1108532044);
            }
            ed => panic!("Unexpected type: {:?}", ed),
        }
    }

    #[test]
    fn test_event_data_log_deserialization() {
        let event_data = r#"{"$schema":"/mediawiki/recentchange/1.0.0","meta":{"uri":"https://www.wikidata.org/wiki/Q881745","request_id":"XjbJEwpAAEgAAIDSKEEAAAEX","id":"1b9134f7-8448-41b7-ac30-6be5acdadcc9","dt":"2020-02-02T13:05:24Z","domain":"www.wikidata.org","stream":"mediawiki.recentchange","topic":"eqiad.mediawiki.recentchange","partition":0,"offset":2146109409},"type":"log","namespace":0,"title":"Q881745","comment":"","timestamp":1580648724,"user":"2A01:E0A:1C8:9A00:F0C3:F1E2:400B:11B2","bot":false,"log_id":0,"log_type":"abusefilter","log_action":"hit","log_params":{"action":"edit","filter":79,"actions":"tag","log":11639581},"log_action_comment":"2A01:E0A:1C8:9A00:F0C3:F1E2:400B:11B2 triggered [[Special:AbuseFilter/79|filter 79]], performing the action \"edit\" on [[Q881745]]. Actions taken: Tag ([[Special:AbuseLog/11639581|details]])","server_url":"https://www.wikidata.org","server_name":"www.wikidata.org","server_script_path":"/w","wiki":"wikidatawiki","parsedcomment":""}"#;

        let result = serde_json::from_str::<EventData>(event_data).unwrap();

        match result {
            EventData::Log { title } => {
                assert_eq!(title, "Q881745");
            }
            ed => panic!("Unexpected type: {:?}", ed),
        }
    }

    #[test]
    fn test_event_data_new_deserialization() {
        let event_data = r#"{"$schema":"/mediawiki/recentchange/1.0.0","meta":{"uri":"https://www.wikidata.org/wiki/Q84233913","request_id":"XjbJAQpAAEoAAA5tji4AAADT","id":"985211a0-516d-49cb-a40a-595820485d73","dt":"2020-02-02T13:05:06Z","domain":"www.wikidata.org","stream":"mediawiki.recentchange","topic":"eqiad.mediawiki.recentchange","partition":0,"offset":2146108941},"id":1146579094,"type":"new","namespace":0,"title":"Q84233913","comment":"/* wbeditentity-create-item:0| */","timestamp":1580648706,"user":"LargeDatasetBot","bot":true,"minor":false,"patrolled":true,"length":{"new":18711},"revision":{"new":1108521371},"server_url":"https://www.wikidata.org","server_name":"www.wikidata.org","server_script_path":"/w","wiki":"wikidatawiki","parsedcomment":"‎<span dir=\"auto\"><span class=\"autocomment\">Created a new Item</span></span>"}"#;

        let result = serde_json::from_str::<EventData>(event_data).unwrap();

        match result {
            EventData::New { title, revision } => {
                assert_eq!(title, "Q84233913");
                assert_eq!(revision.new, 1108521371);
            }
            ed => panic!("Unexpected type: {:?}", ed),
        }
    }
}
