use std::time::Duration;

use futures::future::ready;
use futures::stream::once;
use futures::{Stream, StreamExt};
use log::*;
use serde::{Deserialize, Serialize};
use sse_codec::Event;

use crate::archive::UpdateCommand;
use crate::get_entity::GetEntityClient;

use super::prelude::*;
use crate::events::event_stream::response_to_stream;
use sorted_stream::BufferedSortedStream;
use sorted_stream::Sequential;
use std::cmp::Ordering;

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

pub async fn get_current_event_id() -> EventId {
    // TODO Should be not current but the one before that.

    get_top_event_id(None).await
}

async fn open_new_sse_stream(event_id: Option<String>) -> impl Stream<Item = Event> {
    use hyper::{Body, Client, Request};

    let client = Client::builder().build::<_, hyper::Body>(hyper_rustls::HttpsConnector::new());

    let mut req = Request::builder()
        .method("GET")
        .uri("https://stream.wikimedia.org/v2/stream/recentchange");
    if event_id.is_some() {
        req = req.header("last-event-id", event_id.clone().unwrap());
    }
    let req = req.body(Body::empty()).unwrap();

    trace!("Sending request: {:?}", req);

    let resp = client.request(req).await.unwrap();

    response_to_stream(resp, event_id)
}

pub async fn update_command_stream(
    ty: EntityType,
    event_id: EventId,
) -> impl Stream<Item = UpdateCommand> {
    let client_for_entities = GetEntityClient::default();

    get_proper_event_stream(Some(event_id))
        .await
        .filter(move |e: &ProperEvent| {
            ready(e.data.wiki == WIKIDATA && e.data.namespace == ty.namespace().n())
        })
        .inspect(|e| {
            let known: Vec<&str> = vec!["edit"];

            if !known.contains(&&e.data.event_type[..]) {
                info!("Unknown event type '{}'", e.data.event_type);
            }
        })
        .filter_map(move |event: ProperEvent| {
            let ProperEvent { id: event_id, data } = event;
            let client = client_for_entities.clone();
            let EventData { title, .. } = data;
            let id = ty.parse_from_title(&title).unwrap();
            async move {
                // TODO: Handle new and deleted
                // TODO: Figure out which events have which revisions
                let entity = client.get_entity(id).await?;
                Some(UpdateCommand { event_id, entity })
            }
        })
        .enumerate()
        .map(move |(i, e)| {
            if i % 10usize == 0 {
                info!("Walked {} events for {:?}", i + 1, ty);
            }
            e
        })
}

async fn id_stream(from: Option<EventId>) -> impl Stream<Item = EventId> {
    open_new_sse_stream(from.map(|id| id.to_json_string()))
        .await
        .filter_map(|e: Event| {
            let option: Option<EventId> = match e {
                Event::LastEventId { id } => Some(EventId::new(id)),
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

async fn get_proper_event_stream(event_id: Option<EventId>) -> impl Stream<Item = ProperEvent> {
    let stream = continuous_stream::ContinuousStream::new(
        move |id| {
            let id = id.or_else(|| event_id.clone().map(|i| i.to_json_string()));

            // Rewind EventId couple seconds back. Event stream has a bit random order of events,
            // so to get all of them we should go back a little.
            let id = id.map(|i| {
                EventId::new(i)
                    .rewind(Duration::from_secs(2))
                    .to_json_string()
            });
            once(open_new_sse_stream(id)).flatten()
        },
        1000,
    );

    let stream = stream.chunks(2).map(|ch: Vec<Event>| {
        let s = ch.as_slice();

        match s {
            [Event::LastEventId { id }, Event::Message { data, .. }] => {
                let data = serde_json::from_str(&data).unwrap();
                ProperEvent {
                    id: EventId::new(id.clone()),
                    data,
                }
            }
            _ => panic!(),
        }
    });

    BufferedSortedStream::new(stream.fuse(), 100)
}

struct ProperEvent {
    id: EventId,
    data: EventData,
}

impl Sequential for ProperEvent {
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

    fn to_json_string(&self) -> String {
        serde_json::to_string(&self.parts).unwrap()
    }

    fn timestamp_ms(&self) -> u64 {
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
    timestamp: Option<u64>,
    offset: Option<i8>,
}
mod continuous_stream;

#[derive(Deserialize, Debug, PartialEq)]
struct EventData {
    wiki: String,
    title: String,
    #[serde(rename(deserialize = "type"))]
    event_type: String,
    // Has to be signed because `-1` is namespace for Special pages
    namespace: i64,
    revision: Option<RevisionData>,
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
    use actix_rt;
    use futures::StreamExt;

    use crate::events::{get_proper_event_stream, EventData, EventId};

    use super::get_top_event_id;
    use std::time::Duration;

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
        let stream = get_proper_event_stream(Some(from)).await;
        let event = stream.take(1).collect::<Vec<_>>().await.remove(0);
        event.data
    }

    #[actix_rt::test]
    async fn smoketest_get_proper_event_stream() {
        let stream = get_proper_event_stream(None).await;
        let _event = stream.take(1).collect::<Vec<_>>().await.remove(0);
    }
}
