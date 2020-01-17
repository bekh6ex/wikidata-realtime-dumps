use std::io::{Error, ErrorKind};
use std::sync::Arc;
use std::time::Duration;

use actix::prelude::Stream;
use actix_web::client::{Client, ClientBuilder, Connector, PayloadError};
use futures::future::ready;
use futures::{StreamExt, TryStreamExt};
use log::*;
use serde::Deserialize;
use sse_codec::{decode_stream, Event};

use crate::actor::UpdateCommand;
use crate::get_entity::get_entity;

use super::prelude::*;

const WIKIDATA: &str = "wikidatawiki";

fn create_client() -> Client {
    ClientBuilder::new()
        .timeout(Duration::from_secs(30))
        .connector(Connector::new().timeout(Duration::from_secs(30)).finish())
        .finish()
}

async fn create_stream() -> impl Stream<Item = Event> {
    let client = Arc::new(create_client());

    let response = client
        .get("https://stream.wikimedia.org/v2/stream/recentchange")
        .header("User-Agent", "Actix-web")
        .timeout(Duration::from_secs(600))
        .send()
        .await
        .expect("response");

    info!("Event stream started");
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
                warn!("Error after decoding: {:?}", decoding_result)
            }
            ready(decoding_result.is_ok())
        })
        .map(|r| r.unwrap())
}

pub async fn get_update_stream(ty: EntityType) -> impl Stream<Item = UpdateCommand> {
    let client_for_entities = Arc::new(create_client());

    continuous_stream::ContinuousStream::new(
        |_| futures::stream::once(create_stream()).flatten(),
        1000,
    )
    .filter_map(|event| {
        async move {
            use serde_json::Result;

            match event {
                Event::Message { data, .. } => {
                    let res: Result<EventData> = serde_json::from_str(&data);
                    match res {
                        Ok(result) => Some(result),
                        Err(e) => {
                            error!("Error after deserialization {:?} data={}", e, data);
                            None
                        }
                    }
                }
                _ => None,
            }
        }
    })
    .filter(move |e| ready(e.wiki == WIKIDATA && e.namespace == ty.namespace().n()))
    .filter_map(move |event_data| {
        let client = client_for_entities.clone();
        let ty = ty;
        async move {
            let EventData { title, .. } = event_data;
            let id = ty.parse_from_title(&title).unwrap();

            let client = client;

            let entity_result = get_entity(client, id).await?;
            Some(entity_result.into())
        }
    })
}

mod continuous_stream {
    use core::task::{Context, Poll};
    use std::pin::Pin;
    use std::time::Duration;

    use actix::prelude::Stream;
    use actix_rt::time::{delay_for, Delay};
    use futures::future::{ready, Ready};
    use futures::stream::once;
    use futures::StreamExt;
    use futures_util;
    use futures_util::stream::*;
    use log::*;
    use pin_utils::{unsafe_pinned, unsafe_unpinned};
    use sse_codec::Event;

    type Sleep<X> = Map<Once<Delay>, fn(()) -> Option<X>>;
    type Real<X> = Map<Once<Ready<X>>, fn(X) -> Option<X>>;
    type Chained<X> = Chain<Sleep<X>, Real<X>>;
    type StreamOfStream<X> =
        FilterMap<Chained<X>, Ready<Option<X>>, fn(Option<X>) -> Ready<Option<X>>>;
    type WrapStreamResult<X> = Flatten<StreamOfStream<X>>;

    //    type WrapStreamResultBoxed<X: Stream> = Box<dyn Stream<Item = X::Item> + Sync + Send>;

    pub struct ContinuousStream<St: Stream + 'static, Cr> {
        stream: WrapStreamResult<St>,
        creator: Cr,
        last_event_id: Option<String>,
        retry_interval_ms: u64,
    }

    impl<S, Cr> ContinuousStream<S, Cr>
    where
        S: Stream<Item = Event>,
        Cr: FnMut(Option<String>) -> S,
    {
        unsafe_pinned!(stream: WrapStreamResult<S>);
        unsafe_pinned!(last_event_id: Option<String>);
        unsafe_pinned!(retry_interval_ms: u64);
        unsafe_unpinned!(creator: Cr);

        pub fn new(mut creator: Cr, retry: u64) -> Self {
            let last_event_id = None;
            let new_stream: S = creator(None);
            let duration = Duration::from_millis(retry);

            let wrapped = Self::wrap_stream(new_stream, duration);

            ContinuousStream {
                stream: wrapped,
                creator,
                last_event_id,
                retry_interval_ms: 0,
            }
        }

        fn wrap_stream<St1: Stream + 'static>(
            stream: St1,
            retry: Duration,
        ) -> WrapStreamResult<St1> {
            let sleep: Sleep<St1> = once(delay_for(retry)).map(none as fn(()) -> Option<St1>);

            let real = once(ready::<St1>(stream)).map(some as fn(St1) -> Option<St1>);

            let chain = sleep.chain(real);

            let filtered: StreamOfStream<St1> = chain.filter_map(ready::<Option<St1>>);

            let res = filtered.flatten();
            res
        }
    }

    impl<S, Cr> Stream for ContinuousStream<S, Cr>
    where
        S: Stream<Item = Event>,
        Cr: FnMut(Option<String>) -> S,
    {
        type Item = <S as Stream>::Item;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            match self.as_mut().stream().poll_next(cx) {
                Poll::Ready(Some(event)) => {
                    if let Event::LastEventId { ref id } = event {
                        // TODO Check what standard says about empty string
                        self.as_mut().last_event_id().set(Some(id.clone()))
                    }

                    if let Event::Retry { retry } = event {
                        self.as_mut().retry_interval_ms().set(retry)
                    }

                    Poll::Ready(Some(event))
                }
                Poll::Pending => Poll::Pending,
                Poll::Ready(None) => {
                    info!("Stream failed. Creating new.");

                    let last_event_id = self.as_mut().last_event_id.clone();
                    let new_stream = self.as_mut().creator()(last_event_id);
                    let retry = self.as_ref().retry_interval_ms;
                    let wrapped = Self::wrap_stream(new_stream, Duration::from_millis(retry));

                    self.as_mut().stream().set(wrapped);
                    self.as_mut().poll_next(cx)
                }
            }
        }
    }

    fn some<X>(s: X) -> Option<X> {
        Some(s)
    }

    fn none<X>(_: ()) -> Option<X> {
        None
    }
}

#[derive(Deserialize, Debug)]
struct EventData {
    wiki: String,
    title: String,
    #[serde(rename(deserialize = "type"))]
    event_type: String,
    // Has to be signed because `-1` is namespace for Special pages
    namespace: i64,
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
