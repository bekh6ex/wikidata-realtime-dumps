use core::task::{Context, Poll};
use std::pin::Pin;
use std::time::Duration;

use actix::prelude::Stream;
use actix_rt::time::{sleep, Sleep};
use futures::future::{ready, Ready};
use futures::stream::once;
use futures::StreamExt;
use futures_util::stream::*;
use log::*;
use pin_utils::{unsafe_pinned, unsafe_unpinned};
use sse_codec::Event;

type SleepHere<X> = Map<Once<Sleep>, fn(()) -> Option<X>>;
type Real<X> = Map<Once<Ready<X>>, fn(X) -> Option<X>>;
type Chained<X> = Chain<SleepHere<X>, Real<X>>;
type StreamOfStream<X> = FilterMap<Chained<X>, Ready<Option<X>>, fn(Option<X>) -> Ready<Option<X>>>;
type WrapStreamResult<X> = Flatten<StreamOfStream<X>>;

//    type WrapStreamResultBoxed<X: Stream> = Box<dyn Stream<Item = X::Item> + Sync + Send>;

pub struct ContinuousStream<St: Stream + 'static, Cr> {
    stream: WrapStreamResult<St>,
    creator: Cr,
    last_event_id: Option<String>,
    retry_interval_ms: u64,
}

// TODO: Figure this out. This Unpin might be unsafe.
impl<St: Stream + 'static, Cr> Unpin for ContinuousStream<St, Cr> {}

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

    fn wrap_stream<St1: Stream + 'static>(stream: St1, retry: Duration) -> WrapStreamResult<St1> {
        let sleep: SleepHere<St1> = once(sleep(retry)).map(none as fn(()) -> Option<St1>);

        let real = once(ready::<St1>(stream)).map(some as fn(St1) -> Option<St1>);

        let chain = sleep.chain(real);

        let filtered: StreamOfStream<St1> = chain.filter_map(ready::<Option<St1>>);

        filtered.flatten()
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
                if let Event::Message {
                    id: Some(ref id), ..
                } = event
                {
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
