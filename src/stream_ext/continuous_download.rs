use core::task::{Context, Poll};
use std::pin::Pin;
use std::time::Duration;

use actix::prelude::Stream;
use futures::future::{ready, Ready};
use futures::stream::once;
use futures::StreamExt;
use futures::*;
use futures_timer::Delay;
use futures_util;
use futures_util::stream::*;
use log::*;
use pin_utils::{unsafe_pinned, unsafe_unpinned};

use std::fmt::Debug;


type Sleep<X> = Map<Once<Delay>, fn(()) -> Option<X>>;
type Real<X> = Map<Once<Ready<X>>, fn(X) -> Option<X>>;
type Chained<X> = Chain<Sleep<X>, Real<X>>;
type StreamOfStream<X> = FilterMap<Chained<X>, Ready<Option<X>>, fn(Option<X>) -> Ready<Option<X>>>;
type DelayedStreamResult<X> = Flatten<StreamOfStream<X>>;

pub struct ContinuousDownloadStream<St: Stream + 'static, Cr> {
    stream: DelayedStreamResult<St>,
    creator: Cr,
    position: usize,
    retry_interval_ms: u64,
}

impl<S, Cr, R, E> ContinuousDownloadStream<S, Cr>
where
    S: Stream<Item = Result<R, E>>,
    R: AsRef<[u8]>,
    Cr: FnMut(usize) -> S,
    E: Debug,
{
    unsafe_pinned!(stream: DelayedStreamResult<S>);
    unsafe_pinned!(position: usize);
    unsafe_pinned!(retry_interval_ms: u64);
    unsafe_unpinned!(creator: Cr);

    pub fn new(mut creator: Cr, retry: u64) -> Self {
        let position = 0;
        let new_stream: S = creator(position);
        let duration = Duration::from_millis(retry);

        let wrapped = Self::delay_stream(new_stream, duration);

        ContinuousDownloadStream {
            stream: wrapped,
            creator,
            position,
            retry_interval_ms: retry,
        }
    }

    fn delay_stream<St1: Stream + 'static>(
        stream: St1,
        retry: Duration,
    ) -> DelayedStreamResult<St1> {
        let sleep: Sleep<St1> = once(Delay::new(retry)).map(none as fn(()) -> Option<St1>);

        let real = once(ready::<St1>(stream)).map(some as fn(St1) -> Option<St1>);

        let chain = sleep.chain(real);

        let filtered: StreamOfStream<St1> = chain.filter_map(ready::<Option<St1>>);

        filtered.flatten()
    }
}

impl<S, Cr, R, E> Stream for ContinuousDownloadStream<S, Cr>
where
    S: Stream<Item = Result<R, E>>,
    R: AsRef<[u8]>,
    Cr: FnMut(usize) -> S,
    E: Debug,
{
    type Item = Result<R, E>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match ready!(self.as_mut().stream().poll_next(cx)) {
                Some(Ok(bytes)) => {
                    let old_pos = self.position;
                    self.as_mut().position().set(old_pos + bytes.as_ref().len());

                    return Poll::Ready(Some(Ok(bytes)));
                }
                Some(Err(err)) => {
                    info!("Error happened: {:?}", err);
                    let position = self.position;
                    let new_stream = self.as_mut().creator()(position);
                    let retry = self.as_ref().retry_interval_ms;
                    let wrapped = Self::delay_stream(new_stream, Duration::from_millis(retry));

                    self.as_mut().stream().set(wrapped);
                    continue;
                }
                None => {
                    info!("Stream ended.");
                    return Poll::Ready(None);
                }
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

#[cfg(test)]
mod test {
    use super::*;
    
    
    

    #[async_std::test]
    async fn should_sort_two_out_of_order_with_buffer_1() {
        let stream = ContinuousDownloadStream::new(
            |offset| {
                if offset == 0 {
                    iter(vec![Ok("1"), Err("err1")])
                } else if offset == 1 {
                    iter(vec![Ok("2"), Err("err2")])
                } else if offset == 2 {
                    iter(vec![Ok("3"), Err("err2")])
                } else if offset == 3 {
                    iter(vec![Ok("4")])
                } else {
                    panic!("Unexpected offset: {}", offset)
                }
            },
            0,
        );

        let results = stream.collect::<Vec<_>>().await;

        assert_eq!(results, vec![Ok("1"), Ok("2"), Ok("3"), Ok("4")]);

        //        assert_stream_next!(stream, Ok("1"));
        //        assert_stream_next!(stream, Ok("2"));
        //        assert_stream_pending!(stream);
        //        assert_stream_next!(stream, Ok("3"));
        ////        assert_stream_pending!(stream);
        ////        assert_stream_pending!(stream);
        //        assert_stream_done!(stream);
    }
}
