use std::fmt::Debug;
use std::pin::Pin;

use futures::task::{Context, Poll};
use futures::Stream;
use std::collections::BTreeMap;
use pin_utils::{unsafe_pinned, unsafe_unpinned};
use futures_test::futures_core_reexport::FusedStream;

pub struct BufferedSortedStream<I, St>
where
    I: WithSequentialId,
{
    buffer: BTreeMap<I::Id, I>,
    stream: St,
    max_buffer_size: usize,
}

pub trait WithSequentialId {
    type Id: Ord + Clone;
    fn id(&self) -> Self::Id;
}

impl<I, St> BufferedSortedStream<I, St>
where
    I: WithSequentialId,
    St: FusedStream<Item = I>,
{
    unsafe_pinned!(stream: St);
    // TODO: Is it safe? O_o
    unsafe_unpinned!(buffer: BTreeMap<I::Id, I>);

    fn new(stream: St, max_buffer_size: usize) -> Self {
        BufferedSortedStream {
            stream,
            max_buffer_size,
            buffer: BTreeMap::new()
        }
    }
}

impl<I, St> Stream for BufferedSortedStream<I, St>
where
    I: WithSequentialId,
    St: FusedStream<Item = I>,
{
    type Item = I;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let new_item: Poll<Option<St::Item>> = self.as_mut().stream().poll_next(cx);
            match new_item {
                Poll::Ready(Some(item)) => {
                    let max_buf_size = self.max_buffer_size;
                    let buffer = self.as_mut().buffer();
                    let id = item.id().clone();
                    buffer.insert(item.id().clone(), item);
                    if buffer.len() > max_buf_size {
                        return Poll::Ready(Some(buffer.remove(&id).unwrap()))
                    } else {
                        continue;
                    }
                },
                Poll::Ready(None) => {
                    if self.buffer.len() > 0 {
                        let id =  {
                            let this = self.as_ref();
                            let (id, _) = this.buffer.iter().next().unwrap();
                            id.clone()
                        };

                        let mut buffer = self.as_mut().buffer();

                        return Poll::Ready(Some(buffer.remove(&id).unwrap()))
                    } else {
                        return Poll::Ready(None)
                    }
                },
                Poll::Pending => {
                    return Poll::Pending
                },
            }
        }

    }
}


#[cfg(test)]
mod test {
    use super::*;
    use futures::*;
    use futures::stream::*;
    use futures_test::*;

    #[test]
    fn empty_stream_should_be_done() {
        let mut stream = new(vec![], 0);

        assert_stream_done!(stream);
    }

    #[test]
    fn empty_stream_should_not_panic() {
        let mut stream = new(vec![], 0);

        assert_stream_done!(stream);
        assert_stream_done!(stream);
    }

    #[test]
    fn one_element_zero_buffer_returns_that_element() {
        let mut stream = new(vec![1], 0);

        assert_stream_next!(stream, 1);
    }

    #[test]
    fn one_element_buffer_1_pending_then_returns_it() {
        let mut stream = new(vec![1], 1);

        assert_stream_next!(stream, 1);
    }

    #[test]
    fn should_sort_two_out_of_order_with_buffer_1() {
        let mut stream = new(vec![2,1], 1);

        assert_stream_next!(stream, 1);
        assert_stream_next!(stream, 2);
        assert_stream_done!(stream);
    }

    #[test]
    fn should_not_sort_3_out_of_order_with_buffer_1() {
        let mut stream = new(vec![3,2,1], 1);

        assert_stream_next!(stream, 2);
        assert_stream_next!(stream, 1);
        assert_stream_next!(stream, 3);
        assert_stream_done!(stream);
    }

    fn new(stream: Vec<i32>, buffer_size: usize) -> impl Stream<Item = i32> {
        let stream = iter(stream).fuse();
        BufferedSortedStream::new(stream, buffer_size)
    }

    impl WithSequentialId for i32 {
        type Id = i32;

        fn id(&self) -> i32 {
            self.clone()
        }
    }
}
