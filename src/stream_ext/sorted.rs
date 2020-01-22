use std::pin::Pin;

use super::*;
use futures::task::{Context, Poll};
use futures::{stream::FusedStream, Stream};
use pin_utils::{unsafe_pinned, unsafe_unpinned};
use std::collections::BTreeMap;

pub struct BufferedSortedStream<I, St>
where
    I: Sequential,
{
    buffer: BTreeMap<I::Marker, Vec<I>>,
    stream: St,
    max_buffer_size: usize,
}

impl<I, St> BufferedSortedStream<I, St>
where
    I: Sequential,
    St: FusedStream<Item = I>,
{
    unsafe_pinned!(stream: St);
    // TODO: Is it safe? O_o
    unsafe_unpinned!(buffer: BTreeMap<I::Marker, Vec<I>>);

    pub fn new(stream: St, max_buffer_size: usize) -> Self {
        BufferedSortedStream {
            stream,
            max_buffer_size,
            buffer: BTreeMap::new(),
        }
    }

    fn cleanup_buffer(mut self: Pin<&mut Self>) {
        let empty_items: Vec<I::Marker> = self
            .as_mut()
            .buffer()
            .iter()
            .filter_map(|(k, v)| if v.is_empty() { Some(k.clone()) } else { None })
            .collect();

        for key in empty_items {
            self.as_mut().buffer().remove(&key);
        }
    }
}

impl<I, St> Stream for BufferedSortedStream<I, St>
where
    I: Sequential,
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
                    let seq_marker = item.seq_marker().clone();
                    let vec = buffer.entry(seq_marker.clone()).or_insert(vec![]);
                    vec.push(item);
                    let buffer_len: usize = buffer.values().map(|v| v.len()).sum();
                    if buffer_len > max_buf_size {
                        let first_id = {
                            let this = self.as_ref();
                            let (id, _) = this.buffer.iter().next().unwrap();
                            id.clone()
                        };
                        let buffer = self.as_mut().buffer();
                        let item = buffer.get_mut(&first_id).unwrap().pop().unwrap();
                        self.as_mut().cleanup_buffer();
                        return Poll::Ready(Some(item));
                    } else {
                        continue;
                    }
                }
                Poll::Ready(None) => {
                    if self.buffer.len() > 0 {
                        let first_id = {
                            let this = self.as_ref();
                            let (id, _) = this.buffer.iter().next().unwrap();
                            id.clone()
                        };

                        let buffer = self.as_mut().buffer();

                        let item = buffer.get_mut(&first_id).unwrap().pop().unwrap();
                        self.as_mut().cleanup_buffer();
                        return Poll::Ready(Some(item));
                    } else {
                        return Poll::Ready(None);
                    }
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use futures::stream::*;
    use futures::*;
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
        let mut stream = new(vec![2, 1], 1);

        assert_stream_next!(stream, 1);
        assert_stream_next!(stream, 2);
        assert_stream_done!(stream);
    }

    #[test]
    fn should_not_sort_3_out_of_order_with_buffer_1() {
        let mut stream = new(vec![3, 2, 1], 1);

        assert_stream_next!(stream, 2);
        assert_stream_next!(stream, 1);
        assert_stream_next!(stream, 3);
        assert_stream_done!(stream);
    }
    #[test]
    fn should_sort_numbers_in_random_order() {
        let mut stream = new(vec![5,2,3,1,4], 4);

        assert_stream_next!(stream, 1);
        assert_stream_next!(stream, 2);
        assert_stream_next!(stream, 3);
        assert_stream_next!(stream, 4);
        assert_stream_next!(stream, 5);
        assert_stream_done!(stream);
    }

    #[test]
    fn should_not_loose_elements() {
        let mut stream = new(vec![1, 1], 2);

        assert_stream_next!(stream, 1);
        assert_stream_next!(stream, 1);
        assert_stream_done!(stream);
    }

    fn new(stream: Vec<i32>, buffer_size: usize) -> impl Stream<Item = i32> {
        let stream = iter(stream).fuse();
        BufferedSortedStream::new(stream, buffer_size)
    }

    impl Sequential for i32 {
        type Marker = i32;

        fn seq_marker(&self) -> i32 {
            self.clone()
        }
    }
}
