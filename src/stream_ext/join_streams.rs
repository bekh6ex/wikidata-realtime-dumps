use std::pin::Pin;

use futures::future::Ready;
use futures::stream::Peekable;
use futures::task::{Context, Poll};
use futures::*;
use log::*;

use pin_utils::{unsafe_pinned, unsafe_unpinned};

use crate::actor::SerializedEntity;
use crate::prelude::EntityId;
use futures::future::Either;
use std::cmp::Ordering;

pub struct JoinStreams<IdSt: Stream, DSt: Stream, F> {
    id_stream: Peekable<IdSt>, //TODO: test this Peekable. Without it calls get_entity without waiting for the dump
    dump_stream: Peekable<DSt>,
    get_entity: F,
}

impl<IdSt, DSt, F> Unpin for JoinStreams<IdSt, DSt, F>
where
    IdSt: Stream + Unpin,
    DSt: Stream + Unpin,
{
}

impl<IdSt, DSt, F, Fut> JoinStreams<IdSt, DSt, F>
where
    IdSt: Stream<Item = EntityId>,
    DSt: Stream<Item = SerializedEntity>,
    F: FnMut(EntityId) -> Fut,
    Fut: Future<Output = Option<SerializedEntity>> + 'static,
{
    unsafe_pinned!(id_stream: Peekable<IdSt>);
    unsafe_pinned!(dump_stream: Peekable<DSt>);
    unsafe_unpinned!(get_entity: F);

    pub fn new(id_stream: IdSt, dump_stream: DSt, get_entity: F) -> Self {
        JoinStreams {
            id_stream: id_stream.peekable(),
            dump_stream: dump_stream.peekable(),
            get_entity,
        }
    }
}

impl<IdSt, DSt, F, Fut> Stream for JoinStreams<IdSt, DSt, F>
where
    IdSt: Stream<Item = EntityId>,
    DSt: Stream<Item = SerializedEntity>,
    F: FnMut(EntityId) -> Fut,
    Fut: Future<Output = Option<SerializedEntity>> + 'static,
{
    type Item = Either<Fut, Ready<Option<SerializedEntity>>>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        #![allow(unused_must_use)]

        let new_id: Poll<Option<IdSt::Item>> = self
            .as_mut()
            .id_stream()
            .poll_peek(cx)
            .map(|opt| opt.copied());

        match ready!(new_id) {
            Some(id) => loop {
                let dump_peek = self.as_mut().dump_stream().poll_peek(cx);
                match ready!(dump_peek) {
                    Some(dump_entity) => {
                        debug!("Saw in dump looking for {}: {}", id, dump_entity.id);
                        let dump_entity: &SerializedEntity = dump_entity;
                        match dump_entity.id.cmp(&id) {
                            Ordering::Equal => {
                                self.as_mut().id_stream().poll_next(cx);
                                match self.as_mut().dump_stream().poll_next(cx) {
                                    Poll::Ready(Some(next_from_dump)) => {
                                        debug!("Got from dump: {}", id);

                                        return Poll::Ready(Some(
                                            future::ready(Some(next_from_dump)).right_future(),
                                        ));
                                    }
                                    _ => unreachable!(),
                                };
                            }
                            Ordering::Greater => {
                                self.as_mut().id_stream().poll_next(cx);
                                let fut = (self.as_mut().get_entity())(id);
                                debug!("Getting from EntityData: {}", id);

                                return Poll::Ready(Some(fut.left_future()));
                            }
                            Ordering::Less => {
                                info!("Dropping from dump: {}", dump_entity.id);

                                self.as_mut().dump_stream().poll_next(cx);
                                continue;
                            }
                        }
                    }
                    None => {
                        #[allow(unused_must_use)]
                        self.as_mut().id_stream().poll_next(cx);
                        let fut = (self.as_mut().get_entity())(id);
                        debug!("Got from EntityData: {}", id);
                        return Poll::Ready(Some(fut.left_future()));
                    }
                }
            },
            None => Poll::Ready(None),
        }
    }
}

#[cfg(test)]
mod test {
    use futures::future::*;
    use futures::stream::*;

    use futures_test::*;

    use crate::actor::SerializedEntity;
    use crate::prelude::{EntityId, EntityType, RevisionId};

    use super::*;

    #[test]
    fn should_be_done_when_there_is_nothing_in_id_stream() {
        let id_stream = ids(vec![]);
        let dump_stream = entities(vec![1]);

        let streams = JoinStreams::new(id_stream, dump_stream, panic_on_call);
        let mut streams = streams.filter_map(|f| f);

        assert_stream_done!(streams);
        assert_stream_done!(streams);
    }

    #[test]
    fn should_query_item_if_it_is_not_in_the_dump() {
        let id_stream = ids(vec![1]);
        let dump_stream = entities(vec![]);

        let streams = JoinStreams::new(id_stream, dump_stream, always_find);
        let mut streams = streams.filter_map(|f| f);

        assert_stream_next!(streams, dummy_entity(1));
        assert_stream_done!(streams);
    }

    #[test]
    fn should_not_query_item_if_it_is_in_the_dump() {
        let id_stream = ids(vec![1]);
        let dump_stream = entities(vec![1]);

        let streams = JoinStreams::new(id_stream, dump_stream, panic_on_call);
        let mut streams = streams.filter_map(|f| f);

        assert_stream_next!(streams, dummy_entity(1));
        assert_stream_done!(streams);
    }

    #[test]
    fn should_take_from_dump_if_possible() {
        let id_stream = ids(vec![1, 2]);
        let dump_stream = entities(vec![2]);

        let streams = JoinStreams::new(id_stream, dump_stream, |id| {
            assert_eq!(id.n(), 1);
            ready(Some(dummy_entity(1)))
        });
        let mut streams = streams.filter_map(|f| f);

        assert_stream_next!(streams, dummy_entity(1));
        assert_stream_next!(streams, dummy_entity(2));
        assert_stream_done!(streams);
    }

    #[test]
    fn should_throw_away_previous_items_from_dump() {
        let id_stream = ids(vec![2]);
        let dump_stream = entities(vec![1, 2]);

        let streams = JoinStreams::new(id_stream, dump_stream, panic_on_call);
        let mut streams = streams.filter_map(|f| f);

        assert_stream_next!(streams, dummy_entity(2));
        assert_stream_done!(streams);
    }

    #[test]
    fn should_not_yield_value_when_cannot_find() {
        let id_stream = ids(vec![1, 2]);
        let dump_stream = entities(vec![2]);

        let streams = JoinStreams::new(id_stream, dump_stream, never_find);
        let mut streams = streams.filter_map(|f| f);

        assert_stream_next!(streams, dummy_entity(2));
        assert_stream_done!(streams);
    }

    fn always_find(id: EntityId) -> Ready<Option<SerializedEntity>> {
        ready(Some(dummy_entity(id.n())))
    }

    fn never_find(_id: EntityId) -> Ready<Option<SerializedEntity>> {
        ready(None)
    }

    fn panic_on_call(id: EntityId) -> Ready<Option<SerializedEntity>> {
        panic!("panic_on_call was called with {:?}", id)
    }

    fn ids(ids: Vec<u32>) -> impl FusedStream<Item = EntityId> {
        iter(ids.into_iter().map(|i| EntityType::Item.id(i))).fuse()
    }

    fn entities(ids: Vec<u32>) -> impl FusedStream<Item = SerializedEntity> {
        iter(ids.into_iter().map(dummy_entity)).fuse()
    }

    fn dummy_entity(id: u32) -> SerializedEntity {
        SerializedEntity {
            id: EntityType::Item.id(id),
            revision: RevisionId(0),
            data: "".to_owned(),
        }
    }
}
