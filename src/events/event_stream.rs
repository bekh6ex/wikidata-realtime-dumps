use std::io::{Error, ErrorKind};

use futures::future::ready;

use futures::{Stream, StreamExt, TryStreamExt};
use log::*;

use sse_codec::{decode_stream, Event};

use isahc::prelude::*;

use std::fmt::Debug;

pub(super) fn response_to_stream(
    resp: Response<Body>,
    _event_id: Option<String>,
) -> impl Stream<Item = Event> {
    let body = resp.into_body();

    let decoded_stream = decode_stream(body);
    let event_stream = finish_stream_on_error(decoded_stream);

    let stream = event_stream
        .chunks(2)
        .take_while(|v| {
            let s = v.as_slice();
            match s {
                [Event::LastEventId { .. }, Event::Message { .. }] => ready(true),
                _ => {
                    info!("Stopping stream. Wrong set of messages: {:?}", v);
                    ready(false)
                }
            }
        })
        .filter(|v| {
            let s = v.as_slice();
            match s {
                [Event::LastEventId { .. }, Event::Message { .. }] => ready(true),
                _ => {
                    info!("Stopping stream. Wrong set of messages: {:?}", v);
                    ready(false)
                }
            }
        })
        .map(futures::stream::iter)
        .flatten();

    log_start_of_the_stream(stream)
}

fn finish_stream_on_error<I, E: Debug>(
    stream: impl Stream<Item = Result<I, E>>,
) -> impl Stream<Item = I> {
    stream
        .take_while(|r: &Result<I, E>| {
            if let Err(e) = r {
                warn!("Error after decoding: {:?}", e);
            }

            ready(r.is_ok())
        })
        .map(|r| r.unwrap())
}

fn log_start_of_the_stream(stream: impl Stream<Item = Event>) -> impl Stream<Item = Event> {
    stream.enumerate().map(|(index, ev)| {
        if index == 0 {
            info!("Stream started from {:?}", ev)
        }
        ev
    })
}
