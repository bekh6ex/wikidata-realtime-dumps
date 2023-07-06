use async_compression::futures::bufread::BzDecoder;
use async_std::prelude::*;
use futures::future::ready;
use futures::stream::*;
use futures::{AsyncRead, StreamExt};
use futures_codec::{FramedRead, LinesCodec};
use hyper::body::Bytes;
use hyper::client::connect::dns::GaiResolver;
use hyper::client::HttpConnector;
use hyper::{Body, Client, Request};
use hyper_rustls::HttpsConnector;
use serde::Deserialize;

use continuous_download::ContinuousDownloadStream;
use sorted_stream::BufferedSortedStream;

use crate::http_client::create_hyper_client;
use crate::init::{ArchiveFormat, DumpConfig, DumpFormat};
use crate::prelude::*;
use log::*;
use std::pin::Pin;

pub(crate) async fn get_dump_stream(
    dump_config: DumpConfig,
) -> impl Stream<Item = SerializedEntity> {
    let stream = json_stream(dump_config.clone()).await;
    let stream = convert_to_serialized_entity(dump_config.ty, stream);
    sort_stream(stream)
}

fn sort_stream(
    stream: impl Stream<Item = SerializedEntity>,
) -> impl Stream<Item = SerializedEntity> {
    BufferedSortedStream::new(stream.fuse(), 200)
}

fn convert_to_serialized_entity(
    ty: EntityType,
    stream: impl Stream<Item = String>,
) -> impl Stream<Item = SerializedEntity> {
    //2020-01-29T04:26:13.329976216+00:00 INFO continuous_download - Stream ended.
    //thread 'main' panicked at 'Dump response stream terminated: Custom { kind: UnexpectedEof, error: "bytes remaining in stream" }', src/libcore/result.rs:1165:5
    stream
        .map(|s| {
            async_std::task::spawn(async move {
                let result = serde_json::from_str::<EntityInDump>(&s);
                match result {
                    Ok(e) => Some((e, s)),
                    Err(e) => {
                        warn!("Failed to parse entity: {}", e);
                        None
                    }
                }
            })
        })
        .buffered(num_cpus::get() * 2)
        .filter_map(ready)
        .filter_map(move |(result, s)| {
            ready(match ty.parse_id(&result.id) {
                Err(_) => None,
                Ok(id) => Some(SerializedEntity {
                    id,
                    revision: RevisionId(result.lastrevid),
                    data: s,
                }),
            })
        })
}

async fn do_request(
    client: Client<HttpsConnector<HttpConnector<GaiResolver>>, Body>,
    dump_url: String,
    from: usize,
) -> impl Stream<Item = std::io::Result<Bytes>> {
    let req = Request::builder()
        .method("GET")
        .header("Accept-Encoding", "deflate")
        .header("Range", format!("bytes={}-", from))
        .uri(dump_url);

    let resp = client
        .request(req.body(Body::empty()).unwrap())
        .await
        .expect("Didn't get the response for dump");

    // TODO: Handle HTTP errors
    let body = resp.into_body();

    body.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
}

fn download_dump_with_restarts(
    client: Client<HttpsConnector<HttpConnector<GaiResolver>>, Body>,
    dump_url: String,
) -> impl Stream<Item = std::io::Result<Bytes>> {
    let client = client;
    info!("Downloading dump from {}", dump_url);

    ContinuousDownloadStream::new(
        move |offset| {
            once(Box::pin(do_request(
                client.clone(),
                dump_url.clone(),
                offset,
            )))
            .flatten()
        },
        10_000,
    )
}

pub async fn json_stream(dump_config: DumpConfig) -> impl Stream<Item = String> {
    let client1 = create_hyper_client();
    let client = client1;

    let stream = download_dump_with_restarts(client, dump_config.url.clone());

    let stream = stream.map(|r| r.map(|b: hyper::body::Bytes| bytes::Bytes::from(b.to_vec())));

    let decoder: Pin<Box<dyn AsyncRead>> = match dump_config.archive_format {
        ArchiveFormat::Bzip2 => Box::pin(BzDecoder::new(stream.into_async_read())),
        // ArchiveFormat::Gzip => Box::pin(GzipDecoder::new(stream.into_async_read())),
    };

    let stream = FramedRead::new(decoder, LinesCodec {});

    let stream: Pin<Box<dyn Stream<Item = String>>> = match dump_config.dump_format {
        DumpFormat::WikidataJsonArray => {
            // TODO: Handle last line "]\n"
            let stream = stream
                .skip(1) //First line is always "[\n"
                .map(|r| r.expect("Dump response stream terminated"))
                .map(|mut s: String| {
                    let len = s.len();
                    let tail_len = 2; //For trailing ",\n"
                    s.truncate(len - tail_len);
                    s
                });
            Box::pin(stream)
        }
        DumpFormat::SortedJsonLines => {
            // TODO: Double check. Never tried
            let stream = stream
                .map(|r| r.expect("Dump response stream terminated"))
                .map(|mut s: String| {
                    let len = s.len();
                    let tail_len = 1; //For trailing "\n"
                    s.truncate(len - tail_len);
                    s
                });

            Box::pin(stream)
        }
    };

    stream
}

#[derive(Deserialize)]
struct EntityInDump {
    id: String,
    lastrevid: u64,
}
