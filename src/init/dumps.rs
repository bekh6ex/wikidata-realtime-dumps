use async_compression::stream::BzDecoder;
use async_std::prelude::*;
use futures::future::ready;
use futures::stream::*;

use futures::StreamExt;
use futures_codec::{FramedRead, LinesCodec};
use hyper::{Body, Client, Request};
use log::*;
use serde::Deserialize;

use crate::actor::SerializedEntity;
use crate::http_client::create_client;
use crate::prelude::{EntityType, RevisionId};
use crate::stream_ext::continuous_download::ContinuousDownloadStream;
use crate::stream_ext::sorted::BufferedSortedStream;
use hyper::body::Bytes;
use hyper::client::connect::dns::GaiResolver;
use hyper::client::HttpConnector;
use hyper_rustls::HttpsConnector;

pub(super) async fn get_dump_stream(ty: EntityType) -> impl Stream<Item = SerializedEntity> {
    let stream = json_stream().await;
    let stream = convert_to_serialized_entity(ty, stream);
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
    stream.filter_map(move |s: String| {
        let result = serde_json::from_str::<EntityInDump>(&s)
            .unwrap_or_else(|_| panic!("Wrong entity format: {}", s));

        ready(match ty.parse_id(&result.id) {
            Err(e) => {
                info!("{}", e);
                None
            }
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
    from: usize,
) -> impl Stream<Item = std::io::Result<Bytes>> {
    let req = Request::builder()
        .method("GET")
        .header("Accept-Encoding", "deflate")
        .header("Range", format!("bytes={}-", from))
        .uri("https://dumps.wikimedia.org/other/wikibase/wikidatawiki/latest-all.json.bz2");

    let resp = client
        .request(req.body(Body::empty()).unwrap())
        .await
        .expect("Didn't get the response for dump");

    let body = resp.into_body();

    body.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
}

fn download_dump_with_restarts(
    client: Client<HttpsConnector<HttpConnector<GaiResolver>>, Body>,
) -> impl Stream<Item = std::io::Result<Bytes>> {
    let client = client;
    ContinuousDownloadStream::new(
        move |offset| once(Box::pin(do_request(client.clone(), offset))).flatten(),
        10_000,
    )
}

async fn json_stream() -> impl Stream<Item = String> {
    let client1 = create_client();
    let client = client1;

    let stream = download_dump_with_restarts(client);

    let stream = BzDecoder::new(stream);

    let inner = stream.into_async_read();
    let stream = FramedRead::new(inner, LinesCodec {});

    stream
        .skip(1) //First line is always "[\n"
        .map(|r| r.expect("Dump response stream terminated"))
        .map(|mut s: String| {
            let len = s.len();
            let tail_len = 2; //For trailing ",\n"
            s.truncate(len - tail_len);
            s
        })
}

#[derive(Deserialize)]
struct EntityInDump {
    id: String,
    lastrevid: u64,
}
