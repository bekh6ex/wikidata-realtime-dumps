

use futures::*;

use hyper::client::connect::dns::GaiResolver;
use hyper::client::HttpConnector;
use hyper::{Body, Client as HyperClient, Request, Response, StatusCode};
use hyper_rustls::HttpsConnector;


use log::*;


use serde::de::Deserialize;

type Client = HyperClient<HttpsConnector<HttpConnector<GaiResolver>>, Body>;

pub fn create_client() -> Client {
    HyperClient::builder().build::<_, hyper::Body>(hyper_rustls::HttpsConnector::new())
}

pub async fn get_json<'a, T: Deserialize<'a>>(
    client: &Client,
    url: String,
) -> Result<Option<T>, Error> {
    let req = Request::builder()
        .method("GET")
        .header("Accept-Encoding", "deflate")
        .uri(url.clone())
        .body(Body::empty())
        .unwrap();

    debug!("Sending get request to `{}`", url);
    let response: Response<Body> = client.request(req).await.map_err(Error::GetResponse)?;

    debug!("Got response. status={} url={}", response.status(), url);

    if response.status() == StatusCode::NOT_FOUND {
        return Ok(None);
    } else if response.status() == StatusCode::TOO_MANY_REQUESTS {
        // TODO: Maybe handle 'retry-after' header in response
        return Err(Error::TooManyRequests);
    }

    use bytes::buf::BufExt;
    use bytes::Buf;

    let body = hyper::body::aggregate(response)
        .map_err(Error::GetResponse)
        .await?
        .to_bytes();

    let body_for_error = body.clone();
    let mut de = serde_json::Deserializer::from_reader(body.reader());
    let result: T = T::deserialize(&mut de).map_err(move |e| Error::ResponseFormat {
        cause: e,
        body: std::str::from_utf8(body_for_error.bytes())
            .unwrap()
            .to_owned(),
    })?;

    Ok(Some(result))
}

#[derive(Debug)]
pub enum Error {
    TooManyRequests,
    GetResponse(hyper::Error),
    ResponseFormat {
        cause: serde_json::Error,
        body: String,
    },
}