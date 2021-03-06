use futures::*;
use hyper::{Body, Client as HyperClient, Request, Response, StatusCode};
use hyper::client::connect::dns::GaiResolver;
use hyper::client::HttpConnector;
use hyper_rustls::HttpsConnector;
use isahc::prelude::*;
use log::*;
use rand::Rng;
use serde::de::Deserialize;

use crate::http_client::Error::GetResponse;
use bytes::Bytes;

pub type HClient = HyperClient<HttpsConnector<HttpConnector<GaiResolver>>, Body>;

pub fn create_hyper_client() -> HClient {
    HyperClient::builder()
        .http2_keep_alive_interval(None)
        .http2_only(true)
        .pool_max_idle_per_host(1)
        .build::<_, hyper::Body>(hyper_rustls::HttpsConnector::new())
}

pub fn create_client() -> HttpClient {
    use isahc::config::{RedirectPolicy, VersionNegotiation};
    use isahc::prelude::*;
    use std::time::Duration;

    let client = HttpClient::builder()
        .timeout(Duration::from_secs(20))
        .connect_timeout(Duration::from_secs(10))
        .redirect_policy(RedirectPolicy::None)
        .version_negotiation(VersionNegotiation::http2())
        .build()
        .unwrap();

    client
}

fn generate_session_id() -> String {
    let chars = "ABCDEFGHIJKLMNOPQRSTUzWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut session = [0u8; 20];

    rand::thread_rng().fill(&mut session);
    let session = session
        .iter()
        .map(|b| {
            let index = *b as usize % chars.len();
            chars.as_bytes()[index]
        })
        .collect::<Vec<_>>();
    let session = String::from_utf8_lossy(&session);
    session.into()
}

pub fn get_json<'a, T: Deserialize<'a>>(
    client: &HttpClient,
    url: String,
) -> impl Future<Output = Result<Option<T>, Error>> + Send + '_ {

    let session = generate_session_id();

    let req = Request::builder()
        .method("GET")
        .header("Accept-Encoding", "deflate")
        .header("Cookie", "Session=".to_owned() + &session)
        .uri(url.clone())
        .body(isahc::Body::empty())
        .unwrap();

    debug!("Sending get request to `{}`", url);
    let fut_resp = client.send_async(req);
    async move {
        let mut response: isahc::http::Response<isahc::Body> = fut_resp.await.map_err(Error::GetResponse)?;

        debug!("Got response. status={} url={}", response.status(), url);

        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        } else if response.status() == StatusCode::TOO_MANY_REQUESTS {
            return Err(Error::TooManyRequests);
        } else if response.status() == StatusCode::BAD_REQUEST {
            return Err(Error::BadRequest);
        }

        use bytes::buf::BufExt;
        use bytes::Buf;
        use isahc::ResponseExt;

        let body = response.text_async().await.map_err(|e| GetResponse(isahc::Error::Io(e)))?;

        let body = Bytes::from(body);

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
}

#[derive(Debug)]
pub enum Error {
    BadRequest,
    TooManyRequests,
    GetResponse(isahc::Error),
    ResponseFormat {
        cause: serde_json::Error,
        body: String,
    },
}
