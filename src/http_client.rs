use futures::*;
use hyper::{Body, Client as HyperClient, Request, StatusCode};
use hyper::client::connect::dns::GaiResolver;
use hyper::client::HttpConnector;
use hyper_rustls::HttpsConnector;
use isahc::prelude::*;
use isahc::HttpClient;
use log::*;
use rand::Rng;
use serde::de::Deserialize;

use crate::http_client::Error::GetResponse;
use bytes::Bytes;

pub type HClient = HyperClient<HttpsConnector<HttpConnector<GaiResolver>>, Body>;

pub fn create_hyper_client() -> HClient {
    HyperClient::builder()
        .pool_max_idle_per_host(1)
        .build::<_, hyper::Body>(hyper_rustls::HttpsConnectorBuilder::new()
            .with_native_roots()
            .https_only().enable_all_versions().build()
        )
}

pub fn create_client() -> HttpClient {
    use isahc::config::{RedirectPolicy, VersionNegotiation};
    use isahc::prelude::*;
    use std::time::Duration;

    HttpClient::builder()
        .timeout(Duration::from_secs(20))
        .connect_timeout(Duration::from_secs(10))
        .redirect_policy(RedirectPolicy::None)
        .version_negotiation(VersionNegotiation::http2())
        .build()
        .unwrap()
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
        .body(isahc::AsyncBody::empty())
        .unwrap();

    debug!("Sending get request to `{}`", url);
    let fut_resp = client.send_async(req);
    async move {
        let mut response: isahc::http::Response<isahc::AsyncBody> = fut_resp.await.map_err(|e| Error::GetResponse(format!("{}", e.kind())))?;

        debug!("Got response. status={} url={}", response.status(), url);

        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        } else if response.status() == StatusCode::TOO_MANY_REQUESTS {
            return Err(Error::TooManyRequests);
        } else if response.status() == StatusCode::BAD_REQUEST {
            return Err(Error::BadRequest);
        }

        //Q 120 367 626

        use bytes::Buf;

        let body = response.text().await.map_err(|e| GetResponse(format!("{}", e.kind())))?;

        let body = Bytes::from(body);

        let body_for_error = body.clone();
        let mut de = serde_json::Deserializer::from_reader(body.reader());
        let result: T = T::deserialize(&mut de).map_err(move |e| Error::ResponseFormat {
            cause: e,
            body: std::str::from_utf8(body_for_error.as_ref())
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
    GetResponse(String),
    ResponseFormat {
        cause: serde_json::Error,
        body: String,
    },
}
