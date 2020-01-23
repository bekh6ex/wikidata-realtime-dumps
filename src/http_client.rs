use hyper::body::Bytes;
use hyper::client::connect::dns::GaiResolver;
use hyper::client::HttpConnector;
use hyper_rustls::HttpsConnector;
use hyper::{Body, Client, Request};

pub fn create_client() -> Client<HttpsConnector<HttpConnector<GaiResolver>>, Body> {
    Client::builder().build::<_, hyper::Body>(hyper_rustls::HttpsConnector::new())
}
