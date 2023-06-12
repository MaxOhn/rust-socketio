use adler32::adler32;
use async_stream::try_stream;
use async_trait::async_trait;
use base64::{engine::general_purpose, Engine as _};
use bytes::{BufMut, Bytes, BytesMut};
use futures_util::{Stream, StreamExt};
use http::{HeaderMap, Request, Response};
use hyper::client::{HttpConnector, ResponseFuture};
use hyper::{Body, Client as HyperClient, Error as HyperError};
use hyper_rustls::{HttpsConnector, HttpsConnectorBuilder};
use std::fmt::Debug;
use std::future::IntoFuture;
use std::time::SystemTime;
use std::{pin::Pin, sync::Arc};
use tokio::sync::RwLock;
use url::Url;

use crate::asynchronous::generator::StreamGenerator;
use crate::{asynchronous::transport::AsyncTransport, error::Result, Error};

/// An asynchronous polling type. Makes use of the nonblocking reqwest types and
/// methods.
#[derive(Clone)]
pub struct PollingTransport {
    client: Client,
    base_url: Arc<RwLock<Url>>,
    generator: StreamGenerator<Bytes>,
}

type Connector = HttpsConnector<HttpConnector>;

#[derive(Clone, Debug)]
struct Client {
    client: HyperClient<Connector>,
    headers: HeaderMap,
}

impl Client {
    fn new() -> Self {
        Self::with_headers(HeaderMap::default())
    }

    fn with_headers(headers: HeaderMap) -> Self {
        let mut http = HttpConnector::new();
        http.enforce_http(false);

        let connector = HttpsConnectorBuilder::new()
            .with_webpki_roots()
            .https_or_http()
            .enable_http1()
            .enable_http2()
            .wrap_connector(http);

        let client = HyperClient::builder().build(connector);

        Self { client, headers }
    }

    fn get(&self, url: Url) -> RequestBuilder<'_> {
        let mut req = Request::get(url.as_str());
        let Some(headers) = req.headers_mut() else { unreachable!() };
        headers.extend(self.headers.clone());

        RequestBuilder {
            req,
            body: None,
            client: &self.client,
        }
    }

    fn post(&self, url: Url) -> RequestBuilder<'_> {
        let mut req = Request::post(url.as_str());
        let Some(headers) = req.headers_mut() else { unreachable!() };
        headers.extend(self.headers.clone());

        RequestBuilder {
            req,
            body: None,
            client: &self.client,
        }
    }
}

struct RequestBuilder<'c> {
    req: http::request::Builder,
    body: Option<Body>,
    client: &'c HyperClient<Connector>,
}

impl RequestBuilder<'_> {
    fn body(mut self, body: impl Into<Body>) -> Self {
        self.body = Some(body.into());

        self
    }
}

impl IntoFuture for RequestBuilder<'_> {
    type Output = std::result::Result<Response<Body>, HyperError>;
    type IntoFuture = ResponseFuture;

    fn into_future(self) -> Self::IntoFuture {
        let body = self.body.unwrap_or_else(Body::empty);
        let req = self.req.body(body).unwrap();

        self.client.request(req)
    }
}

impl PollingTransport {
    pub fn new(base_url: Url, opening_headers: Option<HeaderMap>) -> Self {
        let client = match opening_headers {
            Some(map) => Client::with_headers(map),
            None => Client::new(),
        };

        let mut url = base_url;
        url.query_pairs_mut().append_pair("transport", "polling");

        PollingTransport {
            client: client.clone(),
            base_url: Arc::new(RwLock::new(url.clone())),
            generator: StreamGenerator::new(Self::stream(url, client)),
        }
    }

    fn address(mut url: Url) -> Result<Url> {
        let reader = format!("{:#?}", SystemTime::now());
        let hash = adler32(reader.as_bytes()).unwrap();
        url.query_pairs_mut().append_pair("t", &hash.to_string());
        Ok(url)
    }

    fn send_request(url: Url, client: Client) -> impl Stream<Item = Result<Response<Body>>> {
        try_stream! {
            let address = Self::address(url)?;
            let req = client.get(address);

            yield req.await?
        }
    }

    fn stream(
        url: Url,
        client: Client,
    ) -> Pin<Box<dyn Stream<Item = Result<Bytes>> + 'static + Send>> {
        Box::pin(try_stream! {
            loop {
                for await elem in Self::send_request(url.clone(), client.clone()) {
                    for await bytes in elem?.into_body() {
                        yield bytes?;
                    }
                }
            }
        })
    }
}

impl Stream for PollingTransport {
    type Item = Result<Bytes>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.generator.poll_next_unpin(cx)
    }
}

#[async_trait]
impl AsyncTransport for PollingTransport {
    async fn emit(&self, data: Bytes, is_binary_att: bool) -> Result<()> {
        let data_to_send = if is_binary_att {
            // the binary attachment gets `base64` encoded
            let mut packet_bytes = BytesMut::with_capacity(data.len() + 1);
            packet_bytes.put_u8(b'b');

            let encoded_data = general_purpose::STANDARD.encode(data);
            packet_bytes.put(encoded_data.as_bytes());

            packet_bytes.freeze()
        } else {
            data
        };

        let status = self
            .client
            .post(self.address().await?)
            .body(data_to_send)
            .await?
            .status()
            .as_u16();

        if status != 200 {
            let error = Error::IncompleteHttp(status);
            return Err(error);
        }

        Ok(())
    }

    async fn base_url(&self) -> Result<Url> {
        Ok(self.base_url.read().await.clone())
    }

    async fn set_base_url(&self, base_url: Url) -> Result<()> {
        let mut url = base_url;
        if !url
            .query_pairs()
            .any(|(k, v)| k == "transport" && v == "polling")
        {
            url.query_pairs_mut().append_pair("transport", "polling");
        }
        *self.base_url.write().await = url;
        Ok(())
    }
}

impl Debug for PollingTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PollingTransport")
            .field("client", &self.client)
            .field("base_url", &self.base_url)
            .finish()
    }
}

#[cfg(test)]
mod test {
    use crate::asynchronous::transport::AsyncTransport;

    use super::*;
    use std::str::FromStr;

    #[tokio::test]
    async fn polling_transport_base_url() -> Result<()> {
        let url = crate::test::engine_io_server()?.to_string();
        let transport = PollingTransport::new(Url::from_str(&url[..]).unwrap(), None);
        assert_eq!(
            transport.base_url().await?.to_string(),
            url.clone() + "?transport=polling"
        );
        transport
            .set_base_url(Url::parse("https://127.0.0.1")?)
            .await?;
        assert_eq!(
            transport.base_url().await?.to_string(),
            "https://127.0.0.1/?transport=polling"
        );
        assert_ne!(transport.base_url().await?.to_string(), url);

        transport
            .set_base_url(Url::parse("http://127.0.0.1/?transport=polling")?)
            .await?;
        assert_eq!(
            transport.base_url().await?.to_string(),
            "http://127.0.0.1/?transport=polling"
        );
        assert_ne!(transport.base_url().await?.to_string(), url);
        Ok(())
    }
}
