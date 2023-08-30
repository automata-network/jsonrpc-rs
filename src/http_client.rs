use core::time::Duration;
use std::prelude::v1::*;

use crate::{Batchable, JsonrpcRawRequest, JsonrpcResponseRawResult, RpcError};
use net_http::{HttpClient as NetHttpClient, HttpMethod};
use net_http::{HttpRequestBuilder, Uri};

#[derive(Debug)]
pub struct JsonrpcHttpClient {
    endpoint: Uri,
    client: NetHttpClient,
    timeout: Option<Duration>,
}

impl JsonrpcHttpClient {
    pub fn new(endpoint: &str, timeout: Option<Duration>) -> Self {
        let endpoint = Uri::new(endpoint).unwrap();
        Self {
            endpoint,
            client: NetHttpClient::new(),
            timeout,
        }
    }
}

impl crate::RpcClient for JsonrpcHttpClient {
    fn rpc_call(
        &self,
        params: Batchable<JsonrpcRawRequest>,
    ) -> Result<Batchable<JsonrpcResponseRawResult>, RpcError> {
        let body = serde_json::to_vec(&params)
            .map_err(|err| RpcError::SerdeRequestError(format!("{:?}", params), err))?;
        let mut req = HttpRequestBuilder::new_ex(self.endpoint.clone(), Some(body), |req| {
            req.method(HttpMethod::Post);
            req.header("Content-Type", "application/json");
        });
        let response = self
            .client
            .send(&mut req, self.timeout)
            .map_err(|err| RpcError::RecvResponseError(format!("{:?}", err)))?;
        Batchable::parse(&response.body).map_err(|err| {
            RpcError::SerdeResponseError(
                format!("{:?}", params),
                String::from_utf8_lossy(&response.body).into_owned(),
                err,
            )
        })
    }
}
