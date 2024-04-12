use core::panic::RefUnwindSafe;
use std::prelude::v1::*;

use base::trace::Alive;
use std::any::type_name;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};

use crate::{RpcEncrypt, WsClientError};

use super::{
    Batchable, JsonrpcErrorObj, JsonrpcHttpClient, JsonrpcRawRequest, JsonrpcResponseRawResult,
    JsonrpcWsClient, WsClientConfig,
};

use serde::{de::DeserializeOwned, Serialize};
use serde_json::BoxRawValue;

#[derive(Debug)]
pub enum RpcError {
    InitError(String),
    SerdeRequestError(String, serde_json::Error),
    UnexpectedBatchResponse,
    BatchResponseMismatch { want: usize, got: usize },
    UnexpectedSingleResponse,
    ResponseError(String, JsonrpcErrorObj),
    SerdeResponseError(String, String, serde_json::Error),
    RecvResponseError(String),
    SingleFailOnBatchResponse(String, JsonrpcErrorObj),
}

pub trait SyncClientTrait: RpcClient + Send + Sync + 'static + core::fmt::Debug + RefUnwindSafe {}
impl<T> SyncClientTrait for T where T: RpcClient + Send + Sync + 'static + core::fmt::Debug + RefUnwindSafe {}

#[derive(Debug)]
pub struct MixRpcClient {
    clients: Vec<Arc<dyn SyncClientTrait>>,
    idx: AtomicUsize,
    timeout: Option<Duration>,
}

impl MixRpcClient {
    pub fn new(timeout: Option<Duration>) -> Self {
        Self {
            clients: Vec::new(),
            idx: AtomicUsize::new(0),
            timeout,
        }
    }

    pub fn add_endpoint(
        &mut self,
        alive: &Alive,
        endpoints: &[String],
    ) -> Result<(), WsClientError> {
        for endpoint in endpoints {
            if endpoint.starts_with("http") {
                self.add_client(JsonrpcHttpClient::new(&endpoint, self.timeout.clone()));
            } else if endpoint.starts_with("ws") {
                self.add_client(JsonrpcWsClient::new(WsClientConfig {
                    endpoint: endpoint.clone(),
                    ws_frame_size: 64 << 10,
                    auto_resubscribe: true,
                    poll_interval: Duration::from_nanos(0),
                    alive: alive.clone(),
                    keep_alive: None,
                    concurrent_bench: Some(2),
                })?);
            }
        }
        Ok(())
    }

    pub fn add_client<C: SyncClientTrait>(&mut self, client: C) {
        let client: Arc<dyn SyncClientTrait> = Arc::new(client);
        self.clients.push(client);
    }
}

impl RpcClient for MixRpcClient {
    fn rpc_call(
        &self,
        params: Batchable<JsonrpcRawRequest>,
    ) -> Result<Batchable<JsonrpcResponseRawResult>, RpcError> {
        let idx = self.idx.fetch_add(1, Ordering::SeqCst);
        self.clients[idx % self.clients.len()].rpc_call(params)
    }
}

pub trait RpcClient {
    fn rpc_call(
        &self,
        params: Batchable<JsonrpcRawRequest>,
    ) -> Result<Batchable<JsonrpcResponseRawResult>, RpcError>;
}

impl<T, R> RpcClient for T
where
    T: std::ops::Deref<Target = R>,
    R: RpcClient,
{
    fn rpc_call(
        &self,
        params: Batchable<JsonrpcRawRequest>,
    ) -> Result<Batchable<JsonrpcResponseRawResult>, RpcError> {
        self.deref().rpc_call(params)
    }
}

#[derive(Debug, Clone)]
pub struct JsonrpcClient<C: RpcClient> {
    client: C,
}

impl<C: RpcClient> JsonrpcClient<C> {
    pub fn new(client: C) -> Self {
        Self { client }
    }

    pub fn raw(&self) -> &C {
        &self.client
    }

    pub fn sig<R, P>(&self, method: &str, params: &P) -> String
    where
        P: Serialize + std::fmt::Debug,
        R: DeserializeOwned,
    {
        format!("{}{:?} -> {}", method, params, type_name::<R>())
    }

    pub fn req<P>(&self, method: &str, params: &P) -> Result<JsonrpcRawRequest, RpcError>
    where
        P: Serialize + std::fmt::Debug,
    {
        self.req_full::<(), _>(method, params)
    }

    pub fn req_full<R, P>(&self, method: &str, params: &P) -> Result<JsonrpcRawRequest, RpcError>
    where
        P: Serialize + std::fmt::Debug,
        R: DeserializeOwned,
    {
        let req = JsonrpcRawRequest::new(0, method, params)
            .map_err(|err| RpcError::SerdeRequestError(self.sig::<R, _>(method, &params), err))?;
        Ok(req)
    }

    pub fn enc_req_full<R, P, E>(
        &self,
        enc: (&E, &E::EncKey),
        method: &str,
        params: &P,
    ) -> Result<JsonrpcRawRequest, RpcError>
    where
        P: Serialize + std::fmt::Debug,
        R: DeserializeOwned,
        E: RpcEncrypt,
    {
        let enc_type = enc
            .0
            .encrypt(enc.1, params)
            .map_err(|err| RpcError::ResponseError("encrypt fail".into(), err))?;
        let req = JsonrpcRawRequest::new(0, method, &(&enc.1, enc_type))
            .map_err(|err| RpcError::SerdeRequestError(self.sig::<R, _>(method, &params), err))?;
        Ok(req)
    }

    fn _call(
        &self,
        params: Batchable<JsonrpcRawRequest>,
    ) -> Result<Batchable<JsonrpcResponseRawResult>, RpcError> {
        let now = Instant::now();
        glog::debug!(exclude:"dry_run", target: "rpc_body", "body: {:?}", params);
        let methods = params.collect(|item| item.method.clone());
        let result = self.client.rpc_call(params);

        glog::debug!(exclude:"dry_run", target: "rpc_body", "response: {:?}", result);
        glog::debug!(exclude:"dry_run", target: "rpc_time", "{:?}, spend time: {:?}", methods, now.elapsed());
        result
    }

    pub fn rpc<P, R>(&self, method: &str, params: P) -> Result<R, RpcError>
    where
        P: Serialize + std::fmt::Debug,
        R: DeserializeOwned,
    {
        self._rpc::<P, R, ()>(None, method, params)
    }

    pub fn enc_rpc<P, R, E>(
        &self,
        enc: &E,
        key: &E::EncKey,
        method: &str,
        params: P,
    ) -> Result<R, RpcError>
    where
        P: Serialize + std::fmt::Debug,
        R: DeserializeOwned,
        E: RpcEncrypt,
    {
        self._rpc(Some((enc, key)), method, params)
    }

    fn _rpc<P, R, E>(
        &self,
        enc: Option<(&E, &E::EncKey)>,
        method: &str,
        params: P,
    ) -> Result<R, RpcError>
    where
        P: Serialize + std::fmt::Debug,
        R: DeserializeOwned,
        E: RpcEncrypt,
    {
        let request = match enc {
            Some((enc, key)) => self.enc_req_full::<R, _, _>((enc, key), method, &params)?,
            None => self.req_full::<R, _>(method, &params)?,
        };
        let request = Batchable::Single(request);
        let response = self._call(request)?;
        let response = response.single().ok_or(RpcError::UnexpectedBatchResponse)?;
        let response = match response {
            JsonrpcResponseRawResult::Ok(val) => val,
            JsonrpcResponseRawResult::Err(err) => {
                return Err(RpcError::ResponseError(
                    self.sig::<R, _>(method, &params),
                    err.error,
                ));
            }
        };

        if let Some((enc, key)) = enc {
            let val = response.to_json().map_err(|err| {
                RpcError::SerdeResponseError(
                    self.sig::<R, _>(method, &params),
                    response.result.to_string(),
                    err,
                )
            })?;
            let val = enc
                .decrypt(key, val)
                .map_err(|err| RpcError::ResponseError(self.sig::<R, _>(method, &params), err))?;
            return Ok(val);
        }
        response.to_json().map_err(|err| {
            RpcError::SerdeResponseError(
                self.sig::<R, _>(method, &params),
                response.result.to_string(),
                err,
            )
        })
    }

    pub fn multi_chunk_rpc(
        &self,
        list: Vec<JsonrpcRawRequest>,
        size: usize,
    ) -> Result<Vec<BoxRawValue>, RpcError> {
        let mut output = Vec::with_capacity(list.len());
        for req in list.chunks(size) {
            output.extend(self.multi_rpc(req.to_vec())?);
        }
        Ok(output)
    }

    pub fn multi_rpc(&self, list: Vec<JsonrpcRawRequest>) -> Result<Vec<BoxRawValue>, RpcError> {
        let request = Batchable::Batch(list);
        let response = self._call(request)?;
        let response_list = match response {
            Batchable::Batch(list) => list,
            Batchable::Single(JsonrpcResponseRawResult::Err(err)) => {
                return Err(RpcError::ResponseError("multi_rpc".into(), err.error));
            }
            _ => return Err(RpcError::UnexpectedSingleResponse),
        };
        let mut result = Vec::with_capacity(response_list.len());
        for (idx, response) in response_list.into_iter().enumerate() {
            match response {
                JsonrpcResponseRawResult::Ok(response) => {
                    result.push(response.result);
                }
                JsonrpcResponseRawResult::Err(err) => {
                    return Err(RpcError::SingleFailOnBatchResponse(
                        format!("{}", idx),
                        err.error,
                    ));
                }
            }
        }
        Ok(result)
    }

    pub fn batch_chunk_rpc<P, R>(
        &self,
        method: &str,
        params_list: &[P],
        size: usize,
    ) -> Result<Vec<R>, RpcError>
    where
        P: Serialize + std::fmt::Debug,
        R: DeserializeOwned,
    {
        let mut output = Vec::with_capacity(params_list.len());
        for req in params_list.chunks(size) {
            output.extend(self.batch_rpc(method, req)?);
        }
        Ok(output)
    }

    pub fn batch_rpc<P, R>(&self, method: &str, params_list: &[P]) -> Result<Vec<R>, RpcError>
    where
        P: Serialize + std::fmt::Debug,
        R: DeserializeOwned,
    {
        let mut list = Vec::with_capacity(params_list.len());
        for params in params_list {
            list.push(self.req_full::<R, _>(method, params)?);
        }
        let request = Batchable::Batch(list);
        let response = self._call(request)?;
        let mut result = Vec::new();
        let response_list = match response {
            Batchable::Batch(list) => list,
            Batchable::Single(JsonrpcResponseRawResult::Err(err)) => {
                return Err(RpcError::ResponseError(
                    self.sig::<Vec<R>, _>(method, &params_list),
                    err.error,
                ));
            }
            _ => return Err(RpcError::UnexpectedSingleResponse),
        };
        if response_list.len() != params_list.len() {
            return Err(RpcError::BatchResponseMismatch {
                want: params_list.len(),
                got: response_list.len(),
            });
        }
        for (idx, response) in response_list.into_iter().enumerate() {
            match response {
                JsonrpcResponseRawResult::Ok(response) => {
                    result.push(response.to_json().map_err(|err| {
                        RpcError::SerdeResponseError(
                            self.sig::<R, _>(method, &params_list[idx]),
                            response.result.to_string(),
                            err,
                        )
                    })?);
                }
                JsonrpcResponseRawResult::Err(err) => {
                    return Err(RpcError::SingleFailOnBatchResponse(
                        self.sig::<R, _>(method, &params_list[idx]),
                        err.error,
                    ))
                }
            }
        }
        Ok(result)
    }
}
