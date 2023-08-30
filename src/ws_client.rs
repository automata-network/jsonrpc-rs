use core::sync::atomic::{AtomicUsize, Ordering};
use std::prelude::v1::*;

use std::collections::BTreeMap;
use std::sync::{mpsc, Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::{Batchable, JsonrpcErrorObj, JsonrpcRawRequest, JsonrpcResponseRawResult};
use base::{format::summarize_str, trace::Alive};
use net_http::{WsDataType, WsError, WsStreamClient, WsStreamConfig};

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::BoxRawValue;

#[macro_export]
macro_rules! decl_subscriptions {
    ( $($sub_name:ident ( $($arg_name:ident : $arg_ty: ty),* ), $unsub:tt, $t:ty ;)* ) => {
        $(
            #[allow(non_snake_case)]
            fn $sub_name( mgr: & $crate::JsonrpcWsClient, $($arg_name : $arg_ty),* ) -> Result<$crate::WsSubscription<$t>, $crate::WsClientError> {
                mgr.subscribe(stringify!($sub_name), $unsub, $($arg_name,)*)
            }
        )*
    };
}

#[derive(Debug, PartialEq, Eq)]
pub enum WsClientError {
    ChannelEmpty,
    InitError(String),
    SubscribeError(String),
    SubscribeResponse(JsonrpcErrorObj),
    DeserializeError(String),
    Exited,
}

impl From<mpsc::RecvTimeoutError> for WsClientError {
    fn from(err: mpsc::RecvTimeoutError) -> Self {
        match err {
            mpsc::RecvTimeoutError::Timeout => Self::ChannelEmpty,
            mpsc::RecvTimeoutError::Disconnected => Self::Exited,
        }
    }
}

impl From<mpsc::TryRecvError> for WsClientError {
    fn from(err: mpsc::TryRecvError) -> Self {
        match err {
            mpsc::TryRecvError::Empty => Self::ChannelEmpty,
            mpsc::TryRecvError::Disconnected => Self::Exited,
        }
    }
}

impl From<mpsc::RecvError> for WsClientError {
    fn from(_: mpsc::RecvError) -> Self {
        Self::Exited
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[cfg_attr(feature = "sgx_enclave", serde(crate = "serde_sgx"))]
pub struct ServerPushSubscription {
    #[allow(dead_code)]
    pub jsonrpc: String,
    #[allow(dead_code)]
    pub method: String,
    pub params: ServerPushSubscriptionParams,
}

#[derive(Debug, Deserialize, Serialize, Default)]
#[cfg_attr(feature = "sgx_enclave", serde(crate = "serde_sgx"))]
pub struct ServerPushSubscriptionParams {
    pub result: BoxRawValue,
    pub subscription: String,
}

struct SubscribeResponse {
    id: glog::ReqId,
    receiver: mpsc::Receiver<Result<BoxRawValue, WsClientError>>,
}

struct SubscribeRequest {
    method: String,
    params: BoxRawValue,
    unsub_method: String,
}

impl SubscribeRequest {
    fn subscribe(&self, tag: &str, id: u32) -> String {
        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": self.method,
            "params": self.params,
        })
        .to_string();

        glog::info!("[{}] Subscribe {}", tag, request);

        request
    }
}

pub type CallContext = Box<dyn std::any::Any + 'static + Send>;

enum JsonrpcWsAction {
    Jsonrpc {
        req: Batchable<JsonrpcRawRequest>,
        ctx: CallContext,
        resp:
            mpsc::Sender<Result<(CallContext, Batchable<JsonrpcResponseRawResult>), WsClientError>>,
    },
    Reconnect {},
    Subscribe {
        req: SubscribeRequest,
        resp: mpsc::Sender<Result<SubscribeResponse, WsClientError>>,
    },
    Resubscribe {
        info: SubscriptionInfo,
    },
    SubscriptionExit {
        id: glog::ReqId,
    },
}

struct SubscriptionInfo {
    id: glog::ReqId,
    req: SubscribeRequest,
    subscription_id: String,
    sender: mpsc::Sender<Result<BoxRawValue, WsClientError>>,
    // marked as exited, so it wouldn't resubscribe when a reconnect happens.
    exited: bool,
}

impl SubscriptionInfo {
    fn unsubscribe(&mut self, id: u32) -> String {
        self.exited = true;

        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": id,
            "method": self.req.unsub_method,
            "params": [self.subscription_id],
        })
        .to_string();

        glog::info!("Unsubscribe {}", request);

        request
    }
}

#[derive(Clone, Debug)]
pub struct JsonrpcWsClient {
    pub tag: String,
    cfg: WsClientConfig,
    sender: mpsc::SyncSender<JsonrpcWsAction>,
    epoch: Arc<AtomicUsize>,
    handle: Arc<Mutex<Option<JoinHandle<()>>>>,
    alive: Alive,
}

pub trait SubscriptionResultMapper<T, E> {
    fn map_to(&self, t: T) -> Result<E, WsClientError>;
}

pub struct SubscriptionMapper<T, E, M>
where
    M: SubscriptionResultMapper<T, E>,
    for<'de> T: Deserialize<'de>,
{
    sub: WsSubscription<T>,
    mapper: M,
    _marker: std::marker::PhantomData<E>,
}

impl<T, M, E> SubscriptionMapper<T, E, M>
where
    M: SubscriptionResultMapper<T, E>,
    for<'de> T: Deserialize<'de>,
{
    pub fn new(sub: WsSubscription<T>, mapper: M) -> Self {
        Self {
            sub,
            mapper,
            _marker: Default::default(),
        }
    }

    pub fn recv(&self) -> Result<E, WsClientError> {
        let out = self.sub.recv()?;
        Ok(self.mapper.map_to(out)?)
    }

    pub fn recv_timeout(&self, dur: Duration) -> Result<E, WsClientError> {
        let out = self.sub.recv_timeout(dur)?;
        Ok(self.mapper.map_to(out)?)
    }

    pub fn try_recv(&self) -> Result<E, WsClientError> {
        let out = self.sub.try_recv()?;
        Ok(self.mapper.map_to(out)?)
    }
}

pub struct WsSubscription<T>
where
    for<'de> T: Deserialize<'de>,
{
    tag: String,
    pub id: glog::ReqId,
    receiver: mpsc::Receiver<Result<BoxRawValue, WsClientError>>,
    sender: mpsc::SyncSender<JsonrpcWsAction>,
    formatter: Box<dyn Fn(&BoxRawValue) -> Result<T, WsClientError> + Send + Sync>,
}

impl<T> Drop for WsSubscription<T>
where
    for<'de> T: Deserialize<'de>,
{
    fn drop(&mut self) {
        let _ = self
            .sender
            .send(JsonrpcWsAction::SubscriptionExit { id: self.id });
    }
}

impl<T> WsSubscription<T>
where
    T: DeserializeOwned,
{
    pub fn with_formatter<N, F>(self, f: F) -> WsSubscription<N>
    where
        N: DeserializeOwned,
        F: Fn(&BoxRawValue) -> Result<N, WsClientError> + 'static + Send + Sync,
    {
        let mut n: WsSubscription<N> = unsafe { std::mem::transmute(self) };
        n.formatter = Box::new(f);
        n
    }

    /// Errors:
    ///  * [`WsClientError::ChannelEmpty`]: no result.
    ///  * [`WsClientError::Exited`]: the JsonrpcWsClient is dropped.
    ///  * [`WsClientError::SubscribeError`]/[`WsClientError::SubscribeResponse`]: error when resubscribe.
    ///  * [`WsClientError::DeserializeError`]: error when deserialize the value.
    pub fn try_recv(&self) -> Result<T, WsClientError> {
        let value = self.receiver.try_recv()??;
        self.format(&value)
    }

    /// Errors:
    ///  * [`WsClientError::Exited`]: the JsonrpcWsClient is dropped.
    ///  * [`WsClientError::SubscribeError`]/[`WsClientError::SubscribeResponse`]: error when resubscribe.
    ///  * [`WsClientError::DeserializeError`]: error when deserialize the value.
    pub fn recv(&self) -> Result<T, WsClientError> {
        let value = self.receiver.recv()??;
        self.format(&value)
    }

    /// Errors:
    ///  * [`WsClientError::ChannelEmpty`]: no result(timeout).
    ///  * [`WsClientError::Exited`]: the JsonrpcWsClient is dropped.
    ///  * [`WsClientError::SubscribeError`]/[`WsClientError::SubscribeResponse`]: error when resubscribe.
    ///  * [`WsClientError::DeserializeError`]: error when deserialize the value.
    pub fn recv_timeout(&self, dur: Duration) -> Result<T, WsClientError> {
        let value = self.receiver.recv_timeout(dur)??;
        self.format(&value)
    }

    pub fn format(&self, value: &BoxRawValue) -> Result<T, WsClientError> {
        (self.formatter)(value)
    }

    pub fn json_formatter(value: &BoxRawValue) -> Result<T, WsClientError> {
        match serde_json::from_raw_value(value) {
            Ok(val) => Ok(val),
            Err(err) => Err(WsClientError::DeserializeError(format!(
                "{:?}, data: {:?}",
                err, value
            ))),
        }
    }

    pub fn must_recv_within(&self, dur: Duration) -> Result<T, WsClientError> {
        loop {
            match self.recv_timeout(dur) {
                Err(WsClientError::ChannelEmpty) => {
                    self.reconnect();
                    glog::error!(
                        "[{}] subscription not response within {:?}, request to resubscribe",
                        self.tag,
                        dur
                    );
                    continue;
                }
                other => return other,
            }
        }
    }

    pub fn reconnect(&self) {
        let _ = self.sender.send(JsonrpcWsAction::Reconnect {});
    }
}

#[derive(Debug, Clone)]
pub struct WsClientConfig {
    pub endpoint: String,
    pub ws_frame_size: usize,
    pub keep_alive: Option<Duration>,
    // turn on if the subscription request is idempotent
    // e.g. subscribe_storage should not use this because the storage key may changed.
    pub auto_resubscribe: bool,
    pub poll_interval: Duration,
    pub concurrent_bench: Option<usize>,
    // *NOTE*: JsonrpcWsClient will set it to false when it's dropped
    pub alive: Alive,
}

impl Default for WsClientConfig {
    fn default() -> Self {
        Self {
            endpoint: "".into(),
            ws_frame_size: 8 * 1024,
            keep_alive: Some(Duration::from_secs(10)),
            auto_resubscribe: true,
            poll_interval: Duration::from_millis(0),
            alive: Alive::new(),
            concurrent_bench: None,
        }
    }
}

impl Drop for JsonrpcWsClient {
    fn drop(&mut self) {
        let refcount = Arc::strong_count(&self.handle);
        if refcount > 1 {
            return;
        }

        self.alive.shutdown();
        match self.handle.lock().unwrap().take() {
            Some(handle) => {
                glog::info!("[{}] shuting down", self.tag);
                let _ = handle.join();
            }
            None => {}
        }
    }
}

impl JsonrpcWsClient {
    pub fn new(cfg: WsClientConfig) -> Result<Self, WsClientError> {
        let ws_cfg = WsStreamConfig {
            endpoint: cfg
                .endpoint
                .parse()
                .map_err(|err| WsClientError::InitError(format!("parse endpoint {:?}", err)))?,
            frame_size: cfg.ws_frame_size,
            auto_reconnect: true,
            keep_alive: cfg.keep_alive,
            alive: cfg.alive.clone(),
        };
        let epoch = Arc::new(AtomicUsize::new(0));

        let (sender, receiver) = mpsc::sync_channel(8);
        let tag = ws_cfg.endpoint.tag();

        let handle = std::thread::spawn({
            let mut poll_interval = cfg.poll_interval;
            let tag = tag.clone();
            let stream = WsStreamClient::new(ws_cfg).map_err(|err| {
                WsClientError::InitError(format!("create stream: {:?}, tag:{}", err, tag))
            })?;
            let mut thread = SubscriptionThreadContext {
                receiver,
                inflight_reqs: BTreeMap::new(),
                subscriptions: BTreeMap::new(),
                id: 0,
                stream,
                epoch: epoch.clone(),
                tag,
                buffer: Vec::new(),
                auto_resubscribe: cfg.auto_resubscribe,
                alive: cfg.alive.clone(),
            };
            if poll_interval.as_nanos() == 0 {
                let start = Instant::now();
                thread.stream.blocking(1, |n| n.connect()).map_err(|err| {
                    WsClientError::InitError(format!(
                        "create stream: {:?}, tag:{}",
                        err, thread.tag
                    ))
                })?;
                poll_interval = start.elapsed() / 8; // 3 rtt
                glog::info!("[{}] poll interval = {:?}", thread.tag, poll_interval);
            }
            // let mut last_time = Instant::now();
            move || loop {
                let updated = thread.poll();
                if !thread.alive.is_alive() {
                    break;
                }
                if !updated {
                    // if last_time.elapsed().as_secs() > 1 {
                    //     glog::info!(
                    //         "stat: inflight_req:{:?}, {:?}, buf_size: {:?}",
                    //         thread.inflight_reqs.len(),
                    //         thread.subscriptions.len(),
                    //         thread.buffer.capacity(),
                    //     );
                    //     last_time = Instant::now();
                    // }
                    std::thread::sleep(poll_interval);
                }
            }
        });

        Ok(Self {
            tag,
            alive: cfg.alive.clone(),
            cfg,
            epoch,
            sender,
            handle: Arc::new(Mutex::new(Some(handle))),
        })
    }

    pub fn epoch(&self) -> usize {
        self.epoch.load(Ordering::SeqCst)
    }

    pub fn disconnect_all(&self) {
        self.alive.shutdown();
    }

    /// Errors:
    ///  * [`WsClientError::Exited`]:
    ///  * [`WsClientError::SubscribeError`]/[`WsClientError::SubscribeResponse`]
    pub fn subscribe<T, E>(
        &self,
        method: &str,
        unsub_method: &str,
        params: E,
    ) -> Result<WsSubscription<T>, WsClientError>
    where
        T: DeserializeOwned + 'static,
        E: Serialize + std::fmt::Debug,
    {
        let params_tag = format!("{:?}", params);
        let params = serde_json::to_raw_value(&params)
            .map_err(|err| WsClientError::SubscribeError(format!("{:?}", err)))?;

        let (resp, response_receiver) = mpsc::channel();
        let req = SubscribeRequest {
            method: method.into(),
            params,
            unsub_method: unsub_method.into(),
        };
        let msg = JsonrpcWsAction::Subscribe { req, resp };
        match self.sender.send(msg) {
            Ok(_) => {}
            Err(_) => return Err(WsClientError::Exited),
        };
        let response = response_receiver
            .recv()
            .map_err(|_| WsClientError::Exited)??;
        Ok(WsSubscription {
            tag: format!("{}-{}-{}", self.tag, method, params_tag),
            id: response.id,
            receiver: response.receiver,
            sender: self.sender.clone(),
            formatter: Box::new(WsSubscription::json_formatter),
        })
    }

    pub fn jsonrpc(
        &self,
        req: Batchable<JsonrpcRawRequest>,
    ) -> Result<Batchable<JsonrpcResponseRawResult>, WsClientError> {
        let (_, resp) = self.wait_result(self.jsonrpc_async(req)?)?;
        Ok(resp)
    }

    pub fn jsonrpc_concurrent(
        &self,
        req: Batchable<JsonrpcRawRequest>,
        max: usize,
    ) -> Result<Batchable<JsonrpcResponseRawResult>, WsClientError> {
        match req {
            Batchable::Single(req) => self.jsonrpc(Batchable::Single(req)),
            Batchable::Batch(list) => {
                if list.len() < max {
                    return self.jsonrpc(Batchable::Batch(list));
                }
                let mut responses = Vec::with_capacity(list.len());
                let batch_list = split_vec(list, max);
                let mut futures = Vec::with_capacity(batch_list.len());
                for req in batch_list.into_iter() {
                    futures.push(self.jsonrpc_async(Batchable::Batch(req))?);
                }
                for future in futures.into_iter() {
                    let (_, result) = self.wait_result(future)?;
                    match result {
                        Batchable::Single(item) => responses.push(item),
                        Batchable::Batch(response) => {
                            for item in response.into_iter() {
                                responses.push(item);
                            }
                        }
                    }
                }
                Ok(Batchable::Batch(responses))
            }
        }
    }

    pub fn wait_result<T>(
        &self,
        receiver: mpsc::Receiver<Result<T, WsClientError>>,
    ) -> Result<T, WsClientError> {
        let response = receiver.recv().map_err(|_| WsClientError::Exited)??;
        Ok(response)
    }

    pub fn jsonrpc_async_sender(
        &self,
        req: Batchable<JsonrpcRawRequest>,
        ctx: CallContext,
        resp: mpsc::Sender<
            Result<(CallContext, Batchable<JsonrpcResponseRawResult>), WsClientError>,
        >,
    ) -> Result<(), WsClientError> {
        let msg = JsonrpcWsAction::Jsonrpc { req, ctx, resp };
        match self.sender.send(msg) {
            Ok(_) => {}
            Err(_) => return Err(WsClientError::Exited),
        };
        Ok(())
    }

    pub fn jsonrpc_async(
        &self,
        req: Batchable<JsonrpcRawRequest>,
    ) -> Result<
        mpsc::Receiver<Result<(CallContext, Batchable<JsonrpcResponseRawResult>), WsClientError>>,
        WsClientError,
    > {
        let (resp, response_receiver) = mpsc::channel();
        self.jsonrpc_async_sender(req, Box::new(()), resp)?;
        Ok(response_receiver)
    }
}

struct SubscriptionThreadContext {
    tag: String,
    receiver: mpsc::Receiver<JsonrpcWsAction>,
    inflight_reqs: BTreeMap<u32, JsonrpcWsAction>,
    subscriptions: BTreeMap<String, SubscriptionInfo>,
    auto_resubscribe: bool,
    id: u32,
    stream: WsStreamClient,
    epoch: Arc<AtomicUsize>,
    buffer: Vec<u8>,
    alive: Alive,
}

#[allow(dead_code)]
struct ServerResponseValue {
    id: Option<u32>,
    value: serde_json::Value,
}

impl ServerResponseValue {
    #[allow(dead_code)]
    pub fn parse(data: &mut Vec<u8>) -> Result<Self, String> {
        let mut new = Vec::new();
        std::mem::swap(&mut new, data);
        let response: serde_json::Value = match serde_json::from_slice(&new) {
            Ok(value) => value,
            Err(err) => {
                return Err(format!(
                    "receive illegal response: ({}){}, {:?}, disconnect",
                    data.len(),
                    String::from_utf8_lossy(&data),
                    err
                ));
            }
        };
        let response_id = match response.as_array() {
            Some(list) => {
                if let Some(item) = list.get(0) {
                    item.get("id")
                } else {
                    return Err(format!("unexpected response: {:?}", response));
                }
            }
            None => response.get("id"),
        };
        let id = match response_id {
            Some(id) => {
                let id = match id.as_u64() {
                    Some(id) => id as u32,
                    None => {
                        return Err(format!(
                            "id has an unexpected type: {:?}, disconnect",
                            response
                        ));
                    }
                };
                Some(id)
            }
            None => None,
        };
        Ok(Self {
            id,
            value: response,
        })
    }

    #[allow(dead_code)]
    pub fn parse_body<T>(self) -> Result<T, serde_json::Error>
    where
        T: serde::de::DeserializeOwned,
    {
        serde_json::from_value(self.value)
    }
}

#[derive(Debug)]
struct ServerResponseRaw {
    id: Option<u32>,
    buffer: Vec<u8>,
}

impl ServerResponseRaw {
    pub fn parse(data: &mut Vec<u8>) -> Result<Self, String> {
        #[derive(Debug, Deserialize)]
        #[cfg_attr(feature = "sgx_enclave", serde(crate = "serde_sgx"))]
        struct RawResponse {
            id: Option<u32>,
        }
        let mut owned = Vec::new();
        std::mem::swap(&mut owned, data);

        let response: Batchable<RawResponse> = match Batchable::parse(&owned) {
            Ok(value) => value,
            Err(err) => {
                return Err(format!(
                    "receive illegal response: ({}){}, {:?}, disconnect",
                    owned.len(),
                    String::from_utf8_lossy(&owned),
                    err
                ));
            }
        };
        let response_id = match response {
            Batchable::Batch(list) => {
                if let Some(item) = list.get(0) {
                    item.id
                } else {
                    return Err(format!("unexpected response: {:?}", list));
                }
            }
            Batchable::Single(response) => response.id,
        };
        Ok(Self {
            id: response_id,
            buffer: owned,
        })
    }

    pub fn parse_body<T>(self) -> Result<T, String>
    where
        for<'a> T: serde::de::Deserialize<'a>,
    {
        match serde_json::from_slice(&self.buffer) {
            Ok(n) => Ok(n),
            Err(err) => {
                return Err(format!(
                    "illegal response({}): {:?}, disconnect",
                    String::from_utf8_lossy(&self.buffer),
                    err
                ));
            }
        }
    }

    pub fn parse_response(self) -> Result<Batchable<JsonrpcResponseRawResult>, String> {
        match Batchable::parse(&self.buffer) {
            Ok(n) => Ok(n),
            Err(err) => {
                return Err(format!(
                    "illegal response({}): {:?}, disconnect",
                    String::from_utf8_lossy(&self.buffer),
                    err
                ));
            }
        }
    }
}

impl SubscriptionThreadContext {
    fn poll(&mut self) -> bool {
        if !self.try_reconnect_stream() {
            return true;
        }
        let mut updated = false;
        // from manager/subscription
        match self.receiver.try_recv() {
            Ok(act) => {
                updated = true;
                if !self.handle_action(act) {
                    return true;
                }
            }
            Err(mpsc::TryRecvError::Disconnected) => {
                // `JsonrpcWsClient` and all `WsSubscription` dropped
                self.alive.shutdown();
                return false;
            }
            Err(mpsc::TryRecvError::Empty) => {}
        }

        // from remote
        self.buffer.clear();
        self.buffer.shrink_to_fit();
        updated |= match self.stream.read(&mut self.buffer) {
            Ok(ty) => self.handle_response_in_buffer(ty),
            Err(WsError::InitError(msg)) => {
                glog::error!("read fail: {:?}", msg);
                false
            }
            Err(WsError::WouldBlock) => false,
            Err(WsError::ServerDisconnected { temporary: true }) => false,
            Err(WsError::ServerDisconnected { temporary: false }) => {
                // going to exit
                self.alive.shutdown();
                return false;
            }
        };
        updated
    }

    // return: event handled
    fn handle_response_in_buffer(&mut self, ty: WsDataType) -> bool {
        glog::debug!(target: "body", "[{}] new response: {:?} {}", self.tag, ty, summarize_str(&String::from_utf8_lossy(&self.buffer), 1024) );
        let response = match ServerResponseRaw::parse(&mut self.buffer) {
            Ok(response) => response,
            Err(err) => {
                glog::error!("[{}] {}", self.tag, err);
                self.stream.disconnect("parse server response fail");
                return false;
            }
        };
        // let is_batch = self.buffer.get(0).cloned() == Some('[' as u8);
        // if is_batch {
        //     return true;
        // }
        match response.id {
            Some(id) => match self.inflight_reqs.remove(&id) {
                Some(act) => {
                    let result = response.parse_response();
                    match result {
                        Ok(response) => {
                            self.handle_server_response(act, response);
                            true
                        }
                        Err(err) => {
                            glog::error!("[{}] {}", self.tag, err);
                            self.stream.disconnect("parse response fail");
                            return false;
                        }
                    }
                }
                None => {
                    glog::error!(
                        "fail to find the corresponding request: {}, disconnect",
                        String::from_utf8_lossy(&response.buffer),
                    );
                    self.stream
                        .disconnect("fail to find the corresponding request");
                    return false;
                }
            },
            None => {
                let response: ServerPushSubscription = match response.parse_body() {
                    Ok(value) => value,
                    Err(err) => {
                        glog::error!(
                            "[{}] illegal ServerPushSubscription({:?}): {}",
                            self.tag,
                            String::from_utf8_lossy(&self.buffer),
                            err
                        );
                        self.stream.disconnect("illegal ServerPushSubscription");
                        return true;
                    }
                };
                self.handle_server_push(response);
                true
            }
        }
    }

    fn handle_server_response(
        &mut self,
        act: JsonrpcWsAction,
        response: Batchable<JsonrpcResponseRawResult>,
    ) {
        match act {
            JsonrpcWsAction::Reconnect {} => {}
            JsonrpcWsAction::Jsonrpc { req: _, ctx, resp } => {
                // glog::info!("{:?}", response);
                let _ = resp.send(Ok((ctx, response)));
            }
            JsonrpcWsAction::Subscribe { req, resp } => match response.single().unwrap() {
                JsonrpcResponseRawResult::Ok(response) => {
                    let subscription_id = match response.to_json() {
                        Ok(id) => id,
                        Err(_) => {
                            let _ = resp.send(Err(WsClientError::SubscribeError(format!(
                                "illegal result for subscribe response: {:?}",
                                response.result
                            ))));
                            return;
                        }
                    };
                    let (sender, receiver) = mpsc::channel();
                    let id = glog::ReqId::gen();
                    let info = SubscriptionInfo {
                        id: id.clone(),
                        subscription_id,
                        req,
                        sender,
                        exited: false,
                    };
                    self.subscriptions
                        .insert(info.subscription_id.clone(), info);
                    let _ = resp.send(Ok(SubscribeResponse { id, receiver }));
                }
                JsonrpcResponseRawResult::Err(response) => {
                    let _ = resp.send(Err(WsClientError::SubscribeResponse(response.error)));
                }
            },
            JsonrpcWsAction::Resubscribe { mut info } => match response.single().unwrap() {
                JsonrpcResponseRawResult::Ok(response) => {
                    info.subscription_id = match response.to_json() {
                        Ok(id) => id,
                        Err(_) => {
                            let msg = format!(
                                "illegal result for subscribe response: {:?}",
                                response.result
                            );
                            let _ = info.sender.send(Err(WsClientError::SubscribeError(msg)));
                            return;
                        }
                    };
                    self.subscriptions
                        .insert(info.subscription_id.clone(), info);
                }
                JsonrpcResponseRawResult::Err(response) => {
                    glog::error!("error in resubscribe: {:?}", response.error);
                    let _ = info
                        .sender
                        .send(Err(WsClientError::SubscribeResponse(response.error)));
                }
            },
            JsonrpcWsAction::SubscriptionExit { id } => {
                glog::info!(
                    "[{}] receive unsubscribe response for {}: {:?}",
                    self.tag,
                    id,
                    response
                );
                if let Some((sub_id, _)) = self.subscriptions.iter().find(|(_, v)| v.id.eq(&id)) {
                    let sub_id = sub_id.clone();
                    self.subscriptions.remove(&sub_id);
                }
            }
        }
    }

    fn handle_server_push(&mut self, response: ServerPushSubscription) {
        let subscription = match self.subscriptions.get_mut(&response.params.subscription) {
            Some(s) => s,
            None => return,
        };
        match subscription.sender.send(Ok(response.params.result)) {
            Ok(_) => {}
            Err(_) => {
                // maybe our first SubscriptionExit request failed, retry
                self.id += 1;
                let id = subscription.id;
                let sub_id = subscription.subscription_id.clone();
                let request = subscription.unsubscribe(self.id);
                match self.send(request.as_bytes()) {
                    Ok(_) => {
                        self.inflight_reqs
                            .insert(self.id, JsonrpcWsAction::SubscriptionExit { id });
                    }
                    Err(err) => {
                        glog::error!(
                            "[{}][{}] retry sending unsubscribe fail: {:?}",
                            self.tag,
                            sub_id,
                            err
                        );
                    }
                }
            }
        }
    }

    fn try_reconnect_stream(&mut self) -> bool {
        if self.stream.is_connected() {
            return true;
        }
        match self.stream.connect() {
            Ok(_) => {
                self.on_reconnect();
                true
            }
            Err(WsError::WouldBlock) => false,
            Err(err) => {
                glog::error!("[{}] try reconnect fail: {:?}", self.tag, err);
                std::thread::sleep(Duration::from_secs(1));
                false
            }
        }
    }

    fn drain_all<K, V>(map: &mut BTreeMap<K, V>) -> BTreeMap<K, V>
    where
        K: Clone + Ord,
    {
        let mut new_map = BTreeMap::new();
        std::mem::swap(&mut new_map, map);
        new_map
    }

    fn on_reconnect(&mut self) {
        let epoch = self.stream.epoch();
        if self.epoch.load(Ordering::SeqCst) >= epoch {
            return;
        }
        self.epoch.store(epoch, Ordering::SeqCst);

        if !self.auto_resubscribe {
            for (_, req) in Self::drain_all(&mut self.inflight_reqs) {
                // we don't subscribe anymore, so maybe we should let it down
                match req {
                    JsonrpcWsAction::Reconnect {} => {}
                    JsonrpcWsAction::Subscribe { resp, .. } => {
                        // we're not sure whether the server receives the message.
                        let _ = resp.send(Err(WsClientError::Exited));
                    }
                    JsonrpcWsAction::Resubscribe { info } => {
                        let _ = info.sender.send(Err(WsClientError::Exited));
                    }
                    JsonrpcWsAction::SubscriptionExit { .. } => {}
                    JsonrpcWsAction::Jsonrpc { resp, .. } => {
                        let _ = resp.send(Err(WsClientError::Exited));
                    }
                }
            }
            self.subscriptions.clear();
            return;
        }
        for (_, act) in Self::drain_all(&mut self.inflight_reqs) {
            match act {
                JsonrpcWsAction::Reconnect {} => {}
                JsonrpcWsAction::Subscribe { req, resp } => {
                    self.id += 1;
                    match self.send(req.subscribe(&self.tag, self.id).as_bytes()) {
                        Ok(_) => {
                            self.inflight_reqs
                                .insert(self.id, JsonrpcWsAction::Subscribe { req, resp });
                        }
                        Err(err) => {
                            glog::warn!("[{}] try to subscribe fail: {:?}", self.tag, err);
                            let _ = resp.send(Err(WsClientError::Exited));
                        }
                    }
                }
                JsonrpcWsAction::Resubscribe { info } => {
                    if info.exited {
                        continue;
                    }
                    self.id += 1;
                    match self.send(info.req.subscribe(&self.tag, self.id).as_bytes()) {
                        Ok(_) => {
                            self.inflight_reqs
                                .insert(self.id, JsonrpcWsAction::Resubscribe { info });
                        }
                        Err(err) => {
                            glog::warn!("[{}] try to resubscribe fail: {:?}", self.tag, err);
                            let _ = info.sender.send(Err(WsClientError::Exited));
                        }
                    }
                }
                JsonrpcWsAction::SubscriptionExit { .. } => {} // ignore
                JsonrpcWsAction::Jsonrpc { resp, .. } => {
                    let _ = resp.send(Err(WsClientError::Exited));
                }
            }
        }
        for (_, info) in Self::drain_all(&mut self.subscriptions) {
            self.id += 1;
            match self.send(info.req.subscribe(&self.tag, self.id).as_bytes()) {
                Ok(_) => {
                    self.inflight_reqs
                        .insert(self.id, JsonrpcWsAction::Resubscribe { info });
                }
                Err(err) => {
                    glog::warn!("[{}] try to resubscribe fail: {:?}", self.tag, err);
                    let _ = info.sender.send(Err(WsClientError::Exited));
                }
            }
        }
        glog::info!("[{}] reconnect finish", self.tag);
    }

    fn send(&mut self, buf: &[u8]) -> Result<(), WsClientError> {
        glog::debug!(target: "body", "[{}] send body: {}", self.tag, summarize_str(&String::from_utf8_lossy(&buf), 1024));
        match self.stream.write(buf) {
            Ok(_) => Ok(()),
            Err(WsError::ServerDisconnected { temporary: false }) => Err(WsClientError::Exited),
            Err(WsError::ServerDisconnected { temporary: true }) => {
                Err(WsClientError::SubscribeError(format!(
                    "write fail request={:?} fail, temporary down",
                    String::from_utf8_lossy(buf),
                )))
            }
            Err(_) => unreachable!(),
        }
    }

    fn handle_action(&mut self, act: JsonrpcWsAction) -> bool {
        match &act {
            JsonrpcWsAction::Reconnect {} => {
                self.stream.disconnect("client request to reconnect");
                self.try_reconnect_stream();
                return true;
            }
            JsonrpcWsAction::Subscribe { req, resp } => {
                self.id += 1;
                match self.send(req.subscribe(&self.tag, self.id).as_bytes()) {
                    Ok(_) => {
                        self.inflight_reqs.insert(self.id, act);
                    }
                    Err(err) => {
                        let _ = resp.send(Err(err));
                    }
                }
            }
            JsonrpcWsAction::Jsonrpc { req, ctx: _, resp } => {
                let mut req = req.clone();
                let start_id = self.id + 1;
                req.apply(|item| {
                    self.id += 1;
                    item.id = self.id.into();
                });
                // if req.is_batch() {
                //     resp.send(Ok((ctx.clone(), Batchable::Batch(Vec::new()))));
                //     return;
                // }
                let req_result = serde_json::to_vec(&req);
                // if req.is_batch() {
                //     return;
                // }
                match req_result {
                    Ok(buf) => match self.send(&buf) {
                        Ok(_) => {
                            self.inflight_reqs.insert(start_id, act);
                        }
                        Err(err) => {
                            let _ = resp.send(Err(err));
                        }
                    },
                    Err(err) => {
                        let _ =
                            resp.send(Err(WsClientError::DeserializeError(format!("{:?}", err))));
                    }
                }
            }
            JsonrpcWsAction::SubscriptionExit { id } => {
                if let Some((sub_id, info)) =
                    self.subscriptions.iter_mut().find(|(_, v)| v.id.eq(id))
                {
                    self.id += 1;
                    let sub_id = sub_id.clone();
                    let request = info.unsubscribe(self.id);
                    match self.send(request.as_bytes()) {
                        Ok(_) => {
                            self.inflight_reqs.insert(self.id, act);
                        }
                        Err(WsClientError::Exited) => {}
                        Err(err) => {
                            glog::error!("[{}] [{}] unsubscribe fail: {:?}", self.tag, sub_id, err);
                        }
                    }
                }
            }
            JsonrpcWsAction::Resubscribe { .. } => unreachable!(),
        }
        return false;
    }
}

pub fn split_vec<T: Clone>(list: Vec<T>, n: usize) -> Vec<Vec<T>> {
    let mut out = Vec::with_capacity(list.len() / n + 1);
    let mut tmp = Vec::with_capacity(n);
    for item in list.into_iter() {
        tmp.push(item);
        if tmp.len() == n {
            out.push(tmp);
            tmp = Vec::with_capacity(n);
        }
    }
    if tmp.len() > 0 {
        out.push(tmp);
    }
    out
}

impl crate::RpcClient for JsonrpcWsClient {
    fn rpc_call(
        &self,
        params: Batchable<JsonrpcRawRequest>,
    ) -> Result<Batchable<JsonrpcResponseRawResult>, crate::RpcError> {
        let result = match self.cfg.concurrent_bench {
            Some(max) => self.jsonrpc_concurrent(params, max),
            None => self.jsonrpc(params),
        };

        result.map_err(|err| crate::RpcError::RecvResponseError(format!("{:?}", err)))
    }
}
