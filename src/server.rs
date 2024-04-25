use std::prelude::v1::*;
use std::sync::mpsc::TrySendError;

use crate::{
    Batchable, JsonrpcErrorObj, JsonrpcRawRequest, JsonrpcRawResponseFull, RpcEncrypt,
    ServerPushSubscription, ServerPushSubscriptionParams,
};

use super::RpcError;
use base::trace::Alive;
use core::panic::RefUnwindSafe;
use core::time::Duration;
use net_http::{
    HttpMethod, HttpRequestReader, HttpResponse, HttpResponseBuilder, HttpServerConns,
    HttpServerContext, HttpWsServer, HttpWsServerConfig, HttpWsServerContext, HttpWsServerHandler,
    TickResult, WsDataType, WsServerConns,
};
use serde::de::DeserializeOwned;

use serde_json::BoxRawValue;
use std::collections::BTreeMap;
use std::sync::{mpsc, Arc, Mutex};
use std::time::Instant;
use threadpool::ThreadPool;

pub trait RpcServerContext: Sync + Send {}

pub trait RpcServerSubscription<T>: Send + Sync {
    fn methods(&self) -> (&'static str, &'static str, &'static str);
    fn on_subscribe(&self, params: &str) -> Result<String, JsonrpcErrorObj>;
    fn on_unsubscribe(&self, id: &str) -> bool;
    fn on_dispatch<'a>(&self, new_item: &T, ids: Vec<&'a str>) -> Vec<(BoxRawValue, Vec<&'a str>)>;
}

impl<N, T> RpcServerSubscription<N> for Arc<T>
where
    T: RpcServerSubscription<N>,
{
    fn methods(&self) -> (&'static str, &'static str, &'static str) {
        T::methods(&self)
    }
    fn on_dispatch<'a>(&self, new_item: &N, ids: Vec<&'a str>) -> Vec<(BoxRawValue, Vec<&'a str>)> {
        T::on_dispatch(&self, new_item, ids)
    }
    fn on_subscribe(&self, params: &str) -> Result<String, JsonrpcErrorObj> {
        T::on_subscribe(&self, params)
    }
    fn on_unsubscribe(&self, id: &str) -> bool {
        T::on_unsubscribe(&self, id)
    }
}

pub struct RpcServer<H: RefUnwindSafe + Send + Sync + 'static, N: Send + 'static = ()> {
    alive: Alive,
    srv: HttpWsServer<RpcServerProxy<H, N>>,
}

#[derive(Debug)]
pub struct RpcServerConfig {
    pub listen_addr: String,
    pub tls_cert: Vec<u8>,
    pub tls_key: Vec<u8>,
    pub http_max_body_length: Option<usize>,
    pub ws_frame_size: usize,
    pub threads: usize,
    pub max_idle_secs: Option<usize>,
}

impl Default for RpcServerConfig {
    fn default() -> Self {
        Self {
            listen_addr: "".into(),
            tls_cert: Vec::new(),
            tls_key: Vec::new(),
            http_max_body_length: Some(128 << 20),
            ws_frame_size: 64 << 10,
            max_idle_secs: Some(60),
            threads: 8,
        }
    }
}

impl RpcServerConfig {
    pub fn tls(&mut self, path: &str) -> Result<(), String> {
        if path != "" {
            let key = base::fs::read_file(&format!("{}.key", path))?;
            let crt = base::fs::read_file(&format!("{}.crt", path))?;
            self.tls_cert = crt;
            self.tls_key = key;
        }
        Ok(())
    }
}

pub trait RpcServerApi<N: Send>: RefUnwindSafe + Send + Sync + Sized {
    fn init_api(self: &Arc<Self>, srv: &mut RpcServer<Self, N>);
}

impl<H: Send + Sync + RpcServerApi<N>, N: Send> RpcServer<H, N> {
    pub fn api(alive: Alive, cfg: RpcServerConfig, context: Arc<H>) -> Result<Self, RpcError> {
        let mut srv = Self::new(alive, cfg, context.clone())?;
        context.init_api(&mut srv);
        Ok(srv)
    }
}

pub struct RpcArgs<'a, T: DeserializeOwned = ()> {
    pub path: &'a str,
    pub method: &'a str,
    pub params: T,
    pub session: RpcSession,
}

impl<'a> RpcArgs<'a, BoxRawValue> {
    pub fn serde<T: DeserializeOwned>(&self) -> Result<RpcArgs<'a, T>, serde_json::Error> {
        let mut params = self.params.get();
        if params == "[]" {
            params = "null";
        }
        Ok(RpcArgs {
            path: self.path.clone(),
            method: self.method.clone(),
            params: serde_json::from_str(&params)?,
            session: self.session.clone(),
        })
    }
}

impl<'a, T: DeserializeOwned> RpcArgs<'a, T> {
    pub fn map<F, E>(&self, f: F) -> RpcArgs<'a, E>
    where
        F: FnOnce(&T) -> E,
        E: DeserializeOwned,
    {
        RpcArgs {
            path: self.path.clone(),
            method: self.method.clone(),
            params: f(&self.params),
            session: self.session.clone(),
        }
    }
}

impl<H: Send + Sync + RefUnwindSafe, N: Send> RpcServer<H, N> {
    pub fn new(alive: Alive, cfg: RpcServerConfig, context: Arc<H>) -> Result<Self, RpcError> {
        let threads = cfg.threads.max(1);
        let (sender, receiver) = mpsc::sync_channel(threads * 100);
        let (subscription_sender, subscription_receiver) = mpsc::sync_channel(threads * 100);
        let tp = threadpool::Builder::new()
            .num_threads(threads)
            .thread_name("jsonrpc-server".into())
            .build();
        let proxy: RpcServerProxy<H, N> = RpcServerProxy {
            tp: Mutex::new(tp),
            context,
            jsonrpc_methods: BTreeMap::new(),
            http_methods: BTreeMap::new(),
            sender,
            receiver: Mutex::new(receiver),

            subscriptions: Arc::new(Mutex::new(BTreeMap::new())),
            subscription_sender,
            subscription_receiver: Mutex::new(subscription_receiver),
            subscribe_handler: Vec::new(),
        };
        let ws_cfg = HttpWsServerConfig {
            listen_addr: cfg.listen_addr.clone(),
            tls_cert: cfg.tls_cert,
            tls_key: cfg.tls_key,
            http_max_body_length: cfg.http_max_body_length,
            frame_size: cfg.ws_frame_size,
            max_idle_secs: cfg.max_idle_secs,
        };
        let srv = HttpWsServer::new(ws_cfg, proxy)
            .map_err(|err| RpcError::InitError(format!("{}", err)))?;

        glog::info!("jsonrpc server will be running on: {}", cfg.listen_addr);
        Ok(RpcServer { alive, srv })
    }

    pub fn default_jsonrpc<F>(&mut self, f: F)
    where
        F: Fn(&H, RpcArgs<BoxRawValue>) -> Result<BoxRawValue, JsonrpcErrorObj>
            + RefUnwindSafe
            + Send
            + Sync
            + 'static,
    {
        self.srv
            .handler()
            .jsonrpc_methods
            .insert("".into(), Arc::new(f));
    }

    pub fn http_get<F>(&mut self, path: &str, func: F)
    where
        F: Fn(&H, HttpRequestReader) -> HttpResponse + 'static + Sync + Send,
    {
        glog::info!("register http handler: {}", path);
        self.srv
            .handler()
            .http_methods
            .insert(path.into(), Arc::new(func));
    }

    pub fn subscribe<S>(&mut self, handler: S)
    where
        S: RpcServerSubscription<N> + 'static,
    {
        glog::info!("register subscribe handler: {:?}", handler.methods());
        let proxy = self.srv.handler();
        proxy.subscribe_handler.push(Arc::new(handler));
    }

    pub fn jsonrpc<F, P, R>(&mut self, method: &str, func: F)
    where
        F: Fn(&H, RpcArgs<P>) -> Result<R, JsonrpcErrorObj> + 'static + Sync + Send + RefUnwindSafe,
        R: serde::Serialize,
        for<'a> P: serde::de::Deserialize<'a>,
    {
        let wrap = move |ctx: &H, params: RpcArgs<BoxRawValue>| {
            let params = match params.serde() {
                Ok(params) => params,
                Err(err) => {
                    return Err(JsonrpcErrorObj::client(format!(
                        "decode params fail: {:?}, {}, {}",
                        err,
                        std::any::type_name::<P>(),
                        params.params.get(),
                    )))
                }
            };
            let result = func(ctx, params)?;
            match serde_json::to_raw_value(&result) {
                Ok(val) => Ok(val),
                Err(err) => {
                    return Err(JsonrpcErrorObj::server("encode response fail", err));
                }
            }
        };
        glog::info!("register jsonrpc handler: {}", method);
        self.srv
            .handler()
            .jsonrpc_methods
            .insert(method.into(), Arc::new(wrap));
    }

    pub fn context(&mut self) -> Arc<H> {
        self.srv.handler().context.clone()
    }

    pub fn subscription_sender(&mut self) -> mpsc::SyncSender<N> {
        self.srv.handler().subscription_sender.clone()
    }

    pub fn run(&mut self) {
        while self.alive.is_alive() {
            match self.srv.tick() {
                TickResult::Busy => {}
                TickResult::Idle => {
                    base::thread::sleep_ms(10);
                }
                TickResult::Error => {
                    break;
                }
            }
        }
    }
}

impl<H, N: Send> RpcServer<H, N>
where
    H: Send + Sync + RpcEncrypt + RefUnwindSafe,
{
    pub fn jsonrpc_sec<F, P, R>(&mut self, method: &str, func: F)
    where
        F: Fn(&H, RpcArgs<P>) -> Result<R, JsonrpcErrorObj> + 'static + Sync + Send + RefUnwindSafe,
        P: DeserializeOwned,
        R: serde::Serialize,
    {
        let wrap = move |ctx: &H, params: RpcArgs<BoxRawValue>| {
            let enc_args: (H::EncKey, H::EncType) = match serde_json::from_raw_value(&params.params)
            {
                Ok(val) => val,
                Err(err) => {
                    return Err(JsonrpcErrorObj::client(format!(
                        "decode params fail: {:?}, {}, {}",
                        err,
                        std::any::type_name::<P>(),
                        params.params.get(),
                    )))
                }
            };
            let args: P = ctx.decrypt(&enc_args.0, enc_args.1)?;
            let args = RpcArgs {
                method: params.method,
                path: params.path,
                session: params.session,
                params: args,
            };
            let response = func(ctx, args)?;
            let response = ctx.encrypt(&enc_args.0, &response)?;
            serde_json::to_raw_value(&response)
                .map_err(|err| JsonrpcErrorObj::server("encode response fail", err))
        };
        self.srv
            .handler()
            .jsonrpc_methods
            .insert(method.into(), Arc::new(wrap));
    }
}

#[derive(Debug, Eq, Clone, Copy)]
enum RequestType {
    Http,
    Ws(WsDataType),
}

use core::cmp::Ordering;

impl Ord for RequestType {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl PartialOrd<RequestType> for RequestType {
    fn partial_cmp(&self, other: &RequestType) -> Option<Ordering> {
        Some(match self {
            Self::Http => match other {
                Self::Http => Ordering::Equal,
                Self::Ws(_) => Ordering::Less,
            },
            Self::Ws(_) => match other {
                Self::Http => Ordering::Greater,
                Self::Ws(_) => Ordering::Equal,
            },
        })
    }
}

impl PartialEq<RequestType> for RequestType {
    fn eq(&self, other: &RequestType) -> bool {
        match self {
            Self::Http => matches!(other, Self::Http),
            Self::Ws(_) => matches!(other, Self::Ws(_)),
        }
    }
}

#[derive(Debug, Eq, Ord, Clone, Copy)]
pub struct RpcSession {
    ty: RequestType,
    pub cid: usize,
}

impl PartialEq<RpcSession> for RpcSession {
    fn eq(&self, other: &RpcSession) -> bool {
        self.ty.eq(&other.ty) && self.cid == other.cid
    }
}

impl PartialOrd<RpcSession> for RpcSession {
    fn partial_cmp(&self, other: &RpcSession) -> Option<Ordering> {
        Some(match self.ty.partial_cmp(&other.ty)? {
            Ordering::Equal => self.cid.partial_cmp(&other.cid)?,
            other => other,
        })
    }
}

enum JsonrpcMethodHandler<H, N> {
    Call(
        Arc<
            dyn Fn(&H, RpcArgs<BoxRawValue>) -> Result<BoxRawValue, JsonrpcErrorObj>
                + Send
                + Sync
                + RefUnwindSafe,
        >,
    ),
    Subscribe(Arc<dyn RpcServerSubscription<N>>),
    Unsubscribe(Arc<dyn RpcServerSubscription<N>>),
}

impl<H, N> Clone for JsonrpcMethodHandler<H, N> {
    fn clone(&self) -> Self {
        match self {
            Self::Call(n) => Self::Call(n.clone()),
            Self::Subscribe(n) => Self::Subscribe(n.clone()),
            Self::Unsubscribe(n) => Self::Unsubscribe(n.clone()),
        }
    }
}

pub struct RpcServerProxy<H: Send + Sync, N = ()> {
    tp: Mutex<ThreadPool>,
    context: Arc<H>,
    jsonrpc_methods: BTreeMap<
        String,
        Arc<
            dyn Fn(&H, RpcArgs<BoxRawValue>) -> Result<BoxRawValue, JsonrpcErrorObj>
                + RefUnwindSafe
                + Send
                + Sync,
        >,
    >,
    http_methods:
        BTreeMap<String, Arc<dyn Fn(&H, HttpRequestReader) -> HttpResponse + Send + Sync>>,
    sender: mpsc::SyncSender<(RequestType, usize, Vec<u8>)>,
    receiver: Mutex<mpsc::Receiver<(RequestType, usize, Vec<u8>)>>,

    subscription_sender: mpsc::SyncSender<N>,
    subscription_receiver: Mutex<mpsc::Receiver<N>>,
    subscriptions: Arc<Mutex<BTreeMap<String, usize>>>,
    subscribe_handler: Vec<Arc<dyn RpcServerSubscription<N>>>,
}

impl<H: RefUnwindSafe + Send + Sync + 'static, N: Send + 'static> HttpWsServerHandler
    for RpcServerProxy<H, N>
{
    fn on_close_ws_conn(&mut self, conn_id: usize) {
        let mut subscriptions = self.subscriptions.lock().unwrap();
        let mut remove_id = Vec::new();
        for (key, id) in subscriptions.iter() {
            if conn_id == *id {
                remove_id.push(key.clone());
            }
        }
        for id in remove_id {
            subscriptions.remove(&id);
            for handler in &self.subscribe_handler {
                handler.on_unsubscribe(&id);
            }
            glog::info!("remove subscription[{}] due to remote conn closed", id);
        }
    }

    fn on_new_http_request(&mut self, ctx: &mut HttpServerContext, req: HttpRequestReader) {
        let success = match req.method() {
            HttpMethod::Post => {
                self.process_jsonrpc(req.path(), RequestType::Http, ctx.conn_id, req.body())
            }
            _ => self.process_request(ctx.conn_id, req),
        };
        if !success {
            // fail to process this request due to the full channel
            // we should close this connection.
            glog::info!("force close conn {}", ctx.conn_id);
            ctx.is_close = true;
        }
    }

    fn on_new_ws_conn(&mut self, _ctx: &mut HttpWsServerContext) {}

    fn on_new_ws_request(&mut self, ctx: &mut HttpWsServerContext, ty: WsDataType, data: Vec<u8>) {
        let succ = self.process_jsonrpc(ctx.path, RequestType::Ws(ty), ctx.conn_id, &data);
        if !succ {
            glog::info!("force close conn {}", ctx.conn_id);
            ctx.is_close = true;
        }
    }

    fn on_tick(&mut self, http: &mut HttpServerConns, ws: &mut WsServerConns) -> TickResult {
        let mut tick = TickResult::Idle;
        tick |= self.on_tick_response(http, ws);
        tick |= self.on_tick_subscription(ws);
        tick
    }
}

impl<H: RefUnwindSafe + Send + Sync + 'static, N: Send + 'static> RpcServerProxy<H, N> {
    fn execute_in_threadpool<T>(&self, conn_id: usize, task: T) -> bool
    where
        T: FnOnce() + Send + 'static,
    {
        let tp = self.tp.lock().unwrap();
        if tp.queued_count() > tp.max_count() * 2 {
            glog::warn!(
                "rejected request from conn={}, queued={},max={}",
                conn_id,
                tp.queued_count(),
                tp.max_count()
            );
            return false;
        }
        tp.execute(task);
        true
    }

    fn process_request(&self, conn_id: usize, req: HttpRequestReader) -> bool {
        let method = match self.http_methods.get(req.path()) {
            Some(method) => method.clone(),
            None => {
                let response = HttpResponseBuilder::not_found().to_vec();
                return Self::safe_send_to_channel(
                    &self.sender,
                    RequestType::Http,
                    conn_id,
                    response,
                );
            }
        };

        self.execute_in_threadpool(conn_id, {
            let ctx = self.context.clone();
            let sender = self.sender.clone();
            move || {
                let response = method(&ctx, req).to_vec();
                let _ = sender.send((RequestType::Http, conn_id, response));
            }
        })
    }

    fn process_jsonrpc(&self, path: &str, ty: RequestType, conn_id: usize, body: &[u8]) -> bool {
        let start = Instant::now();
        let reqs: Batchable<JsonrpcRawRequest> = match Batchable::parse(body) {
            Ok(n) => n,
            Err(err) => {
                let response = JsonrpcRawResponseFull::err(
                    JsonrpcErrorObj::client(format!("parse request fail: {:?}", err)),
                    None,
                );
                glog::info!(
                    "[elapsed={:?}] served jsonrpc: unknown request",
                    start.elapsed()
                );
                return Self::send_jsonrpc_result(true, &self.sender, ty, conn_id, response.into());
            }
        };
        let default_method = self
            .jsonrpc_methods
            .get("")
            .cloned()
            .map(<JsonrpcMethodHandler<H, N>>::Call);
        let methods = reqs.collect(|item| {
            self.jsonrpc_methods
                .get(&item.method)
                .map(|m| JsonrpcMethodHandler::Call(m.clone()))
                .or_else(|| {
                    if matches!(ty, RequestType::Ws(_)) {
                        for h in &self.subscribe_handler {
                            let (sub, unsub, _) = h.methods();
                            if item.method == sub {
                                return Some(JsonrpcMethodHandler::Subscribe(h.clone()));
                            } else if item.method == unsub {
                                return Some(JsonrpcMethodHandler::Unsubscribe(h.clone()));
                            }
                        }
                    }
                    None
                })
                .or(default_method.as_ref().map(|n| n.clone()))
        });
        let task = {
            let ctx = self.context.clone();
            let path = path.to_owned();
            let sender = self.sender.clone();
            let _subscribe_sender = self.subscription_sender.clone();
            let subscriptions = self.subscriptions.clone();
            let session = RpcSession { ty, cid: conn_id };
            move || {
                let mut idx = 0;
                let responses = reqs.map(|req| {
                    let queue_time = start.elapsed();
                    let method = &methods[idx];
                    idx += 1;
                    let err = match method {
                        Some(JsonrpcMethodHandler::Call(method)) => {
                            let params = RpcArgs {
                                path: &path,
                                method: &req.method,
                                params: req.params,
                                session,
                            };
                            let id = req.id.clone();
                            let run_method = || match method(&ctx, params) {
                                Ok(result) => {
                                    glog::info!(
                                        "[{}elapsed={:?}] served jsonrpc {}",
                                        if queue_time > Duration::from_secs(1) {
                                            format!("queue={:?},", queue_time)
                                        } else {
                                            format!("")
                                        },
                                        start.elapsed(),
                                        req.method,
                                    );
                                    Ok(JsonrpcRawResponseFull::ok(id, result))
                                }
                                Err(err) => {
                                    glog::warn!(
                                        "[{}elapsed={:?}] served jsonrpc {} fail: {}",
                                        if queue_time > Duration::from_secs(1) {
                                            format!("queue={:?},", queue_time)
                                        } else {
                                            format!("")
                                        },
                                        start.elapsed(),
                                        req.method,
                                        err.message,
                                    );
                                    Err(err)
                                }
                            };
                            match std::panic::catch_unwind(run_method) {
                                Ok(Ok(n)) => return n,
                                Ok(Err(err)) => err,
                                Err(err) => {
                                    let info = match err.downcast_ref::<String>() {
                                        Some(info) => info.as_str(),
                                        None => match err.downcast_ref::<&str>() {
                                            Some(info) => *info,
                                            None => "unknown",
                                        },
                                    };
                                    JsonrpcErrorObj::client(format!("server panick: {}", info))
                                }
                            }
                        }
                        Some(JsonrpcMethodHandler::Subscribe(h)) => {
                            match h.on_subscribe(req.params.get()) {
                                Ok(id) => {
                                    let new_id = serde_json::to_raw_value(&id).unwrap();
                                    glog::info!(
                                        "subscription[{}] added -> {}",
                                        id,
                                        req.params.get()
                                    );
                                    subscriptions.lock().unwrap().insert(id, conn_id);
                                    return JsonrpcRawResponseFull::ok(req.id, new_id);
                                }
                                Err(err) => err,
                            }
                        }
                        Some(JsonrpcMethodHandler::Unsubscribe(h)) => {
                            //
                            let id: (String,) = match serde_json::from_raw_value(&req.params) {
                                Ok(n) => n,
                                Err(err) => {
                                    return JsonrpcRawResponseFull::err(
                                        JsonrpcErrorObj::client(format!(
                                            "unexpected params: {:?}",
                                            err
                                        )),
                                        Some(req.id),
                                    );
                                }
                            };
                            subscriptions.lock().unwrap().remove(&id.0);

                            if h.on_unsubscribe(&id.0) {
                                let result = serde_json::to_raw_value(&()).unwrap();
                                return JsonrpcRawResponseFull::ok(req.id, result);
                            } else {
                                JsonrpcErrorObj::client(format!("subscrption not found"))
                            }
                        }
                        None => {
                            JsonrpcErrorObj::client(format!("method not found: {}", req.method))
                        }
                    };
                    JsonrpcRawResponseFull {
                        jsonrpc: "2.0".into(),
                        id: Some(req.id),
                        error: Some(err),
                        result: None,
                    }
                });

                let _ = Self::send_jsonrpc_result(false, &sender, ty, conn_id, responses);
            }
        };

        self.execute_in_threadpool(conn_id, task)
    }

    fn safe_send_to_channel(
        sender: &mpsc::SyncSender<(RequestType, usize, Vec<u8>)>,
        ty: RequestType,
        cid: usize,
        data: Vec<u8>,
    ) -> bool {
        match sender.try_send((ty, cid, data)) {
            Ok(()) => true,
            Err(TrySendError::Disconnected(_)) => false,
            Err(TrySendError::Full(_)) => {
                glog::warn!("fail to send response to conn={}", cid);
                false
            }
        }
    }

    fn send_jsonrpc_result(
        safe: bool,
        sender: &mpsc::SyncSender<(RequestType, usize, Vec<u8>)>,
        ty: RequestType,
        cid: usize,
        response: Batchable<JsonrpcRawResponseFull>,
    ) -> bool {
        let response = serde_json::to_vec(&response).unwrap();
        let response = if matches!(ty, RequestType::Http) {
            HttpResponseBuilder::new(200)
                .close()
                .json(response)
                .to_vec()
        } else {
            response
        };
        if safe {
            Self::safe_send_to_channel(sender, ty, cid, response)
        } else {
            let _ = sender.send((ty, cid, response));
            true
        }
    }

    fn on_tick_response(
        &mut self,
        http: &mut HttpServerConns,
        ws: &mut WsServerConns,
    ) -> TickResult {
        let mut tick = TickResult::Idle;
        {
            let receiver = self.receiver.lock().unwrap();
            for (ty, cid, response) in receiver.try_iter() {
                match ty {
                    RequestType::Http => {
                        if let Err(err) = http.write_to(cid, &response) {
                            glog::error!("write to http fail: {:?}", err);
                        }
                    }
                    RequestType::Ws(ty) => match ws.get_mut(cid) {
                        Some(srv) => {
                            if let Err(err) = srv.write_ty(ty, &response) {
                                glog::error!("write to ws fail: {:?}", err);
                            }
                        }
                        None => {
                            glog::warn!("send response to client fail: not found");
                        }
                    },
                }
                tick.to_busy();
            }
        }
        tick
    }

    fn on_tick_subscription(&mut self, ws: &mut WsServerConns) -> TickResult {
        let mut tick = TickResult::Idle;
        let receiver = self.subscription_receiver.lock().unwrap();
        let mut subscriptions = self.subscriptions.lock().unwrap();
        let ids = subscriptions.keys().map(|n| n.as_str()).collect::<Vec<_>>();

        let mut removed = Vec::new();
        for item in receiver.try_iter() {
            for handler in &self.subscribe_handler {
                let (_, _, method) = handler.methods();
                let mut push = ServerPushSubscription {
                    jsonrpc: "2.0".into(),
                    method: method.into(),
                    params: ServerPushSubscriptionParams::default(),
                };
                for (response, ids) in handler.on_dispatch(&item, ids.clone()) {
                    push.params.result = response;
                    for id in ids {
                        let cid = match subscriptions.get(id) {
                            Some(n) => *n,
                            None => {
                                continue;
                            }
                        };

                        push.params.subscription = id.into();
                        let response = serde_json::to_vec(&push).unwrap();

                        match ws.get_mut(cid) {
                            Some(srv) => {
                                if let Err(err) = srv.write_ty(WsDataType::Text, &response) {
                                    glog::error!("write to ws fail: {:?}", err);
                                }
                                tick.to_busy();
                            }
                            None => {
                                removed.push(id.to_owned());
                                glog::warn!("send response to client fail: not found");
                            }
                        }
                    }
                }
            }
        }

        if removed.len() > 0 {
            glog::info!("remove subscription id: {:?}", removed);
        }
        for item in removed {
            subscriptions.remove(&item);
            tick.to_busy();
        }
        tick
    }
}
