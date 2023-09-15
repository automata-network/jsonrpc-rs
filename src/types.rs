use std::prelude::v1::*;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Batchable<T> {
    Single(T),
    Batch(Vec<T>),
}

impl<T> From<T> for Batchable<T> {
    fn from(n: T) -> Self {
        Self::Single(n)
    }
}

impl<T> From<Vec<T>> for Batchable<T> {
    fn from(n: Vec<T>) -> Self {
        Self::Batch(n)
    }
}

impl<T: std::fmt::Debug> Batchable<T> {
    pub fn single(self) -> Option<T> {
        if let Self::Single(t) = self {
            return Some(t);
        }
        None
    }

    pub fn batch(self) -> Option<Vec<T>> {
        if let Self::Batch(t) = self {
            return Some(t);
        }
        None
    }

    pub fn is_single(&self) -> bool {
        !self.is_batch()
    }

    pub fn is_batch(&self) -> bool {
        match self {
            Self::Batch(_) => true,
            Self::Single(_) => false,
        }
    }

    pub fn to_vec(self) -> Vec<T> {
        match self {
            Batchable::Single(n) => vec![n],
            Batchable::Batch(n) => n,
        }
    }

    pub fn collect<F, E>(&self, f: F) -> Vec<E>
    where
        F: Fn(&T) -> E,
    {
        match self {
            Batchable::Batch(list) => {
                let mut out = Vec::with_capacity(list.len());
                for item in list {
                    out.push(f(item))
                }
                out
            }
            Batchable::Single(item) => vec![f(item)],
        }
    }

    pub fn apply<F>(&mut self, mut f: F)
    where
        F: FnMut(&mut T),
    {
        match self {
            Batchable::Batch(list) => {
                for item in list {
                    f(item)
                }
            }
            Batchable::Single(item) => f(item),
        }
    }

    pub fn map<F, E>(self, mut f: F) -> Batchable<E>
    where
        F: FnMut(T) -> E,
    {
        match self {
            Batchable::Batch(list) => {
                let mut new = Vec::with_capacity(list.len());
                for n in list {
                    new.push(f(n));
                }
                Batchable::Batch(new)
            }
            Batchable::Single(item) => Batchable::Single(f(item)),
        }
    }
}

impl<'de, T> Batchable<T>
where
    T: Deserialize<'de>,
{
    pub fn parse(data: &'de [u8]) -> Result<Self, serde_json::Error> {
        let first_char = data
            .iter()
            .position(|n| n != &(' ' as u8))
            .map(|idx| data[idx]);
        if first_char == Some('[' as u8) {
            Ok(Self::Batch(serde_json::from_slice(data)?))
        } else {
            Ok(Self::Single(serde_json::from_slice(data)?))
        }
    }
}

impl<T> Batchable<T>
where
    T: Clone,
{
    pub fn to_batch(vec: &Vec<Self>) -> Batchable<T> {
        let mut out = Vec::new();
        for item in vec {
            match item {
                Batchable::Single(item) => out.push(item.clone()),
                Batchable::Batch(list) => out.extend_from_slice(list.as_slice()),
            }
        }
        Batchable::Batch(out)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum Id {
    U64(u64),
    String(String),
}

impl From<u64> for Id {
    fn from(val: u64) -> Self {
        Self::U64(val)
    }
}

impl From<u32> for Id {
    fn from(val: u32) -> Self {
        Self::U64(val as _)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JsonrpcErrorObj {
    pub code: i64,
    pub message: String,
    pub data: Option<serde_json::Value>,
}

impl JsonrpcErrorObj {
    pub fn unknown<S>(msg: S) -> Self
    where
        S: std::fmt::Debug,
    {
        Self {
            code: -32400,
            message: format!("{:?}", msg),
            data: None,
        }
    }

    pub fn server<S>(info: &str, msg: S) -> Self
    where
        S: std::fmt::Debug,
    {
        Self {
            code: -32400,
            message: format!("{}: {:?}", info, msg),
            data: None,
        }
    }

    pub fn client(msg: String) -> Self {
        Self {
            code: -32400,
            message: msg,
            data: None,
        }
    }

    pub fn error(code: i64, msg: String) -> Self {
        Self {
            code,
            message: msg,
            data: None,
        }
    }

    pub fn match_err(&self, code: i64, msg: &str, data: Option<&str>) -> bool {
        if self.code != code || self.message != msg {
            return false;
        }
        match &self.data {
            Some(n) => match data {
                Some(d2) => n.as_str() == Some(d2),
                None => false,
            },
            None => data.is_none(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonrpcRawRequest {
    pub jsonrpc: String,
    pub method: String,
    #[serde(default = "default_params")]
    pub params: serde_json::BoxRawValue,
    pub id: Id,
}

fn default_params() -> serde_json::BoxRawValue {
    serde_json::to_raw_value(&vec![]).unwrap()
}

impl JsonrpcRawRequest {
    pub fn new_ex(method: &str, params: serde_json::BoxRawValue, id: u64) -> Self {
        Self {
            jsonrpc: "2.0".to_owned(),
            method: method.to_owned(),
            params,
            id: Id::U64(id),
        }
    }

    pub fn new<E>(id: u64, method: &str, params: &E) -> Result<Self, serde_json::Error>
    where
        E: Serialize,
    {
        let params = serde_json::to_raw_value(params)?;
        Ok(Self::new_ex(method, params, id))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonrpcRawResponseFull {
    pub jsonrpc: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::BoxRawValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<JsonrpcErrorObj>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<Id>,
}

impl JsonrpcRawResponseFull {
    pub fn ok(id: Id, result: serde_json::BoxRawValue) -> Self {
        Self {
            error: None,
            id: Some(id),
            result: Some(result),
            jsonrpc: "2.0".into(),
        }
    }
    pub fn err(err: JsonrpcErrorObj, id: Option<Id>) -> Self {
        Self {
            error: Some(err),
            id,
            result: None,
            jsonrpc: "2.0".into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(try_from = "JsonrpcRawResponseFull")]
pub enum JsonrpcResponseRawResult {
    Ok(JsonrpcRawResponse),
    Err(JsonrpcErrorResponse),
}

impl JsonrpcResponseRawResult {
    pub fn to_full(self) -> JsonrpcRawResponseFull {
        match self {
            JsonrpcResponseRawResult::Ok(v) => JsonrpcRawResponseFull {
                jsonrpc: v.jsonrpc,
                result: Some(v.result),
                error: None,
                id: Some(v.id),
            },
            JsonrpcResponseRawResult::Err(v) => JsonrpcRawResponseFull {
                jsonrpc: v.jsonrpc,
                result: None,
                error: Some(v.error),
                id: v.id,
            }
        }
    }
}

impl From<JsonrpcRawResponse> for JsonrpcResponseRawResult {
    fn from(ok: JsonrpcRawResponse) -> Self {
        Self::Ok(ok)
    }
}

impl From<JsonrpcResponseRawResult> for JsonrpcRawResponseFull {
    fn from(res: JsonrpcResponseRawResult) -> Self {
        match res {
            JsonrpcResponseRawResult::Ok(v) => Self {
                jsonrpc: v.jsonrpc,
                result: Some(v.result),
                error: None,
                id: Some(v.id),
            },
            JsonrpcResponseRawResult::Err(v) => Self {
                jsonrpc: v.jsonrpc,
                result: None,
                error: Some(v.error),
                id: v.id,
            }
        }
    }
}

impl From<JsonrpcRawResponseFull> for JsonrpcResponseRawResult {
    fn from(res: JsonrpcRawResponseFull) -> Self {
        match res.error {
            Some(err) => Self::Err(JsonrpcErrorResponse {
                jsonrpc: res.jsonrpc,
                id: res.id,
                error: err,
            }),
            None => {
                let result = match res.result {
                    Some(result) => result,
                    None => serde_json::RawValue::from_string("null".into()).unwrap(),
                };
                Self::Ok(JsonrpcRawResponse {
                    jsonrpc: res.jsonrpc,
                    result,
                    id: res.id.unwrap(),
                })
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonrpcRawResponse {
    pub jsonrpc: String,
    pub result: serde_json::BoxRawValue,
    pub id: Id,
}

impl JsonrpcRawResponse {
    pub fn to_json<T>(&self) -> Result<T, serde_json::Error>
    where
        T: serde::de::DeserializeOwned,
    {
        serde_json::from_raw_value(&self.result)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JsonrpcErrorResponse {
    pub jsonrpc: String,
    pub error: JsonrpcErrorObj,
    pub id: Option<Id>,
}
