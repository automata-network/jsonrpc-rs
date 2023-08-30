use std::prelude::v1::*;

use serde::{de::DeserializeOwned, Serialize};

use crate::JsonrpcErrorObj;

pub trait RpcEncrypt {
    type EncKey: DeserializeOwned + Serialize;
    type EncType: DeserializeOwned + Serialize;
    fn decrypt<T: DeserializeOwned>(
        &self,
        k: &Self::EncKey,
        val: Self::EncType,
    ) -> Result<T, JsonrpcErrorObj>;
    fn encrypt<T: Serialize>(
        &self,
        k: &Self::EncKey,
        data: &T,
    ) -> Result<Self::EncType, JsonrpcErrorObj>;
}

impl RpcEncrypt for () {
    type EncKey = ();
    type EncType = ();
    fn decrypt<T: DeserializeOwned>(
        &self,
        _k: &Self::EncKey,
        _val: Self::EncType,
    ) -> Result<T, JsonrpcErrorObj> {
        unimplemented!()
    }
    fn encrypt<T: Serialize>(
        &self,
        _k: &Self::EncKey,
        _data: &T,
    ) -> Result<Self::EncType, JsonrpcErrorObj> {
        unimplemented!()
    }
}
