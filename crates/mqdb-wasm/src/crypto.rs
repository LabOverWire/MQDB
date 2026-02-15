// Copyright 2027 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use js_sys::{Array, Uint8Array};
use mqdb::error::{Error, Result};
use std::rc::Rc;
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::JsFuture;
use web_sys::{AesGcmParams, AesKeyGenParams, CryptoKey, Pbkdf2Params, SubtleCrypto};

const NONCE_LEN: usize = 12;
const SALT_LEN: usize = 32;
const PBKDF2_ITERATIONS: u32 = 600_000;

pub(crate) const SALT_KEY: &[u8] = b"_crypto/salt";
pub(crate) const CHECK_KEY: &[u8] = b"_crypto/check";
pub(crate) const CHECK_PLAINTEXT: &[u8] = b"mqdb";

pub(crate) struct CryptoHandle {
    key: CryptoKey,
}

impl CryptoHandle {
    pub(crate) async fn derive_from_passphrase(passphrase: &str, salt: &[u8]) -> Result<Rc<Self>> {
        let subtle = get_subtle()?;
        let password_key = import_password_key(&subtle, passphrase.as_bytes()).await?;
        let salt_js = Uint8Array::from(salt);
        let pbkdf2_params = Pbkdf2Params::new(
            "PBKDF2",
            &JsValue::from_str("SHA-256"),
            PBKDF2_ITERATIONS,
            &salt_js,
        );
        let aes_params = AesKeyGenParams::new("AES-GCM", 256);
        let usages = js_str_array(&["encrypt", "decrypt"]);
        let promise = subtle
            .derive_key_with_object_and_object(
                &pbkdf2_params,
                &password_key,
                &aes_params,
                false,
                &usages,
            )
            .map_err(|e| js_to_err(&e))?;
        let result = JsFuture::from(promise).await.map_err(|e| js_to_err(&e))?;
        let key: CryptoKey = result.unchecked_into();
        Ok(Rc::new(Self { key }))
    }

    pub(crate) async fn encrypt(&self, storage_key: &[u8], plaintext: &[u8]) -> Result<Vec<u8>> {
        let subtle = get_subtle()?;
        let nonce = generate_nonce()?;
        let nonce_js = Uint8Array::from(nonce.as_slice());
        let algo = AesGcmParams::new("AES-GCM", &nonce_js);
        let aad = Uint8Array::from(storage_key);
        algo.set_additional_data(&aad);

        let promise = subtle
            .encrypt_with_object_and_u8_array(&algo, &self.key, plaintext)
            .map_err(|e| js_to_err(&e))?;
        let result = JsFuture::from(promise).await.map_err(|e| js_to_err(&e))?;
        let ciphertext = Uint8Array::new(&result);

        let mut output = Vec::with_capacity(NONCE_LEN + ciphertext.length() as usize);
        output.extend_from_slice(&nonce);
        let mut ct_buf = vec![0u8; ciphertext.length() as usize];
        ciphertext.copy_to(&mut ct_buf);
        output.extend_from_slice(&ct_buf);
        Ok(output)
    }

    pub(crate) async fn decrypt(&self, storage_key: &[u8], data: &[u8]) -> Result<Vec<u8>> {
        if data.len() < NONCE_LEN + 1 {
            return Err(Error::Internal("ciphertext too short".into()));
        }
        let (nonce_bytes, ciphertext) = data.split_at(NONCE_LEN);
        let subtle = get_subtle()?;
        let nonce_js = Uint8Array::from(nonce_bytes);
        let algo = AesGcmParams::new("AES-GCM", &nonce_js);
        let aad = Uint8Array::from(storage_key);
        algo.set_additional_data(&aad);

        let promise = subtle
            .decrypt_with_object_and_u8_array(&algo, &self.key, ciphertext)
            .map_err(|e| js_to_err(&e))?;
        let result = JsFuture::from(promise).await.map_err(|e| js_to_err(&e))?;
        let plaintext = Uint8Array::new(&result);
        let mut output = vec![0u8; plaintext.length() as usize];
        plaintext.copy_to(&mut output);
        Ok(output)
    }
}

pub(crate) fn generate_salt() -> Result<[u8; SALT_LEN]> {
    let crypto = get_crypto()?;
    let mut salt = [0u8; SALT_LEN];
    crypto
        .get_random_values_with_u8_array(&mut salt)
        .map_err(|e| js_to_err(&e))?;
    Ok(salt)
}

fn get_crypto() -> Result<web_sys::Crypto> {
    let window = web_sys::window().ok_or_else(|| Error::Internal("no window".into()))?;
    window
        .crypto()
        .map_err(|_| Error::Internal("crypto API not available".into()))
}

fn get_subtle() -> Result<SubtleCrypto> {
    Ok(get_crypto()?.subtle())
}

fn generate_nonce() -> Result<[u8; NONCE_LEN]> {
    let crypto = get_crypto()?;
    let mut nonce = [0u8; NONCE_LEN];
    crypto
        .get_random_values_with_u8_array(&mut nonce)
        .map_err(|e| js_to_err(&e))?;
    Ok(nonce)
}

async fn import_password_key(subtle: &SubtleCrypto, password: &[u8]) -> Result<CryptoKey> {
    let key_data = Uint8Array::from(password);
    let usages = js_str_array(&["deriveBits", "deriveKey"]);
    let promise = subtle
        .import_key_with_str("raw", &key_data, "PBKDF2", false, &usages)
        .map_err(|e| js_to_err(&e))?;
    let result = JsFuture::from(promise).await.map_err(|e| js_to_err(&e))?;
    Ok(result.unchecked_into())
}

fn js_str_array(values: &[&str]) -> JsValue {
    let arr = Array::new();
    for v in values {
        arr.push(&JsValue::from_str(v));
    }
    arr.into()
}

fn js_to_err(e: &JsValue) -> Error {
    Error::Internal(format!("{e:?}"))
}
