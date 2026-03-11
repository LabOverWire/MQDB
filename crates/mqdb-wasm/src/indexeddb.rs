// Copyright 2025-2026 LabOverWire. All rights reserved.
// SPDX-License-Identifier: AGPL-3.0-only

use js_sys::Uint8Array;
use mqdb::error::{Error, Result};
use mqdb::storage::{AsyncBatchOperations, AsyncStorageBackend};
use std::cell::RefCell;
use std::rc::Rc;
use wasm_bindgen::prelude::*;
use web_sys::{
    IdbDatabase, IdbFactory, IdbKeyRange, IdbObjectStore, IdbObjectStoreParameters,
    IdbOpenDbRequest, IdbRequest, IdbTransaction, IdbTransactionMode,
};

const STORE_NAME: &str = "kv";

type ScanResults = Rc<RefCell<Vec<(Vec<u8>, Vec<u8>)>>>;

pub struct IndexedDbBackend {
    db: Rc<RefCell<Option<IdbDatabase>>>,
}

impl IndexedDbBackend {
    /// Opens or creates an `IndexedDB` database.
    ///
    /// # Errors
    /// Returns an error if `IndexedDB` is not available or the database cannot be opened.
    pub async fn open(db_name: &str) -> Result<Self> {
        let window = web_sys::window().ok_or_else(|| Error::Internal("no window".into()))?;
        let idb: IdbFactory = window
            .indexed_db()
            .map_err(|e| Error::Internal(format!("indexedDB error: {e:?}")))?
            .ok_or_else(|| Error::Internal("indexedDB not available".into()))?;

        let request: IdbOpenDbRequest = idb
            .open_with_u32(db_name, 1)
            .map_err(|e| Error::Internal(format!("open error: {e:?}")))?;

        let db_cell: Rc<RefCell<Option<IdbDatabase>>> = Rc::new(RefCell::new(None));
        let db_cell_success = Rc::clone(&db_cell);

        let onupgradeneeded =
            Closure::once(Box::new(move |event: web_sys::IdbVersionChangeEvent| {
                let Some(target) = event.target() else {
                    return;
                };
                let request: IdbOpenDbRequest = target.unchecked_into();
                let Ok(result) = request.result() else {
                    return;
                };
                let db: IdbDatabase = result.unchecked_into();

                if !db.object_store_names().contains(STORE_NAME) {
                    let params = IdbObjectStoreParameters::new();
                    let _ = db.create_object_store_with_optional_parameters(STORE_NAME, &params);
                }
            }) as Box<dyn FnOnce(_)>);

        request.set_onupgradeneeded(Some(onupgradeneeded.as_ref().unchecked_ref()));
        onupgradeneeded.forget();

        let (tx, rx) = futures_channel::oneshot::channel::<Result<()>>();
        let tx = Rc::new(RefCell::new(Some(tx)));

        let tx_success = Rc::clone(&tx);
        let onsuccess = Closure::once(Box::new(move |event: web_sys::Event| {
            let Some(target) = event.target() else {
                if let Some(sender) = tx_success.borrow_mut().take() {
                    let _ = sender.send(Err(Error::Internal("missing event target".into())));
                }
                return;
            };
            let request: IdbOpenDbRequest = target.unchecked_into();
            let Ok(result) = request.result() else {
                if let Some(sender) = tx_success.borrow_mut().take() {
                    let _ = sender.send(Err(Error::Internal("failed to get IDB result".into())));
                }
                return;
            };
            let db: IdbDatabase = result.unchecked_into();
            *db_cell_success.borrow_mut() = Some(db);
            if let Some(sender) = tx_success.borrow_mut().take() {
                let _ = sender.send(Ok(()));
            }
        }) as Box<dyn FnOnce(_)>);

        let tx_error = Rc::clone(&tx);
        let onerror = Closure::once(Box::new(move |event: web_sys::Event| {
            if let Some(sender) = tx_error.borrow_mut().take() {
                let _ = sender.send(Err(Error::Internal(format!("IDB open error: {event:?}"))));
            }
        }) as Box<dyn FnOnce(_)>);

        request.set_onsuccess(Some(onsuccess.as_ref().unchecked_ref()));
        request.set_onerror(Some(onerror.as_ref().unchecked_ref()));

        onsuccess.forget();
        onerror.forget();

        rx.await
            .map_err(|_| Error::Internal("channel closed".into()))??;

        Ok(Self { db: db_cell })
    }

    fn get_db(&self) -> Result<IdbDatabase> {
        self.db
            .borrow()
            .clone()
            .ok_or_else(|| Error::Internal("database not open".into()))
    }

    fn get_store(&self, mode: IdbTransactionMode) -> Result<(IdbTransaction, IdbObjectStore)> {
        let db = self.get_db()?;
        let tx = db
            .transaction_with_str_and_mode(STORE_NAME, mode)
            .map_err(|e| Error::Internal(format!("transaction error: {e:?}")))?;
        let store = tx
            .object_store(STORE_NAME)
            .map_err(|e| Error::Internal(format!("object store error: {e:?}")))?;
        Ok((tx, store))
    }
}

async fn request_to_future(request: &IdbRequest) -> Result<JsValue> {
    let (tx, rx) = futures_channel::oneshot::channel::<Result<JsValue>>();
    let tx = Rc::new(RefCell::new(Some(tx)));

    let tx_success = Rc::clone(&tx);
    let onsuccess = Closure::once(Box::new(move |_event: web_sys::Event| {
        if let Some(sender) = tx_success.borrow_mut().take() {
            let _ = sender.send(Ok(JsValue::TRUE));
        }
    }) as Box<dyn FnOnce(_)>);

    let tx_error = Rc::clone(&tx);
    let onerror = Closure::once(Box::new(move |event: web_sys::Event| {
        if let Some(sender) = tx_error.borrow_mut().take() {
            let _ = sender.send(Err(Error::Internal(format!("IDB error: {event:?}"))));
        }
    }) as Box<dyn FnOnce(_)>);

    request.set_onsuccess(Some(onsuccess.as_ref().unchecked_ref()));
    request.set_onerror(Some(onerror.as_ref().unchecked_ref()));

    onsuccess.forget();
    onerror.forget();

    rx.await
        .map_err(|_| Error::Internal("channel closed".into()))?
}

async fn request_to_result(request: &IdbRequest) -> Result<JsValue> {
    request_to_future(request).await?;
    request
        .result()
        .map_err(|e| Error::Internal(format!("result error: {e:?}")))
}

fn key_to_js(key: &[u8]) -> Uint8Array {
    Uint8Array::from(key)
}

fn value_to_js(value: &[u8]) -> Uint8Array {
    Uint8Array::from(value)
}

fn js_to_bytes(value: &JsValue) -> Option<Vec<u8>> {
    if value.is_undefined() || value.is_null() {
        return None;
    }
    let arr = Uint8Array::new(value);
    Some(arr.to_vec())
}

impl AsyncStorageBackend for IndexedDbBackend {
    type Batch = IndexedDbBatch;

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let (_tx, store): (IdbTransaction, IdbObjectStore) =
            self.get_store(IdbTransactionMode::Readonly)?;
        let js_key: JsValue = key_to_js(key).into();
        let request: IdbRequest = store
            .get(&js_key)
            .map_err(|e| Error::Internal(format!("get error: {e:?}")))?;
        let result = request_to_result(&request).await?;
        Ok(js_to_bytes(&result))
    }

    async fn insert(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let (_tx, store): (IdbTransaction, IdbObjectStore) =
            self.get_store(IdbTransactionMode::Readwrite)?;
        let js_key: JsValue = key_to_js(key).into();
        let js_value: JsValue = value_to_js(value).into();
        let request: IdbRequest = store
            .put_with_key(&js_value, &js_key)
            .map_err(|e| Error::Internal(format!("put error: {e:?}")))?;
        request_to_future(&request).await?;
        Ok(())
    }

    async fn remove(&self, key: &[u8]) -> Result<()> {
        let (_tx, store): (IdbTransaction, IdbObjectStore) =
            self.get_store(IdbTransactionMode::Readwrite)?;
        let js_key: JsValue = key_to_js(key).into();
        let request: IdbRequest = store
            .delete(&js_key)
            .map_err(|e| Error::Internal(format!("delete error: {e:?}")))?;
        request_to_future(&request).await?;
        Ok(())
    }

    async fn prefix_scan(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let (_tx, store): (IdbTransaction, IdbObjectStore) =
            self.get_store(IdbTransactionMode::Readonly)?;

        let start: JsValue = key_to_js(prefix).into();
        let mut end_bytes = prefix.to_vec();
        if let Some(last) = end_bytes.last_mut() {
            *last = last.saturating_add(1);
        } else {
            end_bytes.push(0xFF);
        }
        let end: JsValue = key_to_js(&end_bytes).into();

        let range = IdbKeyRange::bound(&start, &end)
            .map_err(|e| Error::Internal(format!("range error: {e:?}")))?;

        let request: IdbRequest = store
            .open_cursor_with_range(&range)
            .map_err(|e| Error::Internal(format!("cursor error: {e:?}")))?;

        let results: ScanResults = Rc::new(RefCell::new(Vec::new()));
        let results_clone = Rc::clone(&results);
        let prefix_vec = prefix.to_vec();

        let (tx, rx) = futures_channel::oneshot::channel::<Result<()>>();
        let tx = Rc::new(RefCell::new(Some(tx)));

        let tx_success = Rc::clone(&tx);
        let onsuccess = Closure::wrap(Box::new(move |event: web_sys::Event| {
            let send_err = |msg: &str| {
                if let Some(sender) = tx_success.borrow_mut().take() {
                    let _ = sender.send(Err(Error::Internal(msg.into())));
                }
            };

            let Some(target) = event.target() else {
                send_err("missing cursor event target");
                return;
            };
            let request: IdbRequest = target.unchecked_into();
            let Ok(result) = request.result() else {
                send_err("failed to get cursor result");
                return;
            };

            if result.is_null() || result.is_undefined() {
                if let Some(sender) = tx_success.borrow_mut().take() {
                    let _ = sender.send(Ok(()));
                }
                return;
            }

            let cursor: web_sys::IdbCursorWithValue = result.unchecked_into();
            let Ok(key_js) = cursor.key() else {
                send_err("failed to get cursor key");
                return;
            };
            let Ok(value_js) = cursor.value() else {
                send_err("failed to get cursor value");
                return;
            };

            let key_arr = Uint8Array::new(&key_js);
            let key_bytes = key_arr.to_vec();

            if key_bytes.starts_with(&prefix_vec) {
                let value_arr = Uint8Array::new(&value_js);
                let value_bytes = value_arr.to_vec();
                results_clone.borrow_mut().push((key_bytes, value_bytes));
                let _ = cursor.continue_();
            } else if let Some(sender) = tx_success.borrow_mut().take() {
                let _ = sender.send(Ok(()));
            }
        }) as Box<dyn FnMut(_)>);

        let tx_error = Rc::clone(&tx);
        let onerror = Closure::once(Box::new(move |event: web_sys::Event| {
            if let Some(sender) = tx_error.borrow_mut().take() {
                let _ = sender.send(Err(Error::Internal(format!("cursor error: {event:?}"))));
            }
        }) as Box<dyn FnOnce(_)>);

        request.set_onsuccess(Some(onsuccess.as_ref().unchecked_ref()));
        request.set_onerror(Some(onerror.as_ref().unchecked_ref()));

        onsuccess.forget();
        onerror.forget();

        rx.await
            .map_err(|_| Error::Internal("channel closed".into()))??;

        Ok(results.borrow().clone())
    }

    async fn prefix_count(&self, prefix: &[u8]) -> Result<usize> {
        Ok(self.prefix_scan(prefix).await?.len())
    }

    async fn prefix_scan_keys(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>> {
        Ok(self
            .prefix_scan(prefix)
            .await?
            .into_iter()
            .map(|(k, _)| k)
            .collect())
    }

    async fn range_scan(&self, start: &[u8], end: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let (_tx, store): (IdbTransaction, IdbObjectStore) =
            self.get_store(IdbTransactionMode::Readonly)?;

        let start_js: JsValue = key_to_js(start).into();
        let end_js: JsValue = key_to_js(end).into();

        let range = IdbKeyRange::bound(&start_js, &end_js)
            .map_err(|e| Error::Internal(format!("range error: {e:?}")))?;

        let request: IdbRequest = store
            .open_cursor_with_range(&range)
            .map_err(|e| Error::Internal(format!("cursor error: {e:?}")))?;

        let results: ScanResults = Rc::new(RefCell::new(Vec::new()));
        let results_clone = Rc::clone(&results);
        let end_vec = end.to_vec();

        let (tx, rx) = futures_channel::oneshot::channel::<Result<()>>();
        let tx = Rc::new(RefCell::new(Some(tx)));

        let tx_success = Rc::clone(&tx);
        let onsuccess = Closure::wrap(Box::new(move |event: web_sys::Event| {
            let send_err = |msg: &str| {
                if let Some(sender) = tx_success.borrow_mut().take() {
                    let _ = sender.send(Err(Error::Internal(msg.into())));
                }
            };

            let Some(target) = event.target() else {
                send_err("missing cursor event target");
                return;
            };
            let request: IdbRequest = target.unchecked_into();
            let Ok(result) = request.result() else {
                send_err("failed to get cursor result");
                return;
            };

            if result.is_null() || result.is_undefined() {
                if let Some(sender) = tx_success.borrow_mut().take() {
                    let _ = sender.send(Ok(()));
                }
                return;
            }

            let cursor: web_sys::IdbCursorWithValue = result.unchecked_into();
            let Ok(key_js) = cursor.key() else {
                send_err("failed to get cursor key");
                return;
            };
            let Ok(value_js) = cursor.value() else {
                send_err("failed to get cursor value");
                return;
            };

            let key_arr = Uint8Array::new(&key_js);
            let key_bytes = key_arr.to_vec();

            if key_bytes < end_vec {
                let value_arr = Uint8Array::new(&value_js);
                let value_bytes = value_arr.to_vec();
                results_clone.borrow_mut().push((key_bytes, value_bytes));
                let _ = cursor.continue_();
            } else if let Some(sender) = tx_success.borrow_mut().take() {
                let _ = sender.send(Ok(()));
            }
        }) as Box<dyn FnMut(_)>);

        let tx_error = Rc::clone(&tx);
        let onerror = Closure::once(Box::new(move |event: web_sys::Event| {
            if let Some(sender) = tx_error.borrow_mut().take() {
                let _ = sender.send(Err(Error::Internal(format!("cursor error: {event:?}"))));
            }
        }) as Box<dyn FnOnce(_)>);

        request.set_onsuccess(Some(onsuccess.as_ref().unchecked_ref()));
        request.set_onerror(Some(onerror.as_ref().unchecked_ref()));

        onsuccess.forget();
        onerror.forget();

        rx.await
            .map_err(|_| Error::Internal("channel closed".into()))??;

        Ok(results.borrow().clone())
    }

    fn batch(&self) -> Self::Batch {
        IndexedDbBatch {
            db: Rc::clone(&self.db),
            operations: Vec::new(),
            preconditions: Vec::new(),
        }
    }

    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}

enum BatchOp {
    Insert(Vec<u8>, Vec<u8>),
    Remove(Vec<u8>),
}

struct Precondition {
    key: Vec<u8>,
    expected_value: Vec<u8>,
}

pub struct IndexedDbBatch {
    db: Rc<RefCell<Option<IdbDatabase>>>,
    operations: Vec<BatchOp>,
    preconditions: Vec<Precondition>,
}

impl AsyncBatchOperations for IndexedDbBatch {
    fn insert(&mut self, key: Vec<u8>, value: Vec<u8>) {
        self.operations.push(BatchOp::Insert(key, value));
    }

    fn remove(&mut self, key: Vec<u8>) {
        self.operations.push(BatchOp::Remove(key));
    }

    fn expect_value(&mut self, key: Vec<u8>, expected_value: Vec<u8>) {
        self.preconditions.push(Precondition {
            key,
            expected_value,
        });
    }

    async fn commit(self) -> Result<()> {
        let db = self
            .db
            .borrow()
            .clone()
            .ok_or_else(|| Error::Internal("database not open".into()))?;

        let tx = db
            .transaction_with_str_and_mode(STORE_NAME, IdbTransactionMode::Readwrite)
            .map_err(|e| Error::Internal(format!("transaction error: {e:?}")))?;
        let store = tx
            .object_store(STORE_NAME)
            .map_err(|e| Error::Internal(format!("object store error: {e:?}")))?;

        for precondition in &self.preconditions {
            let js_key = key_to_js(&precondition.key);
            let request = store
                .get(&js_key)
                .map_err(|e| Error::Internal(format!("get error: {e:?}")))?;
            let result = request_to_result(&request).await?;

            let actual = js_to_bytes(&result);
            match actual {
                Some(val) if val == precondition.expected_value => {}
                _ => {
                    tx.abort().ok();
                    return Err(Error::Conflict(
                        "optimistic lock failed: value was modified".into(),
                    ));
                }
            }
        }

        for op in self.operations {
            match op {
                BatchOp::Insert(key, value) => {
                    let js_key = key_to_js(&key);
                    let js_value = value_to_js(&value);
                    let request = store
                        .put_with_key(&js_value, &js_key)
                        .map_err(|e| Error::Internal(format!("put error: {e:?}")))?;
                    request_to_future(&request).await?;
                }
                BatchOp::Remove(key) => {
                    let js_key = key_to_js(&key);
                    let request = store
                        .delete(&js_key)
                        .map_err(|e| Error::Internal(format!("delete error: {e:?}")))?;
                    request_to_future(&request).await?;
                }
            }
        }

        Ok(())
    }
}
