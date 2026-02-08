//! Node.js bindings for StrataDB.
//!
//! This module exposes the StrataDB API to Node.js via NAPI-RS.
//! All data methods are async (backed by `spawn_blocking`) so they never
//! block the Node.js event loop.

#![deny(clippy::all)]

use napi_derive::napi;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use stratadb::{
    AccessMode, BatchVectorEntry, BranchExportResult, BranchId, BranchImportResult,
    BundleValidateResult, CollectionInfo, Command, DistanceMetric, Error as StrataError, FilterOp,
    MergeStrategy, MetadataFilter, OpenOptions, Output, Session, Strata as RustStrata, TxnOptions,
    Value, VersionedBranchInfo, VersionedValue,
};

/// Maximum nesting depth for JSON → Value conversion.
const MAX_JSON_DEPTH: usize = 64;

/// Options for opening a database.
#[napi(object)]
pub struct JsOpenOptions {
    /// Enable automatic text embedding for semantic search.
    pub auto_embed: Option<bool>,
    /// Open in read-only mode.
    pub read_only: Option<bool>,
}

// ---------------------------------------------------------------------------
// Conversion helpers
// ---------------------------------------------------------------------------

/// Convert a JavaScript value to a stratadb Value with depth checking.
fn js_to_value_checked(val: serde_json::Value, depth: usize) -> napi::Result<Value> {
    if depth > MAX_JSON_DEPTH {
        return Err(napi::Error::from_reason(
            "[VALIDATION] JSON nesting depth exceeds maximum of 64",
        ));
    }
    match val {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(b) => Ok(Value::Bool(b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Int(i))
            } else if let Some(f) = n.as_f64() {
                Ok(Value::Float(f))
            } else {
                Ok(Value::Null)
            }
        }
        serde_json::Value::String(s) => Ok(Value::String(s)),
        serde_json::Value::Array(arr) => {
            let mut out = Vec::with_capacity(arr.len());
            for item in arr {
                out.push(js_to_value_checked(item, depth + 1)?);
            }
            Ok(Value::Array(out))
        }
        serde_json::Value::Object(map) => {
            let mut obj = HashMap::new();
            for (k, v) in map {
                obj.insert(k, js_to_value_checked(v, depth + 1)?);
            }
            Ok(Value::Object(obj))
        }
    }
}

/// Validate a vector, rejecting NaN/Infinity, and convert f64 → f32.
fn validate_vector(vec: &[f64]) -> napi::Result<Vec<f32>> {
    let mut out = Vec::with_capacity(vec.len());
    for (i, &f) in vec.iter().enumerate() {
        if f.is_nan() || f.is_infinite() {
            return Err(napi::Error::from_reason(format!(
                "[VALIDATION] Vector element at index {} is not a finite number",
                i
            )));
        }
        out.push(f as f32);
    }
    Ok(out)
}

/// Convert a stratadb Value to a serde_json Value.
fn value_to_js(val: Value) -> serde_json::Value {
    match val {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(b),
        Value::Int(i) => serde_json::Value::Number(i.into()),
        Value::Float(f) => serde_json::Number::from_f64(f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::String(s) => serde_json::Value::String(s),
        Value::Bytes(b) => serde_json::Value::String(base64_encode(&b)),
        Value::Array(arr) => {
            serde_json::Value::Array(arr.into_iter().map(value_to_js).collect())
        }
        Value::Object(map) => {
            let obj: serde_json::Map<String, serde_json::Value> = map
                .into_iter()
                .map(|(k, v)| (k, value_to_js(v)))
                .collect();
            serde_json::Value::Object(obj)
        }
    }
}

/// Simple base64 encoding for bytes.
fn base64_encode(data: &[u8]) -> String {
    use std::io::Write;
    let mut buf = Vec::new();
    let mut encoder = base64_encoder(&mut buf);
    encoder.write_all(data).unwrap();
    drop(encoder);
    String::from_utf8(buf).unwrap()
}

fn base64_encoder(writer: &mut Vec<u8>) -> impl std::io::Write + '_ {
    struct Base64Writer<'a>(&'a mut Vec<u8>);
    impl<'a> std::io::Write for Base64Writer<'a> {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            const ALPHABET: &[u8] =
                b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
            for chunk in buf.chunks(3) {
                let b0 = chunk[0] as usize;
                let b1 = chunk.get(1).copied().unwrap_or(0) as usize;
                let b2 = chunk.get(2).copied().unwrap_or(0) as usize;
                self.0.push(ALPHABET[b0 >> 2]);
                self.0.push(ALPHABET[((b0 & 0x03) << 4) | (b1 >> 4)]);
                if chunk.len() > 1 {
                    self.0.push(ALPHABET[((b1 & 0x0f) << 2) | (b2 >> 6)]);
                } else {
                    self.0.push(b'=');
                }
                if chunk.len() > 2 {
                    self.0.push(ALPHABET[b2 & 0x3f]);
                } else {
                    self.0.push(b'=');
                }
            }
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    Base64Writer(writer)
}

/// Convert a VersionedValue to a JSON object.
fn versioned_to_js(vv: VersionedValue) -> serde_json::Value {
    serde_json::json!({
        "value": value_to_js(vv.value),
        "version": vv.version,
        "timestamp": vv.timestamp,
    })
}

/// Convert stratadb error to napi Error with category prefix.
fn to_napi_err(e: StrataError) -> napi::Error {
    let code = match &e {
        StrataError::KeyNotFound { .. }
        | StrataError::BranchNotFound { .. }
        | StrataError::CollectionNotFound { .. }
        | StrataError::StreamNotFound { .. }
        | StrataError::CellNotFound { .. }
        | StrataError::DocumentNotFound { .. } => "[NOT_FOUND]",

        StrataError::InvalidKey { .. }
        | StrataError::InvalidPath { .. }
        | StrataError::InvalidInput { .. }
        | StrataError::WrongType { .. } => "[VALIDATION]",

        StrataError::VersionConflict { .. }
        | StrataError::TransitionFailed { .. }
        | StrataError::Conflict { .. }
        | StrataError::TransactionConflict { .. } => "[CONFLICT]",

        StrataError::BranchClosed { .. }
        | StrataError::BranchExists { .. }
        | StrataError::CollectionExists { .. }
        | StrataError::TransactionNotActive
        | StrataError::TransactionAlreadyActive => "[STATE]",

        StrataError::DimensionMismatch { .. }
        | StrataError::ConstraintViolation { .. }
        | StrataError::HistoryTrimmed { .. }
        | StrataError::HistoryUnavailable { .. }
        | StrataError::Overflow { .. } => "[CONSTRAINT]",

        StrataError::AccessDenied { .. } => "[ACCESS_DENIED]",

        StrataError::Io { .. }
        | StrataError::Serialization { .. }
        | StrataError::Internal { .. }
        | StrataError::NotImplemented { .. } => "[IO]",
    };
    napi::Error::from_reason(format!("{} {}", code, e))
}

/// Helper to acquire the mutex lock, mapping poison errors.
fn lock_inner(
    inner: &Mutex<RustStrata>,
) -> napi::Result<std::sync::MutexGuard<'_, RustStrata>> {
    inner
        .lock()
        .map_err(|_| napi::Error::from_reason("Lock poisoned"))
}

fn lock_session(
    session: &Mutex<Option<Session>>,
) -> napi::Result<std::sync::MutexGuard<'_, Option<Session>>> {
    session
        .lock()
        .map_err(|_| napi::Error::from_reason("Lock poisoned"))
}

// ---------------------------------------------------------------------------
// Main struct
// ---------------------------------------------------------------------------

/// StrataDB database handle.
///
/// This is the main entry point for interacting with StrataDB from Node.js.
/// All data methods are async — they run on a blocking thread pool so the
/// Node.js event loop is never blocked.
#[napi]
pub struct Strata {
    inner: Arc<Mutex<RustStrata>>,
    session: Arc<Mutex<Option<Session>>>,
}

#[napi]
impl Strata {
    // =========================================================================
    // Factory methods (sync — lightweight, no I/O worth spawning for)
    // =========================================================================

    /// Open a database at the given path.
    #[napi(factory)]
    pub fn open(path: String, options: Option<JsOpenOptions>) -> napi::Result<Self> {
        let auto_embed = options.as_ref().and_then(|o| o.auto_embed).unwrap_or(false);
        let read_only = options.as_ref().and_then(|o| o.read_only).unwrap_or(false);

        #[cfg(feature = "embed")]
        if auto_embed {
            if let Err(e) = strata_intelligence::embed::download::ensure_model() {
                eprintln!("Warning: failed to download model files: {}", e);
            }
        }

        let mut opts = OpenOptions::new();
        if auto_embed {
            opts = opts.auto_embed(true);
        }
        if read_only {
            opts = opts.access_mode(AccessMode::ReadOnly);
        }

        let raw = RustStrata::open_with(&path, opts).map_err(to_napi_err)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(raw)),
            session: Arc::new(Mutex::new(None)),
        })
    }

    /// Create an in-memory database (no persistence).
    #[napi(factory)]
    pub fn cache() -> napi::Result<Self> {
        let raw = RustStrata::cache().map_err(to_napi_err)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(raw)),
            session: Arc::new(Mutex::new(None)),
        })
    }

    // =========================================================================
    // KV Store
    // =========================================================================

    /// Store a key-value pair.
    #[napi(js_name = "kvPut")]
    pub async fn kv_put(&self, key: String, value: serde_json::Value) -> napi::Result<i64> {
        let inner = self.inner.clone();
        let v = js_to_value_checked(value, 0)?;
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard.kv_put(&key, v).map(|n| n as i64).map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get a value by key. Optionally pass `asOf` (microseconds since epoch)
    /// to read as of a past timestamp.
    #[napi(js_name = "kvGet")]
    pub async fn kv_get(&self, key: String, as_of: Option<i64>) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        let as_of_u64 = as_of.map(|t| t as u64);
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let branch = Some(BranchId::from(guard.current_branch()));
            let space = Some(guard.current_space().to_string());
            match guard
                .executor()
                .execute(Command::KvGet {
                    branch,
                    space,
                    key,
                    as_of: as_of_u64,
                })
                .map_err(to_napi_err)?
            {
                Output::MaybeVersioned(Some(vv)) => Ok(value_to_js(vv.value)),
                Output::MaybeVersioned(None) => Ok(serde_json::Value::Null),
                Output::Maybe(Some(v)) => Ok(value_to_js(v)),
                Output::Maybe(None) => Ok(serde_json::Value::Null),
                _ => Err(napi::Error::from_reason("Unexpected output for KvGet")),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Delete a key.
    #[napi(js_name = "kvDelete")]
    pub async fn kv_delete(&self, key: String) -> napi::Result<bool> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard.kv_delete(&key).map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// List keys with optional prefix filter. Optionally pass `asOf` for time-travel.
    #[napi(js_name = "kvList")]
    pub async fn kv_list(
        &self,
        prefix: Option<String>,
        as_of: Option<i64>,
    ) -> napi::Result<Vec<String>> {
        let inner = self.inner.clone();
        let as_of_u64 = as_of.map(|t| t as u64);
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let branch = Some(BranchId::from(guard.current_branch()));
            let space = Some(guard.current_space().to_string());
            match guard
                .executor()
                .execute(Command::KvList {
                    branch,
                    space,
                    prefix,
                    cursor: None,
                    limit: None,
                    as_of: as_of_u64,
                })
                .map_err(to_napi_err)?
            {
                Output::Keys(keys) => Ok(keys),
                _ => Err(napi::Error::from_reason("Unexpected output for KvList")),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get version history for a key.
    #[napi(js_name = "kvHistory")]
    pub async fn kv_history(&self, key: String) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard.kv_getv(&key).map_err(to_napi_err)? {
                Some(versions) => {
                    let arr: Vec<serde_json::Value> =
                        versions.into_iter().map(versioned_to_js).collect();
                    Ok(serde_json::Value::Array(arr))
                }
                None => Ok(serde_json::Value::Null),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // State Cell
    // =========================================================================

    /// Set a state cell value.
    #[napi(js_name = "stateSet")]
    pub async fn state_set(&self, cell: String, value: serde_json::Value) -> napi::Result<i64> {
        let inner = self.inner.clone();
        let v = js_to_value_checked(value, 0)?;
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard
                .state_set(&cell, v)
                .map(|n| n as i64)
                .map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get a state cell value. Optionally pass `asOf` for time-travel.
    #[napi(js_name = "stateGet")]
    pub async fn state_get(
        &self,
        cell: String,
        as_of: Option<i64>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        let as_of_u64 = as_of.map(|t| t as u64);
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let branch = Some(BranchId::from(guard.current_branch()));
            let space = Some(guard.current_space().to_string());
            match guard
                .executor()
                .execute(Command::StateGet {
                    branch,
                    space,
                    cell,
                    as_of: as_of_u64,
                })
                .map_err(to_napi_err)?
            {
                Output::MaybeVersioned(Some(vv)) => Ok(value_to_js(vv.value)),
                Output::MaybeVersioned(None) => Ok(serde_json::Value::Null),
                Output::Maybe(Some(v)) => Ok(value_to_js(v)),
                Output::Maybe(None) => Ok(serde_json::Value::Null),
                _ => Err(napi::Error::from_reason("Unexpected output for StateGet")),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Initialize a state cell if it doesn't exist.
    #[napi(js_name = "stateInit")]
    pub async fn state_init(&self, cell: String, value: serde_json::Value) -> napi::Result<i64> {
        let inner = self.inner.clone();
        let v = js_to_value_checked(value, 0)?;
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard
                .state_init(&cell, v)
                .map(|n| n as i64)
                .map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Compare-and-swap update based on version.
    #[napi(js_name = "stateCas")]
    pub async fn state_cas(
        &self,
        cell: String,
        new_value: serde_json::Value,
        expected_version: Option<i64>,
    ) -> napi::Result<Option<i64>> {
        let inner = self.inner.clone();
        let v = js_to_value_checked(new_value, 0)?;
        let exp = expected_version.map(|n| n as u64);
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard
                .state_cas(&cell, exp, v)
                .map(|opt| opt.map(|n| n as i64))
                .map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get version history for a state cell.
    #[napi(js_name = "stateHistory")]
    pub async fn state_history(&self, cell: String) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard.state_getv(&cell).map_err(to_napi_err)? {
                Some(versions) => {
                    let arr: Vec<serde_json::Value> =
                        versions.into_iter().map(versioned_to_js).collect();
                    Ok(serde_json::Value::Array(arr))
                }
                None => Ok(serde_json::Value::Null),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Event Log
    // =========================================================================

    /// Append an event to the log.
    #[napi(js_name = "eventAppend")]
    pub async fn event_append(
        &self,
        event_type: String,
        payload: serde_json::Value,
    ) -> napi::Result<i64> {
        let inner = self.inner.clone();
        let v = js_to_value_checked(payload, 0)?;
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard
                .event_append(&event_type, v)
                .map(|n| n as i64)
                .map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get an event by sequence number. Optionally pass `asOf` for time-travel.
    #[napi(js_name = "eventGet")]
    pub async fn event_get(
        &self,
        sequence: i64,
        as_of: Option<i64>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        let as_of_u64 = as_of.map(|t| t as u64);
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let branch = Some(BranchId::from(guard.current_branch()));
            let space = Some(guard.current_space().to_string());
            match guard
                .executor()
                .execute(Command::EventGet {
                    branch,
                    space,
                    sequence: sequence as u64,
                    as_of: as_of_u64,
                })
                .map_err(to_napi_err)?
            {
                Output::MaybeVersioned(Some(vv)) => Ok(versioned_to_js(vv)),
                Output::MaybeVersioned(None) => Ok(serde_json::Value::Null),
                Output::Maybe(Some(v)) => Ok(serde_json::json!({ "value": value_to_js(v) })),
                Output::Maybe(None) => Ok(serde_json::Value::Null),
                _ => Err(napi::Error::from_reason("Unexpected output for EventGet")),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// List events by type. Optionally pass `asOf` for time-travel.
    #[napi(js_name = "eventList")]
    pub async fn event_list(
        &self,
        event_type: String,
        as_of: Option<i64>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        let as_of_u64 = as_of.map(|t| t as u64);
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let branch = Some(BranchId::from(guard.current_branch()));
            let space = Some(guard.current_space().to_string());
            match guard
                .executor()
                .execute(Command::EventGetByType {
                    branch,
                    space,
                    event_type,
                    limit: None,
                    after_sequence: None,
                    as_of: as_of_u64,
                })
                .map_err(to_napi_err)?
            {
                Output::VersionedValues(events) => {
                    let arr: Vec<serde_json::Value> =
                        events.into_iter().map(versioned_to_js).collect();
                    Ok(serde_json::Value::Array(arr))
                }
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for EventGetByType",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get total event count.
    #[napi(js_name = "eventLen")]
    pub async fn event_len(&self) -> napi::Result<i64> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard.event_len().map(|n| n as i64).map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // JSON Store
    // =========================================================================

    /// Set a value at a JSONPath.
    #[napi(js_name = "jsonSet")]
    pub async fn json_set(
        &self,
        key: String,
        path: String,
        value: serde_json::Value,
    ) -> napi::Result<i64> {
        let inner = self.inner.clone();
        let v = js_to_value_checked(value, 0)?;
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard
                .json_set(&key, &path, v)
                .map(|n| n as i64)
                .map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get a value at a JSONPath. Optionally pass `asOf` for time-travel.
    #[napi(js_name = "jsonGet")]
    pub async fn json_get(
        &self,
        key: String,
        path: String,
        as_of: Option<i64>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        let as_of_u64 = as_of.map(|t| t as u64);
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let branch = Some(BranchId::from(guard.current_branch()));
            let space = Some(guard.current_space().to_string());
            match guard
                .executor()
                .execute(Command::JsonGet {
                    branch,
                    space,
                    key,
                    path,
                    as_of: as_of_u64,
                })
                .map_err(to_napi_err)?
            {
                Output::MaybeVersioned(Some(vv)) => Ok(value_to_js(vv.value)),
                Output::MaybeVersioned(None) => Ok(serde_json::Value::Null),
                Output::Maybe(Some(v)) => Ok(value_to_js(v)),
                Output::Maybe(None) => Ok(serde_json::Value::Null),
                _ => Err(napi::Error::from_reason("Unexpected output for JsonGet")),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Delete a JSON document.
    #[napi(js_name = "jsonDelete")]
    pub async fn json_delete(&self, key: String, path: String) -> napi::Result<i64> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard
                .json_delete(&key, &path)
                .map(|n| n as i64)
                .map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get version history for a JSON document.
    #[napi(js_name = "jsonHistory")]
    pub async fn json_history(&self, key: String) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard.json_getv(&key).map_err(to_napi_err)? {
                Some(versions) => {
                    let arr: Vec<serde_json::Value> =
                        versions.into_iter().map(versioned_to_js).collect();
                    Ok(serde_json::Value::Array(arr))
                }
                None => Ok(serde_json::Value::Null),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// List JSON document keys. Optionally pass `asOf` for time-travel.
    #[napi(js_name = "jsonList")]
    pub async fn json_list(
        &self,
        limit: u32,
        prefix: Option<String>,
        cursor: Option<String>,
        as_of: Option<i64>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        let as_of_u64 = as_of.map(|t| t as u64);
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let branch = Some(BranchId::from(guard.current_branch()));
            let space = Some(guard.current_space().to_string());
            match guard
                .executor()
                .execute(Command::JsonList {
                    branch,
                    space,
                    prefix,
                    cursor,
                    limit: limit as u64,
                    as_of: as_of_u64,
                })
                .map_err(to_napi_err)?
            {
                Output::JsonListResult { keys, cursor } => Ok(serde_json::json!({
                    "keys": keys,
                    "cursor": cursor,
                })),
                Output::Keys(keys) => Ok(serde_json::json!({
                    "keys": keys,
                    "cursor": serde_json::Value::Null,
                })),
                _ => Err(napi::Error::from_reason("Unexpected output for JsonList")),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Vector Store
    // =========================================================================

    /// Create a vector collection.
    #[napi(js_name = "vectorCreateCollection")]
    pub async fn vector_create_collection(
        &self,
        collection: String,
        dimension: u32,
        metric: Option<String>,
    ) -> napi::Result<i64> {
        let inner = self.inner.clone();
        let m = match metric.as_deref().unwrap_or("cosine") {
            "cosine" => DistanceMetric::Cosine,
            "euclidean" => DistanceMetric::Euclidean,
            "dot_product" | "dotproduct" => DistanceMetric::DotProduct,
            _ => return Err(napi::Error::from_reason("[VALIDATION] Invalid metric")),
        };
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard
                .vector_create_collection(&collection, dimension as u64, m)
                .map(|n| n as i64)
                .map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Delete a vector collection.
    #[napi(js_name = "vectorDeleteCollection")]
    pub async fn vector_delete_collection(&self, collection: String) -> napi::Result<bool> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard
                .vector_delete_collection(&collection)
                .map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// List vector collections.
    #[napi(js_name = "vectorListCollections")]
    pub async fn vector_list_collections(&self) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let collections = guard.vector_list_collections().map_err(to_napi_err)?;
            let arr: Vec<serde_json::Value> =
                collections.into_iter().map(collection_info_to_js).collect();
            Ok(serde_json::Value::Array(arr))
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Insert or update a vector.
    #[napi(js_name = "vectorUpsert")]
    pub async fn vector_upsert(
        &self,
        collection: String,
        key: String,
        vector: Vec<f64>,
        metadata: Option<serde_json::Value>,
    ) -> napi::Result<i64> {
        let inner = self.inner.clone();
        let vec = validate_vector(&vector)?;
        let meta = match metadata {
            Some(m) => Some(js_to_value_checked(m, 0)?),
            None => None,
        };
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard
                .vector_upsert(&collection, &key, vec, meta)
                .map(|n| n as i64)
                .map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get a vector by key. Optionally pass `asOf` for time-travel.
    #[napi(js_name = "vectorGet")]
    pub async fn vector_get(
        &self,
        collection: String,
        key: String,
        as_of: Option<i64>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        let as_of_u64 = as_of.map(|t| t as u64);
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let branch = Some(BranchId::from(guard.current_branch()));
            let space = Some(guard.current_space().to_string());
            match guard
                .executor()
                .execute(Command::VectorGet {
                    branch,
                    space,
                    collection,
                    key,
                    as_of: as_of_u64,
                })
                .map_err(to_napi_err)?
            {
                Output::VectorData(Some(vd)) => {
                    let embedding: Vec<f64> =
                        vd.data.embedding.iter().map(|&f| f as f64).collect();
                    Ok(serde_json::json!({
                        "key": vd.key,
                        "embedding": embedding,
                        "metadata": vd.data.metadata.map(value_to_js),
                        "version": vd.version,
                        "timestamp": vd.timestamp,
                    }))
                }
                Output::VectorData(None) => Ok(serde_json::Value::Null),
                _ => Err(napi::Error::from_reason("Unexpected output for VectorGet")),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Delete a vector.
    #[napi(js_name = "vectorDelete")]
    pub async fn vector_delete(&self, collection: String, key: String) -> napi::Result<bool> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard.vector_delete(&collection, &key).map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Search for similar vectors. Optionally pass `asOf` for time-travel.
    #[napi(js_name = "vectorSearch")]
    pub async fn vector_search(
        &self,
        collection: String,
        query: Vec<f64>,
        k: u32,
        as_of: Option<i64>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        let vec = validate_vector(&query)?;
        let as_of_u64 = as_of.map(|t| t as u64);
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let branch = Some(BranchId::from(guard.current_branch()));
            let space = Some(guard.current_space().to_string());
            match guard
                .executor()
                .execute(Command::VectorSearch {
                    branch,
                    space,
                    collection,
                    query: vec,
                    k: k as u64,
                    filter: None,
                    metric: None,
                    as_of: as_of_u64,
                })
                .map_err(to_napi_err)?
            {
                Output::VectorMatches(matches) => {
                    let arr: Vec<serde_json::Value> = matches
                        .into_iter()
                        .map(|m| {
                            serde_json::json!({
                                "key": m.key,
                                "score": m.score,
                                "metadata": m.metadata.map(value_to_js),
                            })
                        })
                        .collect();
                    Ok(serde_json::Value::Array(arr))
                }
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for VectorSearch",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get statistics for a single collection.
    #[napi(js_name = "vectorCollectionStats")]
    pub async fn vector_collection_stats(
        &self,
        collection: String,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let info = guard
                .vector_collection_stats(&collection)
                .map_err(to_napi_err)?;
            Ok(collection_info_to_js(info))
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Batch insert/update multiple vectors.
    #[napi(js_name = "vectorBatchUpsert")]
    pub async fn vector_batch_upsert(
        &self,
        collection: String,
        vectors: Vec<serde_json::Value>,
    ) -> napi::Result<Vec<i64>> {
        let inner = self.inner.clone();
        // Parse and validate all entries on the JS thread before spawning.
        let batch: Vec<BatchVectorEntry> = vectors
            .into_iter()
            .map(|v| {
                let obj = v
                    .as_object()
                    .ok_or_else(|| napi::Error::from_reason("[VALIDATION] Expected object"))?;
                let key = obj
                    .get("key")
                    .and_then(|k| k.as_str())
                    .ok_or_else(|| napi::Error::from_reason("[VALIDATION] Missing 'key'"))?
                    .to_string();
                let raw_vec: Vec<f64> = obj
                    .get("vector")
                    .and_then(|v| v.as_array())
                    .ok_or_else(|| napi::Error::from_reason("[VALIDATION] Missing 'vector'"))?
                    .iter()
                    .map(|n| {
                        n.as_f64().ok_or_else(|| {
                            napi::Error::from_reason(
                                "[VALIDATION] Vector element is not a number",
                            )
                        })
                    })
                    .collect::<napi::Result<_>>()?;
                let vec = validate_vector(&raw_vec)?;
                let meta = match obj.get("metadata") {
                    Some(m) => Some(js_to_value_checked(m.clone(), 0)?),
                    None => None,
                };
                Ok(BatchVectorEntry {
                    key,
                    vector: vec,
                    metadata: meta,
                })
            })
            .collect::<napi::Result<_>>()?;
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard
                .vector_batch_upsert(&collection, batch)
                .map(|versions| versions.into_iter().map(|v| v as i64).collect())
                .map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Branch Management
    // =========================================================================

    /// Get the current branch name.
    #[napi(js_name = "currentBranch")]
    pub async fn current_branch(&self) -> napi::Result<String> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            Ok(guard.current_branch().to_string())
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Switch to a different branch.
    #[napi(js_name = "setBranch")]
    pub async fn set_branch(&self, branch: String) -> napi::Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let mut guard = inner
                .lock()
                .map_err(|_| napi::Error::from_reason("Lock poisoned"))?;
            guard.set_branch(&branch).map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Create a new empty branch.
    #[napi(js_name = "createBranch")]
    pub async fn create_branch(&self, branch: String) -> napi::Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard.create_branch(&branch).map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Fork the current branch to a new branch, copying all data.
    #[napi(js_name = "forkBranch")]
    pub async fn fork_branch(&self, destination: String) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let info = guard.fork_branch(&destination).map_err(to_napi_err)?;
            Ok(serde_json::json!({
                "source": info.source,
                "destination": info.destination,
                "keysCopied": info.keys_copied,
            }))
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// List all branches.
    #[napi(js_name = "listBranches")]
    pub async fn list_branches(&self) -> napi::Result<Vec<String>> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard.list_branches().map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Delete a branch.
    #[napi(js_name = "deleteBranch")]
    pub async fn delete_branch(&self, branch: String) -> napi::Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard.delete_branch(&branch).map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Check if a branch exists.
    #[napi(js_name = "branchExists")]
    pub async fn branch_exists(&self, name: String) -> napi::Result<bool> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard.branches().exists(&name).map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get branch metadata with version info.
    #[napi(js_name = "branchGet")]
    pub async fn branch_get(&self, name: String) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard.branch_get(&name).map_err(to_napi_err)? {
                Some(info) => Ok(versioned_branch_info_to_js(info)),
                None => Ok(serde_json::Value::Null),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Compare two branches.
    #[napi(js_name = "diffBranches")]
    pub async fn diff_branches(
        &self,
        branch_a: String,
        branch_b: String,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let diff = guard
                .diff_branches(&branch_a, &branch_b)
                .map_err(to_napi_err)?;
            Ok(serde_json::json!({
                "branchA": diff.branch_a,
                "branchB": diff.branch_b,
                "summary": {
                    "totalAdded": diff.summary.total_added,
                    "totalRemoved": diff.summary.total_removed,
                    "totalModified": diff.summary.total_modified,
                },
            }))
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Merge a branch into the current branch.
    #[napi(js_name = "mergeBranches")]
    pub async fn merge_branches(
        &self,
        source: String,
        strategy: Option<String>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        let strat = match strategy.as_deref().unwrap_or("last_writer_wins") {
            "last_writer_wins" => MergeStrategy::LastWriterWins,
            "strict" => MergeStrategy::Strict,
            _ => return Err(napi::Error::from_reason("[VALIDATION] Invalid merge strategy")),
        };
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let target = guard.current_branch().to_string();
            let info = guard
                .merge_branches(&source, &target, strat)
                .map_err(to_napi_err)?;
            let conflicts: Vec<serde_json::Value> = info
                .conflicts
                .into_iter()
                .map(|c| {
                    serde_json::json!({
                        "key": c.key,
                        "space": c.space,
                    })
                })
                .collect();
            Ok(serde_json::json!({
                "keysApplied": info.keys_applied,
                "spacesMerged": info.spaces_merged,
                "conflicts": conflicts,
            }))
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Space Management
    // =========================================================================

    /// Get the current space name.
    #[napi(js_name = "currentSpace")]
    pub async fn current_space(&self) -> napi::Result<String> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            Ok(guard.current_space().to_string())
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Switch to a different space.
    #[napi(js_name = "setSpace")]
    pub async fn set_space(&self, space: String) -> napi::Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let mut guard = inner
                .lock()
                .map_err(|_| napi::Error::from_reason("Lock poisoned"))?;
            guard.set_space(&space).map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// List all spaces in the current branch.
    #[napi(js_name = "listSpaces")]
    pub async fn list_spaces(&self) -> napi::Result<Vec<String>> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard.list_spaces().map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Delete a space and all its data.
    #[napi(js_name = "deleteSpace")]
    pub async fn delete_space(&self, space: String) -> napi::Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard.delete_space(&space).map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Force delete a space even if non-empty.
    #[napi(js_name = "deleteSpaceForce")]
    pub async fn delete_space_force(&self, space: String) -> napi::Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard.delete_space_force(&space).map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Database Operations
    // =========================================================================

    /// Check database connectivity.
    #[napi]
    pub async fn ping(&self) -> napi::Result<String> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard.ping().map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get database info.
    #[napi]
    pub async fn info(&self) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let info = guard.info().map_err(to_napi_err)?;
            Ok(serde_json::json!({
                "version": info.version,
                "uptimeSecs": info.uptime_secs,
                "branchCount": info.branch_count,
                "totalKeys": info.total_keys,
            }))
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Flush writes to disk.
    #[napi]
    pub async fn flush(&self) -> napi::Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard.flush().map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Trigger compaction.
    #[napi]
    pub async fn compact(&self) -> napi::Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            guard.compact().map_err(to_napi_err)
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Bundle Operations
    // =========================================================================

    /// Export a branch to a bundle file.
    #[napi(js_name = "branchExport")]
    pub async fn branch_export(
        &self,
        branch: String,
        path: String,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let result = guard.branch_export(&branch, &path).map_err(to_napi_err)?;
            Ok(branch_export_result_to_js(result))
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Import a branch from a bundle file.
    #[napi(js_name = "branchImport")]
    pub async fn branch_import(&self, path: String) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let result = guard.branch_import(&path).map_err(to_napi_err)?;
            Ok(branch_import_result_to_js(result))
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Validate a bundle file without importing.
    #[napi(js_name = "branchValidateBundle")]
    pub async fn branch_validate_bundle(&self, path: String) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let result = guard.branch_validate_bundle(&path).map_err(to_napi_err)?;
            Ok(bundle_validate_result_to_js(result))
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Transaction Operations
    // =========================================================================

    /// Begin a new transaction.
    #[napi(js_name = "begin")]
    pub async fn begin(&self, read_only: Option<bool>) -> napi::Result<()> {
        let inner = self.inner.clone();
        let session_arc = self.session.clone();
        tokio::task::spawn_blocking(move || {
            let mut session_ref = lock_session(&session_arc)?;
            if session_ref.is_none() {
                let guard = lock_inner(&inner)?;
                *session_ref = Some(guard.session());
            }
            let session = session_ref.as_mut().unwrap();
            let cmd = Command::TxnBegin {
                branch: None,
                options: Some(TxnOptions {
                    read_only: read_only.unwrap_or(false),
                }),
            };
            session.execute(cmd).map_err(to_napi_err)?;
            Ok(())
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Commit the current transaction.
    #[napi]
    pub async fn commit(&self) -> napi::Result<i64> {
        let session_arc = self.session.clone();
        tokio::task::spawn_blocking(move || {
            let mut session_ref = lock_session(&session_arc)?;
            let session = session_ref
                .as_mut()
                .ok_or_else(|| napi::Error::from_reason("[STATE] No transaction active"))?;
            match session.execute(Command::TxnCommit).map_err(to_napi_err)? {
                Output::TxnCommitted { version } => Ok(version as i64),
                _ => Err(napi::Error::from_reason("Unexpected output for TxnCommit")),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Rollback the current transaction.
    #[napi]
    pub async fn rollback(&self) -> napi::Result<()> {
        let session_arc = self.session.clone();
        tokio::task::spawn_blocking(move || {
            let mut session_ref = lock_session(&session_arc)?;
            let session = session_ref
                .as_mut()
                .ok_or_else(|| napi::Error::from_reason("[STATE] No transaction active"))?;
            session.execute(Command::TxnRollback).map_err(to_napi_err)?;
            Ok(())
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get current transaction info.
    #[napi(js_name = "txnInfo")]
    pub async fn txn_info(&self) -> napi::Result<serde_json::Value> {
        let session_arc = self.session.clone();
        tokio::task::spawn_blocking(move || {
            let mut session_ref = lock_session(&session_arc)?;
            if session_ref.is_none() {
                return Ok(serde_json::Value::Null);
            }
            let session = session_ref.as_mut().unwrap();
            match session.execute(Command::TxnInfo).map_err(to_napi_err)? {
                Output::TxnInfo(Some(info)) => Ok(serde_json::json!({
                    "id": info.id,
                    "status": format!("{:?}", info.status).to_lowercase(),
                    "startedAt": info.started_at,
                })),
                Output::TxnInfo(None) => Ok(serde_json::Value::Null),
                _ => Err(napi::Error::from_reason("Unexpected output for TxnInfo")),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Check if a transaction is currently active.
    #[napi(js_name = "txnIsActive")]
    pub async fn txn_is_active(&self) -> napi::Result<bool> {
        let session_arc = self.session.clone();
        tokio::task::spawn_blocking(move || {
            let mut session_ref = lock_session(&session_arc)?;
            if session_ref.is_none() {
                return Ok(false);
            }
            let session = session_ref.as_mut().unwrap();
            match session.execute(Command::TxnIsActive).map_err(to_napi_err)? {
                Output::Bool(active) => Ok(active),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for TxnIsActive",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // State Operations
    // =========================================================================

    /// Delete a state cell.
    #[napi(js_name = "stateDelete")]
    pub async fn state_delete(&self, cell: String) -> napi::Result<bool> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::StateDelete {
                    branch: None,
                    space: None,
                    cell,
                })
                .map_err(to_napi_err)?
            {
                Output::Bool(deleted) => Ok(deleted),
                _ => Err(napi::Error::from_reason("Unexpected output for StateDelete")),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// List state cell names with optional prefix filter. Optionally pass `asOf` for time-travel.
    #[napi(js_name = "stateList")]
    pub async fn state_list(
        &self,
        prefix: Option<String>,
        as_of: Option<i64>,
    ) -> napi::Result<Vec<String>> {
        let inner = self.inner.clone();
        let as_of_u64 = as_of.map(|t| t as u64);
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let branch = Some(BranchId::from(guard.current_branch()));
            let space = Some(guard.current_space().to_string());
            match guard
                .executor()
                .execute(Command::StateList {
                    branch,
                    space,
                    prefix,
                    as_of: as_of_u64,
                })
                .map_err(to_napi_err)?
            {
                Output::Keys(keys) => Ok(keys),
                _ => Err(napi::Error::from_reason("Unexpected output for StateList")),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Versioned Getters
    // =========================================================================

    /// Get a value by key with version info.
    #[napi(js_name = "kvGetVersioned")]
    pub async fn kv_get_versioned(&self, key: String) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard.kv_getv(&key).map_err(to_napi_err)? {
                Some(versions) if !versions.is_empty() => {
                    Ok(versioned_to_js(versions.into_iter().next().unwrap()))
                }
                _ => Ok(serde_json::Value::Null),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get a state cell value with version info.
    #[napi(js_name = "stateGetVersioned")]
    pub async fn state_get_versioned(&self, cell: String) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard.state_getv(&cell).map_err(to_napi_err)? {
                Some(versions) if !versions.is_empty() => {
                    Ok(versioned_to_js(versions.into_iter().next().unwrap()))
                }
                _ => Ok(serde_json::Value::Null),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Get a JSON document value with version info.
    #[napi(js_name = "jsonGetVersioned")]
    pub async fn json_get_versioned(&self, key: String) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard.json_getv(&key).map_err(to_napi_err)? {
                Some(versions) if !versions.is_empty() => {
                    Ok(versioned_to_js(versions.into_iter().next().unwrap()))
                }
                _ => Ok(serde_json::Value::Null),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Pagination
    // =========================================================================

    /// List keys with pagination support. Optionally pass `asOf` for time-travel.
    #[napi(js_name = "kvListPaginated")]
    pub async fn kv_list_paginated(
        &self,
        prefix: Option<String>,
        limit: Option<u32>,
        as_of: Option<i64>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        let as_of_u64 = as_of.map(|t| t as u64);
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let branch = Some(BranchId::from(guard.current_branch()));
            let space = Some(guard.current_space().to_string());
            match guard
                .executor()
                .execute(Command::KvList {
                    branch,
                    space,
                    prefix,
                    cursor: None,
                    limit: limit.map(|l| l as u64),
                    as_of: as_of_u64,
                })
                .map_err(to_napi_err)?
            {
                Output::Keys(keys) => Ok(serde_json::json!({ "keys": keys })),
                _ => Err(napi::Error::from_reason("Unexpected output for KvList")),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// List events by type with pagination support. Optionally pass `asOf` for time-travel.
    #[napi(js_name = "eventListPaginated")]
    pub async fn event_list_paginated(
        &self,
        event_type: String,
        limit: Option<u32>,
        after: Option<i64>,
        as_of: Option<i64>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        let as_of_u64 = as_of.map(|t| t as u64);
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let branch = Some(BranchId::from(guard.current_branch()));
            let space = Some(guard.current_space().to_string());
            match guard
                .executor()
                .execute(Command::EventGetByType {
                    branch,
                    space,
                    event_type,
                    limit: limit.map(|l| l as u64),
                    after_sequence: after.map(|a| a as u64),
                    as_of: as_of_u64,
                })
                .map_err(to_napi_err)?
            {
                Output::VersionedValues(events) => {
                    let arr: Vec<serde_json::Value> =
                        events.into_iter().map(versioned_to_js).collect();
                    Ok(serde_json::Value::Array(arr))
                }
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for EventGetByType",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Enhanced Vector Search
    // =========================================================================

    /// Search for similar vectors with optional filter and metric override.
    /// Optionally pass `asOf` for time-travel.
    #[napi(js_name = "vectorSearchFiltered")]
    pub async fn vector_search_filtered(
        &self,
        collection: String,
        query: Vec<f64>,
        k: u32,
        metric: Option<String>,
        filter: Option<Vec<serde_json::Value>>,
        as_of: Option<i64>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        let vec = validate_vector(&query)?;

        let metric_enum = match metric.as_deref() {
            Some("cosine") => Some(DistanceMetric::Cosine),
            Some("euclidean") => Some(DistanceMetric::Euclidean),
            Some("dot_product") | Some("dotproduct") => Some(DistanceMetric::DotProduct),
            Some(m) => {
                return Err(napi::Error::from_reason(format!(
                    "[VALIDATION] Invalid metric: {}",
                    m
                )))
            }
            None => None,
        };

        let as_of_u64 = as_of.map(|t| t as u64);

        let filter_vec = match filter {
            Some(arr) => {
                let mut filters = Vec::new();
                for item in arr {
                    let obj = item.as_object().ok_or_else(|| {
                        napi::Error::from_reason("[VALIDATION] Filter must be an object")
                    })?;
                    let field = obj
                        .get("field")
                        .and_then(|f| f.as_str())
                        .ok_or_else(|| {
                            napi::Error::from_reason("[VALIDATION] Filter missing 'field'")
                        })?
                        .to_string();
                    let op_str =
                        obj.get("op").and_then(|o| o.as_str()).ok_or_else(|| {
                            napi::Error::from_reason("[VALIDATION] Filter missing 'op'")
                        })?;
                    let op = match op_str {
                        "eq" => FilterOp::Eq,
                        "ne" => FilterOp::Ne,
                        "gt" => FilterOp::Gt,
                        "gte" => FilterOp::Gte,
                        "lt" => FilterOp::Lt,
                        "lte" => FilterOp::Lte,
                        "in" => FilterOp::In,
                        "contains" => FilterOp::Contains,
                        _ => {
                            return Err(napi::Error::from_reason(format!(
                                "[VALIDATION] Invalid filter op: {}",
                                op_str
                            )))
                        }
                    };
                    let value_json = obj.get("value").ok_or_else(|| {
                        napi::Error::from_reason("[VALIDATION] Filter missing 'value'")
                    })?.clone();
                    let value = js_to_value_checked(value_json, 0)?;
                    filters.push(MetadataFilter { field, op, value });
                }
                Some(filters)
            }
            None => None,
        };

        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let branch = Some(BranchId::from(guard.current_branch()));
            let space = Some(guard.current_space().to_string());
            match guard
                .executor()
                .execute(Command::VectorSearch {
                    branch,
                    space,
                    collection,
                    query: vec,
                    k: k as u64,
                    filter: filter_vec,
                    metric: metric_enum,
                    as_of: as_of_u64,
                })
                .map_err(to_napi_err)?
            {
                Output::VectorMatches(matches) => {
                    let arr: Vec<serde_json::Value> = matches
                        .into_iter()
                        .map(|m| {
                            serde_json::json!({
                                "key": m.key,
                                "score": m.score,
                                "metadata": m.metadata.map(value_to_js),
                            })
                        })
                        .collect();
                    Ok(serde_json::Value::Array(arr))
                }
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for VectorSearch",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Space Operations
    // =========================================================================

    /// Create a new space explicitly.
    #[napi(js_name = "spaceCreate")]
    pub async fn space_create(&self, space: String) -> napi::Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::SpaceCreate {
                    branch: None,
                    space,
                })
                .map_err(to_napi_err)?
            {
                Output::Unit => Ok(()),
                _ => Err(napi::Error::from_reason("Unexpected output for SpaceCreate")),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    /// Check if a space exists in the current branch.
    #[napi(js_name = "spaceExists")]
    pub async fn space_exists(&self, space: String) -> napi::Result<bool> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::SpaceExists {
                    branch: None,
                    space,
                })
                .map_err(to_napi_err)?
            {
                Output::Bool(exists) => Ok(exists),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for SpaceExists",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Search
    // =========================================================================

    /// Search across multiple primitives for matching content.
    #[napi]
    pub async fn search(
        &self,
        query: String,
        k: Option<u32>,
        primitives: Option<Vec<String>>,
    ) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::Search {
                    branch: None,
                    space: None,
                    query,
                    k: k.map(|n| n as u64),
                    primitives,
                })
                .map_err(to_napi_err)?
            {
                Output::SearchResults(results) => {
                    let arr: Vec<serde_json::Value> = results
                        .into_iter()
                        .map(|hit| {
                            serde_json::json!({
                                "entity": hit.entity,
                                "primitive": hit.primitive,
                                "score": hit.score,
                                "rank": hit.rank,
                                "snippet": hit.snippet,
                            })
                        })
                        .collect();
                    Ok(serde_json::Value::Array(arr))
                }
                _ => Err(napi::Error::from_reason("Unexpected output for Search")),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Retention
    // =========================================================================

    /// Apply retention policy to trigger garbage collection.
    #[napi(js_name = "retentionApply")]
    pub async fn retention_apply(&self) -> napi::Result<()> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            match guard
                .executor()
                .execute(Command::RetentionApply { branch: None })
                .map_err(to_napi_err)?
            {
                Output::Unit => Ok(()),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for RetentionApply",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Lifecycle
    // =========================================================================

    /// Close the database, releasing all resources.
    ///
    /// After calling `close()`, any further method call on this instance will
    /// fail with a "Lock poisoned" or similar error.  This mirrors the
    /// `client.close()` pattern used by every major Node.js database driver.
    #[napi]
    pub async fn close(&self) -> napi::Result<()> {
        let inner = self.inner.clone();
        let session_arc = self.session.clone();
        tokio::task::spawn_blocking(move || {
            // Drop session first (it borrows the inner DB).
            {
                let mut s = lock_session(&session_arc)?;
                *s = None;
            }
            // Replace the inner Strata with a freshly-opened cache that will
            // be immediately dropped, effectively releasing the original DB.
            let mut guard = inner
                .lock()
                .map_err(|_| napi::Error::from_reason("Lock poisoned"))?;
            let placeholder = RustStrata::cache().map_err(to_napi_err)?;
            *guard = placeholder;
            Ok(())
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }

    // =========================================================================
    // Time Travel
    // =========================================================================

    /// Get the time range (oldest and latest timestamps) for the current branch.
    #[napi(js_name = "timeRange")]
    pub async fn time_range(&self) -> napi::Result<serde_json::Value> {
        let inner = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let guard = lock_inner(&inner)?;
            let branch = Some(BranchId::from(guard.current_branch()));
            match guard
                .executor()
                .execute(Command::TimeRange { branch })
                .map_err(to_napi_err)?
            {
                Output::TimeRange {
                    oldest_ts,
                    latest_ts,
                } => Ok(serde_json::json!({
                    "oldestTs": oldest_ts.map(|t| t as i64),
                    "latestTs": latest_ts.map(|t| t as i64),
                })),
                _ => Err(napi::Error::from_reason(
                    "Unexpected output for TimeRange",
                )),
            }
        })
        .await
        .map_err(|e| napi::Error::from_reason(format!("{}", e)))?
    }
}

// ---------------------------------------------------------------------------
// Top-level functions
// ---------------------------------------------------------------------------

/// Download model files for auto-embedding.
#[napi]
pub fn setup() -> napi::Result<String> {
    #[cfg(feature = "embed")]
    {
        let path = strata_intelligence::embed::download::ensure_model()
            .map_err(|e| napi::Error::from_reason(e))?;
        Ok(path.to_string_lossy().into_owned())
    }

    #[cfg(not(feature = "embed"))]
    {
        Err(napi::Error::from_reason(
            "The 'embed' feature is not enabled in this build",
        ))
    }
}

// ---------------------------------------------------------------------------
// Conversion helpers (free functions)
// ---------------------------------------------------------------------------

fn collection_info_to_js(c: CollectionInfo) -> serde_json::Value {
    serde_json::json!({
        "name": c.name,
        "dimension": c.dimension,
        "metric": format!("{:?}", c.metric).to_lowercase(),
        "count": c.count,
        "indexType": c.index_type,
        "memoryBytes": c.memory_bytes,
    })
}

fn versioned_branch_info_to_js(info: VersionedBranchInfo) -> serde_json::Value {
    serde_json::json!({
        "id": info.info.id.as_str(),
        "status": format!("{:?}", info.info.status).to_lowercase(),
        "createdAt": info.info.created_at,
        "updatedAt": info.info.updated_at,
        "parentId": info.info.parent_id.map(|p| p.as_str().to_string()),
        "version": info.version,
        "timestamp": info.timestamp,
    })
}

fn branch_export_result_to_js(r: BranchExportResult) -> serde_json::Value {
    serde_json::json!({
        "branchId": r.branch_id,
        "path": r.path,
        "entryCount": r.entry_count,
        "bundleSize": r.bundle_size,
    })
}

fn branch_import_result_to_js(r: BranchImportResult) -> serde_json::Value {
    serde_json::json!({
        "branchId": r.branch_id,
        "transactionsApplied": r.transactions_applied,
        "keysWritten": r.keys_written,
    })
}

fn bundle_validate_result_to_js(r: BundleValidateResult) -> serde_json::Value {
    serde_json::json!({
        "branchId": r.branch_id,
        "formatVersion": r.format_version,
        "entryCount": r.entry_count,
        "checksumsValid": r.checksums_valid,
    })
}
