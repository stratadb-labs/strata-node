//! Node.js bindings for StrataDB.
//!
//! This module exposes the StrataDB API to Node.js via NAPI-RS.

#![deny(clippy::all)]

use napi_derive::napi;
use std::collections::HashMap;

use stratadb::{
    CollectionInfo, DistanceMetric, Error as StrataError, MergeStrategy,
    Strata as RustStrata, Value, VersionedValue,
};

/// Convert a JavaScript value to a stratadb Value.
fn js_to_value(val: serde_json::Value) -> Value {
    match val {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Int(i)
            } else if let Some(f) = n.as_f64() {
                Value::Float(f)
            } else {
                Value::Null
            }
        }
        serde_json::Value::String(s) => Value::String(s),
        serde_json::Value::Array(arr) => {
            Value::Array(arr.into_iter().map(js_to_value).collect())
        }
        serde_json::Value::Object(map) => {
            let mut obj = HashMap::new();
            for (k, v) in map {
                obj.insert(k, js_to_value(v));
            }
            Value::Object(obj)
        }
    }
}

/// Convert a stratadb Value to a serde_json Value.
fn value_to_js(val: Value) -> serde_json::Value {
    match val {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(b),
        Value::Int(i) => serde_json::Value::Number(i.into()),
        Value::Float(f) => {
            serde_json::Number::from_f64(f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        }
        Value::String(s) => serde_json::Value::String(s),
        Value::Bytes(b) => {
            // Encode bytes as base64
            serde_json::Value::String(base64_encode(&b))
        }
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
            const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
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
        fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
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

/// Convert stratadb error to napi Error.
fn to_napi_err(e: StrataError) -> napi::Error {
    napi::Error::from_reason(format!("{}", e))
}

/// StrataDB database handle.
///
/// This is the main entry point for interacting with StrataDB from Node.js.
#[napi]
pub struct Strata {
    inner: RustStrata,
}

#[napi]
impl Strata {
    /// Open a database at the given path.
    #[napi(factory)]
    pub fn open(path: String) -> napi::Result<Self> {
        let inner = RustStrata::open(&path).map_err(to_napi_err)?;
        Ok(Self { inner })
    }

    /// Create an in-memory database (no persistence).
    #[napi(factory)]
    pub fn cache() -> napi::Result<Self> {
        let inner = RustStrata::cache().map_err(to_napi_err)?;
        Ok(Self { inner })
    }

    // =========================================================================
    // KV Store
    // =========================================================================

    /// Store a key-value pair.
    #[napi(js_name = "kvPut")]
    pub fn kv_put(&self, key: String, value: serde_json::Value) -> napi::Result<u32> {
        let v = js_to_value(value);
        self.inner.kv_put(&key, v).map(|n| n as u32).map_err(to_napi_err)
    }

    /// Get a value by key.
    #[napi(js_name = "kvGet")]
    pub fn kv_get(&self, key: String) -> napi::Result<serde_json::Value> {
        match self.inner.kv_get(&key).map_err(to_napi_err)? {
            Some(v) => Ok(value_to_js(v)),
            None => Ok(serde_json::Value::Null),
        }
    }

    /// Delete a key.
    #[napi(js_name = "kvDelete")]
    pub fn kv_delete(&self, key: String) -> napi::Result<bool> {
        self.inner.kv_delete(&key).map_err(to_napi_err)
    }

    /// List keys with optional prefix filter.
    #[napi(js_name = "kvList")]
    pub fn kv_list(&self, prefix: Option<String>) -> napi::Result<Vec<String>> {
        self.inner.kv_list(prefix.as_deref()).map_err(to_napi_err)
    }

    /// Get version history for a key.
    #[napi(js_name = "kvHistory")]
    pub fn kv_history(&self, key: String) -> napi::Result<serde_json::Value> {
        match self.inner.kv_getv(&key).map_err(to_napi_err)? {
            Some(versions) => {
                let arr: Vec<serde_json::Value> = versions
                    .into_iter()
                    .map(versioned_to_js)
                    .collect();
                Ok(serde_json::Value::Array(arr))
            }
            None => Ok(serde_json::Value::Null),
        }
    }

    // =========================================================================
    // State Cell
    // =========================================================================

    /// Set a state cell value.
    #[napi(js_name = "stateSet")]
    pub fn state_set(&self, cell: String, value: serde_json::Value) -> napi::Result<u32> {
        let v = js_to_value(value);
        self.inner.state_set(&cell, v).map(|n| n as u32).map_err(to_napi_err)
    }

    /// Get a state cell value.
    #[napi(js_name = "stateGet")]
    pub fn state_get(&self, cell: String) -> napi::Result<serde_json::Value> {
        match self.inner.state_get(&cell).map_err(to_napi_err)? {
            Some(v) => Ok(value_to_js(v)),
            None => Ok(serde_json::Value::Null),
        }
    }

    /// Initialize a state cell if it doesn't exist.
    #[napi(js_name = "stateInit")]
    pub fn state_init(&self, cell: String, value: serde_json::Value) -> napi::Result<u32> {
        let v = js_to_value(value);
        self.inner.state_init(&cell, v).map(|n| n as u32).map_err(to_napi_err)
    }

    /// Compare-and-swap update based on version.
    #[napi(js_name = "stateCas")]
    pub fn state_cas(
        &self,
        cell: String,
        new_value: serde_json::Value,
        expected_version: Option<u32>,
    ) -> napi::Result<Option<u32>> {
        let v = js_to_value(new_value);
        let exp = expected_version.map(|n| n as u64);
        self.inner
            .state_cas(&cell, exp, v)
            .map(|opt| opt.map(|n| n as u32))
            .map_err(to_napi_err)
    }

    /// Get version history for a state cell.
    #[napi(js_name = "stateHistory")]
    pub fn state_history(&self, cell: String) -> napi::Result<serde_json::Value> {
        match self.inner.state_getv(&cell).map_err(to_napi_err)? {
            Some(versions) => {
                let arr: Vec<serde_json::Value> = versions
                    .into_iter()
                    .map(versioned_to_js)
                    .collect();
                Ok(serde_json::Value::Array(arr))
            }
            None => Ok(serde_json::Value::Null),
        }
    }

    // =========================================================================
    // Event Log
    // =========================================================================

    /// Append an event to the log.
    #[napi(js_name = "eventAppend")]
    pub fn event_append(&self, event_type: String, payload: serde_json::Value) -> napi::Result<u32> {
        let v = js_to_value(payload);
        self.inner.event_append(&event_type, v).map(|n| n as u32).map_err(to_napi_err)
    }

    /// Get an event by sequence number.
    #[napi(js_name = "eventGet")]
    pub fn event_get(&self, sequence: u32) -> napi::Result<serde_json::Value> {
        match self.inner.event_get(sequence as u64).map_err(to_napi_err)? {
            Some(vv) => Ok(versioned_to_js(vv)),
            None => Ok(serde_json::Value::Null),
        }
    }

    /// List events by type.
    #[napi(js_name = "eventList")]
    pub fn event_list(&self, event_type: String) -> napi::Result<serde_json::Value> {
        let events = self.inner.event_get_by_type(&event_type).map_err(to_napi_err)?;
        let arr: Vec<serde_json::Value> = events.into_iter().map(versioned_to_js).collect();
        Ok(serde_json::Value::Array(arr))
    }

    /// Get total event count.
    #[napi(js_name = "eventLen")]
    pub fn event_len(&self) -> napi::Result<u32> {
        self.inner.event_len().map(|n| n as u32).map_err(to_napi_err)
    }

    // =========================================================================
    // JSON Store
    // =========================================================================

    /// Set a value at a JSONPath.
    #[napi(js_name = "jsonSet")]
    pub fn json_set(&self, key: String, path: String, value: serde_json::Value) -> napi::Result<u32> {
        let v = js_to_value(value);
        self.inner.json_set(&key, &path, v).map(|n| n as u32).map_err(to_napi_err)
    }

    /// Get a value at a JSONPath.
    #[napi(js_name = "jsonGet")]
    pub fn json_get(&self, key: String, path: String) -> napi::Result<serde_json::Value> {
        match self.inner.json_get(&key, &path).map_err(to_napi_err)? {
            Some(v) => Ok(value_to_js(v)),
            None => Ok(serde_json::Value::Null),
        }
    }

    /// Delete a JSON document.
    #[napi(js_name = "jsonDelete")]
    pub fn json_delete(&self, key: String, path: String) -> napi::Result<u32> {
        self.inner.json_delete(&key, &path).map(|n| n as u32).map_err(to_napi_err)
    }

    /// Get version history for a JSON document.
    #[napi(js_name = "jsonHistory")]
    pub fn json_history(&self, key: String) -> napi::Result<serde_json::Value> {
        match self.inner.json_getv(&key).map_err(to_napi_err)? {
            Some(versions) => {
                let arr: Vec<serde_json::Value> = versions
                    .into_iter()
                    .map(versioned_to_js)
                    .collect();
                Ok(serde_json::Value::Array(arr))
            }
            None => Ok(serde_json::Value::Null),
        }
    }

    /// List JSON document keys.
    #[napi(js_name = "jsonList")]
    pub fn json_list(
        &self,
        limit: u32,
        prefix: Option<String>,
        cursor: Option<String>,
    ) -> napi::Result<serde_json::Value> {
        let (keys, next_cursor) = self
            .inner
            .json_list(prefix, cursor, limit as u64)
            .map_err(to_napi_err)?;
        Ok(serde_json::json!({
            "keys": keys,
            "cursor": next_cursor,
        }))
    }

    // =========================================================================
    // Vector Store
    // =========================================================================

    /// Create a vector collection.
    #[napi(js_name = "vectorCreateCollection")]
    pub fn vector_create_collection(
        &self,
        collection: String,
        dimension: u32,
        metric: Option<String>,
    ) -> napi::Result<u32> {
        let m = match metric.as_deref().unwrap_or("cosine") {
            "cosine" => DistanceMetric::Cosine,
            "euclidean" => DistanceMetric::Euclidean,
            "dot_product" | "dotproduct" => DistanceMetric::DotProduct,
            _ => return Err(napi::Error::from_reason("Invalid metric")),
        };
        self.inner
            .vector_create_collection(&collection, dimension as u64, m)
            .map(|n| n as u32)
            .map_err(to_napi_err)
    }

    /// Delete a vector collection.
    #[napi(js_name = "vectorDeleteCollection")]
    pub fn vector_delete_collection(&self, collection: String) -> napi::Result<bool> {
        self.inner
            .vector_delete_collection(&collection)
            .map_err(to_napi_err)
    }

    /// List vector collections.
    #[napi(js_name = "vectorListCollections")]
    pub fn vector_list_collections(&self) -> napi::Result<serde_json::Value> {
        let collections = self.inner.vector_list_collections().map_err(to_napi_err)?;
        let arr: Vec<serde_json::Value> = collections
            .into_iter()
            .map(collection_info_to_js)
            .collect();
        Ok(serde_json::Value::Array(arr))
    }

    /// Insert or update a vector.
    #[napi(js_name = "vectorUpsert")]
    pub fn vector_upsert(
        &self,
        collection: String,
        key: String,
        vector: Vec<f64>,
        metadata: Option<serde_json::Value>,
    ) -> napi::Result<u32> {
        let vec: Vec<f32> = vector.iter().map(|&f| f as f32).collect();
        let meta = metadata.map(js_to_value);
        self.inner
            .vector_upsert(&collection, &key, vec, meta)
            .map(|n| n as u32)
            .map_err(to_napi_err)
    }

    /// Get a vector by key.
    #[napi(js_name = "vectorGet")]
    pub fn vector_get(&self, collection: String, key: String) -> napi::Result<serde_json::Value> {
        match self.inner.vector_get(&collection, &key).map_err(to_napi_err)? {
            Some(vd) => {
                let embedding: Vec<f64> = vd.data.embedding.iter().map(|&f| f as f64).collect();
                Ok(serde_json::json!({
                    "key": vd.key,
                    "embedding": embedding,
                    "metadata": vd.data.metadata.map(value_to_js),
                    "version": vd.version,
                    "timestamp": vd.timestamp,
                }))
            }
            None => Ok(serde_json::Value::Null),
        }
    }

    /// Delete a vector.
    #[napi(js_name = "vectorDelete")]
    pub fn vector_delete(&self, collection: String, key: String) -> napi::Result<bool> {
        self.inner.vector_delete(&collection, &key).map_err(to_napi_err)
    }

    /// Search for similar vectors.
    #[napi(js_name = "vectorSearch")]
    pub fn vector_search(
        &self,
        collection: String,
        query: Vec<f64>,
        k: u32,
    ) -> napi::Result<serde_json::Value> {
        let vec: Vec<f32> = query.iter().map(|&f| f as f32).collect();
        let matches = self
            .inner
            .vector_search(&collection, vec, k as u64)
            .map_err(to_napi_err)?;
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

    // =========================================================================
    // Branch Management
    // =========================================================================

    /// Get the current branch name.
    #[napi(js_name = "currentBranch")]
    pub fn current_branch(&self) -> String {
        self.inner.current_branch().to_string()
    }

    /// Switch to a different branch.
    #[napi(js_name = "setBranch")]
    pub fn set_branch(&mut self, branch: String) -> napi::Result<()> {
        self.inner.set_branch(&branch).map_err(to_napi_err)
    }

    /// Create a new empty branch.
    #[napi(js_name = "createBranch")]
    pub fn create_branch(&self, branch: String) -> napi::Result<()> {
        self.inner.create_branch(&branch).map_err(to_napi_err)
    }

    /// Fork the current branch to a new branch, copying all data.
    #[napi(js_name = "forkBranch")]
    pub fn fork_branch(&self, destination: String) -> napi::Result<serde_json::Value> {
        let info = self.inner.fork_branch(&destination).map_err(to_napi_err)?;
        Ok(serde_json::json!({
            "source": info.source,
            "destination": info.destination,
            "keysCopied": info.keys_copied,
        }))
    }

    /// List all branches.
    #[napi(js_name = "listBranches")]
    pub fn list_branches(&self) -> napi::Result<Vec<String>> {
        self.inner.list_branches().map_err(to_napi_err)
    }

    /// Delete a branch.
    #[napi(js_name = "deleteBranch")]
    pub fn delete_branch(&self, branch: String) -> napi::Result<()> {
        self.inner.delete_branch(&branch).map_err(to_napi_err)
    }

    /// Compare two branches.
    #[napi(js_name = "diffBranches")]
    pub fn diff_branches(&self, branch_a: String, branch_b: String) -> napi::Result<serde_json::Value> {
        let diff = self
            .inner
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
    }

    /// Merge a branch into the current branch.
    #[napi(js_name = "mergeBranches")]
    pub fn merge_branches(
        &self,
        source: String,
        strategy: Option<String>,
    ) -> napi::Result<serde_json::Value> {
        let strat = match strategy.as_deref().unwrap_or("last_writer_wins") {
            "last_writer_wins" => MergeStrategy::LastWriterWins,
            "strict" => MergeStrategy::Strict,
            _ => return Err(napi::Error::from_reason("Invalid merge strategy")),
        };
        let target = self.inner.current_branch().to_string();
        let info = self
            .inner
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
    }

    // =========================================================================
    // Space Management
    // =========================================================================

    /// Get the current space name.
    #[napi(js_name = "currentSpace")]
    pub fn current_space(&self) -> String {
        self.inner.current_space().to_string()
    }

    /// Switch to a different space.
    #[napi(js_name = "setSpace")]
    pub fn set_space(&mut self, space: String) -> napi::Result<()> {
        self.inner.set_space(&space).map_err(to_napi_err)
    }

    /// List all spaces in the current branch.
    #[napi(js_name = "listSpaces")]
    pub fn list_spaces(&self) -> napi::Result<Vec<String>> {
        self.inner.list_spaces().map_err(to_napi_err)
    }

    /// Delete a space and all its data.
    #[napi(js_name = "deleteSpace")]
    pub fn delete_space(&self, space: String) -> napi::Result<()> {
        self.inner.delete_space(&space).map_err(to_napi_err)
    }

    // =========================================================================
    // Database Operations
    // =========================================================================

    /// Check database connectivity.
    #[napi]
    pub fn ping(&self) -> napi::Result<String> {
        self.inner.ping().map_err(to_napi_err)
    }

    /// Get database info.
    #[napi]
    pub fn info(&self) -> napi::Result<serde_json::Value> {
        let info = self.inner.info().map_err(to_napi_err)?;
        Ok(serde_json::json!({
            "version": info.version,
            "uptimeSecs": info.uptime_secs,
            "branchCount": info.branch_count,
            "totalKeys": info.total_keys,
        }))
    }

    /// Flush writes to disk.
    #[napi]
    pub fn flush(&self) -> napi::Result<()> {
        self.inner.flush().map_err(to_napi_err)
    }

    /// Trigger compaction.
    #[napi]
    pub fn compact(&self) -> napi::Result<()> {
        self.inner.compact().map_err(to_napi_err)
    }
}

/// Convert CollectionInfo to JSON.
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
