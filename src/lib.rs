//! Node.js bindings for StrataDB.
//!
//! This module exposes the StrataDB API to Node.js via NAPI-RS.

#![deny(clippy::all)]

use napi_derive::napi;
use std::collections::HashMap;

use std::cell::RefCell;

use stratadb::{
    BatchVectorEntry, BranchExportResult, BranchImportResult, BundleValidateResult,
    CollectionInfo, Command, DistanceMetric, Error as StrataError, FilterOp, MergeStrategy,
    MetadataFilter, Output, Session, Strata as RustStrata, TxnOptions, Value,
    VersionedBranchInfo, VersionedValue,
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
    /// Session for transaction support. Lazily initialized.
    session: RefCell<Option<Session>>,
}

#[napi]
impl Strata {
    /// Open a database at the given path.
    #[napi(factory)]
    pub fn open(path: String) -> napi::Result<Self> {
        let inner = RustStrata::open(&path).map_err(to_napi_err)?;
        Ok(Self {
            inner,
            session: RefCell::new(None),
        })
    }

    /// Create an in-memory database (no persistence).
    #[napi(factory)]
    pub fn cache() -> napi::Result<Self> {
        let inner = RustStrata::cache().map_err(to_napi_err)?;
        Ok(Self {
            inner,
            session: RefCell::new(None),
        })
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

    /// Get statistics for a single collection.
    #[napi(js_name = "vectorCollectionStats")]
    pub fn vector_collection_stats(&self, collection: String) -> napi::Result<serde_json::Value> {
        let info = self
            .inner
            .vector_collection_stats(&collection)
            .map_err(to_napi_err)?;
        Ok(collection_info_to_js(info))
    }

    /// Batch insert/update multiple vectors.
    ///
    /// Each vector should be an object with 'key', 'vector', and optional 'metadata'.
    #[napi(js_name = "vectorBatchUpsert")]
    pub fn vector_batch_upsert(
        &self,
        collection: String,
        vectors: Vec<serde_json::Value>,
    ) -> napi::Result<Vec<u32>> {
        let batch: Vec<BatchVectorEntry> = vectors
            .into_iter()
            .map(|v| {
                let obj = v
                    .as_object()
                    .ok_or_else(|| napi::Error::from_reason("Expected object"))?;
                let key = obj
                    .get("key")
                    .and_then(|k| k.as_str())
                    .ok_or_else(|| napi::Error::from_reason("Missing 'key'"))?
                    .to_string();
                let vec: Vec<f32> = obj
                    .get("vector")
                    .and_then(|v| v.as_array())
                    .ok_or_else(|| napi::Error::from_reason("Missing 'vector'"))?
                    .iter()
                    .map(|n| n.as_f64().unwrap_or(0.0) as f32)
                    .collect();
                let meta = obj.get("metadata").map(|m| js_to_value(m.clone()));
                Ok(BatchVectorEntry {
                    key,
                    vector: vec,
                    metadata: meta,
                })
            })
            .collect::<napi::Result<_>>()?;
        self.inner
            .vector_batch_upsert(&collection, batch)
            .map(|versions| versions.into_iter().map(|v| v as u32).collect())
            .map_err(to_napi_err)
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

    /// Check if a branch exists.
    #[napi(js_name = "branchExists")]
    pub fn branch_exists(&self, name: String) -> napi::Result<bool> {
        self.inner.branches().exists(&name).map_err(to_napi_err)
    }

    /// Get branch metadata with version info.
    ///
    /// Returns an object with branch info, or null if the branch does not exist.
    #[napi(js_name = "branchGet")]
    pub fn branch_get(&self, name: String) -> napi::Result<serde_json::Value> {
        match self.inner.branch_get(&name).map_err(to_napi_err)? {
            Some(info) => Ok(versioned_branch_info_to_js(info)),
            None => Ok(serde_json::Value::Null),
        }
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

    /// Force delete a space even if non-empty.
    #[napi(js_name = "deleteSpaceForce")]
    pub fn delete_space_force(&self, space: String) -> napi::Result<()> {
        self.inner.delete_space_force(&space).map_err(to_napi_err)
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

    // =========================================================================
    // Bundle Operations
    // =========================================================================

    /// Export a branch to a bundle file.
    #[napi(js_name = "branchExport")]
    pub fn branch_export(&self, branch: String, path: String) -> napi::Result<serde_json::Value> {
        let result = self
            .inner
            .branch_export(&branch, &path)
            .map_err(to_napi_err)?;
        Ok(branch_export_result_to_js(result))
    }

    /// Import a branch from a bundle file.
    #[napi(js_name = "branchImport")]
    pub fn branch_import(&self, path: String) -> napi::Result<serde_json::Value> {
        let result = self.inner.branch_import(&path).map_err(to_napi_err)?;
        Ok(branch_import_result_to_js(result))
    }

    /// Validate a bundle file without importing.
    #[napi(js_name = "branchValidateBundle")]
    pub fn branch_validate_bundle(&self, path: String) -> napi::Result<serde_json::Value> {
        let result = self
            .inner
            .branch_validate_bundle(&path)
            .map_err(to_napi_err)?;
        Ok(bundle_validate_result_to_js(result))
    }

    // =========================================================================
    // Transaction Operations (MVP+1)
    // =========================================================================

    /// Begin a new transaction.
    ///
    /// All subsequent data operations will be part of this transaction until
    /// commit() or rollback() is called.
    #[napi(js_name = "begin")]
    pub fn begin(&self, read_only: Option<bool>) -> napi::Result<()> {
        let mut session_ref = self.session.borrow_mut();
        if session_ref.is_none() {
            *session_ref = Some(self.inner.session());
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
    }

    /// Commit the current transaction.
    ///
    /// Returns the commit version number.
    #[napi]
    pub fn commit(&self) -> napi::Result<u32> {
        let mut session_ref = self.session.borrow_mut();
        let session = session_ref
            .as_mut()
            .ok_or_else(|| napi::Error::from_reason("No transaction active"))?;

        match session.execute(Command::TxnCommit).map_err(to_napi_err)? {
            Output::TxnCommitted { version } => Ok(version as u32),
            _ => Err(napi::Error::from_reason("Unexpected output for TxnCommit")),
        }
    }

    /// Rollback the current transaction.
    #[napi]
    pub fn rollback(&self) -> napi::Result<()> {
        let mut session_ref = self.session.borrow_mut();
        let session = session_ref
            .as_mut()
            .ok_or_else(|| napi::Error::from_reason("No transaction active"))?;

        session.execute(Command::TxnRollback).map_err(to_napi_err)?;
        Ok(())
    }

    /// Get current transaction info.
    ///
    /// Returns an object with 'id', 'status', 'startedAt', or null if no transaction is active.
    #[napi(js_name = "txnInfo")]
    pub fn txn_info(&self) -> napi::Result<serde_json::Value> {
        let mut session_ref = self.session.borrow_mut();
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
    }

    /// Check if a transaction is currently active.
    #[napi(js_name = "txnIsActive")]
    pub fn txn_is_active(&self) -> napi::Result<bool> {
        let mut session_ref = self.session.borrow_mut();
        if session_ref.is_none() {
            return Ok(false);
        }
        let session = session_ref.as_mut().unwrap();

        match session.execute(Command::TxnIsActive).map_err(to_napi_err)? {
            Output::Bool(active) => Ok(active),
            _ => Err(napi::Error::from_reason("Unexpected output for TxnIsActive")),
        }
    }

    // =========================================================================
    // Missing State Operations (MVP+1)
    // =========================================================================

    /// Delete a state cell.
    ///
    /// Returns true if the cell existed and was deleted, false otherwise.
    #[napi(js_name = "stateDelete")]
    pub fn state_delete(&self, cell: String) -> napi::Result<bool> {
        match self
            .inner
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
    }

    /// List state cell names with optional prefix filter.
    #[napi(js_name = "stateList")]
    pub fn state_list(&self, prefix: Option<String>) -> napi::Result<Vec<String>> {
        match self
            .inner
            .executor()
            .execute(Command::StateList {
                branch: None,
                space: None,
                prefix,
            })
            .map_err(to_napi_err)?
        {
            Output::Keys(keys) => Ok(keys),
            _ => Err(napi::Error::from_reason("Unexpected output for StateList")),
        }
    }

    // =========================================================================
    // Versioned Getters (MVP+1)
    // =========================================================================

    /// Get a value by key with version info.
    ///
    /// Returns an object with 'value', 'version', 'timestamp', or null if key doesn't exist.
    #[napi(js_name = "kvGetVersioned")]
    pub fn kv_get_versioned(&self, key: String) -> napi::Result<serde_json::Value> {
        match self.inner.kv_getv(&key).map_err(to_napi_err)? {
            Some(versions) if !versions.is_empty() => {
                Ok(versioned_to_js(versions.into_iter().next().unwrap()))
            }
            _ => Ok(serde_json::Value::Null),
        }
    }

    /// Get a state cell value with version info.
    ///
    /// Returns an object with 'value', 'version', 'timestamp', or null if cell doesn't exist.
    #[napi(js_name = "stateGetVersioned")]
    pub fn state_get_versioned(&self, cell: String) -> napi::Result<serde_json::Value> {
        match self.inner.state_getv(&cell).map_err(to_napi_err)? {
            Some(versions) if !versions.is_empty() => {
                Ok(versioned_to_js(versions.into_iter().next().unwrap()))
            }
            _ => Ok(serde_json::Value::Null),
        }
    }

    /// Get a JSON document value with version info.
    ///
    /// Returns an object with 'value', 'version', 'timestamp', or null if key doesn't exist.
    #[napi(js_name = "jsonGetVersioned")]
    pub fn json_get_versioned(&self, key: String) -> napi::Result<serde_json::Value> {
        match self.inner.json_getv(&key).map_err(to_napi_err)? {
            Some(versions) if !versions.is_empty() => {
                Ok(versioned_to_js(versions.into_iter().next().unwrap()))
            }
            _ => Ok(serde_json::Value::Null),
        }
    }

    // =========================================================================
    // Pagination Improvements (MVP+1)
    // =========================================================================

    /// List keys with pagination support.
    ///
    /// Returns an object with 'keys' array.
    #[napi(js_name = "kvListPaginated")]
    pub fn kv_list_paginated(
        &self,
        prefix: Option<String>,
        limit: Option<u32>,
        cursor: Option<String>,
    ) -> napi::Result<serde_json::Value> {
        match self
            .inner
            .executor()
            .execute(Command::KvList {
                branch: None,
                space: None,
                prefix,
                cursor,
                limit: limit.map(|l| l as u64),
            })
            .map_err(to_napi_err)?
        {
            Output::Keys(keys) => Ok(serde_json::json!({ "keys": keys })),
            _ => Err(napi::Error::from_reason("Unexpected output for KvList")),
        }
    }

    /// List events by type with pagination support.
    ///
    /// Returns a list of event objects. Use `after` to paginate from a sequence number.
    #[napi(js_name = "eventListPaginated")]
    pub fn event_list_paginated(
        &self,
        event_type: String,
        limit: Option<u32>,
        after: Option<u32>,
    ) -> napi::Result<serde_json::Value> {
        match self
            .inner
            .executor()
            .execute(Command::EventGetByType {
                branch: None,
                space: None,
                event_type,
                limit: limit.map(|l| l as u64),
                after_sequence: after.map(|a| a as u64),
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
    }

    // =========================================================================
    // Enhanced Vector Search (MVP+1)
    // =========================================================================

    /// Search for similar vectors with optional filter and metric override.
    #[napi(js_name = "vectorSearchFiltered")]
    pub fn vector_search_filtered(
        &self,
        collection: String,
        query: Vec<f64>,
        k: u32,
        metric: Option<String>,
        filter: Option<Vec<serde_json::Value>>,
    ) -> napi::Result<serde_json::Value> {
        let vec: Vec<f32> = query.iter().map(|&f| f as f32).collect();

        let metric_enum = match metric.as_deref() {
            Some("cosine") => Some(DistanceMetric::Cosine),
            Some("euclidean") => Some(DistanceMetric::Euclidean),
            Some("dot_product") | Some("dotproduct") => Some(DistanceMetric::DotProduct),
            Some(m) => return Err(napi::Error::from_reason(format!("Invalid metric: {}", m))),
            None => None,
        };

        let filter_vec = match filter {
            Some(arr) => {
                let mut filters = Vec::new();
                for item in arr {
                    let obj = item
                        .as_object()
                        .ok_or_else(|| napi::Error::from_reason("Filter must be an object"))?;
                    let field = obj
                        .get("field")
                        .and_then(|f| f.as_str())
                        .ok_or_else(|| napi::Error::from_reason("Filter missing 'field'"))?
                        .to_string();
                    let op_str = obj
                        .get("op")
                        .and_then(|o| o.as_str())
                        .ok_or_else(|| napi::Error::from_reason("Filter missing 'op'"))?;
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
                                "Invalid filter op: {}",
                                op_str
                            )))
                        }
                    };
                    let value_json = obj
                        .get("value")
                        .ok_or_else(|| napi::Error::from_reason("Filter missing 'value'"))?
                        .clone();
                    let value = js_to_value(value_json);
                    filters.push(MetadataFilter { field, op, value });
                }
                Some(filters)
            }
            None => None,
        };

        match self
            .inner
            .executor()
            .execute(Command::VectorSearch {
                branch: None,
                space: None,
                collection,
                query: vec,
                k: k as u64,
                filter: filter_vec,
                metric: metric_enum,
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
    }

    // =========================================================================
    // Space Operations (MVP+1)
    // =========================================================================

    /// Create a new space explicitly.
    ///
    /// Spaces are auto-created on first write, but this allows pre-creation.
    #[napi(js_name = "spaceCreate")]
    pub fn space_create(&self, space: String) -> napi::Result<()> {
        match self
            .inner
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
    }

    /// Check if a space exists in the current branch.
    #[napi(js_name = "spaceExists")]
    pub fn space_exists(&self, space: String) -> napi::Result<bool> {
        match self
            .inner
            .executor()
            .execute(Command::SpaceExists {
                branch: None,
                space,
            })
            .map_err(to_napi_err)?
        {
            Output::Bool(exists) => Ok(exists),
            _ => Err(napi::Error::from_reason("Unexpected output for SpaceExists")),
        }
    }

    // =========================================================================
    // Search (MVP+1)
    // =========================================================================

    /// Search across multiple primitives for matching content.
    ///
    /// Returns a list of objects with 'entity', 'primitive', 'score', 'rank', 'snippet'.
    #[napi]
    pub fn search(
        &self,
        query: String,
        k: Option<u32>,
        primitives: Option<Vec<String>>,
    ) -> napi::Result<serde_json::Value> {
        match self
            .inner
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

/// Convert VersionedBranchInfo to JSON.
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

/// Convert BranchExportResult to JSON.
fn branch_export_result_to_js(r: BranchExportResult) -> serde_json::Value {
    serde_json::json!({
        "branchId": r.branch_id,
        "path": r.path,
        "entryCount": r.entry_count,
        "bundleSize": r.bundle_size,
    })
}

/// Convert BranchImportResult to JSON.
fn branch_import_result_to_js(r: BranchImportResult) -> serde_json::Value {
    serde_json::json!({
        "branchId": r.branch_id,
        "transactionsApplied": r.transactions_applied,
        "keysWritten": r.keys_written,
    })
}

/// Convert BundleValidateResult to JSON.
fn bundle_validate_result_to_js(r: BundleValidateResult) -> serde_json::Value {
    serde_json::json!({
        "branchId": r.branch_id,
        "formatVersion": r.format_version,
        "entryCount": r.entry_count,
        "checksumsValid": r.checksums_valid,
    })
}
