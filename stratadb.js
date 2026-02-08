'use strict';

const native = require('./index.js');
const {
  StrataError,
  NotFoundError,
  ValidationError,
  ConflictError,
  StateError,
  ConstraintError,
  AccessDeniedError,
  IoError,
  toTypedError,
} = require('./lib/errors.js');

// ---------------------------------------------------------------------------
// Wrap every async prototype method so native errors are re-thrown as typed
// StrataError subclasses.
// ---------------------------------------------------------------------------

const NativeStrata = native.Strata;

// Collect all own method names (excluding constructor) from the prototype.
const methodNames = Object.getOwnPropertyNames(NativeStrata.prototype).filter(
  (name) => name !== 'constructor' && typeof NativeStrata.prototype[name] === 'function',
);

for (const name of methodNames) {
  const original = NativeStrata.prototype[name];
  Object.defineProperty(NativeStrata.prototype, name, {
    value: async function (...args) {
      try {
        return await original.apply(this, args);
      } catch (err) {
        throw toTypedError(err);
      }
    },
    writable: true,
    configurable: true,
  });
}

// ---------------------------------------------------------------------------
// Namespace classes — thin wrappers that translate options-object APIs into
// positional calls on the native (flat) methods.
// ---------------------------------------------------------------------------

class KvNamespace {
  constructor(db) {
    this._db = db;
  }

  set(key, value) {
    return this._db.kvPut(key, value);
  }

  get(key, opts) {
    return this._db.kvGet(key, opts?.asOf);
  }

  delete(key) {
    return this._db.kvDelete(key);
  }

  keys(opts) {
    const prefix = opts?.prefix;
    const limit = opts?.limit;
    const asOf = opts?.asOf;
    if (limit != null) {
      return this._db.kvListPaginated(prefix, limit, asOf).then((r) => r.keys);
    }
    return this._db.kvList(prefix, asOf);
  }

  history(key) {
    return this._db.kvHistory(key);
  }

  getVersioned(key) {
    return this._db.kvGetVersioned(key);
  }
}

class StateNamespace {
  constructor(db) {
    this._db = db;
  }

  set(cell, value) {
    return this._db.stateSet(cell, value);
  }

  get(cell, opts) {
    return this._db.stateGet(cell, opts?.asOf);
  }

  init(cell, value) {
    return this._db.stateInit(cell, value);
  }

  cas(cell, newValue, opts) {
    return this._db.stateCas(cell, newValue, opts?.expectedVersion);
  }

  delete(cell) {
    return this._db.stateDelete(cell);
  }

  keys(opts) {
    return this._db.stateList(opts?.prefix, opts?.asOf);
  }

  history(cell) {
    return this._db.stateHistory(cell);
  }

  getVersioned(cell) {
    return this._db.stateGetVersioned(cell);
  }
}

class EventsNamespace {
  constructor(db) {
    this._db = db;
  }

  append(eventType, payload) {
    return this._db.eventAppend(eventType, payload);
  }

  get(sequence, opts) {
    return this._db.eventGet(sequence, opts?.asOf);
  }

  list(eventType, opts) {
    const limit = opts?.limit;
    const after = opts?.after;
    const asOf = opts?.asOf;
    if (limit != null || after != null) {
      return this._db.eventListPaginated(eventType, limit, after, asOf);
    }
    return this._db.eventList(eventType, asOf);
  }

  count() {
    return this._db.eventLen();
  }
}

class JsonNamespace {
  constructor(db) {
    this._db = db;
  }

  set(key, path, value) {
    return this._db.jsonSet(key, path, value);
  }

  get(key, path, opts) {
    return this._db.jsonGet(key, path, opts?.asOf);
  }

  delete(key, path) {
    return this._db.jsonDelete(key, path);
  }

  keys(opts) {
    const limit = opts?.limit ?? 100;
    return this._db.jsonList(limit, opts?.prefix, opts?.cursor, opts?.asOf);
  }

  history(key) {
    return this._db.jsonHistory(key);
  }

  getVersioned(key) {
    return this._db.jsonGetVersioned(key);
  }
}

class VectorNamespace {
  constructor(db) {
    this._db = db;
  }

  createCollection(name, opts) {
    return this._db.vectorCreateCollection(name, opts?.dimension, opts?.metric);
  }

  deleteCollection(name) {
    return this._db.vectorDeleteCollection(name);
  }

  listCollections() {
    return this._db.vectorListCollections();
  }

  stats(collection) {
    return this._db.vectorCollectionStats(collection);
  }

  upsert(collection, key, vector, opts) {
    return this._db.vectorUpsert(collection, key, vector, opts?.metadata);
  }

  get(collection, key, opts) {
    return this._db.vectorGet(collection, key, opts?.asOf);
  }

  delete(collection, key) {
    return this._db.vectorDelete(collection, key);
  }

  batchUpsert(collection, entries) {
    return this._db.vectorBatchUpsert(collection, entries);
  }

  search(collection, query, opts) {
    const k = opts?.limit ?? 10;
    const metric = opts?.metric;
    const filter = opts?.filter;
    const asOf = opts?.asOf;
    if (metric != null || filter != null) {
      return this._db.vectorSearchFiltered(collection, query, k, metric, filter, asOf);
    }
    return this._db.vectorSearch(collection, query, k, asOf);
  }
}

class BranchNamespace {
  constructor(db) {
    this._db = db;
  }

  current() {
    return this._db.currentBranch();
  }

  switch(name) {
    return this._db.setBranch(name);
  }

  create(name) {
    return this._db.createBranch(name);
  }

  fork(destination) {
    return this._db.forkBranch(destination);
  }

  list() {
    return this._db.listBranches();
  }

  delete(name) {
    return this._db.deleteBranch(name);
  }

  exists(name) {
    return this._db.branchExists(name);
  }

  get(name) {
    return this._db.branchGet(name);
  }

  diff(branchA, branchB) {
    return this._db.diffBranches(branchA, branchB);
  }

  merge(source, opts) {
    return this._db.mergeBranches(source, opts?.strategy);
  }

  export(branch, path) {
    return this._db.branchExport(branch, path);
  }

  import(path) {
    return this._db.branchImport(path);
  }

  validateBundle(path) {
    return this._db.branchValidateBundle(path);
  }
}

class SpaceNamespace {
  constructor(db) {
    this._db = db;
  }

  current() {
    return this._db.currentSpace();
  }

  switch(name) {
    return this._db.setSpace(name);
  }

  create(name) {
    return this._db.spaceCreate(name);
  }

  list() {
    return this._db.listSpaces();
  }

  delete(name, opts) {
    if (opts?.force) {
      return this._db.deleteSpaceForce(name);
    }
    return this._db.deleteSpace(name);
  }

  exists(name) {
    return this._db.spaceExists(name);
  }
}

// ---------------------------------------------------------------------------
// Read-only snapshot namespace classes — same read methods, writes throw.
// ---------------------------------------------------------------------------

function readOnlyWrite() {
  throw new StateError('Snapshots are read-only');
}

class KvSnapshot {
  constructor(db, asOf) {
    this._db = db;
    this._asOf = asOf;
  }

  set() { readOnlyWrite(); }

  get(key) {
    return this._db.kvGet(key, this._asOf);
  }

  delete() { readOnlyWrite(); }

  keys(opts) {
    const prefix = opts?.prefix;
    const limit = opts?.limit;
    if (limit != null) {
      return this._db.kvListPaginated(prefix, limit, this._asOf).then((r) => r.keys);
    }
    return this._db.kvList(prefix, this._asOf);
  }

  history(key) {
    return this._db.kvHistory(key);
  }

  getVersioned(key) {
    return this._db.kvGetVersioned(key);
  }
}

class StateSnapshot {
  constructor(db, asOf) {
    this._db = db;
    this._asOf = asOf;
  }

  set() { readOnlyWrite(); }

  get(cell) {
    return this._db.stateGet(cell, this._asOf);
  }

  init() { readOnlyWrite(); }
  cas() { readOnlyWrite(); }
  delete() { readOnlyWrite(); }

  keys(opts) {
    return this._db.stateList(opts?.prefix, this._asOf);
  }

  history(cell) {
    return this._db.stateHistory(cell);
  }

  getVersioned(cell) {
    return this._db.stateGetVersioned(cell);
  }
}

class EventsSnapshot {
  constructor(db, asOf) {
    this._db = db;
    this._asOf = asOf;
  }

  append() { readOnlyWrite(); }

  get(sequence) {
    return this._db.eventGet(sequence, this._asOf);
  }

  list(eventType, opts) {
    const limit = opts?.limit;
    const after = opts?.after;
    if (limit != null || after != null) {
      return this._db.eventListPaginated(eventType, limit, after, this._asOf);
    }
    return this._db.eventList(eventType, this._asOf);
  }

  count() {
    return this._db.eventLen();
  }
}

class JsonSnapshot {
  constructor(db, asOf) {
    this._db = db;
    this._asOf = asOf;
  }

  set() { readOnlyWrite(); }

  get(key, path) {
    return this._db.jsonGet(key, path, this._asOf);
  }

  delete() { readOnlyWrite(); }

  keys(opts) {
    const limit = opts?.limit ?? 100;
    return this._db.jsonList(limit, opts?.prefix, opts?.cursor, this._asOf);
  }

  history(key) {
    return this._db.jsonHistory(key);
  }

  getVersioned(key) {
    return this._db.jsonGetVersioned(key);
  }
}

class VectorSnapshot {
  constructor(db, asOf) {
    this._db = db;
    this._asOf = asOf;
  }

  createCollection() { readOnlyWrite(); }
  deleteCollection() { readOnlyWrite(); }

  listCollections() {
    return this._db.vectorListCollections();
  }

  stats(collection) {
    return this._db.vectorCollectionStats(collection);
  }

  upsert() { readOnlyWrite(); }

  get(collection, key) {
    return this._db.vectorGet(collection, key, this._asOf);
  }

  delete() { readOnlyWrite(); }
  batchUpsert() { readOnlyWrite(); }

  search(collection, query, opts) {
    const k = opts?.limit ?? 10;
    const metric = opts?.metric;
    const filter = opts?.filter;
    if (metric != null || filter != null) {
      return this._db.vectorSearchFiltered(collection, query, k, metric, filter, this._asOf);
    }
    return this._db.vectorSearch(collection, query, k, this._asOf);
  }
}

// ---------------------------------------------------------------------------
// StrataSnapshot — immutable time-travel view returned by db.at(timestamp)
// ---------------------------------------------------------------------------

class StrataSnapshot {
  constructor(db, asOf) {
    this._db = db;
    this._asOf = asOf;
  }

  get kv() {
    return (this._kv ??= new KvSnapshot(this._db, this._asOf));
  }

  get state() {
    return (this._state ??= new StateSnapshot(this._db, this._asOf));
  }

  get events() {
    return (this._events ??= new EventsSnapshot(this._db, this._asOf));
  }

  get json() {
    return (this._json ??= new JsonSnapshot(this._db, this._asOf));
  }

  get vector() {
    return (this._vector ??= new VectorSnapshot(this._db, this._asOf));
  }
}

// ---------------------------------------------------------------------------
// Install namespace getters, at(), and transaction() directly on the
// NativeStrata prototype so they work regardless of which factory method
// created the instance (NativeStrata.open/cache return NativeStrata, not a
// JS subclass).
// ---------------------------------------------------------------------------

Object.defineProperties(NativeStrata.prototype, {
  kv: {
    get() { return (this._kv ??= new KvNamespace(this)); },
    configurable: true,
  },
  state: {
    get() { return (this._state ??= new StateNamespace(this)); },
    configurable: true,
  },
  events: {
    get() { return (this._events ??= new EventsNamespace(this)); },
    configurable: true,
  },
  json: {
    get() { return (this._json ??= new JsonNamespace(this)); },
    configurable: true,
  },
  vector: {
    get() { return (this._vector ??= new VectorNamespace(this)); },
    configurable: true,
  },
  branch: {
    get() { return (this._branch ??= new BranchNamespace(this)); },
    configurable: true,
  },
  space: {
    get() { return (this._space ??= new SpaceNamespace(this)); },
    configurable: true,
  },
});

NativeStrata.prototype.at = function at(timestamp) {
  return new StrataSnapshot(this, timestamp);
};

NativeStrata.prototype.transaction = async function transaction(fn, opts) {
  await this.begin(opts?.readOnly);
  try {
    const result = await fn(this);
    if (opts?.readOnly) {
      await this.rollback();
      return result;
    }
    await this.commit();
    return result;
  } catch (err) {
    try { await this.rollback(); } catch (_) { /* ignore rollback errors */ }
    throw err;
  }
};

// ---------------------------------------------------------------------------
// Create a JS wrapper class that delegates to the native class, wrapping
// the static factory methods with error handling.
// ---------------------------------------------------------------------------

class Strata extends NativeStrata {
  static open(...args) {
    try {
      return NativeStrata.open(...args);
    } catch (err) {
      throw toTypedError(err);
    }
  }

  static cache(...args) {
    try {
      return NativeStrata.cache(...args);
    } catch (err) {
      throw toTypedError(err);
    }
  }
}

// Wrap top-level setup() function.
const originalSetup = native.setup;
function setup(...args) {
  try {
    return originalSetup.apply(null, args);
  } catch (err) {
    throw toTypedError(err);
  }
}

// ---------------------------------------------------------------------------
// Re-export everything.
// ---------------------------------------------------------------------------

module.exports = {
  Strata,
  StrataSnapshot,
  setup,
  // Error classes
  StrataError,
  NotFoundError,
  ValidationError,
  ConflictError,
  StateError,
  ConstraintError,
  AccessDeniedError,
  IoError,
};
