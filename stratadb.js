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

  batchPut(entries) {
    return this._db.kvBatchPut(entries);
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

  batchSet(entries) {
    return this._db.stateBatchSet(entries);
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

  batchAppend(entries) {
    return this._db.eventBatchAppend(entries);
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

  batchSet(entries) {
    return this._db.jsonBatchSet(entries);
  }

  batchGet(entries) {
    return this._db.jsonBatchGet(entries);
  }

  batchDelete(entries) {
    return this._db.jsonBatchDelete(entries);
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

  create(name, opts) {
    return this._db.createBranch(name, opts?.metadata);
  }

  fork(destination) {
    return this._db.forkBranch(destination);
  }

  list(opts) {
    return this._db.listBranches(opts?.limit, opts?.offset);
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

class GraphNamespace {
  constructor(db) {
    this._db = db;
  }

  // Lifecycle
  create(name, opts) {
    return this._db.graphCreate(name, opts?.cascadePolicy);
  }

  delete(name) {
    return this._db.graphDelete(name);
  }

  list() {
    return this._db.graphList();
  }

  info(name) {
    return this._db.graphGetMeta(name);
  }

  // Nodes
  addNode(graph, nodeId, opts) {
    return this._db.graphAddNode(
      graph, nodeId, opts?.entityRef, opts?.properties, opts?.objectType,
    );
  }

  getNode(graph, nodeId) {
    return this._db.graphGetNode(graph, nodeId);
  }

  removeNode(graph, nodeId) {
    return this._db.graphRemoveNode(graph, nodeId);
  }

  listNodes(graph, opts) {
    if (opts?.limit != null) {
      return this._db.graphListNodesPaginated(graph, opts.limit, opts?.cursor);
    }
    return this._db.graphListNodes(graph);
  }

  // Edges
  addEdge(graph, src, dst, edgeType, opts) {
    return this._db.graphAddEdge(
      graph, src, dst, edgeType, opts?.weight, opts?.properties,
    );
  }

  removeEdge(graph, src, dst, edgeType) {
    return this._db.graphRemoveEdge(graph, src, dst, edgeType);
  }

  neighbors(graph, nodeId, opts) {
    return this._db.graphNeighbors(
      graph, nodeId, opts?.direction, opts?.edgeType,
    );
  }

  // Bulk & Traversal
  bulkInsert(graph, data, opts) {
    return this._db.graphBulkInsert(
      graph, data?.nodes ?? [], data?.edges ?? [], opts?.chunkSize,
    );
  }

  bfs(graph, start, maxDepth, opts) {
    return this._db.graphBfs(
      graph, start, maxDepth,
      opts?.maxNodes, opts?.edgeTypes, opts?.direction,
    );
  }

  // Ontology
  defineObjectType(graph, definition) {
    return this._db.graphDefineObjectType(graph, definition);
  }

  getObjectType(graph, name) {
    return this._db.graphGetObjectType(graph, name);
  }

  listObjectTypes(graph) {
    return this._db.graphListObjectTypes(graph);
  }

  deleteObjectType(graph, name) {
    return this._db.graphDeleteObjectType(graph, name);
  }

  defineLinkType(graph, definition) {
    return this._db.graphDefineLinkType(graph, definition);
  }

  getLinkType(graph, name) {
    return this._db.graphGetLinkType(graph, name);
  }

  listLinkTypes(graph) {
    return this._db.graphListLinkTypes(graph);
  }

  deleteLinkType(graph, name) {
    return this._db.graphDeleteLinkType(graph, name);
  }

  freezeOntology(graph) {
    return this._db.graphFreezeOntology(graph);
  }

  ontologyStatus(graph) {
    return this._db.graphOntologyStatus(graph);
  }

  ontologySummary(graph) {
    return this._db.graphOntologySummary(graph);
  }

  listOntologyTypes(graph) {
    return this._db.graphListOntologyTypes(graph);
  }

  nodesByType(graph, objectType) {
    return this._db.graphNodesByType(graph, objectType);
  }

  // Analytics
  wcc(graph) {
    return this._db.graphWcc(graph);
  }

  cdlp(graph, maxIterations, opts) {
    return this._db.graphCdlp(graph, maxIterations, opts?.direction);
  }

  pagerank(graph, opts) {
    return this._db.graphPagerank(
      graph, opts?.damping, opts?.maxIterations, opts?.tolerance,
    );
  }

  lcc(graph) {
    return this._db.graphLcc(graph);
  }

  sssp(graph, source, opts) {
    return this._db.graphSssp(graph, source, opts?.direction);
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
  batchPut() { readOnlyWrite(); }

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
  batchSet() { readOnlyWrite(); }

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
  batchAppend() { readOnlyWrite(); }

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
  batchSet() { readOnlyWrite(); }
  batchDelete() { readOnlyWrite(); }

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

class GraphSnapshot {
  constructor(db) {
    this._db = db;
  }

  // Write operations throw
  create() { readOnlyWrite(); }
  delete() { readOnlyWrite(); }
  addNode() { readOnlyWrite(); }
  removeNode() { readOnlyWrite(); }
  addEdge() { readOnlyWrite(); }
  removeEdge() { readOnlyWrite(); }
  bulkInsert() { readOnlyWrite(); }
  defineObjectType() { readOnlyWrite(); }
  deleteObjectType() { readOnlyWrite(); }
  defineLinkType() { readOnlyWrite(); }
  deleteLinkType() { readOnlyWrite(); }
  freezeOntology() { readOnlyWrite(); }

  // Read operations
  list() { return this._db.graphList(); }
  info(name) { return this._db.graphGetMeta(name); }
  getNode(graph, nodeId) { return this._db.graphGetNode(graph, nodeId); }
  listNodes(graph, opts) {
    if (opts?.limit != null) {
      return this._db.graphListNodesPaginated(graph, opts.limit, opts?.cursor);
    }
    return this._db.graphListNodes(graph);
  }
  neighbors(graph, nodeId, opts) {
    return this._db.graphNeighbors(graph, nodeId, opts?.direction, opts?.edgeType);
  }
  bfs(graph, start, maxDepth, opts) {
    return this._db.graphBfs(graph, start, maxDepth, opts?.maxNodes, opts?.edgeTypes, opts?.direction);
  }
  getObjectType(graph, name) { return this._db.graphGetObjectType(graph, name); }
  listObjectTypes(graph) { return this._db.graphListObjectTypes(graph); }
  getLinkType(graph, name) { return this._db.graphGetLinkType(graph, name); }
  listLinkTypes(graph) { return this._db.graphListLinkTypes(graph); }
  ontologyStatus(graph) { return this._db.graphOntologyStatus(graph); }
  ontologySummary(graph) { return this._db.graphOntologySummary(graph); }
  listOntologyTypes(graph) { return this._db.graphListOntologyTypes(graph); }
  nodesByType(graph, objectType) { return this._db.graphNodesByType(graph, objectType); }
  wcc(graph) { return this._db.graphWcc(graph); }
  cdlp(graph, maxIterations, opts) { return this._db.graphCdlp(graph, maxIterations, opts?.direction); }
  pagerank(graph, opts) { return this._db.graphPagerank(graph, opts?.damping, opts?.maxIterations, opts?.tolerance); }
  lcc(graph) { return this._db.graphLcc(graph); }
  sssp(graph, source, opts) { return this._db.graphSssp(graph, source, opts?.direction); }
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

  get graph() {
    return (this._graph ??= new GraphSnapshot(this._db));
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
  graph: {
    get() { return (this._graph ??= new GraphNamespace(this)); },
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
