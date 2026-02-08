/**
 * TypeScript definitions for StrataDB Node.js SDK
 *
 * All data methods are async and return Promises. Factory methods
 * (Strata.open, Strata.cache) remain synchronous.
 */

// =========================================================================
// Error classes
// =========================================================================

/** Base error for all StrataDB errors. */
export class StrataError extends Error {
  /** Machine-readable error category. */
  code: string;
}
export class NotFoundError extends StrataError {}
export class ValidationError extends StrataError {}
export class ConflictError extends StrataError {}
export class StateError extends StrataError {}
export class ConstraintError extends StrataError {}
export class AccessDeniedError extends StrataError {}
export class IoError extends StrataError {}

// =========================================================================
// Value types
// =========================================================================

/** JSON-compatible value type */
export type JsonValue =
  | null
  | boolean
  | number
  | string
  | JsonValue[]
  | { [key: string]: JsonValue };

/** Versioned value returned by history operations */
export interface VersionedValue {
  value: JsonValue;
  version: number;
  timestamp: number;
}

/** JSON list result with pagination cursor */
export interface JsonListResult {
  keys: string[];
  cursor?: string;
}

/** Vector collection information */
export interface CollectionInfo {
  name: string;
  dimension: number;
  metric: string;
  count: number;
  indexType: string;
  memoryBytes: number;
}

/** Vector data with metadata */
export interface VectorData {
  key: string;
  embedding: number[];
  metadata?: JsonValue;
  version: number;
  timestamp: number;
}

/** Vector search result */
export interface SearchMatch {
  key: string;
  score: number;
  metadata?: JsonValue;
}

/** Fork operation result */
export interface ForkResult {
  source: string;
  destination: string;
  keysCopied: number;
}

/** Branch diff summary */
export interface DiffSummary {
  totalAdded: number;
  totalRemoved: number;
  totalModified: number;
}

/** Branch diff result */
export interface DiffResult {
  branchA: string;
  branchB: string;
  summary: DiffSummary;
}

/** Merge conflict */
export interface MergeConflict {
  key: string;
  space: string;
}

/** Merge operation result */
export interface MergeResult {
  keysApplied: number;
  spacesMerged: number;
  conflicts: MergeConflict[];
}

/** Database information */
export interface DatabaseInfo {
  version: string;
  uptimeSecs: number;
  branchCount: number;
  totalKeys: number;
}

/** Branch metadata with version info */
export interface BranchInfo {
  id: string;
  status: string;
  createdAt: number;
  updatedAt: number;
  parentId?: string;
  version: number;
  timestamp: number;
}

/** Branch export result */
export interface BranchExportResult {
  branchId: string;
  path: string;
  entryCount: number;
  bundleSize: number;
}

/** Branch import result */
export interface BranchImportResult {
  branchId: string;
  transactionsApplied: number;
  keysWritten: number;
}

/** Bundle validation result */
export interface BundleValidateResult {
  branchId: string;
  formatVersion: number;
  entryCount: number;
  checksumsValid: boolean;
}

/** Vector entry for batch upsert */
export interface BatchVectorEntry {
  key: string;
  vector: number[];
  metadata?: JsonValue;
}

/** Transaction info */
export interface TransactionInfo {
  id: string;
  status: string;
  startedAt: number;
}

/** Metadata filter for vector search */
export interface MetadataFilter {
  field: string;
  op: 'eq' | 'ne' | 'gt' | 'gte' | 'lt' | 'lte' | 'in' | 'contains';
  value: JsonValue;
}

/** KV list result with pagination */
export interface KvListResult {
  keys: string[];
}

/** Cross-primitive search result */
export interface SearchHit {
  entity: string;
  primitive: string;
  score: number;
  rank: number;
  snippet?: string;
}

/** Time range for a branch */
export interface TimeRange {
  oldestTs: number | null;
  latestTs: number | null;
}

/** Options for opening a database */
export interface OpenOptions {
  /** Enable automatic text embedding for semantic search. */
  autoEmbed?: boolean;
  /** Open in read-only mode. */
  readOnly?: boolean;
}

// =========================================================================
// Options types for the new namespace API
// =========================================================================

/** Options for KV get */
export interface KvGetOptions {
  asOf?: number;
}

/** Options for KV keys listing */
export interface KvKeysOptions {
  prefix?: string;
  limit?: number;
  asOf?: number;
}

/** Options for state get */
export interface StateGetOptions {
  asOf?: number;
}

/** Options for state CAS */
export interface StateCasOptions {
  expectedVersion?: number;
}

/** Options for state keys listing */
export interface StateKeysOptions {
  prefix?: string;
  asOf?: number;
}

/** Options for event get */
export interface EventGetOptions {
  asOf?: number;
}

/** Options for event listing */
export interface EventListOptions {
  limit?: number;
  after?: number;
  asOf?: number;
}

/** Options for JSON get */
export interface JsonGetOptions {
  asOf?: number;
}

/** Options for JSON keys listing */
export interface JsonKeysOptions {
  limit?: number;
  prefix?: string;
  cursor?: string;
  asOf?: number;
}

/** Options for vector collection creation */
export interface VectorCreateCollectionOptions {
  dimension: number;
  metric?: string;
}

/** Options for vector upsert */
export interface VectorUpsertOptions {
  metadata?: JsonValue;
}

/** Options for vector get */
export interface VectorGetOptions {
  asOf?: number;
}

/** Options for vector search */
export interface VectorSearchOptions {
  limit?: number;
  metric?: string;
  filter?: MetadataFilter[];
  asOf?: number;
}

/** Options for branch merge */
export interface BranchMergeOptions {
  strategy?: string;
}

/** Options for space delete */
export interface SpaceDeleteOptions {
  force?: boolean;
}

/** Options for transaction callback */
export interface TransactionOptions {
  readOnly?: boolean;
}

// =========================================================================
// Namespace interfaces
// =========================================================================

/** KV Store namespace — accessed via `db.kv` */
export interface KvNamespace {
  set(key: string, value: JsonValue): Promise<number>;
  get(key: string, opts?: KvGetOptions): Promise<JsonValue>;
  delete(key: string): Promise<boolean>;
  keys(opts?: KvKeysOptions): Promise<string[]>;
  history(key: string): Promise<VersionedValue[] | null>;
  getVersioned(key: string): Promise<VersionedValue | null>;
}

/** State Cell namespace — accessed via `db.state` */
export interface StateNamespace {
  set(cell: string, value: JsonValue): Promise<number>;
  get(cell: string, opts?: StateGetOptions): Promise<JsonValue>;
  init(cell: string, value: JsonValue): Promise<number>;
  cas(cell: string, newValue: JsonValue, opts?: StateCasOptions): Promise<number | null>;
  delete(cell: string): Promise<boolean>;
  keys(opts?: StateKeysOptions): Promise<string[]>;
  history(cell: string): Promise<VersionedValue[] | null>;
  getVersioned(cell: string): Promise<VersionedValue | null>;
}

/** Event Log namespace — accessed via `db.events` */
export interface EventsNamespace {
  append(eventType: string, payload: JsonValue): Promise<number>;
  get(sequence: number, opts?: EventGetOptions): Promise<VersionedValue | null>;
  list(eventType: string, opts?: EventListOptions): Promise<VersionedValue[]>;
  count(): Promise<number>;
}

/** JSON Document namespace — accessed via `db.json` */
export interface JsonNamespace {
  set(key: string, path: string, value: JsonValue): Promise<number>;
  get(key: string, path: string, opts?: JsonGetOptions): Promise<JsonValue>;
  delete(key: string, path: string): Promise<number>;
  keys(opts?: JsonKeysOptions): Promise<JsonListResult>;
  history(key: string): Promise<VersionedValue[] | null>;
  getVersioned(key: string): Promise<VersionedValue | null>;
}

/** Vector Store namespace — accessed via `db.vector` */
export interface VectorNamespace {
  createCollection(name: string, opts: VectorCreateCollectionOptions): Promise<number>;
  deleteCollection(name: string): Promise<boolean>;
  listCollections(): Promise<CollectionInfo[]>;
  stats(collection: string): Promise<CollectionInfo>;
  upsert(collection: string, key: string, vector: number[], opts?: VectorUpsertOptions): Promise<number>;
  get(collection: string, key: string, opts?: VectorGetOptions): Promise<VectorData | null>;
  delete(collection: string, key: string): Promise<boolean>;
  batchUpsert(collection: string, entries: BatchVectorEntry[]): Promise<number[]>;
  search(collection: string, query: number[], opts?: VectorSearchOptions): Promise<SearchMatch[]>;
}

/** Branch Management namespace — accessed via `db.branch` */
export interface BranchNamespace {
  current(): Promise<string>;
  switch(name: string): Promise<void>;
  create(name: string): Promise<void>;
  fork(destination: string): Promise<ForkResult>;
  list(): Promise<string[]>;
  delete(name: string): Promise<void>;
  exists(name: string): Promise<boolean>;
  get(name: string): Promise<BranchInfo | null>;
  diff(branchA: string, branchB: string): Promise<DiffResult>;
  merge(source: string, opts?: BranchMergeOptions): Promise<MergeResult>;
  export(branch: string, path: string): Promise<BranchExportResult>;
  import(path: string): Promise<BranchImportResult>;
  validateBundle(path: string): Promise<BundleValidateResult>;
}

/** Space Management namespace — accessed via `db.space` */
export interface SpaceNamespace {
  current(): Promise<string>;
  switch(name: string): Promise<void>;
  create(name: string): Promise<void>;
  list(): Promise<string[]>;
  delete(name: string, opts?: SpaceDeleteOptions): Promise<void>;
  exists(name: string): Promise<boolean>;
}

// =========================================================================
// Read-only snapshot namespace interfaces (returned by db.at())
// =========================================================================

/** Read-only KV namespace for snapshots */
export interface KvSnapshotNamespace {
  get(key: string): Promise<JsonValue>;
  keys(opts?: Omit<KvKeysOptions, 'asOf'>): Promise<string[]>;
  history(key: string): Promise<VersionedValue[] | null>;
  getVersioned(key: string): Promise<VersionedValue | null>;
}

/** Read-only State namespace for snapshots */
export interface StateSnapshotNamespace {
  get(cell: string): Promise<JsonValue>;
  keys(opts?: Omit<StateKeysOptions, 'asOf'>): Promise<string[]>;
  history(cell: string): Promise<VersionedValue[] | null>;
  getVersioned(cell: string): Promise<VersionedValue | null>;
}

/** Read-only Events namespace for snapshots */
export interface EventsSnapshotNamespace {
  get(sequence: number): Promise<VersionedValue | null>;
  list(eventType: string, opts?: Omit<EventListOptions, 'asOf'>): Promise<VersionedValue[]>;
  count(): Promise<number>;
}

/** Read-only JSON namespace for snapshots */
export interface JsonSnapshotNamespace {
  get(key: string, path: string): Promise<JsonValue>;
  keys(opts?: Omit<JsonKeysOptions, 'asOf'>): Promise<JsonListResult>;
  history(key: string): Promise<VersionedValue[] | null>;
  getVersioned(key: string): Promise<VersionedValue | null>;
}

/** Read-only Vector namespace for snapshots */
export interface VectorSnapshotNamespace {
  listCollections(): Promise<CollectionInfo[]>;
  stats(collection: string): Promise<CollectionInfo>;
  get(collection: string, key: string): Promise<VectorData | null>;
  search(collection: string, query: number[], opts?: Omit<VectorSearchOptions, 'asOf'>): Promise<SearchMatch[]>;
}

/**
 * Immutable time-travel snapshot returned by `db.at(timestamp)`.
 * Only read operations are available; writes throw StateError.
 */
export class StrataSnapshot {
  readonly kv: KvSnapshotNamespace;
  readonly state: StateSnapshotNamespace;
  readonly events: EventsSnapshotNamespace;
  readonly json: JsonSnapshotNamespace;
  readonly vector: VectorSnapshotNamespace;
}

// =========================================================================
// Main Strata class
// =========================================================================

/**
 * StrataDB database handle.
 *
 * All data methods are async and return Promises.
 * Factory methods (`open`, `cache`) are synchronous.
 */
export class Strata {
  // Factory methods (synchronous)
  static open(path: string, options?: OpenOptions): Strata;
  static cache(): Strata;

  // -----------------------------------------------------------------------
  // Namespace accessors (NEW — preferred API)
  // -----------------------------------------------------------------------

  /** KV Store operations */
  readonly kv: KvNamespace;
  /** State Cell operations */
  readonly state: StateNamespace;
  /** Event Log operations */
  readonly events: EventsNamespace;
  /** JSON Document operations */
  readonly json: JsonNamespace;
  /** Vector Store operations */
  readonly vector: VectorNamespace;
  /** Branch Management operations */
  readonly branch: BranchNamespace;
  /** Space Management operations */
  readonly space: SpaceNamespace;

  // -----------------------------------------------------------------------
  // Time travel
  // -----------------------------------------------------------------------

  /** Create an immutable snapshot at the given timestamp. */
  at(timestamp: number): StrataSnapshot;

  // -----------------------------------------------------------------------
  // Transaction callback
  // -----------------------------------------------------------------------

  /**
   * Execute a function inside a transaction with auto-commit on success
   * and auto-rollback on error.
   */
  transaction<T>(fn: (tx: Strata) => Promise<T>, opts?: TransactionOptions): Promise<T>;

  // -----------------------------------------------------------------------
  // Database Operations
  // -----------------------------------------------------------------------

  ping(): Promise<string>;
  info(): Promise<DatabaseInfo>;
  flush(): Promise<void>;
  compact(): Promise<void>;
  close(): Promise<void>;

  // Search
  search(query: string, k?: number, primitives?: string[]): Promise<SearchHit[]>;

  // Retention
  retentionApply(): Promise<void>;

  // Time Travel
  timeRange(): Promise<TimeRange>;

  // Transaction Operations (manual — prefer `transaction()` callback)
  begin(readOnly?: boolean): Promise<void>;
  commit(): Promise<number>;
  rollback(): Promise<void>;
  txnInfo(): Promise<TransactionInfo | null>;
  txnIsActive(): Promise<boolean>;

}

/**
 * Download model files for auto-embedding.
 */
export function setup(): string;
