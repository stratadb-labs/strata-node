/**
 * TypeScript definitions for StrataDB Node.js SDK
 */

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

/**
 * StrataDB database handle.
 *
 * This is the main entry point for interacting with StrataDB from Node.js.
 */
export class Strata {
  /**
   * Open a database at the given path.
   * @param path - Path to the database directory
   */
  static open(path: string): Strata;

  /**
   * Create an in-memory database (no persistence).
   */
  static cache(): Strata;

  // =========================================================================
  // KV Store
  // =========================================================================

  /**
   * Store a key-value pair.
   * @param key - The key to store
   * @param value - The value to store
   * @returns Version number
   */
  kvPut(key: string, value: JsonValue): number;

  /**
   * Get a value by key.
   * @param key - The key to retrieve
   * @returns The value, or null if not found
   */
  kvGet(key: string): JsonValue;

  /**
   * Delete a key.
   * @param key - The key to delete
   * @returns True if the key was deleted
   */
  kvDelete(key: string): boolean;

  /**
   * List keys with optional prefix filter.
   * @param prefix - Optional prefix to filter keys
   * @returns Array of matching keys
   */
  kvList(prefix?: string): string[];

  /**
   * Get version history for a key.
   * @param key - The key to get history for
   * @returns Array of versioned values, or null if key not found
   */
  kvHistory(key: string): VersionedValue[] | null;

  // =========================================================================
  // State Cell
  // =========================================================================

  /**
   * Set a state cell value.
   * @param cell - The cell name
   * @param value - The value to set
   * @returns Version number
   */
  stateSet(cell: string, value: JsonValue): number;

  /**
   * Get a state cell value.
   * @param cell - The cell name
   * @returns The value, or null if not found
   */
  stateGet(cell: string): JsonValue;

  /**
   * Initialize a state cell if it doesn't exist.
   * @param cell - The cell name
   * @param value - The initial value
   * @returns Version number
   */
  stateInit(cell: string, value: JsonValue): number;

  /**
   * Compare-and-swap update based on version.
   * @param cell - The cell name
   * @param newValue - The new value to set
   * @param expectedVersion - The expected current version
   * @returns New version number if successful, null if CAS failed
   */
  stateCas(cell: string, newValue: JsonValue, expectedVersion?: number): number | null;

  /**
   * Get version history for a state cell.
   * @param cell - The cell name
   * @returns Array of versioned values, or null if cell not found
   */
  stateHistory(cell: string): VersionedValue[] | null;

  // =========================================================================
  // Event Log
  // =========================================================================

  /**
   * Append an event to the log.
   * @param eventType - The type of event
   * @param payload - The event payload
   * @returns Sequence number
   */
  eventAppend(eventType: string, payload: JsonValue): number;

  /**
   * Get an event by sequence number.
   * @param sequence - The sequence number
   * @returns The event, or null if not found
   */
  eventGet(sequence: number): VersionedValue | null;

  /**
   * List events by type.
   * @param eventType - The type of events to list
   * @returns Array of events
   */
  eventList(eventType: string): VersionedValue[];

  /**
   * Get total event count.
   * @returns Number of events
   */
  eventLen(): number;

  // =========================================================================
  // JSON Store
  // =========================================================================

  /**
   * Set a value at a JSONPath.
   * @param key - The document key
   * @param path - The JSONPath
   * @param value - The value to set
   * @returns Version number
   */
  jsonSet(key: string, path: string, value: JsonValue): number;

  /**
   * Get a value at a JSONPath.
   * @param key - The document key
   * @param path - The JSONPath
   * @returns The value, or null if not found
   */
  jsonGet(key: string, path: string): JsonValue;

  /**
   * Delete a JSON document.
   * @param key - The document key
   * @param path - The JSONPath
   * @returns Version number
   */
  jsonDelete(key: string, path: string): number;

  /**
   * Get version history for a JSON document.
   * @param key - The document key
   * @returns Array of versioned values, or null if document not found
   */
  jsonHistory(key: string): VersionedValue[] | null;

  /**
   * List JSON document keys.
   * @param limit - Maximum number of keys to return
   * @param prefix - Optional prefix filter
   * @param cursor - Optional pagination cursor
   * @returns Keys and optional next cursor
   */
  jsonList(limit: number, prefix?: string, cursor?: string): JsonListResult;

  // =========================================================================
  // Vector Store
  // =========================================================================

  /**
   * Create a vector collection.
   * @param collection - Collection name
   * @param dimension - Vector dimension
   * @param metric - Distance metric ("cosine", "euclidean", "dot_product")
   * @returns Version number
   */
  vectorCreateCollection(collection: string, dimension: number, metric?: string): number;

  /**
   * Delete a vector collection.
   * @param collection - Collection name
   * @returns True if the collection was deleted
   */
  vectorDeleteCollection(collection: string): boolean;

  /**
   * List vector collections.
   * @returns Array of collection information
   */
  vectorListCollections(): CollectionInfo[];

  /**
   * Insert or update a vector.
   * @param collection - Collection name
   * @param key - Vector key
   * @param vector - Vector data (array of numbers)
   * @param metadata - Optional metadata
   * @returns Version number
   */
  vectorUpsert(collection: string, key: string, vector: number[], metadata?: JsonValue): number;

  /**
   * Get a vector by key.
   * @param collection - Collection name
   * @param key - Vector key
   * @returns Vector data, or null if not found
   */
  vectorGet(collection: string, key: string): VectorData | null;

  /**
   * Delete a vector.
   * @param collection - Collection name
   * @param key - Vector key
   * @returns True if the vector was deleted
   */
  vectorDelete(collection: string, key: string): boolean;

  /**
   * Search for similar vectors.
   * @param collection - Collection name
   * @param query - Query vector
   * @param k - Number of results to return
   * @returns Array of search matches
   */
  vectorSearch(collection: string, query: number[], k: number): SearchMatch[];

  /**
   * Get statistics for a single collection.
   * @param collection - Collection name
   * @returns Collection information
   */
  vectorCollectionStats(collection: string): CollectionInfo;

  /**
   * Batch insert/update multiple vectors.
   * @param collection - Collection name
   * @param vectors - Array of vector entries
   * @returns Array of version numbers
   */
  vectorBatchUpsert(collection: string, vectors: BatchVectorEntry[]): number[];

  // =========================================================================
  // Branch Management
  // =========================================================================

  /**
   * Get the current branch name.
   */
  currentBranch(): string;

  /**
   * Switch to a different branch.
   * @param branch - Branch name
   */
  setBranch(branch: string): void;

  /**
   * Create a new empty branch.
   * @param branch - Branch name
   */
  createBranch(branch: string): void;

  /**
   * Fork the current branch to a new branch, copying all data.
   * @param destination - Destination branch name
   */
  forkBranch(destination: string): ForkResult;

  /**
   * List all branches.
   */
  listBranches(): string[];

  /**
   * Delete a branch.
   * @param branch - Branch name
   */
  deleteBranch(branch: string): void;

  /**
   * Check if a branch exists.
   * @param name - Branch name
   * @returns True if the branch exists
   */
  branchExists(name: string): boolean;

  /**
   * Get branch metadata with version info.
   * @param name - Branch name
   * @returns Branch info, or null if not found
   */
  branchGet(name: string): BranchInfo | null;

  /**
   * Compare two branches.
   * @param branchA - First branch name
   * @param branchB - Second branch name
   */
  diffBranches(branchA: string, branchB: string): DiffResult;

  /**
   * Merge a branch into the current branch.
   * @param source - Source branch name
   * @param strategy - Merge strategy ("last_writer_wins" or "strict")
   */
  mergeBranches(source: string, strategy?: string): MergeResult;

  // =========================================================================
  // Space Management
  // =========================================================================

  /**
   * Get the current space name.
   */
  currentSpace(): string;

  /**
   * Switch to a different space.
   * @param space - Space name
   */
  setSpace(space: string): void;

  /**
   * List all spaces in the current branch.
   */
  listSpaces(): string[];

  /**
   * Delete a space and all its data.
   * @param space - Space name
   */
  deleteSpace(space: string): void;

  /**
   * Force delete a space even if non-empty.
   * @param space - Space name
   */
  deleteSpaceForce(space: string): void;

  // =========================================================================
  // Database Operations
  // =========================================================================

  /**
   * Check database connectivity.
   */
  ping(): string;

  /**
   * Get database info.
   */
  info(): DatabaseInfo;

  /**
   * Flush writes to disk.
   */
  flush(): void;

  /**
   * Trigger compaction.
   */
  compact(): void;

  // =========================================================================
  // Bundle Operations
  // =========================================================================

  /**
   * Export a branch to a bundle file.
   * @param branch - Branch name
   * @param path - Output file path
   * @returns Export result with counts
   */
  branchExport(branch: string, path: string): BranchExportResult;

  /**
   * Import a branch from a bundle file.
   * @param path - Bundle file path
   * @returns Import result with counts
   */
  branchImport(path: string): BranchImportResult;

  /**
   * Validate a bundle file without importing.
   * @param path - Bundle file path
   * @returns Validation result
   */
  branchValidateBundle(path: string): BundleValidateResult;

  // =========================================================================
  // Transaction Operations (MVP+1)
  // =========================================================================

  /**
   * Begin a new transaction.
   * @param readOnly - Whether the transaction is read-only
   */
  begin(readOnly?: boolean): void;

  /**
   * Commit the current transaction.
   * @returns Commit version number
   */
  commit(): number;

  /**
   * Rollback the current transaction.
   */
  rollback(): void;

  /**
   * Get current transaction info.
   * @returns Transaction info, or null if no transaction is active
   */
  txnInfo(): TransactionInfo | null;

  /**
   * Check if a transaction is currently active.
   */
  txnIsActive(): boolean;

  // =========================================================================
  // Missing State Operations (MVP+1)
  // =========================================================================

  /**
   * Delete a state cell.
   * @param cell - The cell name
   * @returns True if the cell was deleted
   */
  stateDelete(cell: string): boolean;

  /**
   * List state cell names with optional prefix filter.
   * @param prefix - Optional prefix filter
   * @returns Array of cell names
   */
  stateList(prefix?: string): string[];

  // =========================================================================
  // Versioned Getters (MVP+1)
  // =========================================================================

  /**
   * Get a value by key with version info.
   * @param key - The key to retrieve
   * @returns Versioned value, or null if not found
   */
  kvGetVersioned(key: string): VersionedValue | null;

  /**
   * Get a state cell value with version info.
   * @param cell - The cell name
   * @returns Versioned value, or null if not found
   */
  stateGetVersioned(cell: string): VersionedValue | null;

  /**
   * Get a JSON document value with version info.
   * @param key - The document key
   * @returns Versioned value, or null if not found
   */
  jsonGetVersioned(key: string): VersionedValue | null;

  // =========================================================================
  // Pagination Improvements (MVP+1)
  // =========================================================================

  /**
   * List keys with pagination support.
   * @param prefix - Optional prefix filter
   * @param limit - Maximum number of keys
   * @param cursor - Pagination cursor
   * @returns Result with keys array
   */
  kvListPaginated(prefix?: string, limit?: number, cursor?: string): KvListResult;

  /**
   * List events by type with pagination support.
   * @param eventType - Event type to filter
   * @param limit - Maximum number of events
   * @param after - Sequence number to start after
   * @returns Array of events
   */
  eventListPaginated(eventType: string, limit?: number, after?: number): VersionedValue[];

  // =========================================================================
  // Enhanced Vector Search (MVP+1)
  // =========================================================================

  /**
   * Search for similar vectors with optional filter and metric override.
   * @param collection - Collection name
   * @param query - Query vector
   * @param k - Number of results
   * @param metric - Optional metric override ("cosine", "euclidean", "dot_product")
   * @param filter - Optional metadata filters
   * @returns Array of search matches
   */
  vectorSearchFiltered(
    collection: string,
    query: number[],
    k: number,
    metric?: string,
    filter?: MetadataFilter[]
  ): SearchMatch[];

  // =========================================================================
  // Space Operations (MVP+1)
  // =========================================================================

  /**
   * Create a new space explicitly.
   * @param space - Space name
   */
  spaceCreate(space: string): void;

  /**
   * Check if a space exists in the current branch.
   * @param space - Space name
   * @returns True if the space exists
   */
  spaceExists(space: string): boolean;

  // =========================================================================
  // Search (MVP+1)
  // =========================================================================

  /**
   * Search across multiple primitives for matching content.
   * @param query - Search query
   * @param k - Maximum number of results
   * @param primitives - Optional list of primitives to search
   * @returns Array of search hits
   */
  search(query: string, k?: number, primitives?: string[]): SearchHit[];
}
