# strata-node

Node.js SDK for [StrataDB](https://github.com/strata-systems/strata-core) - an embedded database for AI agents.

NAPI-RS bindings embedding the Rust library directly in a native Node.js addon. No network hop, no serialization overhead beyond the Node/Rust boundary.

## Installation

### From npm (coming soon)

```bash
npm install stratadb
```

### From Source

Requires Rust toolchain and Node.js:

```bash
git clone https://github.com/strata-systems/strata-node.git
cd strata-node
npm install
npm run build
```

## Quick Start

```javascript
const { Strata } = require('stratadb');

// Open a database (or use Strata.cache() for in-memory)
const db = Strata.open('/path/to/data');

// Key-value storage
db.kvPut('user:123', 'Alice');
console.log(db.kvGet('user:123'));  // "Alice"

// Branch isolation (like git branches)
db.createBranch('experiment');
db.setBranch('experiment');
console.log(db.kvGet('user:123'));  // null - isolated

// Space organization within branches
db.setSpace('conversations');
db.kvPut('msg_001', 'hello');
```

## Features

### Six Data Primitives

| Primitive | Purpose | Key Methods |
|-----------|---------|-------------|
| **KV Store** | Working memory, config | `kvPut`, `kvGet`, `kvDelete`, `kvList` |
| **Event Log** | Immutable audit trail | `eventAppend`, `eventGet`, `eventList` |
| **State Cell** | CAS-based coordination | `stateSet`, `stateGet`, `stateCas` |
| **JSON Store** | Structured documents | `jsonSet`, `jsonGet`, `jsonDelete` |
| **Vector Store** | Embeddings, similarity search | `vectorUpsert`, `vectorSearch` |
| **Branch** | Data isolation | `createBranch`, `setBranch`, `forkBranch` |

### Vector Operations

```javascript
// Create a collection
db.vectorCreateCollection('embeddings', 384, 'cosine');

// Upsert with array
const embedding = new Array(384).fill(0).map(() => Math.random());
db.vectorUpsert('embeddings', 'doc-1', embedding, { title: 'Hello' });

// Search returns matches with scores
const results = db.vectorSearch('embeddings', embedding, 10);
for (const match of results) {
  console.log(`${match.key}: ${match.score}`);
}
```

### Branch Operations

```javascript
// Fork current branch (copies all data)
db.forkBranch('experiment');

// Compare branches
const diff = db.diffBranches('default', 'experiment');
console.log(`Added: ${diff.summary.totalAdded}`);

// Merge branches
const result = db.mergeBranches('experiment', 'last_writer_wins');
console.log(`Keys applied: ${result.keysApplied}`);
```

### Event Log

```javascript
// Append events
db.eventAppend('tool_call', { tool: 'search', query: 'weather' });
db.eventAppend('tool_call', { tool: 'calculator', expr: '2+2' });

// Get by sequence number
const event = db.eventGet(0);

// List by type
const toolCalls = db.eventList('tool_call');
```

### Compare-and-Swap (Version-based)

```javascript
// Initialize if not exists
db.stateInit('counter', 0);

// CAS is version-based - pass expected version, not expected value
// Get current value and its version via stateSet (returns version)
const version = db.stateSet('counter', 1);

// Update only if version matches
const newVersion = db.stateCas('counter', 2, version);  // (cell, new_value, expected_version)
if (newVersion === null) {
  console.log('CAS failed - version mismatch');
}
```

## API Reference

### Strata

| Method | Description |
|--------|-------------|
| `Strata.open(path)` | Open database at path |
| `Strata.cache()` | Create in-memory database |

### KV Store

| Method | Description |
|--------|-------------|
| `kvPut(key, value)` | Store a value |
| `kvGet(key)` | Get a value (returns null if missing) |
| `kvDelete(key)` | Delete a key |
| `kvList(prefix?)` | List keys |
| `kvHistory(key)` | Get version history |

### State Cell

| Method | Description |
|--------|-------------|
| `stateSet(cell, value)` | Set value |
| `stateGet(cell)` | Get value |
| `stateInit(cell, value)` | Initialize if not exists |
| `stateCas(cell, newValue, expectedVersion?)` | Compare-and-swap (version-based) |

### Event Log

| Method | Description |
|--------|-------------|
| `eventAppend(type, payload)` | Append event |
| `eventGet(sequence)` | Get by sequence |
| `eventList(type)` | List by type |
| `eventLen()` | Get count |

### JSON Store

| Method | Description |
|--------|-------------|
| `jsonSet(key, path, value)` | Set at JSONPath |
| `jsonGet(key, path)` | Get at JSONPath |
| `jsonDelete(key, path)` | Delete |
| `jsonList(limit, prefix?, cursor?)` | List keys |

### Vector Store

| Method | Description |
|--------|-------------|
| `vectorCreateCollection(name, dim, metric?)` | Create collection |
| `vectorDeleteCollection(name)` | Delete collection |
| `vectorListCollections()` | List collections |
| `vectorUpsert(collection, key, vector, metadata?)` | Insert/update |
| `vectorGet(collection, key)` | Get vector |
| `vectorDelete(collection, key)` | Delete vector |
| `vectorSearch(collection, query, k)` | Search |

### Branches

| Method | Description |
|--------|-------------|
| `currentBranch()` | Get current branch |
| `setBranch(name)` | Switch branch |
| `createBranch(name)` | Create empty branch |
| `forkBranch(dest)` | Fork with data copy |
| `listBranches()` | List all branches |
| `deleteBranch(name)` | Delete branch |
| `diffBranches(a, b)` | Compare branches |
| `mergeBranches(source, strategy?)` | Merge into current |

### Spaces

| Method | Description |
|--------|-------------|
| `currentSpace()` | Get current space |
| `setSpace(name)` | Switch space |
| `listSpaces()` | List spaces |
| `deleteSpace(name)` | Delete space |

### Database

| Method | Description |
|--------|-------------|
| `ping()` | Health check |
| `info()` | Get database info |
| `flush()` | Flush to disk |
| `compact()` | Trigger compaction |

## TypeScript

Full TypeScript definitions are included:

```typescript
import { Strata, JsonValue, SearchMatch } from 'stratadb';

const db = Strata.cache();
db.kvPut('key', { count: 42 });
const value: JsonValue = db.kvGet('key');
```

## Development

```bash
# Install dev dependencies
npm install

# Build native module
npm run build

# Run tests
npm test
```

## License

MIT
