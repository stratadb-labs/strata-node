/**
 * Integration tests for the StrataDB Node.js SDK.
 *
 * All methods are async — every call uses `await`.
 */

const {
  Strata,
  StrataSnapshot,
  StrataError,
  NotFoundError,
  ValidationError,
  ConflictError,
  StateError,
  ConstraintError,
} = require('../stratadb');

describe('Strata', () => {
  let db;

  beforeEach(() => {
    db = Strata.cache();
  });

  // =========================================================================
  // KV Store — db.kv
  // =========================================================================

  describe('db.kv', () => {
    test('set and get', async () => {
      await db.kv.set('key1', 'value1');
      expect(await db.kv.get('key1')).toBe('value1');
    });

    test('set and get object', async () => {
      await db.kv.set('config', { theme: 'dark', count: 42 });
      const result = await db.kv.get('config');
      expect(result.theme).toBe('dark');
      expect(result.count).toBe(42);
    });

    test('get missing returns null', async () => {
      expect(await db.kv.get('nonexistent')).toBeNull();
    });

    test('delete', async () => {
      await db.kv.set('to_delete', 'value');
      expect(await db.kv.delete('to_delete')).toBe(true);
      expect(await db.kv.get('to_delete')).toBeNull();
    });

    test('keys without options', async () => {
      await db.kv.set('user:1', 'alice');
      await db.kv.set('user:2', 'bob');
      await db.kv.set('item:1', 'book');
      const allKeys = await db.kv.keys();
      expect(allKeys.length).toBe(3);
    });

    test('keys with prefix', async () => {
      await db.kv.set('user:1', 'alice');
      await db.kv.set('user:2', 'bob');
      await db.kv.set('item:1', 'book');
      const userKeys = await db.kv.keys({ prefix: 'user:' });
      expect(userKeys.length).toBe(2);
    });

    test('keys with limit', async () => {
      await db.kv.set('k1', 1);
      await db.kv.set('k2', 2);
      await db.kv.set('k3', 3);
      const keys = await db.kv.keys({ prefix: 'k', limit: 2 });
      expect(keys.length).toBeLessThanOrEqual(2);
    });

    test('set returns version number', async () => {
      const v = await db.kv.set('vkey', 'val');
      expect(typeof v).toBe('number');
      expect(v).toBeGreaterThan(0);
    });

    test('history', async () => {
      await db.kv.set('hkey', 'v1');
      await db.kv.set('hkey', 'v2');
      const history = await db.kv.history('hkey');
      expect(Array.isArray(history)).toBe(true);
      expect(history.length).toBeGreaterThanOrEqual(1);
      expect(history[0]).toHaveProperty('version');
      expect(history[0]).toHaveProperty('timestamp');
    });

    test('getVersioned', async () => {
      await db.kv.set('vk', 'val');
      const vv = await db.kv.getVersioned('vk');
      expect(vv).not.toBeNull();
      expect(vv.value).toBe('val');
      expect(typeof vv.version).toBe('number');
    });

    test('getVersioned missing returns null', async () => {
      expect(await db.kv.getVersioned('nope')).toBeNull();
    });

    test('get with asOf option', async () => {
      const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
      await db.kv.set('tt', 'v1');
      await sleep(50);
      const range = await db.timeRange();
      const ts = range.latestTs;
      await sleep(50);
      await db.kv.set('tt', 'v2');

      expect(await db.kv.get('tt')).toBe('v2');
      expect(await db.kv.get('tt', { asOf: ts })).toBe('v1');
    });

    test('getVersioned timestamp roundtrip with asOf', async () => {
      await db.kv.set('kv_rt', 'v1');
      const vv = await db.kv.getVersioned('kv_rt');
      await db.kv.set('kv_rt', 'v2');

      expect(await db.kv.get('kv_rt')).toBe('v2');
      expect(await db.kv.get('kv_rt', { asOf: vv.timestamp })).toBe('v1');
    });
  });

  // =========================================================================
  // State Cell — db.state
  // =========================================================================

  describe('db.state', () => {
    test('set and get', async () => {
      await db.state.set('counter', 100);
      expect(await db.state.get('counter')).toBe(100);
    });

    test('init', async () => {
      await db.state.init('status', 'pending');
      expect(await db.state.get('status')).toBe('pending');
    });

    test('cas with expectedVersion', async () => {
      const version = await db.state.set('value', 1);
      const newVersion = await db.state.cas('value', 2, { expectedVersion: version });
      expect(newVersion).not.toBeNull();
      expect(await db.state.get('value')).toBe(2);
      // Wrong version -> CAS fails
      const result = await db.state.cas('value', 3, { expectedVersion: 999 });
      expect(result).toBeNull();
    });

    test('history', async () => {
      await db.state.set('hcell', 'a');
      await db.state.set('hcell', 'b');
      const history = await db.state.history('hcell');
      expect(Array.isArray(history)).toBe(true);
      expect(history.length).toBeGreaterThanOrEqual(1);
    });

    test('delete', async () => {
      await db.state.set('del_cell', 'x');
      const deleted = await db.state.delete('del_cell');
      expect(deleted).toBe(true);
      expect(await db.state.get('del_cell')).toBeNull();
    });

    test('keys with prefix', async () => {
      await db.state.set('cell_a', 1);
      await db.state.set('cell_b', 2);
      const cells = await db.state.keys({ prefix: 'cell_' });
      expect(cells.length).toBe(2);
    });

    test('getVersioned', async () => {
      await db.state.set('vcell', 42);
      const vv = await db.state.getVersioned('vcell');
      expect(vv).not.toBeNull();
      expect(vv.value).toBe(42);
      expect(typeof vv.version).toBe('number');
    });

    test('getVersioned timestamp roundtrip with asOf', async () => {
      await db.state.set('sv_rt', 'v1');
      const vv = await db.state.getVersioned('sv_rt');
      await db.state.set('sv_rt', 'v2');

      expect(await db.state.get('sv_rt')).toBe('v2');
      expect(await db.state.get('sv_rt', { asOf: vv.timestamp })).toBe('v1');
    });
  });

  // =========================================================================
  // Event Log — db.events
  // =========================================================================

  describe('db.events', () => {
    test('append and get', async () => {
      await db.events.append('user_action', { action: 'click', target: 'button' });
      expect(await db.events.count()).toBe(1);

      const event = await db.events.get(0);
      expect(event).not.toBeNull();
      expect(event.value.action).toBe('click');
    });

    test('list by type', async () => {
      await db.events.append('click', { x: 10 });
      await db.events.append('scroll', { y: 100 });
      await db.events.append('click', { x: 20 });

      const clicks = await db.events.list('click');
      expect(clicks.length).toBe(2);
    });

    test('count', async () => {
      expect(await db.events.count()).toBe(0);
      await db.events.append('a', {});
      await db.events.append('b', {});
      expect(await db.events.count()).toBe(2);
    });

    test('list with limit', async () => {
      await db.events.append('page', { n: 1 });
      await db.events.append('page', { n: 2 });
      await db.events.append('page', { n: 3 });
      const events = await db.events.list('page', { limit: 2 });
      expect(Array.isArray(events)).toBe(true);
      expect(events.length).toBeLessThanOrEqual(3);
    });
  });

  // =========================================================================
  // JSON Store — db.json
  // =========================================================================

  describe('db.json', () => {
    test('set and get', async () => {
      await db.json.set('config', '$', { theme: 'dark', lang: 'en' });
      const result = await db.json.get('config', '$');
      expect(result.theme).toBe('dark');
    });

    test('get path', async () => {
      await db.json.set('config', '$', { theme: 'dark', lang: 'en' });
      const theme = await db.json.get('config', '$.theme');
      expect(theme).toBe('dark');
    });

    test('keys', async () => {
      await db.json.set('doc1', '$', { a: 1 });
      await db.json.set('doc2', '$', { b: 2 });
      const result = await db.json.keys();
      expect(result.keys.length).toBe(2);
    });

    test('keys with options', async () => {
      await db.json.set('pre_a', '$', { a: 1 });
      await db.json.set('pre_b', '$', { b: 2 });
      await db.json.set('other', '$', { c: 3 });
      const result = await db.json.keys({ prefix: 'pre_', limit: 10 });
      expect(result.keys.length).toBe(2);
    });

    test('history', async () => {
      await db.json.set('jhist', '$', { v: 1 });
      await db.json.set('jhist', '$', { v: 2 });
      const history = await db.json.history('jhist');
      expect(Array.isArray(history)).toBe(true);
      expect(history.length).toBeGreaterThanOrEqual(1);
    });

    test('delete', async () => {
      await db.json.set('jdel', '$', { x: 1 });
      const version = await db.json.delete('jdel', '$');
      expect(typeof version).toBe('number');
    });

    test('getVersioned', async () => {
      await db.json.set('jv', '$', { data: true });
      const vv = await db.json.getVersioned('jv');
      expect(vv).not.toBeNull();
      expect(typeof vv.version).toBe('number');
    });

    test('getVersioned timestamp roundtrip with asOf', async () => {
      await db.json.set('jv_rt', '$', { v: 1 });
      const vv = await db.json.getVersioned('jv_rt');
      await db.json.set('jv_rt', '$', { v: 2 });

      const current = await db.json.get('jv_rt', '$');
      expect(current.v).toBe(2);
      const past = await db.json.get('jv_rt', '$', { asOf: vv.timestamp });
      expect(past.v).toBe(1);
    });
  });

  // =========================================================================
  // Vector Store — db.vector
  // =========================================================================

  describe('db.vector', () => {
    test('createCollection and listCollections', async () => {
      await db.vector.createCollection('embeddings', { dimension: 4 });
      const collections = await db.vector.listCollections();
      expect(collections.some((c) => c.name === 'embeddings')).toBe(true);
    });

    test('upsert and search', async () => {
      await db.vector.createCollection('embeddings', { dimension: 4 });

      const v1 = [1.0, 0.0, 0.0, 0.0];
      const v2 = [0.0, 1.0, 0.0, 0.0];

      await db.vector.upsert('embeddings', 'v1', v1);
      await db.vector.upsert('embeddings', 'v2', v2);

      const results = await db.vector.search('embeddings', v1, { limit: 2 });
      expect(results.length).toBe(2);
      expect(results[0].key).toBe('v1');
    });

    test('upsert with metadata option', async () => {
      await db.vector.createCollection('docs', { dimension: 4 });
      const vec = [1.0, 0.0, 0.0, 0.0];
      await db.vector.upsert('docs', 'doc1', vec, { metadata: { title: 'Hello' } });

      const result = await db.vector.get('docs', 'doc1');
      expect(result.metadata.title).toBe('Hello');
    });

    test('get', async () => {
      await db.vector.createCollection('vget', { dimension: 4 });
      await db.vector.upsert('vget', 'k1', [1, 0, 0, 0]);
      const result = await db.vector.get('vget', 'k1');
      expect(result).not.toBeNull();
      expect(result.key).toBe('k1');
      expect(result.embedding.length).toBe(4);
      expect(typeof result.version).toBe('number');
    });

    test('get missing returns null', async () => {
      await db.vector.createCollection('vget2', { dimension: 4 });
      expect(await db.vector.get('vget2', 'nope')).toBeNull();
    });

    test('delete', async () => {
      await db.vector.createCollection('vdel', { dimension: 4 });
      await db.vector.upsert('vdel', 'k1', [1, 0, 0, 0]);
      expect(await db.vector.delete('vdel', 'k1')).toBe(true);
      expect(await db.vector.get('vdel', 'k1')).toBeNull();
    });

    test('deleteCollection', async () => {
      await db.vector.createCollection('to_delete', { dimension: 4 });
      expect(await db.vector.deleteCollection('to_delete')).toBe(true);
    });

    test('stats', async () => {
      await db.vector.createCollection('stats', { dimension: 4 });
      await db.vector.upsert('stats', 'k1', [1, 0, 0, 0]);
      const stats = await db.vector.stats('stats');
      expect(stats.name).toBe('stats');
      expect(stats.dimension).toBe(4);
      expect(stats.count).toBeGreaterThanOrEqual(1);
    });

    test('batchUpsert', async () => {
      await db.vector.createCollection('batch', { dimension: 4 });
      const versions = await db.vector.batchUpsert('batch', [
        { key: 'b1', vector: [1, 0, 0, 0] },
        { key: 'b2', vector: [0, 1, 0, 0], metadata: { label: 'two' } },
      ]);
      expect(versions.length).toBe(2);
      versions.forEach((v) => expect(typeof v).toBe('number'));
    });

    test('search with filter', async () => {
      await db.vector.createCollection('vf', { dimension: 4 });
      await db.vector.upsert('vf', 'k1', [1, 0, 0, 0], { metadata: { category: 'a' } });
      await db.vector.upsert('vf', 'k2', [0, 1, 0, 0], { metadata: { category: 'b' } });
      const results = await db.vector.search('vf', [1, 0, 0, 0], {
        limit: 10,
        filter: [{ field: 'category', op: 'eq', value: 'a' }],
      });
      expect(results.length).toBe(1);
      expect(results[0].key).toBe('k1');
    });

    test('rejects non-finite vector values', async () => {
      await db.vector.createCollection('finite_test', { dimension: 4 });
      await expect(
        db.vector.upsert('finite_test', 'k', [1, NaN, 0, 0]),
      ).rejects.toThrow(/not a finite number/);
      await expect(
        db.vector.upsert('finite_test', 'k', [1, 0, Infinity, 0]),
      ).rejects.toThrow(/not a finite number/);
    });
  });

  // =========================================================================
  // Branches — db.branch
  // =========================================================================

  describe('db.branch', () => {
    test('current', async () => {
      expect(await db.branch.current()).toBe('default');
    });

    test('create, list, exists', async () => {
      await db.branch.create('feature');
      const branches = await db.branch.list();
      expect(branches).toContain('default');
      expect(branches).toContain('feature');
      expect(await db.branch.exists('feature')).toBe(true);
    });

    test('switch', async () => {
      await db.kv.set('x', 1);
      await db.branch.create('feature');
      await db.branch.switch('feature');

      expect(await db.kv.get('x')).toBeNull();

      await db.kv.set('x', 2);
      await db.branch.switch('default');
      expect(await db.kv.get('x')).toBe(1);
    });

    test('fork', async () => {
      await db.kv.set('shared', 'original');
      const result = await db.branch.fork('forked');
      expect(result.keysCopied).toBeGreaterThan(0);

      await db.branch.switch('forked');
      expect(await db.kv.get('shared')).toBe('original');
    });

    test('delete', async () => {
      await db.branch.create('to_del');
      await db.branch.delete('to_del');
      const branches = await db.branch.list();
      expect(branches).not.toContain('to_del');
    });

    test('exists returns false for missing', async () => {
      expect(await db.branch.exists('nope')).toBe(false);
    });

    test('get', async () => {
      const info = await db.branch.get('default');
      expect(info).not.toBeNull();
      expect(info.id).toBe('default');
      expect(info).toHaveProperty('status');
      expect(info).toHaveProperty('version');
    });

    test('get missing returns null', async () => {
      expect(await db.branch.get('nonexistent')).toBeNull();
    });

    test('diff', async () => {
      await db.kv.set('d_key', 'val');
      await db.branch.create('diff_b');
      const diff = await db.branch.diff('default', 'diff_b');
      expect(diff).toHaveProperty('summary');
      expect(diff.summary).toHaveProperty('totalAdded');
    });

    test('merge with and without strategy option', async () => {
      await db.kv.set('base', 'val');
      await db.branch.fork('merge_src');
      await db.branch.switch('merge_src');
      await db.kv.set('new_key', 'from_src');
      await db.branch.switch('default');
      const r1 = await db.branch.merge('merge_src');
      expect(r1).toHaveProperty('keysApplied');

      await db.branch.fork('merge_src2');
      await db.branch.switch('merge_src2');
      await db.kv.set('key2', 'from_src');
      await db.branch.switch('default');
      const r2 = await db.branch.merge('merge_src2', { strategy: 'last_writer_wins' });
      expect(r2).toHaveProperty('keysApplied');
    });
  });

  // =========================================================================
  // Spaces — db.space
  // =========================================================================

  describe('db.space', () => {
    test('current', async () => {
      expect(await db.space.current()).toBe('default');
    });

    test('create, list, exists', async () => {
      await db.space.create('ns');
      const spaces = await db.space.list();
      expect(spaces).toContain('default');
      expect(spaces).toContain('ns');
      expect(await db.space.exists('ns')).toBe(true);
    });

    test('switch', async () => {
      await db.kv.set('key', 'value1');
      await db.space.switch('other');
      expect(await db.kv.get('key')).toBeNull();

      await db.kv.set('key', 'value2');
      await db.space.switch('default');
      expect(await db.kv.get('key')).toBe('value1');
    });

    test('exists returns false for missing', async () => {
      expect(await db.space.exists('nonexistent_space')).toBe(false);
    });

    test('delete with force', async () => {
      await db.space.create('to_del_space');
      await db.space.delete('to_del_space', { force: true });
      expect(await db.space.exists('to_del_space')).toBe(false);
    });
  });

  // =========================================================================
  // Database Operations
  // =========================================================================

  describe('Database', () => {
    test('ping', async () => {
      const version = await db.ping();
      expect(version).toBeTruthy();
    });

    test('info', async () => {
      const info = await db.info();
      expect(info.version).toBeTruthy();
      expect(info.branchCount).toBeGreaterThanOrEqual(1);
    });

    test('flush', async () => {
      await db.flush();
    });

    test('compact', async () => {
      await db.compact();
    });
  });

  // =========================================================================
  // Transactions
  // =========================================================================

  describe('Transactions', () => {
    test('begin and commit', async () => {
      await db.begin();
      await expect(db.txnIsActive()).resolves.toBe(true);
      const version = await db.commit();
      expect(typeof version).toBe('number');
    });

    test('begin and rollback', async () => {
      await db.begin();
      await db.rollback();
      await expect(db.txnIsActive()).resolves.toBe(false);
    });

    test('txnIsActive before begin', async () => {
      expect(await db.txnIsActive()).toBe(false);
    });

    test('txnInfo', async () => {
      expect(await db.txnInfo()).toBeNull();
      await db.begin();
      const info = await db.txnInfo();
      expect(info).not.toBeNull();
      expect(info).toHaveProperty('id');
      expect(info).toHaveProperty('status');
      await db.rollback();
    });
  });

  // =========================================================================
  // Retention
  // =========================================================================

  describe('Retention', () => {
    test('retentionApply succeeds', async () => {
      await db.kv.set('r_key', 'val');
      await db.retentionApply();
    });
  });

  // =========================================================================
  // Search
  // =========================================================================

  describe('Search', () => {
    test('cross-primitive search returns array', async () => {
      await db.kv.set('search_key', 'hello world');
      const results = await db.search('hello');
      expect(Array.isArray(results)).toBe(true);
    });

    test('search empty database returns empty array', async () => {
      const fresh = Strata.cache();
      const results = await fresh.search('anything');
      expect(results).toEqual([]);
      await fresh.close();
    });

    test('search with primitives filter', async () => {
      await db.kv.set('s_k', 'data');
      const results = await db.search('data', { primitives: ['kv'] });
      expect(Array.isArray(results)).toBe(true);
    });

    test('search with mode', async () => {
      const results = await db.search('test', { mode: 'keyword' });
      expect(Array.isArray(results)).toBe(true);
    });

    test('search with expand and rerank disabled', async () => {
      const results = await db.search('test', { expand: false, rerank: false });
      expect(Array.isArray(results)).toBe(true);
    });

    test('search with time range', async () => {
      const results = await db.search('test', {
        timeRange: { start: '2020-01-01T00:00:00Z', end: '2030-01-01T00:00:00Z' },
      });
      expect(Array.isArray(results)).toBe(true);
    });

    test('search with all options', async () => {
      const results = await db.search('hello', {
        k: 5,
        primitives: ['kv'],
        mode: 'hybrid',
        expand: false,
        rerank: false,
      });
      expect(Array.isArray(results)).toBe(true);
    });
  });

  // =========================================================================
  // Snapshot API — db.at(timestamp)
  // =========================================================================

  describe('db.at()', () => {
    const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

    test('kv.get reads at snapshot time', async () => {
      await db.kv.set('snap_kv', 'v1');
      await sleep(50);
      const range = await db.timeRange();
      const ts = range.latestTs;
      await sleep(50);
      await db.kv.set('snap_kv', 'v2');

      const snapshot = db.at(ts);
      expect(await snapshot.kv.get('snap_kv')).toBe('v1');
      expect(await db.kv.get('snap_kv')).toBe('v2');
    });

    test('kv.keys reads at snapshot time', async () => {
      await db.kv.set('sk_a', 1);
      await sleep(50);
      const range = await db.timeRange();
      const ts = range.latestTs;
      await sleep(50);
      await db.kv.set('sk_b', 2);

      const snapshot = db.at(ts);
      const past = await snapshot.kv.keys({ prefix: 'sk_' });
      expect(past.length).toBe(1);
    });

    test('snapshot write throws StateError', () => {
      const snapshot = db.at(12345);
      expect(() => snapshot.kv.set('k', 'v')).toThrow(StateError);
      expect(() => snapshot.kv.delete('k')).toThrow(StateError);
      expect(() => snapshot.state.set('c', 'v')).toThrow(StateError);
      expect(() => snapshot.state.init('c', 'v')).toThrow(StateError);
      expect(() => snapshot.state.cas('c', 'v')).toThrow(StateError);
      expect(() => snapshot.state.delete('c')).toThrow(StateError);
      expect(() => snapshot.events.append('t', {})).toThrow(StateError);
      expect(() => snapshot.json.set('k', '$', {})).toThrow(StateError);
      expect(() => snapshot.json.delete('k', '$')).toThrow(StateError);
      expect(() => snapshot.vector.createCollection('c', {})).toThrow(StateError);
      expect(() => snapshot.vector.deleteCollection('c')).toThrow(StateError);
      expect(() => snapshot.vector.upsert('c', 'k', [])).toThrow(StateError);
      expect(() => snapshot.vector.delete('c', 'k')).toThrow(StateError);
      expect(() => snapshot.vector.batchUpsert('c', [])).toThrow(StateError);
    });

    test('state.get reads at snapshot time', async () => {
      await db.state.set('snap_state', 'old');
      await sleep(50);
      const range = await db.timeRange();
      const ts = range.latestTs;
      await sleep(50);
      await db.state.set('snap_state', 'new');

      const snapshot = db.at(ts);
      expect(await snapshot.state.get('snap_state')).toBe('old');
    });

    test('state.keys reads at snapshot time', async () => {
      await db.state.set('tts_a', 1);
      await sleep(50);
      const range = await db.timeRange();
      const ts = range.latestTs;
      await sleep(50);
      await db.state.set('tts_b', 2);

      const snapshot = db.at(ts);
      const past = await snapshot.state.keys({ prefix: 'tts_' });
      expect(past.length).toBe(1);
    });

    test('events.get reads at snapshot time', async () => {
      await db.events.append('snap_evt', { v: 1 });
      const evt = await db.events.get(0);
      const eventTs = evt.timestamp;

      const snapshot = db.at(eventTs);
      const past = await snapshot.events.get(0);
      expect(past).not.toBeNull();

      const early = db.at(1);
      const before = await early.events.get(0);
      expect(before).toBeNull();
    });

    test('events.list reads at snapshot time', async () => {
      await db.events.append('tt_etype', { n: 1 });
      await sleep(50);
      const range = await db.timeRange();
      const ts = range.latestTs;
      await sleep(50);
      await db.events.append('tt_etype', { n: 2 });

      const current = await db.events.list('tt_etype');
      expect(current.length).toBe(2);
      const snapshot = db.at(ts);
      const past = await snapshot.events.list('tt_etype');
      expect(past.length).toBe(1);
    });

    test('json.get reads at snapshot time', async () => {
      await db.json.set('snap_json', '$', { v: 1 });
      await sleep(50);
      const range = await db.timeRange();
      const ts = range.latestTs;
      await sleep(50);
      await db.json.set('snap_json', '$', { v: 2 });

      const snapshot = db.at(ts);
      const val = await snapshot.json.get('snap_json', '$');
      expect(val.v).toBe(1);
    });

    test('json.keys reads at snapshot time', async () => {
      await db.json.set('ttj_a', '$', { x: 1 });
      await sleep(50);
      const range = await db.timeRange();
      const ts = range.latestTs;
      await sleep(50);
      await db.json.set('ttj_b', '$', { x: 2 });

      const snapshot = db.at(ts);
      const past = await snapshot.json.keys({ prefix: 'ttj_' });
      expect(past.keys.length).toBe(1);
    });

    test('vector.search reads at snapshot time', async () => {
      await db.vector.createCollection('snap_vec', { dimension: 4 });
      await db.vector.upsert('snap_vec', 'a', [1, 0, 0, 0]);
      await sleep(50);
      const range = await db.timeRange();
      const ts = range.latestTs;
      await sleep(50);
      await db.vector.upsert('snap_vec', 'b', [0, 1, 0, 0]);

      const snapshot = db.at(ts);
      const results = await snapshot.vector.search('snap_vec', [1, 0, 0, 0], { limit: 10 });
      expect(results.length).toBe(1);
    });

    test('vector.get reads at snapshot time', async () => {
      await db.vector.createCollection('tt_vget', { dimension: 4 });
      await db.vector.upsert('tt_vget', 'k', [1, 0, 0, 0], { metadata: { tag: 'v1' } });
      await sleep(50);
      const range = await db.timeRange();
      const ts = range.latestTs;
      await sleep(50);
      await db.vector.upsert('tt_vget', 'k', [0, 1, 0, 0], { metadata: { tag: 'v2' } });

      const current = await db.vector.get('tt_vget', 'k');
      expect(current.metadata.tag).toBe('v2');
      const snapshot = db.at(ts);
      const past = await snapshot.vector.get('tt_vget', 'k');
      expect(past).not.toBeNull();
      expect(past.metadata.tag).toBe('v1');
    });

    test('timeRange returns oldest and latest', async () => {
      await db.kv.set('tr_key', 'val');
      const range = await db.timeRange();
      expect(range).toHaveProperty('oldestTs');
      expect(range).toHaveProperty('latestTs');
      expect(typeof range.oldestTs).toBe('number');
      expect(typeof range.latestTs).toBe('number');
      expect(range.latestTs).toBeGreaterThanOrEqual(range.oldestTs);
    });

    test('StrataSnapshot is exported', () => {
      expect(StrataSnapshot).toBeDefined();
      const snapshot = db.at(12345);
      expect(snapshot).toBeInstanceOf(StrataSnapshot);
    });
  });

  // =========================================================================
  // Transaction callback — db.transaction()
  // =========================================================================

  describe('db.transaction()', () => {
    test('auto-commits on success', async () => {
      await db.kv.set('pre', 'before');
      await db.transaction(async (tx) => {
        await tx.kv.set('tx_key', 'tx_value');
      });
      expect(await db.kv.get('tx_key')).toBe('tx_value');
    });

    test('auto-rollback on error', async () => {
      await expect(
        db.transaction(async (tx) => {
          throw new Error('intentional');
        }),
      ).rejects.toThrow('intentional');
      // After rollback, no transaction should be active
      expect(await db.txnIsActive()).toBe(false);
    });

    test('returns result from callback', async () => {
      const result = await db.transaction(async (tx) => {
        await tx.kv.set('r_key', 'r_val');
        return 42;
      });
      expect(result).toBe(42);
    });

    test('read-only transaction', async () => {
      await db.kv.set('ro_key', 'ro_val');
      const result = await db.transaction(
        async (tx) => {
          const val = await tx.kv.get('ro_key');
          return val;
        },
        { readOnly: true },
      );
      expect(result).toBe('ro_val');
    });
  });

  // =========================================================================
  // Errors
  // =========================================================================

  describe('Errors', () => {
    test('NotFoundError with correct hierarchy and code', async () => {
      try {
        await db.vector.search('no_such_collection', [1, 0, 0, 0], { limit: 1 });
        fail('Expected error');
      } catch (err) {
        expect(err).toBeInstanceOf(NotFoundError);
        expect(err).toBeInstanceOf(StrataError);
        expect(err).toBeInstanceOf(Error);
        expect(err.code).toBe('NOT_FOUND');
        // negative checks — not other subclasses
        expect(err instanceof ValidationError).toBe(false);
        expect(err instanceof ConflictError).toBe(false);
      }
    });

    test('ValidationError on invalid metric', async () => {
      try {
        await db.vector.createCollection('x', { dimension: 4, metric: 'invalid_metric' });
        fail('Expected error');
      } catch (err) {
        expect(err).toBeInstanceOf(ValidationError);
        expect(err.code).toBe('VALIDATION');
      }
    });

    test('ConstraintError on dimension mismatch', async () => {
      await db.vector.createCollection('dim_test', { dimension: 4 });
      try {
        await db.vector.upsert('dim_test', 'k', [1, 0]); // wrong dimension
        fail('Expected error');
      } catch (err) {
        expect(err).toBeInstanceOf(ConstraintError);
        expect(err.code).toBe('CONSTRAINT');
      }
    });
  });

  // =========================================================================
  // Close lifecycle
  // =========================================================================

  describe('db.close()', () => {
    test('close resolves without error', async () => {
      const tempDb = Strata.cache();
      await tempDb.kv.set('k', 'v');
      await tempDb.close();
    });
  });
});
