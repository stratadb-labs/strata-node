/**
 * Integration tests for the StrataDB Node.js SDK.
 */

const { Strata } = require('../index');

describe('Strata', () => {
  let db;

  beforeEach(() => {
    db = Strata.cache();
  });

  describe('KV Store', () => {
    test('put and get', () => {
      db.kvPut('key1', 'value1');
      expect(db.kvGet('key1')).toBe('value1');
    });

    test('put and get object', () => {
      db.kvPut('config', { theme: 'dark', count: 42 });
      const result = db.kvGet('config');
      expect(result.theme).toBe('dark');
      expect(result.count).toBe(42);
    });

    test('get missing returns null', () => {
      expect(db.kvGet('nonexistent')).toBeNull();
    });

    test('delete', () => {
      db.kvPut('to_delete', 'value');
      expect(db.kvDelete('to_delete')).toBe(true);
      expect(db.kvGet('to_delete')).toBeNull();
    });

    test('list', () => {
      db.kvPut('user:1', 'alice');
      db.kvPut('user:2', 'bob');
      db.kvPut('item:1', 'book');

      const allKeys = db.kvList();
      expect(allKeys.length).toBe(3);

      const userKeys = db.kvList('user:');
      expect(userKeys.length).toBe(2);
    });
  });

  describe('State Cell', () => {
    test('set and get', () => {
      db.stateSet('counter', 100);
      expect(db.stateGet('counter')).toBe(100);
    });

    test('init', () => {
      db.stateInit('status', 'pending');
      expect(db.stateGet('status')).toBe('pending');
    });

    test('cas', () => {
      // CAS is version-based, not value-based
      const version = db.stateSet('value', 1);
      // Try to update with correct version
      const newVersion = db.stateCas('value', 2, version);
      expect(newVersion).not.toBeNull();
      expect(db.stateGet('value')).toBe(2);
      // Try with wrong version - should fail
      const result = db.stateCas('value', 3, 999);
      expect(result).toBeNull(); // CAS failed
    });
  });

  describe('Event Log', () => {
    test('append and get', () => {
      db.eventAppend('user_action', { action: 'click', target: 'button' });
      expect(db.eventLen()).toBe(1);

      const event = db.eventGet(0);
      expect(event).not.toBeNull();
      expect(event.value.action).toBe('click');
    });

    test('list by type', () => {
      db.eventAppend('click', { x: 10 });
      db.eventAppend('scroll', { y: 100 });
      db.eventAppend('click', { x: 20 });

      const clicks = db.eventList('click');
      expect(clicks.length).toBe(2);
    });
  });

  describe('JSON Store', () => {
    test('set and get', () => {
      db.jsonSet('config', '$', { theme: 'dark', lang: 'en' });
      const result = db.jsonGet('config', '$');
      expect(result.theme).toBe('dark');
    });

    test('get path', () => {
      db.jsonSet('config', '$', { theme: 'dark', lang: 'en' });
      const theme = db.jsonGet('config', '$.theme');
      expect(theme).toBe('dark');
    });

    test('list', () => {
      db.jsonSet('doc1', '$', { a: 1 });
      db.jsonSet('doc2', '$', { b: 2 });
      const result = db.jsonList(100); // limit is required
      expect(result.keys.length).toBe(2);
    });
  });

  describe('Vector Store', () => {
    test('create collection', () => {
      db.vectorCreateCollection('embeddings', 4);
      const collections = db.vectorListCollections();
      expect(collections.some((c) => c.name === 'embeddings')).toBe(true);
    });

    test('upsert and search', () => {
      db.vectorCreateCollection('embeddings', 4);

      const v1 = [1.0, 0.0, 0.0, 0.0];
      const v2 = [0.0, 1.0, 0.0, 0.0];

      db.vectorUpsert('embeddings', 'v1', v1);
      db.vectorUpsert('embeddings', 'v2', v2);

      const results = db.vectorSearch('embeddings', v1, 2);
      expect(results.length).toBe(2);
      expect(results[0].key).toBe('v1'); // Most similar
    });

    test('upsert with metadata', () => {
      db.vectorCreateCollection('docs', 4);
      const vec = [1.0, 0.0, 0.0, 0.0];
      db.vectorUpsert('docs', 'doc1', vec, { title: 'Hello' });

      const result = db.vectorGet('docs', 'doc1');
      expect(result.metadata.title).toBe('Hello');
    });
  });

  describe('Branches', () => {
    test('create and list', () => {
      db.createBranch('feature');
      const branches = db.listBranches();
      expect(branches).toContain('default');
      expect(branches).toContain('feature');
    });

    test('switch', () => {
      db.kvPut('x', 1);
      db.createBranch('feature');
      db.setBranch('feature');

      // Data isolated in new branch
      expect(db.kvGet('x')).toBeNull();

      db.kvPut('x', 2);
      db.setBranch('default');
      expect(db.kvGet('x')).toBe(1);
    });

    test('fork', () => {
      db.kvPut('shared', 'original');
      const result = db.forkBranch('forked');
      expect(result.keysCopied).toBeGreaterThan(0);

      db.setBranch('forked');
      expect(db.kvGet('shared')).toBe('original');
    });

    test('current branch', () => {
      expect(db.currentBranch()).toBe('default');
      db.createBranch('test');
      db.setBranch('test');
      expect(db.currentBranch()).toBe('test');
    });
  });

  describe('Spaces', () => {
    test('list spaces', () => {
      const spaces = db.listSpaces();
      expect(spaces).toContain('default');
    });

    test('switch space', () => {
      db.kvPut('key', 'value1');
      db.setSpace('other');
      expect(db.kvGet('key')).toBeNull();

      db.kvPut('key', 'value2');
      db.setSpace('default');
      expect(db.kvGet('key')).toBe('value1');
    });

    test('current space', () => {
      expect(db.currentSpace()).toBe('default');
    });
  });

  describe('Database', () => {
    test('ping', () => {
      const version = db.ping();
      expect(version).toBeTruthy();
    });

    test('info', () => {
      const info = db.info();
      expect(info.version).toBeTruthy();
      expect(info.branchCount).toBeGreaterThanOrEqual(1);
    });
  });
});
