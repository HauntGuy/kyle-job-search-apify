// detail-cache.js — Shared detail page caching utility.
// Eliminates duplicated load/prune/save boilerplate across sources.
// Identical copy lives in actors/01_collect_jobs/src/ and actors/03_score_jobs/src/.

import { log } from 'apify';

/**
 * A simple KV-backed detail cache with TTL-based pruning.
 *
 * Usage:
 *   const cache = new DetailCache({ kvStore, kvKey: 'my_cache.json', ttlDays: 30, label: 'My Source' });
 *   await cache.load();
 *   const entry = cache.get(url);
 *   if (!entry) {
 *     const data = await fetchDetail(url);
 *     cache.set(url, data);
 *   }
 *   await cache.save();
 */
export class DetailCache {
  /**
   * @param {Object} opts
   * @param {Object} opts.kvStore  - Apify KV store instance
   * @param {string} opts.kvKey    - Key name in KV store (e.g., 'gjd_detail_cache.json')
   * @param {number} opts.ttlDays  - TTL in days; entries older than this are pruned on load
   * @param {string} opts.label    - Human-readable label for log messages
   */
  constructor({ kvStore, kvKey, ttlDays, label }) {
    this._kv = kvStore;
    this._kvKey = kvKey;
    this._ttlDays = ttlDays;
    this._label = label || kvKey;
    this._data = {};
    this._dirty = false;
  }

  /** Number of entries currently in cache. */
  get size() { return Object.keys(this._data).length; }

  /**
   * Load cache from KV store and prune expired entries.
   * Safe to call even if KV store is null or the key doesn't exist.
   */
  async load() {
    if (!this._kv) return;
    try {
      const raw = await this._kv.getValue(this._kvKey);
      if (raw && typeof raw === 'object') {
        const cutoff = Date.now() - this._ttlDays * 24 * 60 * 60 * 1000;
        let pruned = 0;
        for (const [key, entry] of Object.entries(raw)) {
          if (entry?.fetchedAt && new Date(entry.fetchedAt).getTime() >= cutoff) {
            this._data[key] = entry;
          } else {
            pruned++;
          }
        }
        if (pruned > 0) {
          log.info(`${this._label} cache: pruned ${pruned} expired entries (TTL ${this._ttlDays}d).`);
        }
        log.info(`${this._label} cache loaded: ${this.size} entries.`);
      }
    } catch (err) {
      log.warning(`${this._label} cache: failed to load: ${err?.message || err}`);
    }
  }

  /**
   * Check if a key exists in the cache.
   * @param {string} key
   * @returns {boolean}
   */
  has(key) {
    return key in this._data;
  }

  /**
   * Get a cached entry, optionally validating it.
   * @param {string} key
   * @param {Function} [validate] - Optional validator: if provided and returns false, entry is treated as missing (forces re-fetch).
   * @returns {Object|null}
   */
  get(key, validate) {
    const entry = this._data[key];
    if (!entry) return null;
    if (validate && !validate(entry)) return null;
    return entry;
  }

  /**
   * Store an entry in the cache. Automatically adds `fetchedAt` timestamp.
   * @param {string} key
   * @param {Object} data - The data to cache (will have fetchedAt added)
   */
  set(key, data) {
    this._data[key] = { ...data, fetchedAt: new Date().toISOString() };
    this._dirty = true;
  }

  /**
   * Save the cache to KV store (unconditionally).
   */
  async save() {
    if (!this._kv) return;
    try {
      await this._kv.setValue(this._kvKey, this._data);
      this._dirty = false;
      log.info(`${this._label} cache saved: ${this.size} entries.`);
    } catch (err) {
      log.warning(`${this._label} cache: failed to save: ${err?.message || err}`);
    }
  }

  /**
   * Save only if there have been new set() calls since the last save.
   * Useful for incremental saves during long fetch loops.
   */
  async saveIfDirty() {
    if (!this._dirty) return;
    if (!this._kv) return;
    try {
      await this._kv.setValue(this._kvKey, this._data);
      this._dirty = false;
    } catch { /* non-fatal */ }
  }
}
