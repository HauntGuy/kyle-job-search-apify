// resilient-fetch.js — Reusable retry + backoff utilities for API calls.
// No external dependencies beyond Apify's log.

import { log } from 'apify';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** Default retryability: network errors, 429, 500+. */
function defaultIsRetryable(err) {
  const status = err?.status || 0;
  return status === 0 || status === 429 || status >= 500;
}

/**
 * Retry a single async function with exponential backoff + jitter.
 *
 * Thrown errors should have `.status` (HTTP status code) and optionally
 * `.retryAfterMs` (parsed from Retry-After header) for best behaviour.
 *
 * @param {Function} fn  Async function to call.
 * @param {Object}  [opts]
 * @param {number}  [opts.retries=5]     Max retry attempts after the initial call.
 * @param {number}  [opts.baseMs=800]    Base delay for exponential backoff.
 * @param {number}  [opts.maxMs=12000]   Max delay cap.
 * @param {Function} [opts.isRetryable]  (err) => boolean.
 * @param {string}  [opts.label='']      Label for log messages.
 * @returns {Promise<*>}  Result of fn().
 */
export async function withRetries(fn, {
  retries = 5,
  baseMs = 800,
  maxMs = 12000,
  isRetryable = defaultIsRetryable,
  label = '',
} = {}) {
  let attempt = 0;
  while (true) {
    try {
      return await fn();
    } catch (e) {
      attempt += 1;
      if (attempt > retries || !isRetryable(e)) throw e;

      // Exponential backoff with jitter (0.7–1.3×)
      let wait = Math.min(maxMs, baseMs * Math.pow(2, attempt - 1));
      wait = Math.floor(wait * (0.7 + Math.random() * 0.6));

      // Respect Retry-After header if present
      const ra = e?.retryAfterMs;
      if (Number.isFinite(ra) && ra > 0) wait = Math.max(wait, Math.floor(ra));

      const tag = label ? `[${label}] ` : '';
      log.warning(
        `${tag}Retryable error (status=${e?.status ?? '?'}). ` +
        `Waiting ${wait}ms, attempt ${attempt}/${retries}.`
      );
      await sleep(wait);
    }
  }
}

/**
 * Process an array of items with concurrency, per-call retries,
 * and final retry passes for hard failures.
 *
 * The processor function must THROW on retryable failure and RETURN on success.
 *
 * @param {Array}    items
 * @param {Function} processor  async (item, index) => result
 * @param {Object}   [opts]
 * @param {number}   [opts.concurrency=4]
 * @param {number}   [opts.retries=5]           Per-call retry attempts.
 * @param {number}   [opts.baseMs=800]
 * @param {number}   [opts.maxMs=12000]
 * @param {number}   [opts.retryPasses=3]       Full retry passes for remaining failures.
 * @param {number}   [opts.retryCooldownMs=15000] Pause before each retry pass.
 * @param {number}   [opts.retryConcurrency]    Concurrency during retry passes (default: same).
 * @param {Function} [opts.isRetryable]
 * @param {string}   [opts.label='']
 * @returns {Promise<{results: Array, failures: Array<{index, error}>, stats: Object}>}
 */
export async function processWithRetries(items, processor, {
  concurrency = 4,
  retries = 5,
  baseMs = 800,
  maxMs = 12000,
  retryPasses = 3,
  retryCooldownMs = 15000,
  retryConcurrency,
  isRetryable = defaultIsRetryable,
  label = '',
} = {}) {
  const rc = retryConcurrency ?? concurrency;
  const stats = { calls: 0, retries: 0, hardFailures: 0 };
  const FAIL = Symbol('FAILED');
  const results = new Array(items.length);
  const errors = new Array(items.length).fill(null);

  // Run a batch of indices through the processor with concurrency control
  async function runBatch(indices, conc) {
    let cursor = 0;
    async function worker() {
      while (cursor < indices.length) {
        const pos = cursor++;
        if (pos >= indices.length) return;
        const idx = indices[pos];
        try {
          results[idx] = await withRetries(
            async () => { stats.calls++; return await processor(items[idx], idx); },
            {
              retries,
              baseMs,
              maxMs,
              isRetryable: (err) => {
                stats.retries++;
                return isRetryable(err);
              },
              label,
            },
          );
          errors[idx] = null;
        } catch (err) {
          results[idx] = FAIL;
          errors[idx] = err;
        }
      }
    }
    const workers = [];
    for (let i = 0; i < Math.max(1, conc); i++) workers.push(worker());
    await Promise.all(workers);
  }

  // --- Initial pass ---
  const allIndices = items.map((_, i) => i);
  await runBatch(allIndices, concurrency);

  const tag = label ? `[${label}] ` : '';
  const initialFails = errors.filter(Boolean).length;
  if (initialFails > 0) {
    log.info(`${tag}Initial pass: ${initialFails}/${items.length} items failed.`);
  }

  // --- Retry passes ---
  for (let pass = 1; pass <= retryPasses; pass++) {
    const failedIndices = [];
    for (let i = 0; i < items.length; i++) {
      if (errors[i]) failedIndices.push(i);
    }
    if (failedIndices.length === 0) break;

    log.info(`${tag}Retry pass ${pass}/${retryPasses}: ${failedIndices.length} items. Cooling down ${retryCooldownMs}ms...`);
    await sleep(retryCooldownMs);
    await runBatch(failedIndices, rc);

    const stillFailed = failedIndices.filter((i) => errors[i]).length;
    log.info(`${tag}Retry pass ${pass}: fixed ${failedIndices.length - stillFailed}/${failedIndices.length}.`);
  }

  // Collect final failures
  const failures = [];
  for (let i = 0; i < items.length; i++) {
    if (errors[i]) {
      stats.hardFailures++;
      failures.push({ index: i, error: errors[i] });
    }
  }

  return { results, failures, stats };
}

/**
 * Parse a Retry-After header value into milliseconds.
 */
function parseRetryAfterMs(value) {
  if (!value) return null;
  const v = String(value).trim();
  if (/^\d+$/.test(v)) return Number(v) * 1000;
  const ms = Date.parse(v);
  if (Number.isFinite(ms)) {
    const delta = ms - Date.now();
    return delta > 0 ? delta : null;
  }
  return null;
}

/**
 * Fetch JSON with proper error enrichment for retry logic.
 * Thrown errors have `.status` and `.retryAfterMs` properties.
 *
 * @param {string}  url
 * @param {Object}  [headers={}]
 * @param {Object}  [fetchOpts={}]  Extra options for fetch (signal, method, etc.)
 * @returns {Promise<Object>}  Parsed JSON response.
 */
export async function fetchJsonRetryable(url, headers = {}, fetchOpts = {}) {
  let res;
  try {
    res = await fetch(url, {
      method: 'GET',
      headers: { ...headers, Accept: 'application/json' },
      ...fetchOpts,
    });
  } catch (fetchErr) {
    const err = new Error(`Fetch error for ${url}: ${fetchErr?.message || fetchErr}`);
    err.status = 0; // network error
    throw err;
  }

  if (!res.ok) {
    const body = await res.text().catch(() => '');
    const err = new Error(`HTTP ${res.status} for ${url}: ${body.slice(0, 500)}`);
    err.status = res.status;
    err.retryAfterMs = parseRetryAfterMs(res.headers?.get?.('retry-after'));
    throw err;
  }

  const text = await res.text();
  try {
    return JSON.parse(text);
  } catch (e) {
    throw new Error(`Non-JSON response from ${url}: ${e?.message}\n${text.slice(0, 500)}`);
  }
}
