// actors/03_score_jobs/src/main.js
// Scores merged jobs with an LLM using an external rubric file, writes scored + accepted datasets,
// and produces accepted.xlsx + scored.xlsx in the KV store.

import { Actor, log } from 'apify';
import ExcelJS from 'exceljs';
import http from 'node:http';
import https from 'node:https';
import { processWithRetries } from './resilient-fetch.js';
import { DetailCache } from './detail-cache.js';

// Disable HTTP keep-alive for the legacy HTTP module.  When scoring with
// reasoning models (GPT-5), requests take 10-30s each.  During that time
// idle keep-alive connections to the Apify platform can get dropped by the
// server, causing ECONNRESET crashes.  Fresh connections per request avoids this.
http.globalAgent = new http.Agent({ keepAlive: false });
https.globalAgent = new https.Agent({ keepAlive: false });

// Prevent ECONNRESET / socket-close errors from crashing the process.
// These are transient network issues; use console directly (not `log`)
// because the Apify logger may not be initialized when these fire.
const TRANSIENT_CODES = new Set(['ECONNRESET', 'ETIMEDOUT', 'EPIPE', 'ECONNABORTED', 'UND_ERR_SOCKET']);
function isTransientError(err) {
  if (!err) return false;
  if (TRANSIENT_CODES.has(err.code)) return true;
  const msg = String(err.message || '').toLowerCase();
  return msg.includes('aborted') || msg.includes('socket hang up') || msg.includes('econnreset');
}

process.on('uncaughtException', (err, origin) => {
  if (isTransientError(err)) {
    console.warn(`[CAUGHT] Transient ${origin}: ${err.code || err.message}`);
    return; // swallow — the retry logic will handle the failed request
  }
  console.error(`[FATAL] Uncaught exception:`, err?.stack || err);
  process.exit(1);
});

process.on('unhandledRejection', (reason) => {
  if (isTransientError(reason)) {
    console.warn(`[CAUGHT] Transient unhandledRejection: ${reason?.code || reason?.message}`);
    return; // swallow
  }
  console.error(`[FATAL] Unhandled rejection:`, reason?.stack || reason);
  process.exit(1);
});

function nowIso() {
  return new Date().toISOString();
}

function safeRunId(runId) {
  if (!runId) return null;
  return String(runId).replace(/[^a-zA-Z0-9._-]/g, '-').slice(0, 80);
}

function makeRunId() {
  return new Date().toISOString().replace(/[:.]/g, '-');
}

function datasetName(prefix, kind, runId) {
  const p = String(prefix || 'jobsearch-v3').replace(/[^a-zA-Z0-9._-]/g, '-');
  const r = safeRunId(runId) || makeRunId();
  return `${p}--${kind}--${r}`;
}

async function sleep(ms) {
  await new Promise((r) => setTimeout(r, ms));
}

async function fetchText(url, headers = {}) {
  const u = url.includes('?') ? `${url}&cb=${Date.now()}` : `${url}?cb=${Date.now()}`;
  const res = await fetch(u, { method: 'GET', headers });
  const text = await res.text();
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}: ${text.slice(0, 500)}`);
  return text;
}

async function fetchJson(url, headers = {}) {
  const text = await fetchText(url, { ...headers, Accept: 'application/json' });
  try { return JSON.parse(text); }
  catch (e) { throw new Error(`Non-JSON response from ${url}: ${e?.message || e}\n${text.slice(0, 500)}`); }
}

async function loadConfig(input) {
  if (input?.config && typeof input.config === 'object') return input.config;
  const configUrl = input?.configUrl || process.env.JOBSEARCH_CONFIG_URL || process.env.CONFIG_URL;
  if (!configUrl) throw new Error('Missing configUrl (set in task input, or JOBSEARCH_CONFIG_URL env var).');
  return await fetchJson(configUrl);
}

async function loadRubricText(rubricUrl) {
  if (!rubricUrl) throw new Error('Missing scoring.rubricUrl in config.');
  return await fetchText(rubricUrl);
}

function stripCodeFences(s) {
  return String(s || '')
    .replace(/^\s*```(?:json)?\s*/i, '')
    .replace(/\s*```\s*$/i, '')
    .trim();
}

function extractResponseText(resp) {
  const parts = [];
  const output = resp?.output;
  if (Array.isArray(output)) {
    for (const o of output) {
      if (o && typeof o.text === 'string') parts.push(o.text);
      const content = o?.content;
      if (Array.isArray(content)) {
        for (const c of content) {
          if (!c) continue;
          if (typeof c.text === 'string') parts.push(c.text);
          // Some variants may place text under "content"
          if (typeof c.content === 'string') parts.push(c.content);
        }
      }
    }
  }

  const joined = parts.join('').trim();
  return joined;
}

function toInt(v, fallback = 0) {
  const n = Number(v);
  if (!Number.isFinite(n)) return fallback;
  return Math.round(n);
}

function normalizeLocationOk(v) {
  const s = String(v ?? '').toLowerCase().trim();
  if (s === 'yes' || s === 'true') return 'yes';
  if (s === 'no' || s === 'false') return 'no';
  return 'unknown';
}

async function listDatasetItems(datasetId, limit) {
  const client = Actor.apifyClient;
  const items = [];
  let offset = 0;
  const pageSize = 250;

  while (items.length < limit) {
    const { items: batch } = await client.dataset(datasetId).listItems({
      offset,
      limit: Math.min(pageSize, limit - items.length),
      clean: true,
    });
    if (!batch || batch.length === 0) break;
    items.push(...batch);
    offset += batch.length;
  }

  return items;
}

async function parseRetryAfterMs(value) {
  if (!value) return null;
  const v = String(value).trim();
  if (!v) return null;

  // If Retry-After is seconds
  if (/^\d+$/.test(v)) return Number(v) * 1000;

  // If Retry-After is a HTTP date
  const ms = Date.parse(v);
  if (Number.isFinite(ms)) {
    const delta = ms - Date.now();
    return delta > 0 ? delta : null;
  }
  return null;
}

async function callOpenAIJson({ apiKey, model, messages, maxOutputTokens = 700, stats }) {
  // Uses the OpenAI Responses API.
  // NOTE: This actor expects OPENAI_API_KEY to be set as an env var.
  const url = 'https://api.openai.com/v1/responses';

  if (stats) stats.calls += 1;

  // GPT-5 family models are reasoning models — they use output tokens for
  // both invisible reasoning AND the visible JSON response.  We need a much
  // higher max_output_tokens to avoid "incomplete" responses.
  const isGpt5 = model.startsWith('gpt-5');
  const effectiveMaxTokens = isGpt5 ? Math.max(maxOutputTokens, 4096) : maxOutputTokens;
  const payload = {
    model,
    input: messages,
    ...(isGpt5
      ? { reasoning: { effort: 'medium' } }
      : { temperature: 0 }),
    max_output_tokens: effectiveMaxTokens,
    text: { format: { type: 'json_object' } },
  };

  // Reasoning models can take 30-60s per request; use a generous timeout.
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), 120000); // 2 minutes
  let res;
  try {
    res = await fetch(url, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(payload),
      signal: controller.signal,
    });
  } catch (fetchErr) {
    clearTimeout(timeoutId);
    // Network errors (ECONNRESET, ETIMEDOUT, AbortError) → retryable
    const err = new Error(`OpenAI fetch error: ${fetchErr?.message || fetchErr}`);
    err.status = 0;
    if (stats) stats.serverErrors += 1;
    throw err;
  }

  const retryAfterMs = await parseRetryAfterMs(res.headers?.get?.('retry-after'));

  let json;
  try {
    json = await res.json();
  } catch (bodyErr) {
    clearTimeout(timeoutId);
    const err = new Error(`OpenAI response body error: ${bodyErr?.message || bodyErr}`);
    err.status = res.status;
    if (stats) stats.serverErrors += 1;
    throw err;
  }
  clearTimeout(timeoutId);

  if (!res.ok) {
    const msg = json?.error?.message || JSON.stringify(json).slice(0, 500);
    const code = json?.error?.code || res.status;
    const err = new Error(`OpenAI API error (${res.status} / ${code}): ${msg}`);
    err.status = res.status;
    err.body = json;
    err.retryAfterMs = retryAfterMs;

    if (stats) {
      if (res.status === 429) stats.rateLimit429 += 1;
      if (res.status >= 500) stats.serverErrors += 1;
    }

    throw err;
  }

  // Accumulate token usage from the response
  if (stats && json.usage) {
    stats.inputTokens += json.usage.input_tokens || 0;
    stats.outputTokens += json.usage.output_tokens || 0;
  }

  const text = extractResponseText(json);
  if (!text) {
    const err = new Error(`OpenAI response missing output text: ${JSON.stringify(json).slice(0, 700)}`);
    err.status = 200; // Non-retryable (API succeeded but response was empty)
    throw err;
  }

  const cleaned = stripCodeFences(text);
  try {
    return JSON.parse(cleaned);
  } catch (e) {
    const err = new Error(`Model did not return valid JSON: ${e?.message || e}\n${cleaned.slice(0, 700)}`);
    err.status = 200; // Non-retryable (API succeeded but returned invalid JSON)
    throw err;
  }
}

// withRetries and mapWithConcurrency removed — now using processWithRetries from resilient-fetch.js

function truncate(s, maxChars) {
  const t = String(s || '');
  if (t.length <= maxChars) return t;
  return t.slice(0, maxChars) + '\n\n[TRUNCATED]';
}

// --------------- Score cache helpers ---------------

const SCORING_FORMAT_VERSION = 'v12'; // v12: ai_work_arrangement fix (workMode changes for many jobs); URL health check; non-USD salary suppression

function extractRubricVersion(rubricText) {
  const match = String(rubricText || '').match(/^(?:#\s+)?Rubric:.*?\((v\d+)\)/i);
  return match ? match[1] : null;
}

// Normalize company name for cache key matching (same logic as 02_merge_dedup)
function normalizeCompany(name) {
  if (!name) return '';
  return String(name)
    .toLowerCase()
    .replace(/\b(inc|llc|corp|ltd|co|studios|studio|games|entertainment|interactive|digital|group|holdings|technologies|technology|the)\b/gi, '')
    .replace(/[^a-z0-9]/g, '')
    .trim();
}

// Normalize title for cache key matching (same logic as 02_merge_dedup)
function normalizeTitle(title) {
  if (!title) return '';
  return String(title)
    .toLowerCase()
    .replace(/\s*\([^)]*\)\s*/g, ' ')
    .replace(/[,\-–—:\/|]/g, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function lookupCache(cacheMap, job) {
  if (!cacheMap) return null;
  // Try primary key from merge step
  if (job.key && cacheMap.has(job.key)) return cacheMap.get(job.key);
  // Try URL-based keys
  if (job.url) {
    const k = `url:${job.url}`;
    if (cacheMap.has(k)) return cacheMap.get(k);
  }
  if (job.applyUrl) {
    const k = `url:${job.applyUrl}`;
    if (cacheMap.has(k)) return cacheMap.get(k);
  }
  // Try company+title key
  const c = normalizeCompany(job.company);
  const t = normalizeTitle(job.title);
  if (c && t) {
    const k = `ct:${c}|${t}`;
    if (cacheMap.has(k)) return cacheMap.get(k);
  }
  return null;
}

// --------------- Location gating (deterministic, outside LLM) ---------------

/**
 * Determine if a job's location is acceptable, using pre-normalized fields from collector.
 * job.workMode:   'Remote' | 'Hybrid' | 'On-Site' | ''
 * job.commutable: true | false | null (set by collector's normalizeLocationFields)
 *
 * Remote jobs always pass (foreign Remote is fine — no location restriction).
 */
function computeLocationOk(job) {
  // If applicantLocationRequirements explicitly excludes US, reject even if Remote
  if (job._usExcluded) return 'no';
  const isRemote = String(job.workMode || '') === 'Remote';
  if (isRemote) return 'yes';
  if (job.commutable === true) return 'yes';
  if (job.commutable === false) return 'no';
  // commutable is null/undefined → unknown (no location, bare "USA", ambiguous city)
  return 'unknown';
}

// --------------- XLSX helpers ---------------

function friendlySourceName(sourceId) {
  if (!sourceId) return '';
  const s = String(sourceId);
  if (s.startsWith('fantastic_')) return 'Fantastic';
  if (s.startsWith('linkedin_')) return 'LinkedIn';
  if (s.startsWith('builtin_'))  return 'Built In';
  if (s.startsWith('usajobs_'))  return 'USAJobs';
  if (s === 'gracklehq')         return 'GrackleHQ';
  if (s.startsWith('gamejobs_co')) return 'GameJobs';
  return s;
}

// --------------- Position Type Detection ---------------

function detectPositionType(job) {
  // 1. Check normalized employmentType field from collector
  const et = String(job.employmentType || '');
  if (et) {
    // Collector normalizes to exact values; use direct match
    if (et === 'Internship') return 'Internship';
    if (et === 'Freelance') return 'Freelance';
    if (et === 'Temporary') return 'Temporary';
    if (et === 'Volunteer') return 'Volunteer';
    if (et === 'Part-Time') return 'Part-Time';
    if (et === 'Full-Time') return 'Full-Time';
    // Fallback for un-normalized values (e.g., from enrichDescriptions)
    const etLow = et.toLowerCase();
    if (etLow.includes('intern')) return 'Internship';
    if (etLow.includes('freelance') || etLow.includes('contract') || etLow.includes('1099')) return 'Freelance';
    if (etLow.includes('temporary') || etLow.includes('temp') || etLow.includes('seasonal')) return 'Temporary';
    if (etLow.includes('volunteer')) return 'Volunteer';
    if (etLow.includes('part-time') || etLow.includes('part time')) return 'Part-Time';
    if (etLow.includes('full-time') || etLow.includes('full time')) return 'Full-Time';
  }

  // 2. Check job title
  const title = String(job.title || '');
  if (/\bintern(ship)?\b/i.test(title)) return 'Internship';
  if (/\b(contract|contractor|freelance)\b/i.test(title)) return 'Freelance';
  if (/\bpart[\s-]?time\b/i.test(title)) return 'Part-Time';

  // 3. Check description (first 2000 chars for performance)
  const desc = String(job.description || '').slice(0, 2000).toLowerCase();
  if (/\binternship\b/.test(desc) && /\bintern\b/.test(desc)) return 'Internship';
  if (/\b(contract position|contract role|independent contractor|1099)\b/.test(desc)) return 'Freelance';
  if (/\bpart[\s-]?time\b/.test(desc) && !/\bfull[\s-]?time\b/.test(desc)) return 'Part-Time';

  return 'Full-Time';
}

// Remote signal patterns (same as collector, used by enrichDescriptions for newly-fetched descriptions)
const REMOTE_SIGNAL_PATTERNS = [
  /\bremote position\b/, /\bremote role\b/, /\bwork remotely\b/,
  /\bfully remote\b/, /\b100% remote\b/, /\bremote[\s-]?first\b/,
  /\bremote work\b/, /\bremote opportunity\b/,
];

// --------------- Commutable towns (subset of collector's list, for Built In enrichment) ---------------

const COMMUTABLE_TOWNS = new Set([
  'acton', 'andover', 'arlington', 'ashland', 'ayer', 'bedford', 'belmont',
  'beverly', 'billerica', 'bolton', 'boston', 'boxborough', 'braintree',
  'brookline', 'burlington', 'cambridge', 'canton', 'carlisle', 'chelmsford',
  'chelsea', 'concord', 'danvers', 'dedham', 'dover', 'dracut', 'dunstable',
  'everett', 'foxborough', 'framingham', 'grafton', 'groton', 'harvard',
  'holliston', 'hopkinton', 'hudson', 'lawrence', 'lexington', 'lincoln',
  'littleton', 'lowell', 'lynn', 'lynnfield', 'malden', 'marlborough',
  'maynard', 'medfield', 'medford', 'medway', 'melrose', 'methuen', 'milford',
  'millis', 'milton', 'natick', 'needham', 'newton', 'north andover',
  'north reading', 'northborough', 'norwood', 'peabody', 'pepperell', 'quincy',
  'reading', 'revere', 'salem', 'saugus', 'sherborn', 'shirley', 'shrewsbury',
  'somerville', 'southborough', 'stoneham', 'stow', 'sudbury', 'tewksbury',
  'townsend', 'tyngsborough', 'wakefield', 'walpole', 'waltham', 'watertown',
  'wayland', 'wellesley', 'westborough', 'westford', 'weston', 'wilmington',
  'winchester', 'woburn', 'worcester',
]);

/**
 * Apply Built In JSON-LD structured data to a job object.
 * Updates location, workMode, commutable, postedAt, salary, and expired status.
 * Returns true if the job should be rejected (non-USA applicant requirement or expired).
 */
function applyBuiltInStructuredData(job, data, index, preLocationMap) {
  let changed = false;

  // Basic fields
  if (data.employmentType && !job.employmentType) job.employmentType = data.employmentType;
  if (data.datePosted && !job.postedAt) job.postedAt = data.datePosted;
  if (data.salary && !job.salary) job.salary = data.salary;

  // jobLocationType: TELECOMMUTE = Remote
  if (data.jobLocationType === 'TELECOMMUTE' && job.workMode !== 'Remote') {
    job.workMode = 'Remote';
    changed = true;
  }

  // applicantLocationRequirements: if USA is not listed, job may not be for US applicants.
  // Only reject NON-remote jobs — many foreign companies mark remote jobs with their own
  // country in applicantLocationRequirements even when they hire worldwide.
  const appCountries = data.applicantCountries || [];
  if (appCountries.length > 0 && !appCountries.includes('USA') && !appCountries.includes('US')) {
    job.commutable = false;
    if (job.workMode !== 'Remote') {
      job._usExcluded = true;
    }
    // Update location to the first listed country if we don't have a better one
    if (!job.location || job.location === '') {
      job.location = appCountries[0]; // Already ISO3 from Built In
    }
    changed = true;
  }

  // jobLocation address: use for location enrichment
  const addr = data.jobLocationAddress || {};
  if (addr.addressCountry) {
    const country = addr.addressCountry; // Already ISO3 from Built In (e.g., "USA", "GBR", "TUR")
    if (country === 'USA' || country === 'US') {
      // US job — check commutability from city
      const city = (addr.addressLocality || '').trim().toLowerCase();
      const region = (addr.addressRegion || '').trim();
      if (city && COMMUTABLE_TOWNS.has(city) && /^(MA|Massachusetts)$/i.test(region)) {
        job.commutable = true;
        job.location = `${addr.addressLocality} MA`;
        changed = true;
      } else if (region) {
        // US but not commutable — set location to "City ST" or just state
        const stateAbbrev = region.length === 2 ? region.toUpperCase() : '';
        if (city && stateAbbrev) {
          job.location = `${addr.addressLocality} ${stateAbbrev}`;
        } else if (stateAbbrev) {
          job.location = stateAbbrev;
        }
        job.commutable = false;
        changed = true;
      }
    } else {
      // Foreign country
      job.commutable = false;
      if (!job.location || job.location === '' || !/^[A-Z]{3}$/.test(job.location)) {
        job.location = country; // Use ISO3 from structured data
      }
      changed = true;
    }
  }

  // validThrough: if expired, mark job
  if (data.validThrough) {
    const expiry = new Date(data.validThrough);
    if (!isNaN(expiry.getTime()) && expiry < new Date()) {
      job._expired = true;
    }
  }

  // Remote signal check from description
  if (job.workMode !== 'Remote' && data.description) {
    const desc = data.description.slice(0, 3000).toLowerCase();
    if (REMOTE_SIGNAL_PATTERNS.some(p => p.test(desc))) {
      job.workMode = 'Remote';
      changed = true;
    }
  }

  if (changed) {
    preLocationMap.set(index, computeLocationOk(job));
  }

  return false; // don't reject here — let the normal filters handle it
}

// --------------- Built In Description Enrichment ---------------

const BUILTIN_DESC_CACHE_TTL_DAYS = 30;
const BUILTIN_FETCH_DELAY_MS = 3000;

// --------------- LinkedIn Detail Enrichment (via bestscrapers actor) ---------------

const LINKEDIN_DETAIL_CACHE_TTL_DAYS = 30;
const LINKEDIN_DETAIL_ACTOR_ID = 'aQNKZRc1yXowQlLKL';
const LINKEDIN_DETAIL_WAIT_SECS = 60;
const LINKEDIN_DETAIL_DELAY_MS = 1000; // delay between calls to avoid hammering

/**
 * Extract the numeric LinkedIn job ID from a URL like
 * "https://www.linkedin.com/jobs/view/game-designer-at-foo-4380262477"
 * or from a raw job ID field.
 */
function extractLinkedInJobId(job) {
  // Try the url field first
  for (const u of [job.url, job.applyUrl, job.linkedinUrl]) {
    if (!u) continue;
    const m = String(u).match(/jobs\/view\/(?:.*?[-/])?(\d{8,})/);
    if (m) return m[1];
  }
  // Try job.id — LinkedIn IDs from Fantastic are like "L-4380262477"
  const rawId = String(job.id || '');
  const m2 = rawId.match(/(\d{8,})/);
  if (m2) return m2[1];
  return null;
}

/**
 * Call the bestscrapers/linkedin-job-details-scraper for a single job URL.
 * Returns the .data object from the scraper output, or null on failure.
 */
async function fetchLinkedInDetail(jobId, apifyToken) {
  const jobUrl = `https://www.linkedin.com/jobs/view/${jobId}`;
  const runUrl = `https://api.apify.com/v2/acts/${LINKEDIN_DETAIL_ACTOR_ID}/runs?token=${apifyToken}&waitForFinish=${LINKEDIN_DETAIL_WAIT_SECS}`;
  const res = await fetch(runUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ job_url: jobUrl }),
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(`bestscrapers run failed (HTTP ${res.status}): ${text.slice(0, 300)}`);
  }
  const run = await res.json();
  const datasetId = run?.data?.defaultDatasetId;
  if (!datasetId) throw new Error('bestscrapers run returned no datasetId');

  const itemsUrl = `https://api.apify.com/v2/datasets/${datasetId}/items?token=${apifyToken}`;
  const itemsRes = await fetch(itemsUrl);
  if (!itemsRes.ok) throw new Error(`Dataset fetch failed (HTTP ${itemsRes.status})`);
  const items = await itemsRes.json();
  return items?.[0]?.data || null;
}

/**
 * Strip HTML tags from a string and produce clean plain text.
 */
function stripHtmlTags(html) {
  return String(html || '')
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/p>/gi, '\n\n')
    .replace(/<\/li>/gi, '\n')
    .replace(/<li[^>]*>/gi, '• ')
    .replace(/<[^>]+>/g, '')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&nbsp;/g, ' ')
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

/**
 * Format a schema.org baseSalary object into a human-readable string.
 * Handles MonetaryAmount with QuantitativeValue, plain numbers, and strings.
 */
function formatBaseSalary(baseSalary) {
  if (!baseSalary) return '';
  if (typeof baseSalary === 'string') return baseSalary;
  if (typeof baseSalary === 'number') return `$${baseSalary.toLocaleString()}`;
  // MonetaryAmount: { currency, value: { minValue, maxValue, unitText } }
  const currency = baseSalary.currency || 'USD';
  const sym = currency === 'USD' ? '$' : currency === 'GBP' ? '£' : currency === 'EUR' ? '€' : `${currency} `;
  const val = baseSalary.value || baseSalary;
  if (typeof val === 'number') return `${sym}${val.toLocaleString()}`;
  const min = val.minValue ?? val.min;
  const max = val.maxValue ?? val.max;
  const unit = val.unitText || baseSalary.unitText || '';
  const unitSuffix = unit.toLowerCase().startsWith('year') ? '/yr'
    : unit.toLowerCase().startsWith('hour') ? '/hr'
    : unit.toLowerCase().startsWith('month') ? '/mo'
    : unit ? `/${unit}` : '';
  if (min != null && max != null) return `${sym}${Number(min).toLocaleString()}-${sym}${Number(max).toLocaleString()}${unitSuffix}`;
  if (min != null) return `${sym}${Number(min).toLocaleString()}+${unitSuffix}`;
  if (max != null) return `Up to ${sym}${Number(max).toLocaleString()}${unitSuffix}`;
  return '';
}

/**
 * Fetch a Built In job page and extract the description from JSON-LD structured data.
 * Returns { description, employmentType, datePosted, salary } or null if not found.
 */
async function fetchBuiltInDescription(url) {
  const res = await fetch(url, {
    method: 'GET',
    headers: {
      'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
      'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
      'Accept-Language': 'en-US,en;q=0.9',
    },
  });
  if (!res.ok) {
    const err = new Error(`HTTP ${res.status} for ${url}`);
    err.status = res.status;
    throw err;
  }
  const html = await res.text();

  // Extract JSON-LD structured data containing JobPosting
  // Note: Built In encodes the "+" as &#x2B; in the type attribute, so we match both
  const ldJsonMatches = html.matchAll(/<script\s+type=["']application\/ld(?:\+|&#x2B;)json["'][^>]*>([\s\S]*?)<\/script>/gi);
  for (const match of ldJsonMatches) {
    try {
      const data = JSON.parse(match[1]);
      const postings = [];
      if (data['@type'] === 'JobPosting') postings.push(data);
      if (Array.isArray(data['@graph'])) {
        for (const item of data['@graph']) {
          if (item['@type'] === 'JobPosting') postings.push(item);
        }
      }
      for (const posting of postings) {
        if (posting.description) {
          // Extract applicantLocationRequirements — can be object or array of objects
          const alr = posting.applicantLocationRequirements;
          let applicantCountries = [];
          if (Array.isArray(alr)) {
            applicantCountries = alr.map(c => c?.name || '').filter(Boolean);
          } else if (alr?.name) {
            applicantCountries = [alr.name];
          }

          // Extract jobLocation address
          const addr = posting.jobLocation?.address || {};
          const jobLocationAddress = {
            addressCountry: addr.addressCountry || '',
            addressLocality: addr.addressLocality || '',
            addressRegion: addr.addressRegion || '',
          };

          return {
            description: stripHtmlTags(posting.description),
            employmentType: posting.employmentType || '',
            datePosted: posting.datePosted || '',
            salary: formatBaseSalary(posting.baseSalary) || '',
            validThrough: posting.validThrough || '',
            jobLocationType: posting.jobLocationType || '',   // TELECOMMUTE = remote
            applicantCountries,                                // e.g. ["USA"] or ["GBR","DEU","POL"]
            jobLocationAddress,                                // { addressCountry, addressLocality, addressRegion }
          };
        }
      }
    } catch { /* ignore malformed JSON-LD */ }
  }
  return null;
}

// --------------- Location Formatting ---------------

/**
 * Format location for XLSX display.
 * With pre-normalized fields, this is now trivial — location is already clean.
 * For accepted.xlsx: combines workMode + location into a single "Where" string.
 * For scored.xlsx: location is shown as-is (workMode is a separate column).
 */
function formatLocationForDisplay(job, combineWorkMode = false) {
  const loc = String(job.location || '').trim();
  if (!combineWorkMode) return loc;
  // Combine for scored.xlsx "Where" column (legacy)
  const wm = String(job.workMode || '');
  if (!wm) return loc;
  if (!loc) return wm;
  return `${wm} | ${loc}`;
}

/**
 * Format "Job Type" column for accepted.xlsx: workMode | positionType | location
 */
function formatJobTypeForDisplay(job) {
  const parts = [
    String(job.workMode || '').trim(),
    String(job.positionType || 'Full-Time').trim(),
    String(job.location || '').trim(),
  ].filter(Boolean);
  return parts.join(' | ');
}

function extractRootDomainUrl(urlStr) {
  if (!urlStr) return '';
  try {
    const u = new URL(urlStr.startsWith('http') ? urlStr : `https://${urlStr}`);
    return `${u.protocol}//${u.hostname}`;
  } catch {
    return '';
  }
}

function friendlyDomainName(urlStr, fallbackCompany) {
  if (!urlStr) return fallbackCompany || '';
  try {
    const hostname = new URL(urlStr.startsWith('http') ? urlStr : `https://${urlStr}`).hostname.toLowerCase();
    // Known ATS / job-board platforms
    if (hostname.includes('lever.co')) return 'Lever';
    if (hostname.includes('ashbyhq.com')) return 'Ashby';
    if (hostname.includes('greenhouse.io')) return 'Greenhouse';
    if (hostname.includes('workday.com') || hostname.includes('myworkday.com')) return 'Workday';
    if (hostname.includes('indeed.com')) return 'Indeed';
    if (hostname.includes('linkedin.com')) return 'LinkedIn';
    if (hostname.includes('glassdoor.com')) return 'Glassdoor';
    if (hostname.includes('smartrecruiters.com')) return 'SmartRecruiters';
    if (hostname.includes('breezy.hr')) return 'Breezy HR';
    if (hostname.includes('recruitee.com')) return 'Recruitee';
    if (hostname.includes('bamboohr.com')) return 'BambooHR';
    if (hostname.includes('icims.com')) return 'iCIMS';
    if (hostname.includes('ultipro.com') || hostname.includes('ukg.com')) return 'UKG';
    if (hostname.includes('jobvite.com')) return 'Jobvite';
    if (hostname.includes('taleo.net')) return 'Taleo';
    if (hostname.includes('successfactors.com')) return 'SuccessFactors';
    if (hostname.includes('applytojob.com')) return 'ApplyToJob';
    if (hostname.includes('ziprecruiter.com')) return 'ZipRecruiter';
    if (hostname.includes('remotive.com')) return 'Remotive';
    if (hostname.includes('remoteok.com')) return 'RemoteOK';
    if (hostname.includes('dice.com')) return 'Dice';
    if (hostname.includes('angel.co') || hostname.includes('wellfound.com')) return 'Wellfound';
    if (hostname.includes('ycombinator.com')) return 'Y Combinator';
    // Fallback: strip common prefixes + TLD, capitalize
    let name = hostname.replace(/^(www\.|jobs\.|careers\.|apply\.|career\.)/, '');
    name = name.replace(/\.(com|org|net|io|co|gg|dev|us|gov)$/, '');
    if (name) return name.charAt(0).toUpperCase() + name.slice(1);
    return fallbackCompany || '';
  } catch {
    return fallbackCompany || '';
  }
}

function calculateAgeDays(postedAt) {
  if (!postedAt) return '';
  try {
    const posted = new Date(postedAt);
    if (isNaN(posted.getTime())) return '';
    const diffMs = Date.now() - posted.getTime();
    return Math.max(0, Math.floor(diffMs / (1000 * 60 * 60 * 24)));
  } catch {
    return '';
  }
}

function ensureProtocol(url) {
  if (!url) return '';
  const s = String(url).trim();
  if (!s) return '';
  if (!/^https?:\/\//i.test(s)) return `https://${s}`;
  return s;
}

/**
 * Standardize a salary string into consistent formats:
 *   Hourly:  "$50 - $120/hr"
 *   Annual:  "$66K - $89K/yr"
 *   Monthly: "$5K - $8K/mo"
 *
 * Returns { display: string, note: string|null }
 *   display = cleaned salary string (or '' if garbage)
 *   note = warning string if non-USD or garbage detected (for email/diagnostics)
 */
function standardizeSalary(raw, company, title) {
  if (!raw && raw !== 0) return { display: '', note: null };
  // Collapse whitespace (newlines, tabs, multi-spaces) into single spaces
  const s = String(raw).replace(/\s+/g, ' ').trim();
  if (!s) return { display: '', note: null };

  const jobLabel = `${company || '?'} — ${title || '?'}`;

  // --- Detect garbage: no digits at all, or suspiciously low numbers with no context ---
  // e.g., "1-3 Annually"
  const digitCount = (s.match(/\d/g) || []).length;
  if (digitCount === 0) {
    return { display: '', note: `Garbage salary removed for ${jobLabel}: "${s}"` };
  }
  // Numbers where the max is < 100 and it says "annually" → garbage
  const garbageMatch = s.match(/^(\d{1,2})\s*[-–—]\s*(\d{1,2})\s*(?:annually|yearly|per\s*year)/i);
  if (garbageMatch && Number(garbageMatch[2]) < 100) {
    return { display: '', note: `Garbage salary removed for ${jobLabel}: "${s}"` };
  }

  // --- Detect currency ---
  // If no $ symbol present but has digits, might be non-USD
  const hasDollar = s.includes('$');
  const hasEuro = s.includes('€');
  const hasPound = s.includes('£');
  const hasExplicitUSD = /\bUSD\b/i.test(s);
  const hasExplicitCurrency = /\b(EUR|GBP|CAD|AUD|JPY|SEK|NOK|DKK|CHF|INR|IDR|PLN|RON|BRL|MXN|SGD|NZD|ZAR)\b/i.test(s);
  const currencySymbol = hasDollar || hasExplicitUSD ? '$'
    : hasEuro ? '€'
    : hasPound ? '£'
    : '';
  const nonUsdNote = (!hasDollar && !hasExplicitUSD && !hasEuro && !hasPound && !hasExplicitCurrency && digitCount >= 2)
    ? null  // No currency info at all — assume USD and add $
    : (hasEuro || hasPound || hasExplicitCurrency)
      ? `Non-USD salary for ${jobLabel}: "${s}"`
      : null;

  // Determine the prefix to use: if original had $, €, or £ we use it; if it had
  // explicit "USD" we use "$"; if NO currency indicator at all, we assume USD and use "$".
  // If non-USD currency, leave symbol off (use '').
  const prefix = (hasEuro || hasPound || hasExplicitCurrency) ? '' : '$';

  // --- Parse the salary string ---
  // Remove currency words/symbols for easier parsing
  let cleaned = s
    .replace(/(?:USD|EUR|GBP|CAD|AUD)\s*/gi, '')
    .replace(/[$€£]/g, '')
    .trim();

  // Detect period suffix: /yr, /hr, /mo, annually, hourly, per hour, per year, etc.
  let period = '';
  const periodMatch = cleaned.match(/(?:\/\s*|per\s*)(yr|year|annually|annual|hr|hour|hourly|mo|month|monthly)/i);
  if (periodMatch) {
    const p = periodMatch[1].toLowerCase();
    if (p.startsWith('yr') || p.startsWith('year') || p.startsWith('annual')) period = '/yr';
    else if (p.startsWith('hr') || p.startsWith('hour')) period = '/hr';
    else if (p.startsWith('mo') || p.startsWith('month')) period = '/mo';
  }
  if (!period && /\bannually\b|\bper\s*year\b|\b\/yr\b|\byear\b/i.test(cleaned)) period = '/yr';
  if (!period && /\bhourly\b|\bper\s*hour\b|\b\/hr\b/i.test(cleaned)) period = '/hr';
  if (!period && /\bmonthly\b|\bper\s*month\b|\b\/mo\b/i.test(cleaned)) period = '/mo';

  // Strip period text for number parsing
  cleaned = cleaned
    .replace(/\s*(?:\/\s*)?(?:yr|year|annually|annual|hr|hour|hourly|mo|month|monthly|per\s*(?:year|hour|month|annum))\b/gi, '')
    .replace(/\s*(?:a\s+year|an\s+hour)\b/gi, '')
    .trim();

  // Extract numbers — handle "120K", "120,000", "120000", "120000.00", "120000.0"
  function parseNum(str) {
    let n = str.replace(/,/g, '').trim();
    const kMatch = n.match(/^([\d.]+)\s*[kK]$/);
    if (kMatch) return Number(kMatch[1]) * 1000;
    return Number(n);
  }

  // Try range: "120K-170K", "120,000 - 170,000", "$50.00 - $120.00", "120000.0 - 230000.0"
  const rangeMatch = cleaned.match(/([\d,.]+\s*[kK]?)\s*[-–—]\s*([\d,.]+\s*[kK]?)/);
  if (rangeMatch) {
    let lo = parseNum(rangeMatch[1]);
    let hi = parseNum(rangeMatch[2]);
    if (Number.isFinite(lo) && Number.isFinite(hi) && lo > 0 && hi > 0) {
      // Infer period from magnitude if not explicit
      if (!period) {
        if (hi >= 10000) period = '/yr';
        else if (hi < 500) period = '/hr';
      }
      const result = formatRange(lo, hi, period, prefix);
      return { display: result, note: nonUsdNote };
    }
  }

  // Try single value: "120K", "135000", "Up to $200,000"
  const upToMatch = cleaned.match(/up\s+to\s+([\d,.]+\s*[kK]?)/i);
  const singleMatch = upToMatch || cleaned.match(/^([\d,.]+\s*[kK]?)$/);
  if (singleMatch) {
    const val = parseNum(singleMatch[1]);
    if (Number.isFinite(val) && val > 0) {
      if (!period) {
        if (val >= 10000) period = '/yr';
        else if (val < 500) period = '/hr';
      }
      const formatted = formatSingleAmount(val, period, prefix);
      const label = upToMatch ? `Up to ${formatted}` : formatted;
      return { display: label, note: nonUsdNote };
    }
  }

  // Fallback: if we can't parse it but it has digits, pass through cleaned version
  // but still remove ".00" cents
  let fallback = s.replace(/\.00\b/g, '').replace(/\s+/g, ' ').trim();
  return { display: fallback, note: nonUsdNote };
}

/** Format a numeric amount for display: "$120K" or "$50" */
function formatSingleAmount(val, period, prefix) {
  if (period === '/yr' || period === '/mo') {
    // Annual/monthly: use K notation if >= 1000
    if (val >= 1000) {
      const k = val / 1000;
      const kStr = k === Math.floor(k) ? String(Math.floor(k)) : k.toFixed(1).replace(/\.0$/, '');
      return `${prefix}${kStr}K${period}`;
    }
    return `${prefix}${Math.round(val)}${period}`;
  }
  // Hourly: plain number, remove .00 cents
  const rounded = val === Math.floor(val) ? String(Math.floor(val)) : val.toFixed(2).replace(/\.00$/, '');
  return `${prefix}${rounded}${period}`;
}

/** Format a range: "$50 - $120/hr" or "$120K - $170K/yr" */
function formatRange(lo, hi, period, prefix) {
  if (period === '/yr' || period === '/mo') {
    // Use K notation if both values are >= 1000
    if (lo >= 1000 && hi >= 1000) {
      const loK = lo / 1000;
      const hiK = hi / 1000;
      const loStr = loK === Math.floor(loK) ? String(Math.floor(loK)) : loK.toFixed(1).replace(/\.0$/, '');
      const hiStr = hiK === Math.floor(hiK) ? String(Math.floor(hiK)) : hiK.toFixed(1).replace(/\.0$/, '');
      return `${prefix}${loStr}K - ${prefix}${hiStr}K${period}`;
    }
    return `${prefix}${Math.round(lo)} - ${prefix}${Math.round(hi)}${period}`;
  }
  // Hourly: plain numbers
  const loStr = lo === Math.floor(lo) ? String(Math.floor(lo)) : lo.toFixed(2).replace(/\.00$/, '');
  const hiStr = hi === Math.floor(hi) ? String(Math.floor(hi)) : hi.toFixed(2).replace(/\.00$/, '');
  return `${prefix}${loStr} - ${prefix}${hiStr}${period}`;
}

/**
 * Set a cell to a clickable hyperlink with display text.
 * If url is empty, just sets the cell to plain text.
 */
function setCellHyperlink(cell, url, text) {
  const displayText = String(text || '').trim();
  const href = ensureProtocol(url);
  if (href) {
    cell.value = { text: displayText || href, hyperlink: href };
    cell.font = { ...cell.font, color: { argb: 'FF0563C1' }, underline: true };
  } else {
    cell.value = displayText;
  }
}

/**
 * For accepted jobs without LinkedIn URLs, search LinkedIn's public guest page
 * and swap in the LinkedIn URL if the same job is found.
 * Fallback: keep existing URL (Indeed, Greenhouse, etc.).
 *
 * Uses a persistent cache (linkedinUrlCache) keyed by normalizedCompany|normalizedTitle
 * to avoid re-searching LinkedIn for the same job on subsequent runs.
 * Cache stores the LinkedIn URL if found, or null if confirmed not on LinkedIn.
 *
 * Returns { enrichedCount, linkedinUrlCache } — the updated cache to persist.
 */
async function enrichLinkedInUrls(acceptedJobs, prevLinkedinUrlCache) {
  const isLiUrl = (u) => /linkedin\.com\/jobs/i.test(String(u || ''));
  const cache = { ...(prevLinkedinUrlCache || {}) }; // copy — will be persisted

  const nonLinkedin = acceptedJobs.filter(
    (j) => j?.evaluation?.accepted && !isLiUrl(j.applyUrl) && !isLiUrl(j.url)
  );
  if (nonLinkedin.length === 0) return { enrichedCount: 0, linkedinUrlCache: cache };

  log.info(`LinkedIn enrichment: ${nonLinkedin.length} non-LinkedIn accepted jobs. Cache has ${Object.keys(cache).length} entries.`);

  // Cache key: same normalizeCompany+normalizeTitle used by the score cache
  function liCacheKey(job) {
    const c = normalizeCompany(job.company);
    const t = normalizeTitle(job.title);
    return (c && t) ? `${c}|${t}` : '';
  }

  // Normalize company name to a slug fragment for matching LinkedIn URL patterns
  // LinkedIn URLs use: /jobs/view/{title-slug}-at-{company-slug}-{id}
  function companySlug(name) {
    return String(name || '')
      .toLowerCase()
      .replace(/[,.'"""'']/g, '')
      .replace(/\b(inc|llc|ltd|corp|co|pte|gmbh|sarl|sa|ag|plc|lp|l\.?p\.?)\b/gi, '')
      .replace(/[^a-z0-9]+/g, '-')
      .replace(/^-+|-+$/g, '');
  }

  async function searchLinkedIn(title, company) {
    const q = `${title} ${company}`.trim();
    const searchUrl = `https://www.linkedin.com/jobs/search/?keywords=${encodeURIComponent(q)}&location=United%20States`;
    try {
      const res = await fetch(searchUrl, {
        headers: { 'User-Agent': 'Mozilla/5.0 (compatible; job-pipeline/1.0)' },
        signal: AbortSignal.timeout(10000),
      });
      const html = await res.text();

      // Extract all /jobs/view/ URLs from the HTML
      const urlMatches = html.match(/https?:\/\/[a-z.]*linkedin\.com\/jobs\/view\/[^"'?\s]+/gi) || [];
      // Deduplicate
      return [...new Set(urlMatches)];
    } catch (err) {
      log.warning(`LinkedIn search failed for "${q}": ${err?.message || err}`);
      return [];
    }
  }

  let enrichedCount = 0;
  let cacheHitCount = 0;
  const toSearch = []; // jobs that need a live LinkedIn search

  // Phase 1: check cache
  for (const job of nonLinkedin) {
    const key = liCacheKey(job);
    if (!key) { toSearch.push(job); continue; }

    if (key in cache) {
      cacheHitCount++;
      const cachedUrl = cache[key];
      if (cachedUrl) {
        job.applyUrl = cachedUrl;
        enrichedCount++;
        log.info(`LinkedIn enriched (cached): "${job.title}" at ${job.company}`);
      }
      // cachedUrl === null means "confirmed not on LinkedIn" — skip
    } else {
      toSearch.push(job);
    }
  }

  if (cacheHitCount > 0) {
    log.info(`LinkedIn cache: ${cacheHitCount} hits, ${toSearch.length} new searches needed.`);
  }

  // Phase 2: search LinkedIn for uncached jobs
  const BATCH_SIZE = 5;
  for (let i = 0; i < toSearch.length; i += BATCH_SIZE) {
    const batch = toSearch.slice(i, i + BATCH_SIZE);
    const searches = await Promise.all(
      batch.map((j) => searchLinkedIn(j.title || '', j.company || ''))
    );

    for (let k = 0; k < batch.length; k++) {
      const job = batch[k];
      const urls = searches[k];
      const key = liCacheKey(job);

      if (urls.length === 0) {
        if (key) cache[key] = null; // remember: not on LinkedIn
        continue;
      }

      const slug = companySlug(job.company);
      if (!slug) { if (key) cache[key] = null; continue; }

      // Find a URL whose slug contains the company name after "-at-"
      // AND whose title slug exactly matches the job title.
      // LinkedIn URLs look like: /jobs/view/{title-slug}-at-{company-slug}-{id}
      // Job titles are identical across job boards, so we require an exact match
      // to avoid dangerous false matches (e.g., "Game Developer" → "Senior Game Developer").
      const titleSlug = String(job.title || '')
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '-')
        .replace(/^-+|-+$/g, '');

      let match = null;
      for (const u of urls) {
        const lower = u.toLowerCase();
        const companyMatch = lower.includes(`-at-${slug}-`) || lower.endsWith(`-at-${slug}`);
        if (!companyMatch) continue;
        const atIdx = lower.indexOf(`-at-${slug}`);
        if (atIdx < 0) continue;
        const titlePart = lower.slice(lower.lastIndexOf('/') + 1, atIdx);
        if (titlePart === titleSlug) { match = u; break; }
      }

      if (match) {
        const cleanUrl = match.split(/['">\s]/)[0];
        log.info(`LinkedIn enriched: "${job.title}" at ${job.company} → ${cleanUrl}`);
        job.applyUrl = cleanUrl;
        enrichedCount++;
        if (key) cache[key] = cleanUrl;
      } else {
        if (urls.some(u => u.toLowerCase().includes(`-at-${slug}`))) {
          log.info(`LinkedIn enrichment skipped: "${job.title}" at ${job.company} — company found on LinkedIn but no exact title match`);
        }
        if (key) cache[key] = null; // company found on LinkedIn but no matching job
      }
    }

    // Delay between batches
    if (i + BATCH_SIZE < toSearch.length) {
      await new Promise((r) => setTimeout(r, 2000));
    }
  }

  log.info(`LinkedIn enrichment: ${enrichedCount} of ${nonLinkedin.length} enriched (${cacheHitCount} cached, ${toSearch.length} searched).`);
  return { enrichedCount, linkedinUrlCache: cache };
}

// Build an XLSX workbook buffer from an array of scored jobs
async function buildScoredXlsx(jobs, {
  includeJobIds = false,
  scoringFormatVersion = null,
  rubricVersion = null,
  isAcceptedSheet = false,
  runNumber = null,
} = {}) {
  const workbook = new ExcelJS.Workbook();
  const ws = workbook.addWorksheet('Jobs');

  // Define column headers, widths, and keys — conditionally include some columns
  const colDefs = [
    { header: 'Company',       width: 26, key: 'company' },
    { header: 'Job Title',     width: 46, key: 'title' },
    { header: 'Salary',        width: 26, key: 'salary' },
  ];
  if (isAcceptedSheet) {
    colDefs.push({ header: 'Where', width: 40, key: 'jobType' }); // workMode | positionType | location
    colDefs.push({ header: 'Where Found', width: 19, key: 'whereFound' });
    colDefs.push({ header: 'Age (days)',  width: 11, key: 'age' });
    colDefs.push({ header: 'Sources', width: 30, key: 'sources' });
  } else {
    colDefs.push({ header: 'Location',   width: 30, key: 'location' });
    colDefs.push({ header: 'Work Mode',  width: 12, key: 'workMode' });
    colDefs.push({ header: 'Commutable', width: 12, key: 'commutable' });
    colDefs.push({ header: 'Position Type', width: 14, key: 'positionType' });
    colDefs.push({ header: 'Score', width: 8, key: 'score' });
    colDefs.push({ header: 'Role',  width: 22, key: 'role' });
    colDefs.push({ header: 'Age (days)',  width: 11, key: 'age' });
    colDefs.push({ header: 'Where Found', width: 19, key: 'whereFound' });
    colDefs.push({ header: 'Sources', width: 30, key: 'sources' });
  }
  colDefs.push({ header: 'Reason',    width: 52, key: 'reason' });
  if (!isAcceptedSheet) {
    colDefs.push({ header: 'Tags', width: 36, key: 'tags' });
  }
  colDefs.push({ header: 'Red Flags', width: 42, key: 'redFlags' });
  if (includeJobIds) {
    colDefs.push({ header: 'Job IDs', width: 30, key: 'jobIds' });
  }

  // Set column widths only (don't use ws.columns with header — it auto-creates row 1)
  for (let i = 0; i < colDefs.length; i++) {
    ws.getColumn(i + 1).width = colDefs[i].width;
  }

  // Run number row (accepted.xlsx only — bold, large font)
  const hasRunRow = isAcceptedSheet && runNumber != null;
  if (hasRunRow) {
    const runRow = ws.addRow([`Run: ${runNumber}`]);
    runRow.font = { bold: true, size: 16 };
  }

  // Metadata row (scored.xlsx only — when version info is provided)
  const hasMetaRow = !!(scoringFormatVersion || rubricVersion);
  if (hasMetaRow) {
    const metaRow = ws.addRow([
      'Scoring Format:', scoringFormatVersion || '',
      'Rubric', rubricVersion || '',
    ]);
    metaRow.font = { italic: true, color: { argb: 'FF888888' } };
  }

  // Header row (bold)
  const headerRow = ws.addRow(colDefs.map((c) => c.header));
  headerRow.font = { bold: true };

  // Freeze pane: freeze header row + first 2 columns
  const topRows = (hasRunRow ? 1 : 0) + (hasMetaRow ? 1 : 0) + 1; // run + meta + header
  const freezeYSplit = topRows;
  ws.views = [{ state: 'frozen', xSplit: 2, ySplit: freezeYSplit }];

  // Pre-compute column indices for hyperlinks
  const colIdx = (key) => colDefs.findIndex(c => c.key === key) + 1;
  const companyCol = colIdx('company');
  const titleCol = colIdx('title');
  const whereFoundCol = colIdx('whereFound');

  for (const j of jobs) {
    const ev = j.evaluation || {};
    const tags = Array.isArray(ev.tags) ? ev.tags.join('; ') : '';
    const redFlags = Array.isArray(ev.red_flags) ? ev.red_flags.join(' ') : '';
    const roleStr = Array.isArray(ev.role) ? ev.role.join(', ') : (ev.role || '');

    const salResult = standardizeSalary(j.salary || ev.salary_extracted || '', j.company, j.title);
    const salary = salResult.display;
    const score = ev.score ?? 0;
    const ageDays = calculateAgeDays(j.earliestPostedAt || j.postedAt);
    const sourcesArr = Array.isArray(j.sources) ? j.sources : [j.source].filter(Boolean);
    const sourcesStr = sourcesArr.filter(Boolean).join(', ');
    const reason = ev.reason_short || '';
    const positionType = j.positionType || 'Full-Time';

    const valueMap = {
      company:      j.company || '',
      title:        j.title || '',
      salary,
      jobType:      formatJobTypeForDisplay(j),           // combined for accepted.xlsx
      location:     j.location || '',                     // separate for scored.xlsx
      workMode:     j.workMode || '',                     // separate for scored.xlsx
      commutable:   j.commutable === true ? 'Yes' : j.commutable === false ? 'No' : '',
      positionType,
      score,
      role:         roleStr,
      age:          ageDays === '' ? '' : ageDays,
      whereFound:   '',  // will become hyperlink
      sources:      sourcesStr,
      reason,
      tags,
      redFlags,
      jobIds:       (j.sourceJobIds || []).join(', '),
    };

    const row = ws.addRow(colDefs.map(c => valueMap[c.key]));

    // Company hyperlink
    const companyUrl = j.companyUrl || ev.company_url || '';
    setCellHyperlink(row.getCell(companyCol), companyUrl, j.company || '');

    // Job Title hyperlink
    const jobUrl = j.applyUrl || j.url || '';
    setCellHyperlink(row.getCell(titleCol), jobUrl, j.title || '');

    // Where Found hyperlink
    const foundUrl = j.url || j.applyUrl || '';
    const rootUrl = extractRootDomainUrl(foundUrl);
    const foundName = friendlyDomainName(foundUrl, j.company);
    setCellHyperlink(row.getCell(whereFoundCol), rootUrl, foundName);
  }

  return await workbook.xlsx.writeBuffer();
}

Actor.main(async () => {
  const input = (await Actor.getInput()) || {};
  const config = await loadConfig(input);

  const scoringCfg = config?.scoring || {};
  if (scoringCfg.enabled === false) {
    log.warning('Scoring disabled by config.scoring.enabled=false');
    return;
  }

  const apiKey = process.env.OPENAI_API_KEY;
  if (!apiKey) throw new Error('Missing OPENAI_API_KEY env var on 03_score_jobs actor.');

  const runId = input.runId || makeRunId();
  const kvStoreName = input.kvStoreName || config.kvStoreName || 'job-pipeline-v3';
  const datasetPrefix = input.datasetPrefix || config.datasetPrefix || 'jobsearch-v3';

  const mergedDatasetName = input.mergedDatasetName || datasetName(datasetPrefix, 'merged', runId);
  const scoredDatasetName = input.scoredDatasetName || datasetName(datasetPrefix, 'scored', runId);
  const acceptedDatasetName = input.acceptedDatasetName || datasetName(datasetPrefix, 'accepted', runId);

  const kv = await Actor.openKeyValueStore(kvStoreName);
  const mergedDataset = await Actor.openDataset(mergedDatasetName);
  const scoredDataset = await Actor.openDataset(scoredDatasetName);
  const acceptedDataset = await Actor.openDataset(acceptedDatasetName);

  // Read run number from pipeline metadata (set by orchestrator)
  let runNumber = null;
  try {
    const runMeta = await kv.getValue('run_meta.json');
    if (runMeta?.runNumber) runNumber = runMeta.runNumber;
  } catch { /* ignore — run number is optional metadata */ }

  const startedAt = nowIso();

  const model = String(scoringCfg.model || 'gpt-4o-mini');
  const threshold = Number(scoringCfg.threshold ?? 70) || 70;
  const gateOnLocation = !!scoringCfg.gateOnLocation;
  // Debug: capture full LLM prompt+response for specific job IDs (array in config.scoring.debugJobIds)
  const debugJobIds = new Set(Array.isArray(scoringCfg.debugJobIds) ? scoringCfg.debugJobIds : []);
  if (debugJobIds.size) log.info(`LLM debug enabled for ${debugJobIds.size} job IDs: ${[...debugJobIds].join(', ')}`);
  const debugLlmResults = [];  // collected during scoring, written to KV at end
  const rawConcurrency = Number(scoringCfg.concurrency ?? 4) || 4;
  // Reasoning models (GPT-5+) hold connections open much longer (10-30s vs 1-2s).
  // Cap concurrency at 4 to reduce ECONNRESET / socket errors.
  const concurrency = model.startsWith('gpt-5') ? Math.min(rawConcurrency, 4) : rawConcurrency;
  const maxDescChars = Number(scoringCfg.maxDescriptionChars ?? 12000) || 12000;
  const rubricUrl = String(scoringCfg.rubricUrl || '');

  const rubricText = await loadRubricText(rubricUrl);
  const currentRubricVersion = extractRubricVersion(rubricText);

  log.info(`Scoring merged dataset "${mergedDatasetName}" -> scored="${scoredDatasetName}", accepted="${acceptedDatasetName}"`);
  log.info(`Model=${model}, threshold=${threshold}, concurrency=${concurrency}, gateOnLocation=${gateOnLocation}`);
  log.info(`Rubric version: ${currentRubricVersion || '(unknown)'}, Scoring format: ${SCORING_FORMAT_VERSION}`);

  const mergedJobs = [];
  const pageSize = 250;
  for (let offset = 0; ; offset += pageSize) {
    const { items } = await mergedDataset.getData({ offset, limit: pageSize });
    if (!items || items.length === 0) break;
    mergedJobs.push(...items);
  }
  log.info(`Loaded ${mergedJobs.length} merged jobs.`);

  // --- Score cache: reuse evaluations from previous run if rubric + format unchanged ---
  let cacheMap = null;  // Map<key, scoredJob>
  let cacheHits = 0;
  const scoreCache = await kv.getValue('score_cache.json');
  const cacheFormatOk = scoreCache?.scoringFormatVersion === SCORING_FORMAT_VERSION;
  const cacheRubricOk = scoreCache?.rubricVersion === currentRubricVersion;

  if (cacheFormatOk && cacheRubricOk && scoreCache?.scoredDatasetName) {
    try {
      const prevDataset = await Actor.openDataset(scoreCache.scoredDatasetName);
      const prevJobs = [];
      for (let offset = 0; ; offset += 250) {
        const { items } = await prevDataset.getData({ offset, limit: 250 });
        if (!items || items.length === 0) break;
        prevJobs.push(...items);
      }
      // Build multi-key cache map for robust matching
      cacheMap = new Map();
      for (const pj of prevJobs) {
        if (!pj.evaluation) continue;
        const keys = [];
        if (pj.key) keys.push(pj.key);
        if (pj.url) keys.push(`url:${pj.url}`);
        if (pj.applyUrl && pj.applyUrl !== pj.url) keys.push(`url:${pj.applyUrl}`);
        const c = normalizeCompany(pj.company);
        const t = normalizeTitle(pj.title);
        if (c && t) keys.push(`ct:${c}|${t}`);
        for (const k of keys) cacheMap.set(k, pj);
      }
      log.info(`Score cache loaded: ${prevJobs.length} jobs, ${cacheMap.size} keys (format ${SCORING_FORMAT_VERSION}, rubric ${currentRubricVersion})`);
    } catch (err) {
      log.warning(`Failed to load score cache: ${err?.message || err}. Scoring all jobs fresh.`);
      cacheMap = null;
    }
  } else {
    const reasons = [];
    if (!scoreCache) reasons.push('no cache exists');
    if (scoreCache && !cacheFormatOk) reasons.push(`format changed (${scoreCache.scoringFormatVersion} → ${SCORING_FORMAT_VERSION})`);
    if (scoreCache && !cacheRubricOk) reasons.push(`rubric changed (${scoreCache.rubricVersion} → ${currentRubricVersion})`);
    if (scoreCache && !scoreCache.scoredDatasetName) reasons.push('missing dataset name');
    log.info(`Score cache invalidated: ${reasons.join('; ')}. Scoring all jobs fresh.`);
  }

  // --- Blocklist: manually rejected jobs that should never appear in accepted ---
  const blocklist = (await kv.getValue('blocklist.json')) || {};
  const blockedCompanies = Object.keys(blocklist._companies || {}).map(n => n.toLowerCase());
  delete blocklist._companies;
  const blocklistIds = new Set(Object.keys(blocklist));
  if (blocklistIds.size > 0 || blockedCompanies.length > 0) {
    log.info(`Loaded blocklist with ${blocklistIds.size} job IDs and ${blockedCompanies.length} blocked companies.`);
  }

  const openAiStats = {
    calls: 0,
    retries: 0,
    rateLimit429: 0,
    serverErrors: 0,
    maxRetryAfterMs: 0,
    hardFailures: 0,
    inputTokens: 0,
    outputTokens: 0,
  };

  // --- Pre-LLM enrichment: detect position type and remote status ---
  for (const job of mergedJobs) {
    job.positionType = detectPositionType(job);
    // Enrich employmentType so the LLM sees it in the prompt
    if (!job.employmentType && job.positionType !== 'Full-Time') {
      job.employmentType = job.positionType;
    }
    // enrichRemoteStatus absorbed into collector's normalizeJob pass
  }

  // --- Pre-compute location for all jobs and skip location_ok = "no" ---
  let locationSkipped = 0;
  let llmLocationResolved = 0;
  const preLocationMap = new Map(); // idx → location_ok
  for (let i = 0; i < mergedJobs.length; i++) {
    preLocationMap.set(i, computeLocationOk(mergedJobs[i]));
  }
  const noLocationCount = [...preLocationMap.values()].filter(v => v === 'no').length;
  const unknownLocationCount = [...preLocationMap.values()].filter(v => v === 'unknown').length;
  log.info(`Location pre-filter: ${noLocationCount} jobs are location_ok=no (skipped). ${unknownLocationCount} jobs are location_ok=unknown (sent to LLM for location determination).`);

  // --- Pre-compute title-based disqualifiers (skip LLM, save cost) ---
  // Kyle is entry-level; any title implying seniority or management is not a fit.
  const TITLE_DQ_PATTERNS = [
    { re: /\bManager\b/i,                    label: 'Manager' },
    { re: /\b(Senior|Sr\.?)\b/i,             label: 'Senior/Sr.' },
    { re: /\bLead\b/i,                       label: 'Lead' },
    { re: /\bPrincipal\b/i,                  label: 'Principal' },
    { re: /\bDirector\b/i,                   label: 'Director' },
    { re: /\b(Head of|Head,)\b/i,            label: 'Head of' },
    { re: /\b(VP|Vice President)\b/i,        label: 'VP' },
    { re: /\bChief\b/i,                      label: 'Chief' },
    { re: /\bStaff\b/i,                      label: 'Staff' },
  ];
  let titleSkipped = 0;
  let blocklistedCount = 0;

  function titleDisqualifyReason(title) {
    for (const { re, label } of TITLE_DQ_PATTERNS) {
      if (re.test(title)) return `Title contains "${label}" — too senior for Kyle's current experience level.`;
    }
    return null;
  }

  const preTitleDqCount = mergedJobs.filter(j => titleDisqualifyReason(j.title || '')).length;
  log.info(`Title pre-filter: ${preTitleDqCount} jobs have disqualifying seniority titles and will be skipped.`);

  // --- Pre-compute date_validthrough expiration (skip LLM for expired listings) ---
  const nowMs = Date.now();
  let expiredSkipped = 0;
  const preExpiredSet = new Set(); // indices of expired jobs
  for (let i = 0; i < mergedJobs.length; i++) {
    const dvt = mergedJobs[i].raw?.date_validthrough || '';
    if (!dvt) continue;
    try {
      const expiry = new Date(dvt);
      if (!isNaN(expiry.getTime()) && expiry.getTime() < nowMs) {
        preExpiredSet.add(i);
      }
    } catch { /* ignore bad dates */ }
  }
  log.info(`Expired pre-filter: ${preExpiredSet.size} jobs have date_validthrough in the past and will be skipped.`);

  // --- Snapshot key fields before enrichment (for enrichmentLog tracking) ---
  const preEnrichSnapshot = new Map();
  for (let i = 0; i < mergedJobs.length; i++) {
    const j = mergedJobs[i];
    preEnrichSnapshot.set(i, {
      workMode: j.workMode || '',
      salary: j.salary || '',
      employmentType: j.employmentType || '',
      location: j.location || '',
      description: (j.description || '').length,
      url: j.url || '',
      applyUrl: j.applyUrl || '',
    });
  }

  // --- Built In description enrichment (fetch missing descriptions for pre-filter survivors) ---
  const builtInEnrichment = { total: 0, cached: 0, fetched: 0, failed: 0, failedUrls: [] };

  const builtInCache = new DetailCache({ kvStore: kv, kvKey: 'builtin_desc_cache.json', ttlDays: BUILTIN_DESC_CACHE_TTL_DAYS, label: 'Built In desc' });
  await builtInCache.load();

  // Identify Built In jobs that pass all pre-filters and have empty descriptions
  const builtInJobsToEnrich = [];
  for (let i = 0; i < mergedJobs.length; i++) {
    const job = mergedJobs[i];
    if (preLocationMap.get(i) === 'no') continue;
    if (titleDisqualifyReason(job.title || '')) continue;
    if (preExpiredSet.has(i)) continue;
    const sources = Array.isArray(job.sources) ? job.sources : [job.source].filter(Boolean);
    if (!sources.some(s => String(s).startsWith('builtin_'))) continue;
    if (job.description && job.description.trim().length > 0) continue;
    builtInJobsToEnrich.push({ job, index: i });
  }

  builtInEnrichment.total = builtInJobsToEnrich.length;
  if (builtInJobsToEnrich.length > 0) {
    log.info(`Built In description enrichment: ${builtInJobsToEnrich.length} jobs need descriptions.`);

    for (const { job, index } of builtInJobsToEnrich) {
      const url = job.url || job.applyUrl;
      if (!url) { builtInEnrichment.failed++; continue; }

      // Check cache first (re-fetch if missing structured data fields added in v10)
      const cached = builtInCache.get(url, entry => entry?.description && ('applicantCountries' in entry || 'jobLocationAddress' in entry));
      if (cached) {
        job.description = cached.description;
        applyBuiltInStructuredData(job, cached, index, preLocationMap);
        builtInEnrichment.cached++;
        continue;
      }

      // Fetch from Built In
      try {
        const result = await fetchBuiltInDescription(url);
        if (result?.description) {
          job.description = result.description;
          builtInCache.set(url, {
            description: result.description,
            employmentType: result.employmentType || '',
            datePosted: result.datePosted || '',
            salary: result.salary || '',
            validThrough: result.validThrough || '',
            jobLocationType: result.jobLocationType || '',
            applicantCountries: result.applicantCountries || [],
            jobLocationAddress: result.jobLocationAddress || {},
          });
          builtInEnrichment.fetched++;
          applyBuiltInStructuredData(job, result, index, preLocationMap);
          log.info(`Built In enriched: "${job.title}" at ${job.company} (${result.description.length} chars, country=${result.jobLocationAddress?.addressCountry || '?'})`);
        } else {
          builtInEnrichment.failed++;
          builtInEnrichment.failedUrls.push(url);
          log.warning(`Built In: no description found on page for "${job.title}" at ${job.company}`);
        }
      } catch (err) {
        builtInEnrichment.failed++;
        builtInEnrichment.failedUrls.push(url);
        log.warning(`Built In fetch failed for "${job.title}" at ${job.company}: ${err?.message || err}`);
      }

      // Delay between fetches to avoid anti-bot measures
      await sleep(BUILTIN_FETCH_DELAY_MS);
    }

    // Add jobs discovered as expired during enrichment to the preExpiredSet
    let biExpiredCount = 0;
    for (const { job, index } of builtInJobsToEnrich) {
      if (job._expired && !preExpiredSet.has(index)) {
        preExpiredSet.add(index);
        biExpiredCount++;
      }
    }
    if (biExpiredCount > 0) log.info(`Built In enrichment: ${biExpiredCount} additional expired jobs found via validThrough.`);

    log.info(`Built In enrichment done: ${builtInEnrichment.fetched} fetched, ${builtInEnrichment.cached} cached, ${builtInEnrichment.failed} failed.`);
  }

  await builtInCache.save();

  // --- LinkedIn detail enrichment (fetch workMode for LinkedIn jobs missing it) ---
  // Uses bestscrapers/linkedin-job-details-scraper to get remote_allow, job_type, salary
  // that the Fantastic scraper doesn't reliably provide.
  const linkedinEnrichment = { total: 0, cached: 0, fetched: 0, failed: 0, remoteFound: 0 };
  const apifyToken = process.env.APIFY_TOKEN;

  const linkedinCache = new DetailCache({ kvStore: kv, kvKey: 'linkedin_detail_cache.json', ttlDays: LINKEDIN_DETAIL_CACHE_TTL_DAYS, label: 'LinkedIn detail' });
  await linkedinCache.load();

  // Identify LinkedIn jobs that pass all pre-filters and have blank workMode
  const linkedinJobsToEnrich = [];
  for (let i = 0; i < mergedJobs.length; i++) {
    const job = mergedJobs[i];
    if (preLocationMap.get(i) === 'no') continue;
    if (titleDisqualifyReason(job.title || '')) continue;
    if (preExpiredSet.has(i)) continue;
    if (job.workMode && job.employmentType) continue; // already has both — no need
    const sources = Array.isArray(job.sources) ? job.sources : [job.source].filter(Boolean);
    if (!sources.some(s => /^(linkedin_|fantastic_)/.test(String(s)))) continue;
    const linkedinJobId = extractLinkedInJobId(job);
    if (!linkedinJobId) continue;
    linkedinJobsToEnrich.push({ job, index: i, linkedinJobId });
  }

  linkedinEnrichment.total = linkedinJobsToEnrich.length;
  if (linkedinJobsToEnrich.length > 0 && apifyToken) {
    log.info(`LinkedIn detail enrichment: ${linkedinJobsToEnrich.length} jobs need workMode lookup.`);

    for (const { job, index, linkedinJobId } of linkedinJobsToEnrich) {
      // Check cache first
      const cached = linkedinCache.get(linkedinJobId);
      if (cached) {
        if (cached.remote_allow) {
          job.workMode = 'Remote';
          preLocationMap.set(index, computeLocationOk(job));
          linkedinEnrichment.remoteFound++;
        }
        if (cached.job_type && !job.employmentType) {
          job.employmentType = cached.job_type;
          job.positionType = detectPositionType(job); // re-detect after employmentType update
        }
        if (cached.salary && !job.salary) {
          job.salary = cached.salary;
        }
        linkedinEnrichment.cached++;
        continue;
      }

      // Call the bestscrapers actor
      try {
        const detail = await fetchLinkedInDetail(linkedinJobId, apifyToken);
        if (detail) {
          linkedinCache.set(linkedinJobId, {
            remote_allow: !!detail.remote_allow,
            job_type: detail.job_type || '',
            salary: detail.salary_display || '',
          });
          if (detail.remote_allow) {
            job.workMode = 'Remote';
            preLocationMap.set(index, computeLocationOk(job));
            linkedinEnrichment.remoteFound++;
          }
          if (detail.job_type && !job.employmentType) {
            job.employmentType = detail.job_type;
            job.positionType = detectPositionType(job); // re-detect after employmentType update
          }
          if (detail.salary_display && !job.salary) {
            job.salary = detail.salary_display;
          }
          linkedinEnrichment.fetched++;
          log.info(`LinkedIn enriched: "${job.title}" at ${job.company} → remote_allow=${detail.remote_allow}, job_type=${detail.job_type || '(none)'}`);
        } else {
          linkedinEnrichment.failed++;
          log.warning(`LinkedIn detail: no data returned for "${job.title}" at ${job.company} (job ${linkedinJobId})`);
        }
      } catch (err) {
        linkedinEnrichment.failed++;
        log.warning(`LinkedIn detail failed for "${job.title}" at ${job.company} (job ${linkedinJobId}): ${err?.message || err}`);
      }

      await sleep(LINKEDIN_DETAIL_DELAY_MS);
    }

    log.info(`LinkedIn detail enrichment done: ${linkedinEnrichment.fetched} fetched, ${linkedinEnrichment.cached} cached, ${linkedinEnrichment.failed} failed, ${linkedinEnrichment.remoteFound} found to be Remote.`);
  } else if (linkedinJobsToEnrich.length > 0 && !apifyToken) {
    log.warning(`LinkedIn detail enrichment skipped: APIFY_TOKEN not available (${linkedinJobsToEnrich.length} jobs would benefit).`);
  }

  await linkedinCache.save();

  // --- Compute enrichmentLog for each job (diff pre vs post enrichment) ---
  for (let i = 0; i < mergedJobs.length; i++) {
    const pre = preEnrichSnapshot.get(i);
    if (!pre) continue;
    const j = mergedJobs[i];
    const changes = [];
    if ((j.workMode || '') !== pre.workMode) changes.push(`workMode: "${pre.workMode}" -> "${j.workMode || ''}"`);
    if ((j.salary || '') !== pre.salary) changes.push(`salary: "${pre.salary}" -> "${j.salary || ''}"`);
    if ((j.employmentType || '') !== pre.employmentType) changes.push(`employmentType: "${pre.employmentType}" -> "${j.employmentType || ''}"`);
    if ((j.location || '') !== pre.location) changes.push(`location: "${pre.location}" -> "${j.location || ''}"`);
    const newDescLen = (j.description || '').length;
    if (newDescLen !== pre.description) changes.push(`description: ${pre.description} chars -> ${newDescLen} chars`);
    if ((j.url || '') !== pre.url) changes.push(`url: "${pre.url}" -> "${j.url || ''}"`);
    if ((j.applyUrl || '') !== pre.applyUrl) changes.push(`applyUrl: "${pre.applyUrl}" -> "${j.applyUrl || ''}"`);
    if (changes.length > 0) {
      j.enrichmentLog = changes;
    }
  }

  // --- Scoring helper (reused for initial pass and re-score passes) ---
  async function scoreOneJob(job, idx) {
    // --- Pre-filter gates run BEFORE cache check ---
    // These gates override cached scores from previous runs, ensuring
    // that newly-added filters (e.g. Senior/Lead title block) apply
    // retroactively to jobs that were scored before the filter existed.

    // 1) Title seniority gate — deterministic reject, overrides cache
    const titleDqReason = titleDisqualifyReason(job.title || '');
    if (titleDqReason) {
      titleSkipped += 1;
      return {
        ...job,
        filterReason: `title_skip: ${titleDqReason}`,
        evaluation: {
          accept: false,
          accepted: false,
          score: 0,
          confidence: 1.0,
          location_ok: preLocationMap.get(idx) || computeLocationOk(job),
          reason_short: titleDqReason,
          reasons: [titleDqReason],
          red_flags: ['Title disqualified (pre-filter).'],
          tags: [],
          salary_extracted: '',
          company_url: '',
          role: [],
        },
        scoredAt: nowIso(),
      };
    }

    // 2) Position type gate — reject Volunteer and Internship positions
    const pt = job.positionType || detectPositionType(job);
    if (pt === 'Volunteer' || pt === 'Internship') {
      titleSkipped += 1; // reuse titleSkipped counter for simplicity
      return {
        ...job,
        filterReason: `position_type_skip: ${pt}`,
        evaluation: {
          accept: false,
          accepted: false,
          score: 0,
          confidence: 1.0,
          location_ok: preLocationMap.get(idx) || computeLocationOk(job),
          reason_short: `${pt} position — not a paid full-time role.`,
          reasons: [`${pt} position filtered out.`],
          red_flags: [`${pt} position (pre-filter).`],
          tags: [],
          salary_extracted: '',
          company_url: '',
          role: [],
        },
        scoredAt: nowIso(),
      };
    }

    // 3) Location gate — deterministic reject for confirmed non-commutable.
    //    'unknown' jobs (ambiguous city, commutable: null) proceed to LLM for
    //    location determination — the LLM reads the description for signals like
    //    salary currency, "right to work in" phrases, benefits, etc.
    let locationOk = preLocationMap.get(idx) || computeLocationOk(job);
    if (locationOk === 'no') {
      locationSkipped += 1;
      return {
        ...job,
        filterReason: `location_skip: ${job.workMode || 'unknown'} | ${job.location || 'unknown'} | commutable=${job.commutable}`,
        evaluation: {
          accept: false,
          accepted: false,
          score: 0,
          confidence: 1.0,
          location_ok: locationOk,
          reason_short: 'Location not commutable to Lexington, MA and not remote.',
          reasons: ['Location outside commutable zone.'],
          red_flags: ['Location disqualified (pre-filter).'],
          tags: [],
          salary_extracted: '',
          company_url: '',
          role: [],
        },
        scoredAt: nowIso(),
      };
    }

    // 3) Expired listing gate — deterministic reject, overrides cache
    if (preExpiredSet.has(idx)) {
      expiredSkipped += 1;
      return {
        ...job,
        filterReason: 'expired: date_validthrough in the past',
        evaluation: {
          accept: false,
          accepted: false,
          score: 0,
          confidence: 1.0,
          location_ok: locationOk,
          reason_short: 'Listing has expired (date_validthrough in the past).',
          reasons: ['Expired listing.'],
          red_flags: ['Expired listing (pre-filter).'],
          tags: [],
          salary_extracted: '',
          company_url: '',
          role: [],
        },
        scoredAt: nowIso(),
      };
    }

    // --- Blocklist check (before cache, so blocklisted jobs are always excluded) ---
    const jobIdList = job.sourceJobIds || [];
    const isBlocklisted = jobIdList.some(id => blocklistIds.has(id));
    const isCompanyBlocked = blockedCompanies.length > 0 &&
      blockedCompanies.includes((job.company || '').toLowerCase());
    if (isBlocklisted || isCompanyBlocked) {
      blocklistedCount++;
      const reason = isCompanyBlocked
        ? `Company "${job.company}" is on the blocklist.`
        : 'Job is on the blocklist.';
      return {
        ...job,
        filterReason: `blocklisted: ${isCompanyBlocked ? 'company=' + job.company : jobIdList.join(',')}`,
        evaluation: {
          accept: false,
          accepted: false,
          score: 0,
          confidence: 1.0,
          location_ok: locationOk,
          reason_short: reason,
          reasons: [isCompanyBlocked ? `Company blocklisted: ${job.company}` : 'Manually blocklisted.'],
          red_flags: ['Blocklisted.'],
          tags: [],
          salary_extracted: '',
          company_url: '',
          role: [],
          blocklisted: true,
        },
        scoredAt: nowIso(),
      };
    }

    // --- Cache check (only after all gates pass) ---
    const cached = lookupCache(cacheMap, job);
    if (cached?.evaluation) {
      cacheHits++;
      return {
        ...job,
        filterReason: `cache_hit: score=${cached.evaluation.score}, accept=${cached.evaluation.accept}`,
        evaluation: { ...cached.evaluation },
        scoredAt: cached.scoredAt || nowIso(),
        cachedFrom: 'previous_run',
      };
    }

    // --- LLM scoring (cache miss, all gates passed) ---

    const jobForPrompt = {
      title: job.title || '',
      company: job.company || '',
      location: job.location || '',
      workMode: job.workMode || '',
      url: job.url || '',
      applyUrl: job.applyUrl || '',
      sources: job.sources || [job.source].filter(Boolean),
      postedAt: job.postedAt || '',
      salary: job.salary || '',
      employmentType: job.employmentType || '',
      description: truncate(job.description || '', maxDescChars),
    };

    // --- Build LLM prompt ---
    // For unknown-location jobs, add extra instructions for location determination.
    const locationDeterminationPrompt = locationOk === 'unknown'
      ? '\n\nIMPORTANT — LOCATION DETERMINATION: The geographic location for this job is ambiguous ' +
        '(just a city name with no country, or blank). Please carefully examine the job description ' +
        'for clues about the country:\n' +
        '- Salary currency (PLN, GBP, CAD, EUR, etc. = non-US; USD/$ = likely US)\n' +
        '- "Right to work in [country]" or "eligible to work in [country]" phrases\n' +
        '- Benefits: NHS = UK, RRSP = Canada, 401(k) = US, "umowa o pracę" = Poland\n' +
        '- "Based in [city, country]" or "office in [location]" phrases\n' +
        '- Language requirements (e.g., Polish, German) suggesting a specific country\n' +
        '- Company headquarters location mentioned in description\n' +
        'Return is_us (true if US, false if non-US, null if truly unknown) and ' +
        'location_country (ISO3 code like "GBR", "POL", "CAN" if you can determine it, or "" if unknown).\n' +
        'Also determine if the job is within commuting distance (~45 min drive) of Lexington, Massachusetts — return is_commutable_to_lexington_ma (true/false/null).'
      : '';

    const messages = [
      {
        role: 'system',
        content:
          rubricText +
          '\n\n' +
          'Return ONLY valid JSON, no markdown. Ensure fields: accept, score, confidence, reason_short, reasons, red_flags, tags, salary_extracted, company_url, role, location, work_mode, is_us, location_country.\n' +
          'location: Your best determination of the job\'s geographic location (e.g., "Boston MA", "JPN", "USA"). Use the provided location as a starting point, but refine based on the description.\n' +
          'work_mode: One of "Remote", "Hybrid", "On-Site", or "". Refine based on the description.\n' +
          'salary_extracted: If the job description mentions any compensation info (salary range, hourly rate, annual pay, "up to $X", "$X-$Y/yr", OTE, etc.), extract it as a string (e.g., "$90,000-$120,000/yr", "$45/hr"). Look for explicit ranges, "base salary", "total compensation", "pay band". Return "" if no salary information is found.\n' +
          'is_us: true if this job is located in the United States, false if not, null if truly unknown.\n' +
          'location_country: ISO3 country code (e.g., "GBR", "POL", "USA") if determinable, or "" if unknown.' +
          locationDeterminationPrompt,
      },
      {
        role: 'user',
        content: JSON.stringify(jobForPrompt, null, 2),
      },
    ];

    // LLM call — errors propagate to processWithRetries for retry handling
    const evaluation = await callOpenAIJson({ apiKey, model, messages, stats: openAiStats });

    // Debug: capture full prompt+response for specific jobs
    const isDebugJob = jobIdList.some(id => debugJobIds.has(id));
    if (isDebugJob) {
      debugLlmResults.push({
        jobIds: jobIdList,
        title: job.title,
        company: job.company,
        prompt: messages,
        response: evaluation,
      });
      log.info(`[DEBUG] Captured LLM prompt+response for ${job.title} (${jobIdList.join(', ')})`);
    }

    const score = toInt(evaluation.score ?? evaluation.Score ?? 0, 0);
    const accept = !!(evaluation.accept ?? evaluation.Accept);

    // Use LLM location/workMode only when the collector left them empty.
    // The collector's normalizeLocationFields() is authoritative; LLM values
    // were overwriting normalized ISO3 codes with ad-hoc strings like
    // "Cambridge UK" or "Remote" (causing "Remote | Remote" in accepted.xlsx).
    const llmLocation = evaluation.location;
    const llmWorkMode = evaluation.work_mode;
    if (!job.location && llmLocation && typeof llmLocation === 'string' && llmLocation.trim()) {
      job.location = llmLocation.trim();
    }
    if (!job.workMode && llmWorkMode && typeof llmWorkMode === 'string' && llmWorkMode.trim()) {
      let wm = llmWorkMode.trim();
      if (wm === 'RemoteOK' || wm === 'RemoteOnly') wm = 'Remote';
      job.workMode = wm;
    }

    // --- Tier 3: LLM location resolution for ambiguous-location jobs ---
    // Use LLM's is_us / location_country / is_commutable_to_lexington_ma to
    // resolve ambiguous locations and re-evaluate the location gate.
    if (locationOk === 'unknown') {
      const llmIsUS = evaluation.is_us;
      const llmCountry = evaluation.location_country;
      const llmCommutable = evaluation.is_commutable_to_lexington_ma;

      if (llmIsUS === false) {
        // LLM determined this is NOT in the US → foreign, not commutable
        job.commutable = false;
        if (llmCountry && typeof llmCountry === 'string' && /^[A-Z]{3}$/.test(llmCountry) && llmCountry !== 'USA') {
          job.location = llmCountry;
        }
        llmLocationResolved += 1;
      } else if (llmIsUS === true) {
        // LLM determined this IS in the US
        if (llmCommutable === true) {
          // Commutable to Lexington, MA — accept
          job.commutable = true;
          if (llmLocation && typeof llmLocation === 'string' && llmLocation.trim()) {
            job.location = llmLocation.trim();
          }
        } else {
          // US but not commutable — reject unless remote
          job.commutable = false;
          if (llmLocation && typeof llmLocation === 'string' && llmLocation.trim()) {
            job.location = llmLocation.trim();
          }
        }
        llmLocationResolved += 1;
      }
      // else: LLM couldn't determine (is_us: null) → stays commutable: null

      // Re-evaluate location gate with updated info
      locationOk = computeLocationOk(job);
      preLocationMap.set(idx, locationOk);

      // If still unknown or no after LLM determination, reject
      if (locationOk === 'no' || locationOk === 'unknown') {
        locationSkipped += 1;
        return {
          ...job,
          filterReason: `location_skip_post_llm: ${job.workMode || 'unknown'} | ${job.location || 'unknown'} | is_us=${evaluation.is_us} | country=${evaluation.location_country}`,
          evaluation: {
            ...evaluation,
            accept: false,
            accepted: false,
            score: 0,
            confidence: 1.0,
            location_ok: locationOk,
            reason_short: locationOk === 'no'
              ? 'Location not commutable to Lexington, MA and not remote (determined by LLM).'
              : 'Location could not be confirmed as commutable to Lexington, MA.',
            reasons: [locationOk === 'no'
              ? `Location outside commutable zone (LLM determined: ${llmCountry || llmIsUS === false ? 'non-US' : 'unknown'}).`
              : 'Location ambiguous even after LLM analysis — assumed non-commutable.'],
            red_flags: ['Location disqualified (post-LLM).'],
            tags: evaluation.tags || [],
            salary_extracted: evaluation.salary_extracted || '',
            company_url: evaluation.company_url || '',
            role: evaluation.role || [],
          },
          scoredAt: nowIso(),
        };
      }
    }

    const accepted =
      accept &&
      score >= threshold &&
      (!gateOnLocation || locationOk === 'yes');

    const filterDesc = accepted
      ? `accepted: score=${score}`
      : `llm_rejected: score=${score}, accept=${accept}${score < threshold ? ', below_threshold' : ''}`;

    return {
      ...job,
      filterReason: filterDesc,
      evaluation: {
        ...evaluation,
        score,
        accept,
        accepted,
        location_ok: locationOk,
      },
      scoredAt: nowIso(),
    };
  }

  // --- Scoring with automatic retries and retry passes ---
  // scoreOneJob returns results for deterministic filters (cache, location, title, expired)
  // and throws for LLM errors — processWithRetries handles per-call retries and retry passes.
  const { results: rawResults, failures: scoringFailures, stats: retryStats } = await processWithRetries(
    mergedJobs,
    scoreOneJob,
    {
      concurrency,
      retries: 8,
      baseMs: 800,
      maxMs: 30000,
      retryPasses: 3,
      retryCooldownMs: 15000,
      retryConcurrency: Math.min(concurrency, 4),
      label: 'LLM scoring',
    },
  );
  openAiStats.retries = retryStats.retries;

  // Convert remaining failures to error-marker results
  const results = [...rawResults];
  for (const { index, error } of scoringFailures) {
    const job = mergedJobs[index];
    const locationOk = preLocationMap.get(index) || computeLocationOk(job);
    openAiStats.hardFailures += 1;
    log.error(`Scoring failed for idx=${index} title="${job.title}": ${error?.message || error}`);
    results[index] = {
      ...job,
      filterReason: `scoring_error: ${error?.message || error}`,
      evaluation: {
        accept: false,
        accepted: false,
        score: 0,
        confidence: 0,
        location_ok: locationOk,
        reason_short: 'Scoring error',
        reasons: [],
        red_flags: [String(error?.message || error)],
        tags: [],
      },
      scoredAt: nowIso(),
      scoringError: String(error?.stack || error),
    };
  }

  const totalSkipped = locationSkipped + titleSkipped + expiredSkipped;
  log.info(`Scoring done. ${cacheHits} cache hits, ${locationSkipped} skipped (bad location, incl. ${llmLocationResolved} resolved by LLM), ${titleSkipped} skipped (bad title), ${expiredSkipped} skipped (expired), ${mergedJobs.length - totalSkipped - cacheHits} sent to LLM. ${scoringFailures.length} hard failures.`);

  // --- Post-scoring filters ---

  // Note: Senior/Lead/Manager/etc. filtering now happens in the pre-LLM title filter.
  // These jobs never reach the LLM, so there is no post-LLM senior filter needed.
  const seniorTier3Filtered = 0;

  // 2) Check LinkedIn URLs for "No longer accepting applications"
  //    Only check accepted jobs with LinkedIn job URLs — small number of fetches.
  //    "Closed" results are cached across runs to avoid re-pinging LinkedIn for dead jobs.
  //    "Open" results are NOT cached — we re-check each run since jobs can close any time.
  const linkedinClosedCache = new Set(scoreCache?.linkedinClosedUrls || []);
  let linkedinClosedCount = 0;
  let linkedinClosedCacheHits = 0;

  async function isLinkedInJobClosed(jobUrl) {
    // Extract numeric job ID from URL for the guest API endpoint.
    // Guest API returns ~17KB vs ~200KB for the full page — much faster.
    // LinkedIn URLs: /jobs/view/{slug}-{id} or /jobs/view/{id}
    const idMatch = String(jobUrl).match(/(\d{5,})(?:[/?#]|$)/);
    const guestApiUrl = idMatch
      ? `https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/${idMatch[1]}`
      : jobUrl; // fallback to full URL if ID extraction fails

    try {
      const res = await fetch(guestApiUrl, {
        headers: { 'User-Agent': 'Mozilla/5.0 (compatible; job-pipeline/1.0)' },
        signal: AbortSignal.timeout(10000),
      });
      const html = await res.text();

      // Primary check: "No longer accepting applications" (works for fully-closed jobs)
      if (html.includes('No longer accepting applications')) return true;

      // Secondary check: absence of Apply button (catches jobs closed only for
      // logged-in users, where LinkedIn hides the CTA but doesn't show the
      // "No longer accepting" text to anonymous fetches).
      // Verify the page loaded a valid job first (has a title) to avoid
      // false positives on error pages.
      const hasJobTitle = html.includes('topcard__title');
      const hasApplyButton = html.includes('top-card-layout__cta--primary');
      if (hasJobTitle && !hasApplyButton) return true;

      return false;
    } catch {
      return false; // on error, assume still open (don't penalize)
    }
  }

  function markJobClosed(r) {
    linkedinClosedCount++;
    const jobUrl = r.applyUrl || r.url || '';
    linkedinClosedCache.add(jobUrl);
    r.evaluation.accepted = false;
    r.evaluation.accept = false;
    r.evaluation.red_flags = [...(r.evaluation.red_flags || []), 'LinkedIn: No longer accepting applications'];
    r.evaluation.reason_short = `${r.evaluation.reason_short || ''} [CLOSED]`.trim();
  }

  const linkedinAccepted = results.filter(
    (r) => r?.evaluation?.accepted && /linkedin\.com\/jobs/i.test(r.applyUrl || r.url || '')
  );

  if (linkedinAccepted.length > 0) {
    // Mark jobs known-closed from cache (no HTTP fetch needed)
    const toCheck = [];
    for (const r of linkedinAccepted) {
      const jobUrl = r.applyUrl || r.url || '';
      if (linkedinClosedCache.has(jobUrl)) {
        linkedinClosedCacheHits++;
        markJobClosed(r);
      } else {
        toCheck.push(r);
      }
    }

    if (linkedinClosedCacheHits > 0) {
      log.info(`LinkedIn closed cache: ${linkedinClosedCacheHits} known-closed jobs skipped.`);
    }

    if (toCheck.length > 0) {
      log.info(`Checking ${toCheck.length} accepted LinkedIn jobs for closed listings...`);

      // Check in batches of 10 to avoid overwhelming LinkedIn
      const LI_BATCH = 10;
      for (let i = 0; i < toCheck.length; i += LI_BATCH) {
        const batch = toCheck.slice(i, i + LI_BATCH);
        const checks = await Promise.all(
          batch.map((r) => isLinkedInJobClosed(r.applyUrl || r.url || ''))
        );
        for (let k = 0; k < batch.length; k++) {
          if (checks[k]) markJobClosed(batch[k]);
        }
        // Small delay between batches to be polite
        if (i + LI_BATCH < toCheck.length) await new Promise((r) => setTimeout(r, 1000));
      }
    }
    log.info(`LinkedIn check: ${linkedinClosedCount} closed (${linkedinClosedCacheHits} cached, ${linkedinClosedCount - linkedinClosedCacheHits} new). ${linkedinAccepted.length - linkedinClosedCount} open.`);
  }

  // Log blocklisted count
  if (blocklistedCount > 0) {
    log.info(`Blocklist: ${blocklistedCount} job(s) excluded from accepted.`);
  }

  // Push scored + accepted datasets
  const scoredBatch = 200;
  let acceptedCount = 0;

  const acceptedJobs = [];
  for (let i = 0; i < results.length; i += scoredBatch) {
    const batch = results.slice(i, i + scoredBatch);
    await scoredDataset.pushData(batch);

    const accepted = batch.filter((j) => j?.evaluation?.accepted);
    if (accepted.length) {
      acceptedCount += accepted.length;
      acceptedJobs.push(...accepted);
      await acceptedDataset.pushData(accepted);
    }
  }

  // 3) LinkedIn URL enrichment — for accepted jobs without LinkedIn URLs,
  //    search LinkedIn's public guest page to find the same job and swap in the LinkedIn URL.
  //    Uses a persistent cache to avoid re-searching LinkedIn for known jobs.
  const { enrichedCount: linkedinEnrichCount, linkedinUrlCache } =
    await enrichLinkedInUrls(acceptedJobs, scoreCache?.linkedinUrlCache);

  // 3b) Check newly-enriched LinkedIn URLs for "No longer accepting applications".
  //     Jobs sourced from Built In / Fantastic may have been enriched with a LinkedIn URL
  //     in step 3 — those URLs weren't checked in step 2 since they didn't exist yet.
  if (linkedinEnrichCount > 0) {
    const enrichedWithLinkedin = acceptedJobs.filter(
      (r) => r?.evaluation?.accepted && /linkedin\.com\/jobs/i.test(r.applyUrl || '')
    );
    // Only check jobs whose LinkedIn URL isn't already in the closed cache
    const enrichedToCheck = enrichedWithLinkedin.filter(
      (r) => !linkedinClosedCache.has(r.applyUrl || '')
    );
    if (enrichedToCheck.length > 0) {
      log.info(`Checking ${enrichedToCheck.length} enriched LinkedIn URLs for closed listings...`);
      const checks = await Promise.all(
        enrichedToCheck.map((r) => isLinkedInJobClosed(r.applyUrl || ''))
      );
      let enrichedClosedCount = 0;
      for (let k = 0; k < enrichedToCheck.length; k++) {
        if (checks[k]) {
          markJobClosed(enrichedToCheck[k]);
          enrichedClosedCount++;
        }
      }
      if (enrichedClosedCount > 0) {
        log.info(`LinkedIn enriched-URL check: ${enrichedClosedCount} closed out of ${enrichedToCheck.length} checked.`);
      }
    }
  }

  // --- Post-scoring salary scrape for accepted jobs missing salary ---
  const salaryEnrichment = { attempted: 0, found: 0, failed: 0 };
  const salaryScrapeCandidates = acceptedJobs.filter(j => {
    const sal = j.salary || j.evaluation?.salary_extracted || '';
    return !sal && (j.url || j.applyUrl);
  });
  if (salaryScrapeCandidates.length > 0) {
    log.info(`Salary scrape: ${salaryScrapeCandidates.length} accepted jobs missing salary. Fetching job pages...`);
    for (const job of salaryScrapeCandidates) {
      const url = job.applyUrl || job.url;
      if (!url) continue;
      salaryEnrichment.attempted++;
      try {
        const res = await fetch(url, {
          method: 'GET',
          headers: {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
          },
          signal: AbortSignal.timeout(10000),
          redirect: 'follow',
        });
        if (!res.ok) { salaryEnrichment.failed++; continue; }
        const html = await res.text();

        // Try JSON-LD baseSalary
        let foundSalary = '';
        const ldMatches = html.matchAll(/<script\s+type=["']application\/ld(?:\+|&#x2B;)json["'][^>]*>([\s\S]*?)<\/script>/gi);
        for (const match of ldMatches) {
          try {
            const data = JSON.parse(match[1]);
            const postings = [];
            if (data['@type'] === 'JobPosting') postings.push(data);
            if (Array.isArray(data['@graph'])) {
              for (const item of data['@graph']) {
                if (item['@type'] === 'JobPosting') postings.push(item);
              }
            }
            for (const posting of postings) {
              const sal = formatBaseSalary(posting.baseSalary);
              if (sal) { foundSalary = sal; break; }
              // Also check datePosted while we're here
              if (posting.datePosted && !job.postedAt) {
                job.postedAt = posting.datePosted;
              }
            }
          } catch { /* ignore malformed JSON-LD */ }
          if (foundSalary) break;
        }

        // Fallback: regex for salary patterns in HTML
        if (!foundSalary) {
          const salaryMatch = html.match(/\$[\d,]+(?:\.\d{2})?\s*[-–—]\s*\$[\d,]+(?:\.\d{2})?\s*(?:\/\s*(?:yr|year|hr|hour|mo|month|annually))?/i);
          if (salaryMatch) foundSalary = salaryMatch[0].trim();
        }

        if (foundSalary) {
          job.salary = foundSalary;
          salaryEnrichment.found++;
          log.info(`Salary scraped for "${job.title}" at ${job.company}: ${foundSalary}`);
        }
      } catch (err) {
        salaryEnrichment.failed++;
      }
      // Small delay between fetches
      await new Promise(r => setTimeout(r, 500));
    }
    log.info(`Salary scrape done: ${salaryEnrichment.found} found, ${salaryEnrichment.failed} failed out of ${salaryEnrichment.attempted} attempted.`);
  }

  // --- Standardize salary fields on accepted jobs and collect notes ---
  const salaryNotes = [];
  for (const job of acceptedJobs) {
    const rawSal = job.salary || job.evaluation?.salary_extracted || '';
    if (!rawSal) continue;
    const { display, note } = standardizeSalary(rawSal, job.company, job.title);
    // Write standardized salary back to BOTH fields so XLSX builder picks it up
    job.salary = display;
    if (job.evaluation?.salary_extracted) {
      job.evaluation.salary_extracted = display;
    }
    if (note) {
      // Suppress non-USD salary notes for foreign remote jobs — a foreign
      // currency is expected and not noteworthy for Remote jobs abroad.
      const isForeignRemote = job.workMode === 'Remote' && job.location &&
        !['USA', 'MA', 'NY', 'NJ', 'CT', 'NH', 'VT', 'ME', 'RI'].includes(job.location) &&
        note.startsWith('Non-USD');
      if (!isForeignRemote) {
        salaryNotes.push(note);
        log.info(`Salary note: ${note}`);
      }
    }
  }
  if (salaryNotes.length > 0) {
    log.info(`Salary standardization: ${salaryNotes.length} note(s) generated.`);
  }

  // --- URL health check for accepted jobs ---
  // Remove jobs whose URL returns 404 (listing removed by employer).
  // Skip LinkedIn URLs (they have their own closed-job check above).
  const urlCheckResults = { checked: 0, dead: 0 };
  const deadUrls = [];
  const nonLinkedinAccepted = acceptedJobs.filter(j =>
    j?.evaluation?.accepted && !/linkedin\.com/i.test(j.url || '') && !/linkedin\.com/i.test(j.applyUrl || '')
  );
  if (nonLinkedinAccepted.length > 0) {
    log.info(`URL health check: ${nonLinkedinAccepted.length} non-LinkedIn accepted jobs...`);
    for (const job of nonLinkedinAccepted) {
      const url = job.applyUrl || job.url;
      if (!url) continue;
      urlCheckResults.checked++;
      try {
        const res = await fetch(url, {
          method: 'HEAD',
          headers: { 'User-Agent': 'Mozilla/5.0 (compatible; job-pipeline/1.0)' },
          signal: AbortSignal.timeout(8000),
          redirect: 'follow',
        });
        if (res.status === 404 || res.status === 410) {
          urlCheckResults.dead++;
          deadUrls.push(`${job.company} — ${job.title}: ${url} (HTTP ${res.status})`);
          job.evaluation.accepted = false;
          job.filterReason = `url_dead: HTTP ${res.status} — listing removed`;
          log.info(`URL dead (${res.status}): "${job.title}" at ${job.company}`);
        }
      } catch (err) {
        // Network errors, timeouts — don't penalize, assume URL is alive
      }
      await new Promise(r => setTimeout(r, 300));
    }
    // Remove dead jobs from acceptedJobs array
    const beforeCount = acceptedJobs.length;
    for (let i = acceptedJobs.length - 1; i >= 0; i--) {
      if (acceptedJobs[i].filterReason?.startsWith('url_dead:')) {
        acceptedJobs.splice(i, 1);
      }
    }
    log.info(`URL health check done: ${urlCheckResults.checked} checked, ${urlCheckResults.dead} dead (removed from accepted).`);
  }

  // Save score_cache.json BEFORE building XLSX — if XLSX generation OOMs,
  // we don't lose the LLM scoring results and LinkedIn caches.
  await kv.setValue('score_cache.json', {
    scoringFormatVersion: SCORING_FORMAT_VERSION,
    rubricVersion: currentRubricVersion,
    scoredDatasetName,
    linkedinUrlCache: linkedinUrlCache || {},
    linkedinClosedUrls: [...linkedinClosedCache],
    cachedAt: nowIso(),
  });
  log.info('Score cache saved (pre-XLSX).');

  // Build accepted.xlsx + scored.xlsx (Excel format with hyperlinks, frozen panes, bold headers)
  const xlsxContentType = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';

  // Sort accepted jobs by score descending so best matches appear first
  const acceptedSorted = [...acceptedJobs].sort((a, b) => (b.evaluation?.score ?? 0) - (a.evaluation?.score ?? 0));
  const acceptedXlsx = await buildScoredXlsx(acceptedSorted, { isAcceptedSheet: true, runNumber });
  await kv.setValue('accepted.xlsx', acceptedXlsx, { contentType: xlsxContentType });
  if (runNumber) await kv.setValue(`accepted_R${runNumber}.xlsx`, acceptedXlsx, { contentType: xlsxContentType });

  // scored.xlsx contains ALL scored jobs (accepted + rejected) for review
  // Sort by score descending so best matches appear first
  const allSorted = [...results].sort((a, b) => (b.evaluation?.score ?? 0) - (a.evaluation?.score ?? 0));
  const scoredXlsx = await buildScoredXlsx(allSorted, {
    includeJobIds: true,
    scoringFormatVersion: SCORING_FORMAT_VERSION,
    rubricVersion: currentRubricVersion,
  });
  await kv.setValue('scored.xlsx', scoredXlsx, { contentType: xlsxContentType });
  if (runNumber) await kv.setValue(`scored_R${runNumber}.xlsx`, scoredXlsx, { contentType: xlsxContentType });

  // Compute estimated LLM cost from token counts
  const MODEL_PRICING = {
    'gpt-4.1-mini':  { input: 0.40, output: 1.60 },  // $ per 1M tokens
    'gpt-4.1':       { input: 2.00, output: 8.00 },
    'gpt-4.1-nano':  { input: 0.10, output: 0.40 },
    'gpt-4o-mini':   { input: 0.15, output: 0.60 },
    'gpt-4o':        { input: 2.50, output: 10.00 },
    'gpt-5-mini':    { input: 1.10, output: 4.40 },
  };
  const pricing = MODEL_PRICING[model] || null;
  let estimatedCostUsd = null;
  if (pricing && (openAiStats.inputTokens > 0 || openAiStats.outputTokens > 0)) {
    estimatedCostUsd = (
      (openAiStats.inputTokens / 1_000_000) * pricing.input +
      (openAiStats.outputTokens / 1_000_000) * pricing.output
    );
    estimatedCostUsd = Math.round(estimatedCostUsd * 10000) / 10000; // 4 decimal places
    log.info(`LLM cost estimate: $${estimatedCostUsd.toFixed(4)} (${openAiStats.inputTokens.toLocaleString()} input + ${openAiStats.outputTokens.toLocaleString()} output tokens @ ${model})`);
  }

  const finishedAt = nowIso();
  const report = {
    runId,
    startedAt,
    finishedAt,
    kvStoreName,
    datasetPrefix,
    mergedDatasetName: mergedDatasetName,
    scoredDatasetName,
    acceptedDatasetName,
    totalMerged: mergedJobs.length,
    locationSkipped,
    llmLocationResolved,
    titleSkipped,
    expiredSkipped,
    sentToLlm: mergedJobs.length - locationSkipped - titleSkipped - expiredSkipped - cacheHits,
    totalScored: results.length,
    accepted: acceptedJobs.length,
    seniorTier3Filtered,
    blocklisted: blocklistedCount,
    linkedinClosed: linkedinClosedCount,
    linkedinClosedCacheHits,
    linkedinEnriched: linkedinEnrichCount,
    builtInEnrichment: {
      total: builtInEnrichment.total,
      cached: builtInEnrichment.cached,
      fetched: builtInEnrichment.fetched,
      failed: builtInEnrichment.failed,
    },
    linkedinDetailEnrichment: {
      total: linkedinEnrichment.total,
      cached: linkedinEnrichment.cached,
      fetched: linkedinEnrichment.fetched,
      failed: linkedinEnrichment.failed,
      remoteFound: linkedinEnrichment.remoteFound,
    },
    salaryEnrichment,
    salaryNotes,
    threshold,
    gateOnLocation,
    model,
    rubricUrl,
    scoreCache: {
      enabled: !!cacheMap,
      scoringFormatVersion: SCORING_FORMAT_VERSION,
      rubricVersion: currentRubricVersion,
      prevScoringFormatVersion: scoreCache?.scoringFormatVersion || null,
      prevRubricVersion: scoreCache?.rubricVersion || null,
      cacheHits,
      cacheMisses: mergedJobs.length - cacheHits,
    },
    openai: {
      ...openAiStats,
      estimatedCostUsd,
    },
  };

  // Count any remaining unscored jobs after all re-score passes
  const unscoredJobs = results.filter((j) => j.scoringError);
  report.unscoredCount = unscoredJobs.length;

  if (unscoredJobs.length > 0) {
    report.warnings = report.warnings || [];
    report.warnings.unshift(
      `${unscoredJobs.length} jobs remain unscored, due to rate limits`
    );
    log.warning(`${unscoredJobs.length} jobs remain unscored after all retry passes.`);
  }

  if (openAiStats.rateLimit429 > 0) {
    report.warnings = report.warnings || [];
    report.warnings.push(
      `OpenAI rate limit (HTTP 429) occurred ${openAiStats.rateLimit429} times. Requests were retried with exponential backoff. ` +
      `If this persists, lower scoring.concurrency or request higher rate limits.`
    );
  }

  await kv.setValue('scoring_report.json', report);

  // Write debug LLM data if any jobs were captured
  if (debugLlmResults.length) {
    await kv.setValue('debug_llm.json', { capturedAt: nowIso(), jobs: debugLlmResults });
    log.info(`Wrote debug_llm.json with ${debugLlmResults.length} captured LLM interactions.`);
  }

  // score_cache.json already saved above (pre-XLSX) to survive OOM during XLSX generation.

  const costStr = estimatedCostUsd != null ? ` LLM cost: $${estimatedCostUsd.toFixed(4)}.` : '';
  const cacheStr = cacheHits > 0 ? ` Cache hits: ${cacheHits}.` : '';
  log.info(`Scoring complete. accepted=${acceptedJobs.length}/${results.length}.${costStr}${cacheStr} accepted.xlsx + scored.xlsx written to KV store.`);
});