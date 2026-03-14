// actors/03_score_jobs/src/main.js
// Scores merged jobs with an LLM using an external rubric file, writes scored + accepted datasets,
// and produces accepted.xlsx + scored.xlsx in the KV store.

import { Actor, log } from 'apify';
import ExcelJS from 'exceljs';
import http from 'node:http';
import https from 'node:https';
import { processWithRetries } from './resilient-fetch.js';

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
      : { temperature: 0.2 }),
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

const SCORING_FORMAT_VERSION = 'v3'; // v3: isForeign() catches foreign cities/ISO2, RemoteOnly+foreign → unknown

function extractRubricVersion(rubricText) {
  const match = String(rubricText || '').match(/^#\s+Rubric:.*?\((v\d+)\)/i);
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
 * Determine if a job's location is acceptable, using pre-normalized fields.
 * job.workMode: 'RemoteOK' | 'RemoteOnly' | 'Hybrid' | 'On-Site' | ''
 * job.location: normalized geography (e.g., 'Boston MA', 'DEU', 'USA', '')
 */
function computeLocationOk(job) {
  const wm = String(job.workMode || '');
  const loc = String(job.location || '').trim();

  // RemoteOnly — acceptable only if location is US-based or empty.
  // Foreign RemoteOnly (e.g., "RemoteOnly | FRA") likely means remote within that country, not US.
  if (wm === 'RemoteOnly') {
    if (isForeign(loc)) return 'unknown'; // Remote + foreign → LLM decides
    return 'yes'; // Remote + US or empty → fine
  }

  // RemoteOK → acceptable if the location is US-based (even if not MA)
  // Foreign RemoteOK is ambiguous — let LLM decide
  if (wm === 'RemoteOK') {
    if (isForeign(loc)) return 'unknown'; // Remote + foreign → LLM decides
    return 'yes'; // Remote + US city → fine
  }

  // No location at all → unknown
  if (!loc) return 'unknown';

  // Foreign location → not commutable
  if (isForeign(loc)) return 'no';

  // Check for MA location
  if (/\bMA\b/.test(loc)) {
    // Has a city before MA? Check if commutable
    const match = loc.match(/^(.+?)\s+MA$/);
    if (match) {
      const city = match[1].toLowerCase().trim();
      if (COMMUTABLE_TOWNS.has(city)) return 'yes';
      return 'no'; // MA but not commutable
    }
    return 'yes'; // bare "MA"
  }

  // "USA" (generic) → unknown, could be anywhere
  if (loc === 'USA') return 'unknown';

  // US state abbreviation (2 letters) that isn't MA → no
  if (/^[A-Z]{2}$/.test(loc) && loc !== 'MA') return 'no';

  // US city + non-MA state → no
  if (/\s[A-Z]{2}$/.test(loc) && !loc.endsWith(' MA')) return 'no';

  // Couldn't determine → unknown (LLM will decide)
  return 'unknown';
}

/**
 * Broader foreign detection — catches ISO3 codes, ISO2 suffixes (e.g., "Limassol CY"),
 * and known foreign city names that the collector couldn't resolve to ISO3.
 */
const ISO3_FOREIGN_CODES = new Set([
  'AFG','ALB','DZA','AND','AGO','ARG','ARM','AUS','AUT','AZE','BHR','BGD','BLR','BEL',
  'BLZ','BEN','BTN','BOL','BIH','BWA','BRA','BRN','BGR','BFA','BDI','KHM','CMR','CAN',
  'CPV','CAF','TCD','CHL','CHN','COL','COM','COG','CRI','HRV','CUB','CYP','CZE','DNK',
  'DJI','DOM','ECU','EGY','SLV','GNQ','ERI','EST','SWZ','ETH','FJI','FIN','FRA','GAB',
  'GMB','GEO','DEU','GHA','GRC','GTM','GIN','GNB','GUY','HTI','HND','HKG','HUN','ISL',
  'IND','IDN','IRN','IRQ','IRL','ISR','ITA','JAM','JPN','JOR','KAZ','KEN','KWT','KGZ',
  'LAO','LVA','LBN','LSO','LBR','LBY','LIE','LTU','LUX','MDG','MWI','MYS','MDV','MLI',
  'MLT','MRT','MUS','MEX','MDA','MNG','MNE','MAR','MOZ','MMR','NAM','NPL','NLD','NZL',
  'NIC','NER','NGA','MKD','NOR','OMN','PAK','PAN','PNG','PRY','PER','PHL','POL','PRT',
  'QAT','ROU','RUS','RWA','SAU','SEN','SRB','SGP','SVK','SVN','SOM','ZAF','KOR','ESP',
  'LKA','SDN','SUR','SWE','CHE','SYR','TWN','TJK','TZA','THA','TGO','TTO','TUN','TUR',
  'TKM','UGA','UKR','ARE','GBR','URY','UZB','VEN','VNM','YEM','ZMB','ZWE',
]);

const KNOWN_FOREIGN_CITIES = new Set([
  'amsterdam','athens','bangkok','barcelona','beijing','belfast','berlin','bogota','brussels',
  'bucharest','budapest','buenos aires','cairo','cambridge uk','cape town','copenhagen','cork',
  'delhi','dublin','dubai','edinburgh','frankfurt','geneva','gothenburg','guadalajara','hamburg',
  'helsinki','hong kong','istanbul','jakarta','johannesburg','karachi','kiev','krakow','kuala lumpur',
  'lagos','lahore','lima','limassol','lisbon','london','lyon','madrid','malmo','manila','marseille',
  'melbourne','mexico city','milan','montreal','moscow','mumbai','munich','nairobi','new delhi',
  'nicosia','oslo','oxford','paris','prague','riga','rio de janeiro','riyadh','rome','santiago',
  'sao paulo','seoul','shanghai','singapore','sofia','stockholm','sydney','taipei','tallinn',
  'tbilisi','tel aviv','tokyo','toronto','vancouver','vienna','vilnius','warsaw','zurich',
]);

// ISO2 country codes (foreign only — excludes US)
const ISO2_FOREIGN = new Set([
  'AD','AE','AF','AG','AL','AM','AO','AR','AT','AU','AZ','BA','BB','BD','BE','BF','BG',
  'BH','BI','BJ','BN','BO','BR','BS','BT','BW','BY','BZ','CA','CD','CF','CG','CH','CI',
  'CL','CM','CN','CO','CR','CU','CV','CY','CZ','DE','DJ','DK','DM','DO','DZ','EC','EE',
  'EG','ER','ES','ET','FI','FJ','FR','GA','GB','GD','GE','GH','GM','GN','GQ','GR','GT',
  'GW','GY','HK','HN','HR','HT','HU','ID','IE','IL','IN','IQ','IR','IS','IT','JM','JO',
  'JP','KE','KG','KH','KP','KR','KW','KZ','LA','LB','LI','LK','LR','LS','LT','LU','LV',
  'LY','MA','MC','MD','ME','MG','MK','ML','MM','MN','MR','MT','MU','MV','MW','MX','MY',
  'MZ','NA','NE','NG','NI','NL','NO','NP','NZ','OM','PA','PE','PG','PH','PK','PL','PT',
  'PY','QA','RO','RS','RU','RW','SA','SB','SC','SD','SE','SG','SI','SK','SL','SN','SO',
  'SR','SS','SV','SY','SZ','TD','TG','TH','TJ','TL','TM','TN','TR','TT','TW','TZ','UA',
  'UG','UY','UZ','VA','VE','VN','YE','ZA','ZM','ZW',
]);

// US state abbreviations — used to disambiguate ISO2 country codes that collide with US states
const US_STATES_2 = new Set([
  'AL','AK','AZ','AR','CA','CO','CT','DE','FL','GA','HI','ID','IL','IN','IA','KS','KY',
  'LA','ME','MD','MA','MI','MN','MS','MO','MT','NE','NV','NH','NJ','NM','NY','NC','ND',
  'OH','OK','OR','PA','RI','SC','SD','TN','TX','UT','VT','VA','WA','WV','WI','WY','DC',
]);

function isForeign(loc) {
  if (!loc) return false;

  // ISO3 foreign code (e.g., "FRA", "GBR")
  if (ISO3_FOREIGN_CODES.has(loc)) return true;

  // "City ISO2" pattern (e.g., "Limassol CY", "Jakarta ID")
  // Only used for multi-word locations — bare 2-letter codes overlap with US states (CA, DE, GA, etc.)
  const iso2Match = loc.match(/^.+\s([A-Z]{2})$/);
  if (iso2Match) {
    const code = iso2Match[1];
    // Only flag as foreign if the ISO2 code is unambiguously foreign (not a US state)
    if (ISO2_FOREIGN.has(code) && !US_STATES_2.has(code)) return true;
  }

  // Known foreign city name (e.g., "Istanbul", "Vancouver", "Warsaw")
  if (KNOWN_FOREIGN_CITIES.has(loc.toLowerCase())) return true;

  // "Remote CET" or timezone-based locations suggesting Europe
  if (/\bCET\b|\bGMT[+-]\d/.test(loc)) return true;

  // Region names that are foreign (North America is ambiguous — could include Canada — let LLM decide via 'unknown')
  if (/\b(Eastern Europe|Western Europe|EMEA|APAC|LATAM)\b/i.test(loc)) return true;

  return false;
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
  // 1. Check employmentType field (set by collector for Fantastic/LinkedIn sources)
  const et = String(job.employmentType || '').toLowerCase();
  if (et) {
    if (et.includes('intern'))                                           return 'Internship';
    if (et.includes('contract') || et.includes('freelance') || et.includes('temporary')) return 'Contract';
    if (et.includes('part-time') || et.includes('part time'))            return 'Part-Time';
    if (et.includes('full-time') || et.includes('full time'))            return 'Full-Time';
  }

  // 2. Check job title
  const title = String(job.title || '');
  if (/\bintern(ship)?\b/i.test(title))                                 return 'Internship';
  if (/\b(contract|contractor|freelance)\b/i.test(title))               return 'Contract';
  if (/\bpart[\s-]?time\b/i.test(title))                                return 'Part-Time';

  // 3. Check description (first 2000 chars for performance)
  const desc = String(job.description || '').slice(0, 2000).toLowerCase();
  if (/\binternship\b/.test(desc) && /\bintern\b/.test(desc))           return 'Internship';
  if (/\b(contract position|contract role|independent contractor|1099)\b/.test(desc)) return 'Contract';
  if (/\bpart[\s-]?time\b/.test(desc) && !/\bfull[\s-]?time\b/.test(desc)) return 'Part-Time';

  return 'Full-Time';
}

// --------------- Remote Status Enrichment ---------------

function enrichRemoteStatus(job) {
  const wm = String(job.workMode || '');
  // Already has a remote-related work mode from collector normalization
  if (wm === 'RemoteOK' || wm === 'RemoteOnly') return;

  let isRemote = false;

  // Check employmentType for "Remote"
  const et = String(job.employmentType || '').toLowerCase();
  if (et.includes('remote')) isRemote = true;

  // Check description for strong remote signals
  if (!isRemote) {
    const desc = String(job.description || '').slice(0, 3000).toLowerCase();
    isRemote = [
      /\bremote position\b/, /\bremote role\b/, /\bwork remotely\b/,
      /\bfully remote\b/, /\b100% remote\b/, /\bremote[\s-]?first\b/,
      /\bremote work\b/, /\bremote opportunity\b/,
    ].some(p => p.test(desc));
  }

  if (isRemote) {
    const loc = String(job.location || '');
    // Determine RemoteOK vs RemoteOnly based on whether there's a city
    if (!loc || loc === 'USA' || (/^[A-Z]{2}$/.test(loc) && loc.length === 2) ||
        (loc.length === 3 && /^[A-Z]{3}$/.test(loc) && loc !== 'USA')) {
      job.workMode = 'RemoteOnly';
    } else {
      job.workMode = 'RemoteOK';
    }
  }
}

// --------------- Built In Description Enrichment ---------------

const BUILTIN_DESC_CACHE_TTL_DAYS = 30;
const BUILTIN_FETCH_DELAY_MS = 3000;

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
 * Fetch a Built In job page and extract the description from JSON-LD structured data.
 * Returns { description, employmentType } or null if not found.
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
          return {
            description: stripHtmlTags(posting.description),
            employmentType: posting.employmentType || '',
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
  // Combine for accepted.xlsx "Where" column
  const wm = String(job.workMode || '');
  if (!wm) return loc;
  if (!loc) return wm;
  return `${wm} | ${loc}`;
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

function formatSalary(raw) {
  if (!raw && raw !== 0) return '';
  const s = String(raw).trim();
  if (!s) return '';

  // Already formatted (contains $ or k) — pass through
  if (/[$k]/i.test(s)) return s;

  // Range like "90000-120000" or "90000 - 120000"
  const rangeMatch = s.match(/^(\d{4,})\s*[-–—]\s*(\d{4,})$/);
  if (rangeMatch) {
    const lo = Number(rangeMatch[1]).toLocaleString('en-US');
    const hi = Number(rangeMatch[2]).toLocaleString('en-US');
    return `$${lo}–$${hi}`;
  }

  // Plain number like "135000"
  const num = Number(s);
  if (Number.isFinite(num) && num >= 1000) {
    return `$${num.toLocaleString('en-US')}`;
  }

  // Anything else — pass through as-is
  return s;
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
      const match = urls.find((u) => {
        const lower = u.toLowerCase();
        return lower.includes(`-at-${slug}-`) || lower.endsWith(`-at-${slug}`);
      });

      if (match) {
        const cleanUrl = match.split(/['">\s]/)[0];
        log.info(`LinkedIn enriched: "${job.title}" at ${job.company} → ${cleanUrl}`);
        job.applyUrl = cleanUrl;
        enrichedCount++;
        if (key) cache[key] = cleanUrl;
      } else {
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
  includeSearchTerms = false,
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
    colDefs.push({ header: 'Where', width: 36, key: 'where' }); // combined workMode + location
  } else {
    colDefs.push({ header: 'Location',  width: 30, key: 'location' });
    colDefs.push({ header: 'Work Mode', width: 12, key: 'workMode' });
  }
  colDefs.push({ header: 'Position Type', width: 14, key: 'positionType' });
  if (!isAcceptedSheet) {
    colDefs.push({ header: 'Score', width: 8, key: 'score' });
    colDefs.push({ header: 'Role',  width: 22, key: 'role' });
  }
  colDefs.push({ header: 'Age (days)',  width: 11, key: 'age' });
  colDefs.push({ header: 'Where Found', width: 19, key: 'whereFound' });
  if (!isAcceptedSheet) {
    colDefs.push({ header: 'Sources', width: 19, key: 'sources' });
  }
  colDefs.push({ header: 'Reason',    width: 52, key: 'reason' });
  if (!isAcceptedSheet) {
    colDefs.push({ header: 'Tags', width: 36, key: 'tags' });
  }
  colDefs.push({ header: 'Red Flags', width: 42, key: 'redFlags' });
  if (includeSearchTerms) {
    colDefs.push({ header: 'Search Terms', width: 40, key: 'searchTerms' });
  }
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

    const salary = formatSalary(j.salary || ev.salary_extracted || '');
    const score = ev.score ?? 0;
    const ageDays = calculateAgeDays(j.earliestPostedAt || j.postedAt);
    const sourcesArr = Array.isArray(j.sources) ? j.sources : [j.source].filter(Boolean);
    const sourcesStr = sourcesArr.map(friendlySourceName).filter(Boolean).join(', ');
    const reason = ev.reason_short || '';
    const positionType = j.positionType || 'Full-Time';

    const valueMap = {
      company:      j.company || '',
      title:        j.title || '',
      salary,
      where:        formatLocationForDisplay(j, true),   // combined for accepted.xlsx
      location:     j.location || '',                     // separate for scored.xlsx
      workMode:     j.workMode || '',                     // separate for scored.xlsx
      positionType,
      score,
      role:         roleStr,
      age:          ageDays === '' ? '' : ageDays,
      whereFound:   '',  // will become hyperlink
      sources:      sourcesStr,
      reason,
      tags,
      redFlags,
      searchTerms:  (j.searchTerms || []).join('; '),
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
    enrichRemoteStatus(job); // must be before computeLocationOk
  }

  // --- Pre-compute location for all jobs and skip location_ok = "no" ---
  let locationSkipped = 0;
  const preLocationMap = new Map(); // idx → location_ok
  for (let i = 0; i < mergedJobs.length; i++) {
    preLocationMap.set(i, computeLocationOk(mergedJobs[i]));
  }
  const noLocationCount = [...preLocationMap.values()].filter(v => v === 'no').length;
  log.info(`Location pre-filter: ${noLocationCount} jobs are location_ok=no and will be skipped (not sent to LLM).`);

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

  // --- Built In description enrichment (fetch missing descriptions for pre-filter survivors) ---
  const builtInEnrichment = { total: 0, cached: 0, fetched: 0, failed: 0, failedUrls: [] };

  // Load description cache from KV store
  let builtInDescCache = {};
  try {
    const rawCache = await kv.getValue('builtin_desc_cache.json');
    if (rawCache && typeof rawCache === 'object') {
      const cutoff = Date.now() - BUILTIN_DESC_CACHE_TTL_DAYS * 24 * 60 * 60 * 1000;
      for (const [url, entry] of Object.entries(rawCache)) {
        if (entry?.fetchedAt && new Date(entry.fetchedAt).getTime() >= cutoff) {
          builtInDescCache[url] = entry;
        }
      }
      const pruned = Object.keys(rawCache).length - Object.keys(builtInDescCache).length;
      if (pruned > 0) log.info(`Built In desc cache: pruned ${pruned} expired entries (TTL ${BUILTIN_DESC_CACHE_TTL_DAYS}d).`);
      log.info(`Built In desc cache loaded: ${Object.keys(builtInDescCache).length} entries.`);
    }
  } catch (err) {
    log.warning(`Failed to load Built In desc cache: ${err?.message || err}`);
  }

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

    for (const { job } of builtInJobsToEnrich) {
      const url = job.url || job.applyUrl;
      if (!url) { builtInEnrichment.failed++; continue; }

      // Check cache first
      const cached = builtInDescCache[url];
      if (cached?.description) {
        job.description = cached.description;
        if (cached.employmentType && !job.employmentType) {
          job.employmentType = cached.employmentType;
        }
        builtInEnrichment.cached++;
        continue;
      }

      // Fetch from Built In
      try {
        const result = await fetchBuiltInDescription(url);
        if (result?.description) {
          job.description = result.description;
          builtInDescCache[url] = {
            description: result.description,
            employmentType: result.employmentType || '',
            fetchedAt: nowIso(),
          };
          builtInEnrichment.fetched++;
          if (result.employmentType && !job.employmentType) {
            job.employmentType = result.employmentType;
          }
          log.info(`Built In enriched: "${job.title}" at ${job.company} (${result.description.length} chars)`);
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

    log.info(`Built In enrichment done: ${builtInEnrichment.fetched} fetched, ${builtInEnrichment.cached} cached, ${builtInEnrichment.failed} failed.`);
  }

  // Save description cache
  try {
    await kv.setValue('builtin_desc_cache.json', builtInDescCache);
  } catch (err) {
    log.warning(`Failed to save Built In desc cache: ${err?.message || err}`);
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

    // 2) Location gate — deterministic reject, overrides cache
    const locationOk = preLocationMap.get(idx) || computeLocationOk(job);
    if (locationOk === 'no') {
      locationSkipped += 1;
      return {
        ...job,
        evaluation: {
          accept: false,
          accepted: false,
          score: 0,
          confidence: 1.0,
          location_ok: 'no',
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

    // --- Cache check (only after all gates pass) ---
    const cached = lookupCache(cacheMap, job);
    if (cached?.evaluation) {
      cacheHits++;
      return {
        ...job,
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

    const messages = [
      {
        role: 'system',
        content:
          rubricText +
          '\n\n' +
          'Return ONLY valid JSON, no markdown. Ensure fields: accept, score, confidence, reason_short, reasons, red_flags, tags, salary_extracted, company_url, role, location, work_mode.\n' +
          'location: Your best determination of the job\'s geographic location (e.g., "Boston MA", "JPN", "USA"). Use the provided location as a starting point, but refine based on the description.\n' +
          'work_mode: One of "RemoteOK", "RemoteOnly", "Hybrid", "On-Site", or "". Refine based on the description.',
      },
      {
        role: 'user',
        content: JSON.stringify(jobForPrompt, null, 2),
      },
    ];

    // LLM call — errors propagate to processWithRetries for retry handling
    const evaluation = await callOpenAIJson({ apiKey, model, messages, stats: openAiStats });

    const score = toInt(evaluation.score ?? evaluation.Score ?? 0, 0);
    const accept = !!(evaluation.accept ?? evaluation.Accept);

    // Enhance location/workMode from LLM response if provided
    const llmLocation = evaluation.location;
    const llmWorkMode = evaluation.work_mode;
    if (llmLocation && typeof llmLocation === 'string' && llmLocation.trim()) {
      job.location = llmLocation.trim();
    }
    if (llmWorkMode && typeof llmWorkMode === 'string' && llmWorkMode.trim()) {
      job.workMode = llmWorkMode.trim();
    }

    const accepted =
      accept &&
      score >= threshold &&
      (!gateOnLocation || locationOk === 'yes');

    return {
      ...job,
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
  log.info(`Scoring done. ${cacheHits} cache hits, ${locationSkipped} skipped (bad location), ${titleSkipped} skipped (bad title), ${expiredSkipped} skipped (expired), ${mergedJobs.length - totalSkipped - cacheHits} sent to LLM. ${scoringFailures.length} hard failures.`);

  // --- Post-scoring filters ---

  // Note: Senior/Lead/Manager/etc. filtering now happens in the pre-LLM title filter.
  // These jobs never reach the LLM, so there is no post-LLM senior filter needed.
  const seniorTier3Filtered = 0;

  // 2) Check LinkedIn URLs for "No longer accepting applications"
  //    Only check accepted jobs with LinkedIn job URLs — small number of fetches.
  //    "Closed" results are cached across runs to avoid re-pinging LinkedIn for dead jobs.
  //    "Open" results are NOT cached — we re-check each run since jobs can close any time.
  const linkedinClosedCache = new Set(scoreCache?.linkedinClosedUrls || []);
  const linkedinAccepted = results.filter(
    (r) => r?.evaluation?.accepted && /linkedin\.com\/jobs/i.test(r.applyUrl || r.url || '')
  );
  let linkedinClosedCount = 0;
  let linkedinClosedCacheHits = 0;

  if (linkedinAccepted.length > 0) {
    // Mark jobs known-closed from cache (no HTTP fetch needed)
    const toCheck = [];
    for (const r of linkedinAccepted) {
      const jobUrl = r.applyUrl || r.url || '';
      if (linkedinClosedCache.has(jobUrl)) {
        linkedinClosedCacheHits++;
        linkedinClosedCount++;
        r.evaluation.accepted = false;
        r.evaluation.accept = false;
        r.evaluation.red_flags = [...(r.evaluation.red_flags || []), 'LinkedIn: No longer accepting applications'];
        r.evaluation.reason_short = `${r.evaluation.reason_short || ''} [CLOSED]`.trim();
      } else {
        toCheck.push(r);
      }
    }

    if (linkedinClosedCacheHits > 0) {
      log.info(`LinkedIn closed cache: ${linkedinClosedCacheHits} known-closed jobs skipped.`);
    }

    if (toCheck.length > 0) {
      log.info(`Checking ${toCheck.length} accepted LinkedIn jobs for closed listings...`);

      async function isLinkedInJobClosed(jobUrl) {
        try {
          const res = await fetch(jobUrl, {
            headers: { 'User-Agent': 'Mozilla/5.0 (compatible; job-pipeline/1.0)' },
            signal: AbortSignal.timeout(10000),
          });
          const html = await res.text();
          return html.includes('No longer accepting applications');
        } catch {
          return false; // on error, assume still open (don't penalize)
        }
      }

      // Check in batches of 10 to avoid overwhelming LinkedIn
      const LI_BATCH = 10;
      for (let i = 0; i < toCheck.length; i += LI_BATCH) {
        const batch = toCheck.slice(i, i + LI_BATCH);
        const checks = await Promise.all(
          batch.map((r) => isLinkedInJobClosed(r.applyUrl || r.url || ''))
        );
        for (let k = 0; k < batch.length; k++) {
          if (checks[k]) {
            linkedinClosedCount++;
            const jobUrl = batch[k].applyUrl || batch[k].url || '';
            linkedinClosedCache.add(jobUrl); // cache for future runs
            batch[k].evaluation.accepted = false;
            batch[k].evaluation.accept = false;
            batch[k].evaluation.red_flags = [...(batch[k].evaluation.red_flags || []), 'LinkedIn: No longer accepting applications'];
            batch[k].evaluation.reason_short = `${batch[k].evaluation.reason_short || ''} [CLOSED]`.trim();
          }
        }
        // Small delay between batches to be polite
        if (i + LI_BATCH < toCheck.length) await new Promise((r) => setTimeout(r, 1000));
      }
    }
    log.info(`LinkedIn check: ${linkedinClosedCount} closed (${linkedinClosedCacheHits} cached, ${linkedinClosedCount - linkedinClosedCacheHits} new). ${linkedinAccepted.length - linkedinClosedCount} open.`);
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

  // Build accepted.xlsx + scored.xlsx (Excel format with hyperlinks, frozen panes, bold headers)
  const xlsxContentType = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';

  // Sort accepted jobs by score descending so best matches appear first
  const acceptedSorted = [...acceptedJobs].sort((a, b) => (b.evaluation?.score ?? 0) - (a.evaluation?.score ?? 0));
  const acceptedXlsx = await buildScoredXlsx(acceptedSorted, { isAcceptedSheet: true, runNumber });
  await kv.setValue('accepted.xlsx', acceptedXlsx, { contentType: xlsxContentType });

  // scored.xlsx contains ALL scored jobs (accepted + rejected) for review
  // Sort by score descending so best matches appear first
  const allSorted = [...results].sort((a, b) => (b.evaluation?.score ?? 0) - (a.evaluation?.score ?? 0));
  const scoredXlsx = await buildScoredXlsx(allSorted, {
    includeSearchTerms: true,
    includeJobIds: true,
    scoringFormatVersion: SCORING_FORMAT_VERSION,
    rubricVersion: currentRubricVersion,
  });
  await kv.setValue('scored.xlsx', scoredXlsx, { contentType: xlsxContentType });

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
    titleSkipped,
    expiredSkipped,
    sentToLlm: mergedJobs.length - locationSkipped - titleSkipped - expiredSkipped - cacheHits,
    totalScored: results.length,
    accepted: acceptedJobs.length,
    seniorTier3Filtered,
    linkedinClosed: linkedinClosedCount,
    linkedinClosedCacheHits,
    linkedinEnriched: linkedinEnrichCount,
    builtInEnrichment: {
      total: builtInEnrichment.total,
      cached: builtInEnrichment.cached,
      fetched: builtInEnrichment.fetched,
      failed: builtInEnrichment.failed,
    },
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

  // Write score_cache.json for next run's LLM cache + LinkedIn cache
  await kv.setValue('score_cache.json', {
    scoringFormatVersion: SCORING_FORMAT_VERSION,
    rubricVersion: currentRubricVersion,
    scoredDatasetName,
    linkedinUrlCache: linkedinUrlCache || {},
    linkedinClosedUrls: [...linkedinClosedCache],
    cachedAt: nowIso(),
  });

  const costStr = estimatedCostUsd != null ? ` LLM cost: $${estimatedCostUsd.toFixed(4)}.` : '';
  const cacheStr = cacheHits > 0 ? ` Cache hits: ${cacheHits}.` : '';
  log.info(`Scoring complete. accepted=${acceptedJobs.length}/${results.length}.${costStr}${cacheStr} accepted.xlsx + scored.xlsx written to KV store.`);
});