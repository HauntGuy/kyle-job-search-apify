// actors/03_score_jobs/src/main.js
// Scores merged jobs with an LLM using an external rubric file, writes scored + accepted datasets,
// and produces accepted.xlsx + scored.xlsx in the KV store.

import { Actor, log } from 'apify';
import ExcelJS from 'exceljs';
import http from 'node:http';
import https from 'node:https';

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
    throw new Error(`OpenAI response missing output text: ${JSON.stringify(json).slice(0, 700)}`);
  }

  const cleaned = stripCodeFences(text);
  try {
    return JSON.parse(cleaned);
  } catch (e) {
    throw new Error(`Model did not return valid JSON: ${e?.message || e}\n${cleaned.slice(0, 700)}`);
  }
}

async function withRetries(fn, { retries = 5, baseMs = 800, maxMs = 12000, stats } = {}) {
  let attempt = 0;
  while (true) {
    try {
      return await fn();
    } catch (e) {
      attempt += 1;

      const status = e?.status || 0;
      const retryable = status === 0 || status === 429 || status >= 500;

      if (attempt > retries || !retryable) throw e;

      if (stats) stats.retries += 1;

      // Exponential backoff with jitter
      let wait = Math.min(maxMs, baseMs * Math.pow(2, attempt - 1));
      const jitterFactor = 0.7 + Math.random() * 0.6; // 0.7–1.3
      wait = Math.floor(wait * jitterFactor);

      // Respect Retry-After if present
      const ra = e?.retryAfterMs;
      if (Number.isFinite(ra) && ra > 0) {
        wait = Math.max(wait, Math.floor(ra));
        if (stats) stats.maxRetryAfterMs = Math.max(stats.maxRetryAfterMs || 0, Math.floor(ra));
      }

      log.warning(`Retryable OpenAI error (status=${status}). Waiting ${wait}ms then retrying... (attempt ${attempt}/${retries})`);
      await sleep(wait);
    }
  }
}

async function mapWithConcurrency(items, concurrency, mapper) {
  const results = new Array(items.length);
  let idx = 0;

  async function worker() {
    while (true) {
      const myIdx = idx;
      idx += 1;
      if (myIdx >= items.length) return;
      results[myIdx] = await mapper(items[myIdx], myIdx);
    }
  }

  const workers = [];
  for (let i = 0; i < Math.max(1, concurrency); i++) workers.push(worker());
  await Promise.all(workers);
  return results;
}

function truncate(s, maxChars) {
  const t = String(s || '');
  if (t.length <= maxChars) return t;
  return t.slice(0, maxChars) + '\n\n[TRUNCATED]';
}

// --------------- Score cache helpers ---------------

const SCORING_FORMAT_VERSION = 'v1'; // bump when scored.xlsx columns change

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
 * Determine location_ok from the structured location string.
 * Returns "yes", "no", or "unknown".
 *
 * Format examples:
 *   "Remote | Boston, Massachusetts, United States"
 *   "Boston, Massachusetts, United States; Cambridge, Massachusetts, United States"
 *   "" (blank)
 */
function computeLocationOk(locationStr) {
  const loc = String(locationStr || '').trim();
  if (!loc) return 'unknown';

  const lower = loc.toLowerCase();

  // If "remote" appears anywhere, it's OK
  if (/\bremote\b/i.test(loc)) return 'yes';

  // Split on semicolons (multi-location listings) and pipes (Remote | City)
  const parts = loc.split(/[;|]/).map(p => p.trim()).filter(Boolean);

  for (const part of parts) {
    const partLower = part.toLowerCase();
    // Check if this part mentions Massachusetts
    if (partLower.includes('massachusetts')) {
      // Extract city name: "Boston, Massachusetts, United States" → "boston"
      const city = partLower.split(',')[0].trim();
      // State-level entry like "Massachusetts, United States" (no specific city) →
      // job is available somewhere in MA, give benefit of the doubt
      if (city === 'massachusetts') return 'yes';
      if (COMMUTABLE_TOWNS.has(city)) return 'yes';
      // Massachusetts but not in commutable list → keep checking other locations
    }
  }

  // If any part mentioned Massachusetts but no commutable town was found,
  // and there are non-MA locations too, it depends on whether MA is an option.
  // If ALL locations are non-MA/non-remote → "no"
  // If some are MA but not commutable → "no"
  // If we couldn't parse anything meaningful → "unknown"
  const hasAnyCity = parts.some(p => p.includes(','));
  if (hasAnyCity) return 'no';

  return 'unknown';
}

// --------------- XLSX helpers ---------------

function friendlySourceName(sourceId) {
  if (!sourceId) return '';
  const s = String(sourceId);
  if (s.startsWith('fantastic_')) return 'Fantastic';
  if (s.startsWith('linkedin_')) return 'LinkedIn';
  if (s.startsWith('mantiks_')) return 'Mantiks';
  const map = {
    'fantastic_feed': 'Fantastic',
    'linkedin_jobs': 'LinkedIn',
    'remotive': 'Remotive',
    'remoteok': 'RemoteOK',
    'rapidapi_jsearch': 'JSearch',
    'rapidapi_mantiks': 'Mantiks',
  };
  return map[s] || s;
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

// Build an XLSX workbook buffer from an array of scored jobs
async function buildScoredXlsx(jobs, {
  includeSearchTerms = false,
  includeJobIds = false,
  scoringFormatVersion = null,
  rubricVersion = null,
} = {}) {
  const workbook = new ExcelJS.Workbook();
  const ws = workbook.addWorksheet('Jobs');

  // Define column headers and widths
  const colDefs = [
    { header: 'Company',      width: 26 },
    { header: 'Job Title',    width: 46 },
    { header: 'Salary',       width: 26 },
    { header: 'Where',        width: 36 },
    { header: 'Score',        width: 8  },
    { header: 'Role',         width: 22 },
    { header: 'Age (days)',   width: 11 },
    { header: 'Where Found',  width: 19 },
    { header: 'Sources',      width: 19 },
    { header: 'Reason',       width: 52 },
    { header: 'Tags',         width: 36 },
    { header: 'Red Flags',    width: 42 },
  ];

  if (includeSearchTerms) {
    colDefs.push({ header: 'Search Terms', width: 40 });
  }
  if (includeJobIds) {
    colDefs.push({ header: 'Job IDs', width: 30 });
  }

  // Set column widths only (don't use ws.columns with header — it auto-creates row 1)
  for (let i = 0; i < colDefs.length; i++) {
    ws.getColumn(i + 1).width = colDefs[i].width;
  }

  // Metadata row (scored.xlsx only — when version info is provided)
  const hasMetaRow = !!(scoringFormatVersion || rubricVersion);
  if (hasMetaRow) {
    // Row 1: "Scoring Format:" | "v1" | "Rubric" | "v13"
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
  const freezeYSplit = hasMetaRow ? 2 : 1;
  ws.views = [{ state: 'frozen', xSplit: 2, ySplit: freezeYSplit }];

  for (const j of jobs) {
    const ev = j.evaluation || {};
    const tags = Array.isArray(ev.tags) ? ev.tags.join('; ') : '';
    const redFlags = Array.isArray(ev.red_flags) ? ev.red_flags.join(' ') : '';
    const roleStr = Array.isArray(ev.role) ? ev.role.join(', ') : (ev.role || '');

    const salary = formatSalary(j.salary || ev.salary_extracted || '');
    const where = j.location || '';
    const score = ev.score ?? 0;
    const ageDays = calculateAgeDays(j.earliestPostedAt || j.postedAt);
    const sourcesArr = Array.isArray(j.sources) ? j.sources : [j.source].filter(Boolean);
    const sourcesStr = sourcesArr.map(friendlySourceName).filter(Boolean).join(', ');
    const reason = ev.reason_short || '';

    const rowData = [
      j.company || '',                     // 1: Company (will become hyperlink)
      j.title || '',                       // 2: Job Title (will become hyperlink)
      salary,                              // 3: Salary
      where,                               // 4: Where
      score,                               // 5: Score
      roleStr,                             // 6: Role
      ageDays === '' ? '' : ageDays,       // 7: Age (days)
      '',                                  // 8: Where Found (will become hyperlink)
      sourcesStr,                          // 9: Sources
      reason,                              // 10: Reason
      tags,                                // 11: Tags
      redFlags,                            // 12: Red Flags
    ];

    if (includeSearchTerms) {
      rowData.push((j.searchTerms || []).join('; '));
    }
    if (includeJobIds) {
      rowData.push((j.sourceJobIds || []).join(', '));
    }

    const row = ws.addRow(rowData);

    // Company hyperlink
    const companyUrl = j.companyUrl || ev.company_url || '';
    setCellHyperlink(row.getCell(1), companyUrl, j.company || '');

    // Job Title hyperlink
    const jobUrl = j.applyUrl || j.url || '';
    setCellHyperlink(row.getCell(2), jobUrl, j.title || '');

    // Where Found hyperlink
    const foundUrl = j.url || j.applyUrl || '';
    const rootUrl = extractRootDomainUrl(foundUrl);
    const foundName = friendlyDomainName(foundUrl, j.company);
    setCellHyperlink(row.getCell(8), rootUrl, foundName);
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

  // --- Pre-compute location for all jobs and skip location_ok = "no" ---
  let locationSkipped = 0;
  const preLocationMap = new Map(); // idx → location_ok
  for (let i = 0; i < mergedJobs.length; i++) {
    preLocationMap.set(i, computeLocationOk(mergedJobs[i].location));
  }
  const noLocationCount = [...preLocationMap.values()].filter(v => v === 'no').length;
  log.info(`Location pre-filter: ${noLocationCount} jobs are location_ok=no and will be skipped (not sent to LLM).`);

  // --- Pre-compute title-based disqualifiers (skip LLM, save cost) ---
  // "Manager" in title: Kyle has no management/CSM experience yet.
  const TITLE_DQ_MANAGER = /\bManager\b/i;
  let titleSkipped = 0;

  function titleDisqualifyReason(title) {
    if (TITLE_DQ_MANAGER.test(title)) return 'Title contains "Manager" — requires management/CSM experience Kyle does not have yet.';
    return null;
  }

  const preTitleDqCount = mergedJobs.filter(j => titleDisqualifyReason(j.title || '')).length;
  log.info(`Title pre-filter: ${preTitleDqCount} jobs have disqualified titles (Manager) and will be skipped.`);

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

  // --- Scoring helper (reused for initial pass and re-score passes) ---
  async function scoreOneJob(job, idx) {
    // Check score cache first — reuse evaluation from previous run
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

    const locationOk = preLocationMap.get(idx) || computeLocationOk(job.location);

    // Skip LLM call entirely for location_ok = "no" — deterministic reject
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

    // Skip LLM call for disqualified titles — deterministic reject
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
          location_ok: locationOk,
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

    // Skip LLM call for expired listings — deterministic reject
    if (preExpiredSet.has(idx)) {
      expiredSkipped += 1;
      const dvt = job.raw?.date_validthrough || '';
      return {
        ...job,
        evaluation: {
          accept: false,
          accepted: false,
          score: 0,
          confidence: 1.0,
          location_ok: locationOk,
          reason_short: `Listing expired (date_validthrough=${dvt}).`,
          reasons: [`Listing expired — date_validthrough=${dvt}.`],
          red_flags: ['Expired listing (pre-filter).'],
          tags: [],
          salary_extracted: '',
          company_url: '',
          role: [],
        },
        scoredAt: nowIso(),
      };
    }

    // For location_ok = "yes" or "unknown", send to LLM without location info
    const jobForPrompt = {
      title: job.title || '',
      company: job.company || '',
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
          'Return ONLY valid JSON, no markdown. Ensure fields: accept, score, confidence, reason_short, reasons, red_flags, tags, salary_extracted, company_url, role.',
      },
      {
        role: 'user',
        content: JSON.stringify(jobForPrompt, null, 2),
      },
    ];

    try {
      const evaluation = await withRetries(
        () => callOpenAIJson({ apiKey, model, messages, stats: openAiStats }),
        { retries: 8, baseMs: 800, maxMs: 30000, stats: openAiStats }
      );

      const score = toInt(evaluation.score ?? evaluation.Score ?? 0, 0);
      const accept = !!(evaluation.accept ?? evaluation.Accept);

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
    } catch (err) {
      if (err?.status || String(err?.message || '').includes('OpenAI')) openAiStats.hardFailures += 1;
      log.error(`Scoring failed for idx=${idx} title="${job.title}": ${err?.message || err}`);

      return {
        ...job,
        evaluation: {
          accept: false,
          accepted: false,
          score: 0,
          confidence: 0,
          location_ok: locationOk,
          reason_short: 'Scoring error',
          reasons: [],
          red_flags: [String(err?.message || err)],
          tags: [],
        },
        scoredAt: nowIso(),
        scoringError: String(err?.stack || err),
      };
    }
  }

  // --- Initial scoring pass ---
  const results = await mapWithConcurrency(mergedJobs, concurrency, scoreOneJob);
  const totalSkipped = locationSkipped + titleSkipped + expiredSkipped;
  log.info(`Initial pass done. ${cacheHits} cache hits, ${locationSkipped} skipped (bad location), ${titleSkipped} skipped (bad title), ${expiredSkipped} skipped (expired), ${mergedJobs.length - totalSkipped - cacheHits} sent to LLM.`);

  // --- Re-score passes for failed jobs (up to 3 additional attempts) ---
  const maxRescoringPasses = 3;
  for (let pass = 1; pass <= maxRescoringPasses; pass++) {
    const failedIndices = [];
    for (let i = 0; i < results.length; i++) {
      if (results[i].scoringError) failedIndices.push(i);
    }

    if (failedIndices.length === 0) {
      log.info(`All jobs scored successfully (no failures to re-score).`);
      break;
    }

    log.info(`Re-scoring pass ${pass}/${maxRescoringPasses}: ${failedIndices.length} failed jobs. Waiting 15s for rate limits to cool...`);
    await sleep(15000);

    const failedJobs = failedIndices.map((i) => ({ job: mergedJobs[i], origIdx: i }));
    const rescored = await mapWithConcurrency(
      failedJobs,
      Math.min(concurrency, 4),
      async (item, _batchIdx) => scoreOneJob(item.job, item.origIdx)
    );

    let fixed = 0;
    for (let k = 0; k < failedIndices.length; k++) {
      if (!rescored[k].scoringError) fixed += 1;
      results[failedIndices[k]] = rescored[k];
    }

    log.info(`Re-scoring pass ${pass}: fixed ${fixed}/${failedIndices.length} jobs.`);
  }

  // --- Post-scoring filters ---

  // 1) "Senior/Sr." filter for Tier 3 roles only (score < 85).
  //    Tier 1 (Game Designer) and Tier 2 (Programmer) Senior titles are kept.
  const TITLE_SENIOR = /\b(Senior|Sr\.?)\b/i;
  let seniorTier3Filtered = 0;
  for (const r of results) {
    if (!r?.evaluation?.accepted) continue;
    const score = r.evaluation.score ?? 0;
    const title = r.title || '';
    if (TITLE_SENIOR.test(title) && score < 85) {
      seniorTier3Filtered++;
      r.evaluation.accepted = false;
      r.evaluation.accept = false;
      r.evaluation.red_flags = [...(r.evaluation.red_flags || []), 'Senior title on Tier 3 role — requires more experience.'];
      r.evaluation.reason_short = `${r.evaluation.reason_short || ''} [SENIOR-FILTERED]`.trim();
    }
  }
  if (seniorTier3Filtered > 0) log.info(`Filtered out ${seniorTier3Filtered} Senior-titled Tier 3 jobs.`);

  // 2) Check LinkedIn URLs for "No longer accepting applications"
  //    Only check accepted jobs with LinkedIn job URLs — small number of fetches.
  const linkedinAccepted = results.filter(
    (r) => r?.evaluation?.accepted && /linkedin\.com\/jobs/i.test(r.applyUrl || r.url || '')
  );
  let linkedinClosedCount = 0;

  if (linkedinAccepted.length > 0) {
    log.info(`Checking ${linkedinAccepted.length} accepted LinkedIn jobs for closed listings...`);

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
    for (let i = 0; i < linkedinAccepted.length; i += LI_BATCH) {
      const batch = linkedinAccepted.slice(i, i + LI_BATCH);
      const checks = await Promise.all(
        batch.map((r) => isLinkedInJobClosed(r.applyUrl || r.url || ''))
      );
      for (let k = 0; k < batch.length; k++) {
        if (checks[k]) {
          linkedinClosedCount++;
          batch[k].evaluation.accepted = false;
          batch[k].evaluation.accept = false;
          batch[k].evaluation.red_flags = [...(batch[k].evaluation.red_flags || []), 'LinkedIn: No longer accepting applications'];
          batch[k].evaluation.reason_short = `${batch[k].evaluation.reason_short || ''} [CLOSED]`.trim();
        }
      }
      // Small delay between batches to be polite
      if (i + LI_BATCH < linkedinAccepted.length) await new Promise((r) => setTimeout(r, 1000));
    }
    log.info(`LinkedIn check: ${linkedinClosedCount} of ${linkedinAccepted.length} are closed.`);
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

  // Build accepted.xlsx + scored.xlsx (Excel format with hyperlinks, frozen panes, bold headers)
  const xlsxContentType = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';

  // Sort accepted jobs by score descending so best matches appear first
  const acceptedSorted = [...acceptedJobs].sort((a, b) => (b.evaluation?.score ?? 0) - (a.evaluation?.score ?? 0));
  const acceptedXlsx = await buildScoredXlsx(acceptedSorted);
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
    log.warning(`${unscoredJobs.length} jobs remain unscored after ${maxRescoringPasses} re-score passes.`);
  }

  if (openAiStats.rateLimit429 > 0) {
    report.warnings = report.warnings || [];
    report.warnings.push(
      `OpenAI rate limit (HTTP 429) occurred ${openAiStats.rateLimit429} times. Requests were retried with exponential backoff. ` +
      `If this persists, lower scoring.concurrency or request higher rate limits.`
    );
  }

  await kv.setValue('scoring_report.json', report);

  // Write score_cache.json for next run's cache + Mantiks detail skip
  const mantikIds = [];
  for (const r of results) {
    for (const sid of (r.sourceJobIds || [])) {
      if (typeof sid === 'string' && sid.startsWith('M:')) mantikIds.push(sid.slice(2));
    }
  }
  await kv.setValue('score_cache.json', {
    scoringFormatVersion: SCORING_FORMAT_VERSION,
    rubricVersion: currentRubricVersion,
    scoredDatasetName,
    mantikIds,
    cachedAt: nowIso(),
  });

  const costStr = estimatedCostUsd != null ? ` LLM cost: $${estimatedCostUsd.toFixed(4)}.` : '';
  const cacheStr = cacheHits > 0 ? ` Cache hits: ${cacheHits}.` : '';
  log.info(`Scoring complete. accepted=${acceptedJobs.length}/${results.length}.${costStr}${cacheStr} accepted.xlsx + scored.xlsx written to KV store.`);
});