// actors/03_score_jobs/src/main.js
// Scores merged jobs with an LLM using an external rubric file, writes scored + accepted datasets,
// and produces accepted.csv in the KV store.

import { Actor, log } from 'apify';

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

  const payload = {
    model,
    input: messages,
    temperature: 0.2,
    max_output_tokens: maxOutputTokens,
    text: { format: { type: 'json_object' } },
  };

  const res = await fetch(url, {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${apiKey}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify(payload),
  });

  const retryAfterMs = await parseRetryAfterMs(res.headers?.get?.('retry-after'));

  const json = await res.json().catch(async () => ({ _raw: await res.text() }));

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
      const retryable = status === 429 || status >= 500;

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

function csvEscape(value) {
  const s = (value ?? '').toString();
  const needs = /[",\n]/.test(s);
  const out = s.replace(/"/g, '""');
  return needs ? `"${out}"` : out;
}

function joinSources(sources) {
  const arr = Array.isArray(sources) ? sources : [];
  return arr.filter(Boolean).join(';');
}

// --------------- CSV helpers for Google Sheets HYPERLINK format ---------------

function friendlySourceName(sourceId) {
  const map = {
    'fantastic_feed': 'Fantastic',
    'linkedin_jobs': 'LinkedIn',
    'remotive': 'Remotive',
    'remoteok': 'RemoteOK',
    'rapidapi_jsearch': 'JSearch',
  };
  return map[sourceId] || sourceId || '';
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

function hyperlinkFormula(url, text) {
  const safeText = String(text || '').trim();
  if (!url) return safeText;
  let safeUrl = String(url).trim();
  if (!safeUrl) return safeText;
  // Ensure URL has protocol
  if (!/^https?:\/\//i.test(safeUrl)) safeUrl = `https://${safeUrl}`;
  return `=HYPERLINK("${safeUrl}","${safeText || safeUrl}")`;
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

// Build a CSV string from an array of scored jobs using the standard column format
function buildScoredCsv(jobs) {
  const header = [
    'Company', 'Job Title', 'Salary', 'Where', 'Score',
    'Age (days)', 'Where Found', 'Sources', 'Reason', 'Tags', 'Red Flags'
  ];

  const lines = [header.map(csvEscape).join(',')];

  for (const j of jobs) {
    const ev = j.evaluation || {};
    const tags = Array.isArray(ev.tags) ? ev.tags.join('; ') : '';
    const redFlags = Array.isArray(ev.red_flags) ? ev.red_flags.join('; ') : '';

    // Company: structured companyUrl → LLM company_url → plain text
    const companyUrlFinal = j.companyUrl || ev.company_url || '';
    const companyCell = hyperlinkFormula(companyUrlFinal, j.company || '');

    // Job Title: clickable link to job (LinkedIn URL preferred via merge step)
    const jobUrl = j.applyUrl || j.url || '';
    const jobTitleCell = hyperlinkFormula(jobUrl, j.title || '');

    // Salary: structured → LLM-extracted → formatted
    const salary = formatSalary(j.salary || ev.salary_extracted || '');

    // Where: location
    const where = j.location || '';

    // Score
    const score = ev.score ?? 0;

    // Age (days): days since earliest posting date
    const ageDays = calculateAgeDays(j.earliestPostedAt || j.postedAt);

    // Where Found: root domain of the job listing URL, with friendly name
    const foundUrl = j.url || j.applyUrl || '';
    const rootUrl = extractRootDomainUrl(foundUrl);
    const foundName = friendlyDomainName(foundUrl, j.company);
    const whereFoundCell = hyperlinkFormula(rootUrl, foundName);

    // Sources: friendly names of job feeds that found this job
    const sourcesArr = Array.isArray(j.sources) ? j.sources : [j.source].filter(Boolean);
    const sourcesStr = sourcesArr.map(friendlySourceName).filter(Boolean).join(', ');

    // Reason, Tags, Red Flags
    const reason = ev.reason_short || '';

    const row = [
      companyCell, jobTitleCell, salary, where, score,
      ageDays, whereFoundCell, sourcesStr, reason, tags, redFlags,
    ];

    lines.push(row.map(csvEscape).join(','));
  }

  return lines.join('\n') + '\n';
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
  const concurrency = Number(scoringCfg.concurrency ?? 4) || 4;
  const maxDescChars = Number(scoringCfg.maxDescriptionChars ?? 12000) || 12000;
  const rubricUrl = String(scoringCfg.rubricUrl || '');

  const rubricText = await loadRubricText(rubricUrl);

  log.info(`Scoring merged dataset "${mergedDatasetName}" -> scored="${scoredDatasetName}", accepted="${acceptedDatasetName}"`);
  log.info(`Model=${model}, threshold=${threshold}, concurrency=${concurrency}, gateOnLocation=${gateOnLocation}`);

  const mergedJobs = [];
  const pageSize = 250;
  for (let offset = 0; ; offset += pageSize) {
    const { items } = await mergedDataset.getData({ offset, limit: pageSize });
    if (!items || items.length === 0) break;
    mergedJobs.push(...items);
  }
  log.info(`Loaded ${mergedJobs.length} merged jobs.`);

  const openAiStats = {
    calls: 0,
    retries: 0,
    rateLimit429: 0,
    serverErrors: 0,
    maxRetryAfterMs: 0,
    hardFailures: 0,
  };

  const results = await mapWithConcurrency(mergedJobs, concurrency, async (job, idx) => {
    const jobForPrompt = {
      title: job.title || '',
      company: job.company || '',
      location: job.location || '',
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
          'Return ONLY valid JSON, no markdown. Ensure fields: accept, score, confidence, location_ok, reason_short, reasons, red_flags, tags, salary_extracted, company_url.',
      },
      {
        role: 'user',
        content: JSON.stringify(jobForPrompt, null, 2),
      },
    ];

    try {
      const evaluation = await withRetries(
        () => callOpenAIJson({ apiKey, model, messages, stats: openAiStats }),
        { retries: 5, baseMs: 800, maxMs: 12000, stats: openAiStats }
      );

      const score = toInt(evaluation.score ?? evaluation.Score ?? 0, 0);
      const accept = !!(evaluation.accept ?? evaluation.Accept);
      const locationOk = normalizeLocationOk(evaluation.location_ok ?? evaluation.locationOk ?? evaluation.location);

      const accepted =
        accept &&
        score >= threshold &&
        (!gateOnLocation || locationOk === 'yes');

      const scored = {
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

      return scored;
    } catch (err) {
      if (err?.status || String(err?.message || '').includes('OpenAI')) openAiStats.hardFailures += 1;
      log.error(`Scoring failed for idx=${idx} title="${job.title}": ${err?.message || err}`);

      const scored = {
        ...job,
        evaluation: {
          accept: false,
          accepted: false,
          score: 0,
          confidence: 0,
          location_ok: 'unknown',
          reason_short: 'Scoring error',
          reasons: [],
          red_flags: [String(err?.message || err)],
          tags: [],
        },
        scoredAt: nowIso(),
        scoringError: String(err?.stack || err),
      };
      return scored;
    }
  });

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

  // Build accepted.csv + scored.csv (Google Sheets HYPERLINK format)
  const acceptedCsv = buildScoredCsv(acceptedJobs);
  await kv.setValue('accepted.csv', acceptedCsv, { contentType: 'text/csv; charset=utf-8' });

  // scored.csv contains ALL scored jobs (accepted + rejected) for review
  // Sort by score descending so best matches appear first
  const allSorted = [...results].sort((a, b) => (b.evaluation?.score ?? 0) - (a.evaluation?.score ?? 0));
  const scoredCsv = buildScoredCsv(allSorted);
  await kv.setValue('scored.csv', scoredCsv, { contentType: 'text/csv; charset=utf-8' });

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
    totalScored: results.length,
    accepted: acceptedJobs.length,
    threshold,
    gateOnLocation,
    model,
    rubricUrl,
    openai: openAiStats,
  };

  if (openAiStats.rateLimit429 > 0) {
    report.warnings = report.warnings || [];
    report.warnings.push(
      `OpenAI rate limit (HTTP 429) occurred ${openAiStats.rateLimit429} times. Requests were retried with exponential backoff. ` +
      `If this persists, lower scoring.concurrency or request higher rate limits.`
    );
  }

  await kv.setValue('scoring_report.json', report);

  log.info(`Scoring complete. accepted=${acceptedJobs.length}/${results.length}. accepted.csv + scored.csv written to KV store.`);
});