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

async function fetchText(url, headers = {}) {
  const u = url.includes('?') ? `${url}&cb=${Date.now()}` : `${url}?cb=${Date.now()}`;
  const res = await fetch(u, { method: 'GET', headers });
  const text = await res.text();
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}: ${text.slice(0, 500)}`);
  return text;
}

async function fetchJson(url, headers = {}) {
  const text = await fetchText(url, { ...headers, 'Accept': 'application/json' });
  try {
    return JSON.parse(text);
  } catch (e) {
    throw new Error(`Config at ${url} is not valid JSON: ${e?.message || e}`);
  }
}

async function loadConfig(input) {
  if (input?.config && typeof input.config === 'object') return input.config;

  const configUrl =
    input?.configUrl ||
    process.env.JOBSEARCH_CONFIG_URL ||
    process.env.CONFIG_URL;

  if (!configUrl) {
    throw new Error('Missing configUrl (set in task input, or JOBSEARCH_CONFIG_URL env var).');
  }
  return await fetchJson(configUrl);
}

function csvEscape(val) {
  const s = String(val ?? '');
  if (/[",\n]/.test(s)) return `"${s.replace(/"/g, '""')}"`;
  return s;
}

function toInt(v, d = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? Math.round(n) : d;
}

function toFloat(v, d = 0) {
  const n = Number(v);
  return Number.isFinite(n) ? n : d;
}

function normalizeLocationOk(v) {
  if (v === true) return 'yes';
  if (v === false) return 'no';
  if (!v) return 'unknown';
  const s = String(v).toLowerCase();
  if (s.includes('yes') || s.includes('ok') || s.includes('true')) return 'yes';
  if (s.includes('no') || s.includes('false')) return 'no';
  return 'unknown';
}

async function sleep(ms) {
  await new Promise((r) => setTimeout(r, ms));
}

async function callOpenAIJson({ apiKey, model, messages, maxOutputTokens = 700 }) {
  // Uses the OpenAI Responses API.
  // NOTE: This actor expects OPENAI_API_KEY to be set as an env var.
  const url = 'https://api.openai.com/v1/responses';

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

  const json = await res.json().catch(async () => ({ _raw: await res.text() }));

  if (!res.ok) {
    const msg = json?.error?.message || JSON.stringify(json).slice(0, 500);
    const code = json?.error?.code || res.status;
    const err = new Error(`OpenAI API error (${res.status} / ${code}): ${msg}`);
    err.status = res.status;
    err.body = json;
    throw err;
  }

  const text = json?.output_text;
  if (!text) {
    throw new Error(`OpenAI response missing output_text: ${JSON.stringify(json).slice(0, 500)}`);
  }

  try {
    return JSON.parse(text);
  } catch (e) {
    throw new Error(`Model did not return valid JSON: ${e?.message || e}\n${text.slice(0, 500)}`);
  }
}

async function withRetries(fn, { retries = 3, baseMs = 800 } = {}) {
  let attempt = 0;
  while (true) {
    try {
      return await fn();
    } catch (e) {
      attempt += 1;
      const status = e?.status || 0;
      const retryable = status === 429 || status >= 500;
      if (attempt > retries || !retryable) throw e;
      const wait = baseMs * Math.pow(2, attempt - 1);
      log.warning(`Retryable error (status=${status}). Waiting ${wait}ms then retrying...`);
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

  const model = String(scoringCfg.model || 'gpt-4o-mini');
  const threshold = toInt(scoringCfg.threshold ?? 70, 70);
  const gateOnLocation = !!scoringCfg.gateOnLocation;
  const concurrency = toInt(scoringCfg.concurrency ?? 4, 4);
  const maxDescChars = toInt(scoringCfg.maxDescriptionChars ?? 12000, 12000);

  const rubricUrl = String(scoringCfg.rubricUrl || '').trim();
  if (!rubricUrl) throw new Error('Missing config.scoring.rubricUrl (should point to a text/markdown rubric file).');

  const rubricText = await fetchText(rubricUrl, { 'Accept': 'text/plain,text/markdown,text/*' });

  const kv = await Actor.openKeyValueStore(kvStoreName);

  const mergedInfo = await kv.getValue('merged_dataset.json');
  if (!mergedInfo?.name) throw new Error('Missing merged_dataset.json in KV store (run 02_merge_dedup first).');

  const mergedDataset = await Actor.openDataset(mergedInfo.name);

  const scoredDatasetName = datasetName(datasetPrefix, 'scored', runId);
  const acceptedDatasetName = datasetName(datasetPrefix, 'accepted', runId);
  const scoredDataset = await Actor.openDataset(scoredDatasetName);
  const acceptedDataset = await Actor.openDataset(acceptedDatasetName);

  log.info(`Scoring merged dataset "${mergedInfo.name}" -> scored="${scoredDatasetName}", accepted="${acceptedDatasetName}"`);
  log.info(`Model=${model}, threshold=${threshold}, concurrency=${concurrency}, gateOnLocation=${gateOnLocation}`);

  const startedAt = nowIso();

  // Load all merged jobs (paginate)
  const pageSize = 100;
  const mergedJobs = [];
  for (let offset = 0; ; offset += pageSize) {
    const { items } = await mergedDataset.getData({ offset, limit: pageSize });
    if (!items || items.length === 0) break;
    mergedJobs.push(...items);
  }
  log.info(`Loaded ${mergedJobs.length} merged jobs.`);

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
          'Return ONLY valid JSON, no markdown. Ensure fields: accept, score, confidence, location_ok, reason_short, reasons, red_flags, tags.',
      },
      {
        role: 'user',
        content: JSON.stringify(jobForPrompt, null, 2),
      },
    ];

    try {
      const evaluation = await withRetries(
        () => callOpenAIJson({ apiKey, model, messages }),
        { retries: 3, baseMs: 900 }
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

  for (let i = 0; i < results.length; i += scoredBatch) {
    const batch = results.slice(i, i + scoredBatch);
    await scoredDataset.pushData(batch);

    const accepted = batch.filter((j) => j?.evaluation?.accepted);
    if (accepted.length) {
      acceptedCount += accepted.length;
      await acceptedDataset.pushData(accepted);
    }
  }

  // Build accepted.csv (Google Sheets-friendly)
  const acceptedJobs = results.filter((j) => j?.evaluation?.accepted);

  const header = [
    'score',
    'company',
    'title',
    'location',
    'sources',
    'apply_link',
    'apply_url',
    'job_link',
    'job_url',
    'posted_at',
    'reason_short',
    'tags',
    'red_flags',
  ];

  const rows = [header.join(',')];

  for (const j of acceptedJobs) {
    const score = toInt(j?.evaluation?.score ?? 0, 0);
    const company = j.company || '';
    const title = j.title || '';
    const location = j.location || '';
    const sources = (j.sources || [j.source].filter(Boolean)).join('; ');
    const applyUrl = j.applyUrl || j.url || '';
    const jobUrl = j.url || '';

    const applyLink = applyUrl ? `=HYPERLINK("${applyUrl.replace(/"/g, '""')}","apply")` : '';
    const jobLink = jobUrl ? `=HYPERLINK("${jobUrl.replace(/"/g, '""')}","job")` : '';

    const postedAt = j.postedAt || '';
    const reasonShort = j?.evaluation?.reason_short || '';
    const tags = Array.isArray(j?.evaluation?.tags) ? j.evaluation.tags.join('; ') : (j?.evaluation?.tags || '');
    const redFlags = Array.isArray(j?.evaluation?.red_flags) ? j.evaluation.red_flags.join('; ') : (j?.evaluation?.red_flags || '');

    const row = [
      score,
      csvEscape(company),
      csvEscape(title),
      csvEscape(location),
      csvEscape(sources),
      csvEscape(applyLink),
      csvEscape(applyUrl),
      csvEscape(jobLink),
      csvEscape(jobUrl),
      csvEscape(postedAt),
      csvEscape(reasonShort),
      csvEscape(tags),
      csvEscape(redFlags),
    ].join(',');

    rows.push(row);
  }

  const acceptedCsv = rows.join('\n') + '\n';
  await kv.setValue('accepted.csv', acceptedCsv, { contentType: 'text/csv; charset=utf-8' });

  const finishedAt = nowIso();

  const scoredInfo = { name: scoredDatasetName, itemCount: results.length };
  const acceptedInfo = { name: acceptedDatasetName, itemCount: acceptedJobs.length };

  await kv.setValue('scored_dataset.json', scoredInfo);
  await kv.setValue('accepted_dataset.json', acceptedInfo);

  const report = {
    runId,
    startedAt,
    finishedAt,
    kvStoreName,
    datasetPrefix,
    mergedDatasetName: mergedInfo.name,
    scoredDatasetName,
    acceptedDatasetName,
    totalMerged: mergedJobs.length,
    totalScored: results.length,
    accepted: acceptedJobs.length,
    threshold,
    gateOnLocation,
    model,
    rubricUrl,
  };
  await kv.setValue('scoring_report.json', report);

  log.info(`Scoring complete. accepted=${acceptedJobs.length}/${results.length}. accepted.csv written to KV store.`);
});
