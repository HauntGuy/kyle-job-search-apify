// actors/01_collect_jobs/src/main.js
// Collect jobs from multiple configured sources and write normalized records to a per-run dataset.

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
    throw new Error(`Non-JSON response from ${url}: ${e?.message || e}\n${text.slice(0, 500)}`);
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

function getEnvOrNull(name) {
  const v = process.env[name];
  if (!v) return null;
  return String(v);
}

function requireEnvs(required = []) {
  const missing = [];
  for (const k of required) {
    if (!process.env[k]) missing.push(k);
  }
  return missing;
}

function firstString(...vals) {
  for (const v of vals) {
    if (typeof v === 'string' && v.trim()) return v.trim();
  }
  return '';
}

function asArray(v) {
  if (!v) return [];
  return Array.isArray(v) ? v : [v];
}

function toIsoOrEmpty(v) {
  if (!v) return '';
  const s = String(v).trim();
  if (!s) return '';
  // If it's already ISO-ish, keep it
  if (/\d{4}-\d{2}-\d{2}T/.test(s)) return s;
  // Try Date.parse
  const ms = Date.parse(s);
  if (!Number.isFinite(ms)) return s;
  return new Date(ms).toISOString();
}

function canonicalizeUrl(u) {
  if (!u) return '';
  try {
    const url = new URL(u);
    url.hash = '';
    // Remove common tracking params
    const dropPrefixes = ['utm_', 'fbclid', 'gclid', 'msclkid', 'ref', 'src', 'source', 'tracking', 'trk'];
    for (const [k] of Array.from(url.searchParams.entries())) {
      const lk = k.toLowerCase();
      if (dropPrefixes.some((p) => lk === p || lk.startsWith(p))) url.searchParams.delete(k);
    }
    return url.toString();
  } catch {
    return String(u);
  }
}

function normalizeGeneric(sourceId, raw) {
  const title = firstString(raw.title, raw.job_title, raw.position, raw.role, raw.jobTitle);
  const company = firstString(
    raw.organization,
    raw.company,
    raw.companyName,
    raw.employer_name,
    raw.employerName,
    raw.company_name
  );
  const location = firstString(
    raw.location,
    raw.job_location,
    raw.jobLocation,
    raw.candidate_required_location,
    raw.city,
    raw.job_city,
    raw.job_state
  );

  const url = firstString(raw.url, raw.job_url, raw.jobUrl, raw.link, raw.job_google_link, raw.job_apply_link);
  const applyUrl = firstString(raw.apply_url, raw.applyUrl, raw.job_apply_link, raw.application_url, raw.apply_link);

  const description = firstString(raw.description_text, raw.description, raw.job_description, raw.descriptionText);

  const postedAt = firstString(raw.job_posted_at_datetime_utc, raw.publication_date, raw.date, raw.postedAt);

  const out = {
    source: sourceId,
    fetchedAt: nowIso(),
    title,
    company,
    location,
    url: canonicalizeUrl(url),
    applyUrl: canonicalizeUrl(applyUrl || url),
    postedAt: toIsoOrEmpty(postedAt),
    description,
    // Keep original raw for debugging
    raw,
  };

  // Best-effort source id
  out.sourceJobId = firstString(raw.id, raw.job_id, raw.jobId, raw.guid);

  return out;
}

function normalizeFantasticFeed(sourceId, raw) {
  // Fantastic feed fields are relatively stable, but still do best-effort.
  const title = firstString(raw.title);
  const company = firstString(raw.organization, raw.company, raw.company_name);
  const url = firstString(raw.url, raw.job_url);
  const applyUrl = firstString(raw.apply_url, raw.applyUrl, raw.apply_link, raw.application_url, url);

  const locationsDerived = asArray(raw.locations_derived).filter(Boolean);
  const locationsRaw = asArray(raw.locations_raw).filter(Boolean);
  const remote = !!raw.remote_derived || (typeof raw.work_arrangement_derived === 'string' && raw.work_arrangement_derived.toLowerCase().includes('remote'));
  const hybrid = !!raw.hybrid_derived || (typeof raw.work_arrangement_derived === 'string' && raw.work_arrangement_derived.toLowerCase().includes('hybrid'));

  const locParts = [];
  if (remote) locParts.push('Remote');
  if (hybrid) locParts.push('Hybrid');
  if (locationsDerived.length) locParts.push(locationsDerived.join('; '));
  else if (locationsRaw.length) locParts.push(locationsRaw.join('; '));
  else if (raw.location) locParts.push(String(raw.location));

  const description = firstString(raw.description_text, raw.description, raw.description_html);
  const postedAt = firstString(raw.date_posted, raw.posted_at, raw.postedAt, raw.updated_at, raw.updatedAt);

  const salary = firstString(raw.salary_range_derived, raw.salary_range, raw.salary);
  const employmentType = firstString(raw.employment_type_derived, raw.employment_type);

  const out = {
    source: sourceId,
    fetchedAt: nowIso(),
    title,
    company,
    location: locParts.filter(Boolean).join(' | '),
    url: canonicalizeUrl(url),
    applyUrl: canonicalizeUrl(applyUrl),
    postedAt: toIsoOrEmpty(postedAt),
    description,
    salary,
    employmentType,
    raw,
  };

  out.sourceJobId = firstString(raw.id, raw.job_id, raw.jobId);
  return out;
}

function normalizeJSearch(sourceId, raw) {
  // https://jsearch.p.rapidapi.com/search
  const title = firstString(raw.job_title, raw.title);
  const company = firstString(raw.employer_name, raw.company_name, raw.company);
  const location = firstString(raw.job_location, [raw.job_city, raw.job_state, raw.job_country].filter(Boolean).join(', '));
  const url = firstString(raw.job_google_link, raw.job_apply_link, raw.job_link);
  const applyUrl = firstString(raw.job_apply_link, raw.job_google_link, url);
  const postedAt = firstString(raw.job_posted_at_datetime_utc, raw.job_posted_at_timestamp);
  const description = firstString(raw.job_description);
  const employmentType = firstString(raw.job_employment_type);
  const salary = firstString(raw.job_salary, raw.job_min_salary && raw.job_max_salary ? `${raw.job_min_salary}-${raw.job_max_salary}` : '');

  const out = {
    source: sourceId,
    fetchedAt: nowIso(),
    title,
    company,
    location,
    url: canonicalizeUrl(url),
    applyUrl: canonicalizeUrl(applyUrl),
    postedAt: toIsoOrEmpty(postedAt),
    description,
    employmentType,
    salary,
    raw,
    sourceJobId: firstString(raw.job_id),
  };
  return out;
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

async function runApifyActorSource(source, globalMaxItemsPerSource) {
  const actorId = String(source.actorId);
  const input = source.input || {};
  const maxItems = Math.min(
    Number(source.maxItems || input.maxItems || globalMaxItemsPerSource || 200) || 200,
    globalMaxItemsPerSource || 200
  );

  log.info(`[${source.id}] Calling Apify actor ${actorId} (maxItems=${maxItems})`);
  const run = await Actor.call(actorId, input);

  const status = run?.status || 'UNKNOWN';
  if (status !== 'SUCCEEDED') {
    throw new Error(`[${source.id}] Called actor did not succeed (status=${status}, runId=${run?.id || 'unknown'})`);
  }

  const datasetId = run?.defaultDatasetId;
  if (!datasetId) throw new Error(`[${source.id}] Missing defaultDatasetId in Actor.call result`);

  const rawItems = await listDatasetItems(datasetId, maxItems);
  log.info(`[${source.id}] Fetched ${rawItems.length} items from dataset ${datasetId}`);

  const adapter = String(source.adapter || 'generic');
  const jobs = rawItems.map((it) => {
    if (adapter === 'fantastic_feed') return normalizeFantasticFeed(source.id, it);
    if (adapter === 'jsearch') return normalizeJSearch(source.id, it);
    return normalizeGeneric(source.id, it);
  });

  return { jobs, meta: { actorId, runId: run?.id || null, datasetId, itemCount: jobs.length } };
}

async function runRapidApiJSearch(source) {
  const apiKey = getEnvOrNull('RAPIDAPI_KEY');
  if (!apiKey) throw new Error(`[${source.id}] Missing RAPIDAPI_KEY env var`);

  const query = String(source.query || '').trim();
  if (!query) throw new Error(`[${source.id}] Missing source.query`);

  const page = Number(source.page || 1) || 1;
  const numPages = Number(source.num_pages || 1) || 1;
  const datePosted = String(source.date_posted || 'all');

  const params = new URLSearchParams({
    query,
    page: String(page),
    num_pages: String(numPages),
    date_posted: datePosted,
  });

  const url = `https://jsearch.p.rapidapi.com/search?${params.toString()}`;
  log.info(`[${source.id}] GET ${url}`);

  const json = await fetchJson(url, {
    'X-RapidAPI-Key': apiKey,
    'X-RapidAPI-Host': 'jsearch.p.rapidapi.com',
  });

  const items = Array.isArray(json?.data) ? json.data : [];
  const jobs = items.map((it) => normalizeJSearch(source.id, it));

  return { jobs, meta: { itemCount: jobs.length } };
}

async function runRemotive(source) {
  const q = String(source.query || '').trim();
  const limit = Number(source.limit || 200) || 200;

  const params = new URLSearchParams();
  if (q) params.set('search', q);
  // Remotive supports "limit" in practice, but if it doesn't, we just slice.
  params.set('limit', String(limit));

  const url = `https://remotive.com/api/remote-jobs?${params.toString()}`;
  log.info(`[${source.id}] GET ${url}`);

  const json = await fetchJson(url);
  const items = Array.isArray(json?.jobs) ? json.jobs : [];

  const jobs = items.slice(0, limit).map((it) => normalizeGeneric(source.id, it));
  return { jobs, meta: { itemCount: jobs.length } };
}

async function runRemoteOk(source) {
  const url = 'https://remoteok.com/api';
  log.info(`[${source.id}] GET ${url}`);

  // RemoteOK asks for a user agent; some setups may block otherwise.
  const json = await fetchJson(url, { 'User-Agent': 'Mozilla/5.0 (jobsearch-bot)' });

  const items = Array.isArray(json) ? json : [];
  // First element is usually metadata; job items have "position"
  const jobItems = items.filter((it) => it && typeof it === 'object' && (it.position || it.company));

  const jobs = jobItems.map((it) => normalizeGeneric(source.id, it));
  return { jobs, meta: { itemCount: jobs.length } };
}

async function runSource(source, config) {
  const globalMax = Number(config?.run?.maxItemsPerSource || 300) || 300;

  const type = String(source.type || '').toLowerCase();

  if (type === 'apify_actor') return await runApifyActorSource(source, globalMax);
  if (type === 'rapidapi_jsearch') return await runRapidApiJSearch(source);
  if (type === 'remotive') return await runRemotive(source);
  if (type === 'remoteok') return await runRemoteOk(source);

  throw new Error(`[${source.id}] Unknown source.type=${source.type}`);
}

Actor.main(async () => {
  const input = (await Actor.getInput()) || {};
  const config = await loadConfig(input);

  const runId = input.runId || makeRunId();
  const kvStoreName = input.kvStoreName || config.kvStoreName || 'job-pipeline-v3';
  const datasetPrefix = input.datasetPrefix || config.datasetPrefix || 'jobsearch-v3';

  const kv = await Actor.openKeyValueStore(kvStoreName);

  const rawDatasetName = datasetName(datasetPrefix, 'raw', runId);
  const rawDataset = await Actor.openDataset(rawDatasetName);

  const startedAt = nowIso();
  const report = {
    runId,
    startedAt,
    kvStoreName,
    datasetPrefix,
    rawDatasetName,
    sources: [],
    totals: { collected: 0, pushed: 0, skipped: 0, errors: 0 },
  };

  const sources = Array.isArray(config?.sources) ? config.sources : [];
  const maxTotal = Number(config?.run?.maxTotalItems || 1200) || 1200;

  // Collect sequentially for simplicity; each source may have its own rate limits.
  const allJobs = [];

  for (const src of sources) {
    const source = { ...src };
    if (!source.id) source.id = source.actorId || source.type || 'source';

    if (source.enabled === false) {
      report.sources.push({ id: source.id, status: 'disabled' });
      report.totals.skipped += 1;
      continue;
    }

    const missing = requireEnvs(source.requiredEnv || []);
    if (missing.length) {
      report.sources.push({ id: source.id, status: 'skipped_missing_env', missingEnv: missing });
      report.totals.skipped += 1;
      continue;
    }

    const started = Date.now();
    try {
      const { jobs, meta } = await runSource(source, config);

      // Enforce maxTotal across all sources
      const remaining = Math.max(0, maxTotal - allJobs.length);
      const trimmed = remaining > 0 ? jobs.slice(0, remaining) : [];

      allJobs.push(...trimmed);

      report.sources.push({
        id: source.id,
        type: source.type,
        status: 'ok',
        ms: Date.now() - started,
        itemCount: jobs.length,
        usedCount: trimmed.length,
        meta,
      });

      report.totals.collected += jobs.length;

      if (allJobs.length >= maxTotal) {
        log.info(`Reached maxTotalItems=${maxTotal}; stopping further source collection.`);
        break;
      }
    } catch (err) {
      report.sources.push({
        id: source.id,
        type: source.type,
        status: 'error',
        ms: Date.now() - started,
        error: String(err?.message || err),
      });
      report.totals.errors += 1;

      log.error(`[${source.id}] Source failed: ${err?.stack || err}`);

      if (config?.run?.stopOnCollectorErrors) {
        throw err;
      }
    }
  }

  // Push normalized jobs to dataset in batches
  const batchSize = 200;
  let pushed = 0;
  for (let i = 0; i < allJobs.length; i += batchSize) {
    const batch = allJobs.slice(i, i + batchSize);
    await rawDataset.pushData(batch);
    pushed += batch.length;
  }
  report.totals.pushed = pushed;

  const finishedAt = nowIso();
  report.finishedAt = finishedAt;
  report.durationSecs = Math.round((Date.parse(finishedAt) - Date.parse(startedAt)) / 1000);

  // Store dataset info + report
  const datasetInfo = { id: rawDataset.getId?.() || null, name: rawDatasetName, itemCount: pushed };
  await kv.setValue('raw_dataset.json', datasetInfo);
  await kv.setValue('collect_report.json', report);

  log.info(`Collection complete. Pushed ${pushed} jobs to dataset ${rawDatasetName}`);
});
