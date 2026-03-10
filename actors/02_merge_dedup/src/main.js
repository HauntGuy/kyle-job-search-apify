// actors/02_merge_dedup/src/main.js
// Read raw jobs dataset, merge + de-duplicate, and write merged jobs to a per-run dataset.

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

function canonicalizeUrl(u) {
  if (!u) return '';
  try {
    const url = new URL(u);
    url.hash = '';
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

// Normalize company name for fuzzy dedup across sources
function normalizeCompany(name) {
  if (!name) return '';
  return String(name)
    .toLowerCase()
    .replace(/\b(inc|llc|corp|ltd|co|studios|studio|games|entertainment|interactive|digital|group|holdings|technologies|technology|the)\b/gi, '')
    .replace(/[^a-z0-9]/g, '')   // strip all punctuation/spaces
    .trim();
}

// Normalize title for fuzzy dedup across sources
function normalizeTitle(title) {
  if (!title) return '';
  return String(title)
    .toLowerCase()
    .replace(/\s*\([^)]*\)\s*/g, ' ')       // strip parentheticals like (Remote), (Full-time)
    .replace(/[,\-–—:\/|]/g, ' ')            // normalize separators to spaces
    .replace(/\s+/g, ' ')                     // collapse whitespace
    .trim();
}

// Generate multiple dedup keys per job for cross-source matching
function makeKeys(job) {
  const keys = [];

  // URL-based key (catches exact same listing URL)
  const url = canonicalizeUrl(job.applyUrl || job.url);
  if (url) keys.push(`url:${url}`);

  // Source-specific ID key
  if (job.source && job.sourceJobId) keys.push(`id:${job.source}:${job.sourceJobId}`);

  // Company + Title key (catches same job across different sources)
  const c = normalizeCompany(job.company);
  const t = normalizeTitle(job.title);
  if (c && t) keys.push(`ct:${c}|${t}`);

  // Fallback: if nothing else, use fuzzy location-inclusive key
  if (keys.length === 0) {
    const loc = (job.location || '').toLowerCase().trim();
    keys.push(`fuzzy:${(job.company || '').toLowerCase().trim()}|${(job.title || '').toLowerCase().trim()}|${loc}`);
  }

  return keys;
}

function jobIdPrefix(sourceId) {
  const s = String(sourceId || '');
  if (s.startsWith('fantastic_')) return 'F';
  if (s.startsWith('linkedin_')) return 'L';
  if (s.startsWith('mantiks_')) return 'M';
  if (s === 'rapidapi_jsearch') return 'J';
  if (s === 'remotive') return 'R';
  return '?';
}

function isLinkedInUrl(u) {
  if (!u) return false;
  return /linkedin\.com\/jobs/i.test(String(u));
}

function mergeTwo(a, b, preferLinkedInApply) {
  // Combine sources
  const sources = new Set([...(a.sources || [a.source]).filter(Boolean), ...(b.sources || [b.source]).filter(Boolean)]);
  const urls = new Set([...(a.urls || [a.url]).filter(Boolean), ...(b.urls || [b.url]).filter(Boolean)]);
  const applyUrls = new Set([...(a.applyUrls || [a.applyUrl]).filter(Boolean), ...(b.applyUrls || [b.applyUrl]).filter(Boolean)]);

  const merged = { ...a };

  merged.sources = Array.from(sources);
  merged.urls = Array.from(urls);
  merged.applyUrls = Array.from(applyUrls);

  // Prefer an applyUrl
  const candidates = merged.applyUrls.length ? merged.applyUrls : merged.urls;
  if (preferLinkedInApply) {
    const li = candidates.find(isLinkedInUrl);
    if (li) merged.applyUrl = li;
    else merged.applyUrl = candidates[0] || merged.applyUrl;
  } else {
    merged.applyUrl = candidates[0] || merged.applyUrl;
  }

  // Prefer the "best" url field too
  merged.url = merged.url || merged.applyUrl || merged.urls[0] || '';

  // Union searchTerms
  const searchTerms = new Set([...(a.searchTerms || []), ...(b.searchTerms || [])]);
  merged.searchTerms = Array.from(searchTerms);

  // Union sourceJobIds (prefixed IDs like "F:12345", "L:67890")
  const aIds = a.sourceJobIds || [];
  const bIds = (b.sourceJobIds && b.sourceJobIds.length > 0)
    ? b.sourceJobIds
    : (b.sourceJobId ? [`${jobIdPrefix(b.source)}:${b.sourceJobId}`] : []);
  merged.sourceJobIds = Array.from(new Set([...aIds, ...bIds]));

  // Prefer longer description
  const descA = a.description || '';
  const descB = b.description || '';
  merged.description = descB.length > descA.length ? descB : descA;

  // Fill blanks
  merged.title = merged.title || b.title || '';
  merged.company = merged.company || b.company || '';
  // Prefer a real company website over a linkedin.com/company page
  const aCompanyUrl = merged.companyUrl || '';
  const bCompanyUrl = b.companyUrl || '';
  const aIsLinkedIn = /linkedin\.com\/company/i.test(aCompanyUrl);
  const bIsLinkedIn = /linkedin\.com\/company/i.test(bCompanyUrl);
  if (!aCompanyUrl || (aIsLinkedIn && bCompanyUrl && !bIsLinkedIn)) {
    merged.companyUrl = bCompanyUrl;
  }
  merged.location = merged.location || b.location || '';
  merged.postedAt = merged.postedAt || b.postedAt || '';
  merged.salary = merged.salary || b.salary || '';
  merged.employmentType = merged.employmentType || b.employmentType || '';

  // Track the earliest postedAt across all merged sources (for "Age (days)")
  const aPosted = a.earliestPostedAt || a.postedAt || '';
  const bPosted = b.earliestPostedAt || b.postedAt || '';
  if (aPosted && bPosted) {
    merged.earliestPostedAt = aPosted < bPosted ? aPosted : bPosted;
  } else {
    merged.earliestPostedAt = aPosted || bPosted || '';
  }

  // Preserve raw examples (keep only one or two)
  merged.rawExamples = merged.rawExamples || [];
  if (a.raw) merged.rawExamples.push({ source: a.source, raw: a.raw });
  if (b.raw) merged.rawExamples.push({ source: b.source, raw: b.raw });
  merged.rawExamples = merged.rawExamples.slice(0, 2);
  delete merged.raw;

  merged.mergedAt = nowIso();
  return merged;
}

Actor.main(async () => {
  const input = (await Actor.getInput()) || {};
  const config = await loadConfig(input);

  const runId = input.runId || makeRunId();
  const kvStoreName = input.kvStoreName || config.kvStoreName || 'job-pipeline-v3';
  const datasetPrefix = input.datasetPrefix || config.datasetPrefix || 'jobsearch-v3';

  const kv = await Actor.openKeyValueStore(kvStoreName);

  const rawInfo = await kv.getValue('raw_dataset.json');
  if (!rawInfo?.name) throw new Error('Missing raw_dataset.json in KV store (run 01_collect_jobs first).');

  const rawDataset = await Actor.openDataset(rawInfo.name);

  const mergedDatasetName = datasetName(datasetPrefix, 'merged', runId);
  const mergedDataset = await Actor.openDataset(mergedDatasetName);

  const preferLinkedInApply = config?.merge?.preferLinkedInApply !== false;

  log.info(`Merging raw dataset "${rawInfo.name}" -> "${mergedDatasetName}" (preferLinkedInApply=${preferLinkedInApply})`);

  const startedAt = nowIso();

  const maxItems = Number(config?.run?.maxTotalItems || rawInfo.itemCount || 5000) || 5000;
  const pageSize = 250;

  const groups = new Map();       // groupId -> merged job
  const keyToGroup = new Map();   // dedupKey -> groupId
  let nextGroupId = 0;
  let scanned = 0;
  let duplicates = 0;

  for (let offset = 0; offset < maxItems; offset += pageSize) {
    const { items } = await rawDataset.getData({ offset, limit: pageSize });
    if (!items || items.length === 0) break;

    for (const job of items) {
      scanned += 1;
      const keys = makeKeys(job);

      // Check if ANY key matches an existing group
      let matchedGroupId = null;
      let matchedKey = null;
      for (const k of keys) {
        if (keyToGroup.has(k)) {
          matchedGroupId = keyToGroup.get(k);
          matchedKey = k;
          break;
        }
      }

      if (matchedGroupId != null) {
        // Merge with existing group
        duplicates += 1;
        const existing = groups.get(matchedGroupId);
        const merged = mergeTwo(existing, job, preferLinkedInApply);
        groups.set(matchedGroupId, merged);
        // Register ALL of this job's keys so future matches also find this group
        for (const k of keys) keyToGroup.set(k, matchedGroupId);
        log.info(`Dedup match [${matchedKey}]: "${job.title}" at "${job.company}" (source=${job.source})`);
      } else {
        // New group
        const groupId = nextGroupId++;
        const base = {
          ...job,
          key: keys[0],
          sources: [job.source].filter(Boolean),
          urls: [job.url].filter(Boolean),
          applyUrls: [job.applyUrl].filter(Boolean),
          searchTerms: job.searchTerms || [],
          sourceJobIds: job.sourceJobId ? [`${jobIdPrefix(job.source)}:${job.sourceJobId}`] : [],
          earliestPostedAt: job.postedAt || '',
          mergedAt: nowIso(),
        };
        groups.set(groupId, base);
        for (const k of keys) keyToGroup.set(k, groupId);
      }
    }
  }

  // Push merged
  const mergedJobs = Array.from(groups.values());

  const batchSize = 200;
  for (let i = 0; i < mergedJobs.length; i += batchSize) {
    await mergedDataset.pushData(mergedJobs.slice(i, i + batchSize));
  }

  const finishedAt = nowIso();

  const mergedInfo = {
    name: mergedDatasetName,
    itemCount: mergedJobs.length,
  };

  const report = {
    runId,
    startedAt,
    finishedAt,
    kvStoreName,
    datasetPrefix,
    rawDatasetName: rawInfo.name,
    mergedDatasetName,
    scanned,
    merged: mergedJobs.length,
    duplicates,
    preferLinkedInApply,
  };

  await kv.setValue('merged_dataset.json', mergedInfo);
  await kv.setValue('merge_report.json', report);

  log.info(`Merge complete. scanned=${scanned}, merged=${mergedJobs.length}, duplicates=${duplicates}`);
});
