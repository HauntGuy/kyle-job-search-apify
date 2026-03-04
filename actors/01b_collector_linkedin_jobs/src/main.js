// 01b_collector_linkedin_jobs/src/main.js â€” v0.1.0
// Calls Fantastic.jobs Advanced LinkedIn Job Search API and writes normalized results to KV store.
// If the LinkedIn actor schema differs, set linkedin.inputOverride in task input.

import { Actor } from 'apify';

function slugify(input) {
  return (input || '')
    .toString()
    .toLowerCase()
    .replace(/https?:\/\//g, '')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 60) || null;
}

function best(obj, keys, fallback = '') {
  for (const k of keys) {
    const v = obj?.[k];
    if (typeof v === 'string' && v.trim()) return v.trim();
  }
  return fallback;
}

function normalizeLinkedInItem(it) {
  const title = best(it, ['title', 'jobTitle', 'positionTitle'], '');
  const url = best(it, ['url', 'jobUrl', 'applyUrl', 'link'], '');
  const company = best(it, ['company', 'companyName', 'organizationName'], '') ||
                  best(it?.company || {}, ['name', 'companyName'], 'Unknown');
  const location = best(it, ['location', 'jobLocation', 'locationName'], '');
  const published = best(it, ['datePosted', 'postedAt', 'listedAt', 'publishedAt'], '');

  return {
    title,
    url,
    location,
    published,
    salary: '',
    raw: it,
    source: 'linkedin',
    company,
    company_slug: slugify(company),
    whereFound: 'LinkedIn'
  };
}

Actor.main(async () => {
  const input = await Actor.getInput() || {};
  const kvStoreName = (input.kvStoreName || 'job-pipeline').toString();
  const outputKey = (input.outputKey || 'collector_linkedin_jobs.json').toString();
  const snapshotKey = (input.snapshotKey || 'snapshot_01b_linkedin.json').toString();
  const manifestKey = 'manifest_01b_linkedin.log';

  const kv = await Actor.openKeyValueStore(kvStoreName);

  const cfg = input.linkedin || {};
  const actorId = cfg.actorId || 'fantastic-jobs/advanced-linkedin-job-search-api';
  const limit = Number(cfg.limit || 200);

  const liInput = cfg.inputOverride || {
    limit,
    keywords: cfg.keywords || ['Unity'],
    location: cfg.location || 'United States',
    workplaceTypes: cfg.workplaceTypes || ['REMOTE', 'HYBRID', 'ON_SITE']
  };

  const manifest = [];
  manifest.push(`CALL actor ${actorId} input=${JSON.stringify(liInput)}`);

  const run = await Actor.call(actorId, liInput);
  const datasetId = run?.defaultDatasetId;
  if (!datasetId) throw new Error(`LinkedIn actor call succeeded but no defaultDatasetId returned (runId=${run?.id || 'unknown'})`);

  const client = Actor.apifyClient;
  const { items } = await client.dataset(datasetId).listItems({ limit });

  const normalized = (items || []).map(normalizeLinkedInItem).filter(r => (r.url || '').trim());

  const seen = new Set();
  const deduped = [];
  for (const r of normalized) {
    const u = (r.url || '').trim();
    if (!u) continue;
    if (seen.has(u)) continue;
    seen.add(u);
    deduped.push(r);
  }

  const snapshot = {
    startedAt: new Date().toISOString(),
    fetchMode: 'linkedin_jobs_api',
    records_normalized: deduped.length,
    linkedin: { runId: run?.id || null, datasetId, returned: deduped.length },
    finishedAt: new Date().toISOString()
  };

  await kv.setValue(manifestKey, manifest.join('\n'));
  await kv.setValue(outputKey, deduped);
  await kv.setValue(snapshotKey, snapshot);

  console.log(`01b complete. Wrote ${deduped.length} -> ${kvStoreName}/${outputKey}`);
});
