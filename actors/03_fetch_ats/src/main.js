// 03_fetch_ats/main.js — v2.4 (Feed mode default) 
// Fetch jobs via Fantastic.jobs “Career Site Job Listing Feed” (6-month active jobs).
// Writes to KV store "job-pipeline":
// - merged.json (normalized records)
// - manifest.log (every fetch + status/errors)
// - fetch_snapshot.json (counters)

import { Actor } from 'apify';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function safeLower(s) { return (s || '').toString().toLowerCase(); }

function slugify(input) {
  return (input || '')
    .toString()
    .toLowerCase()
    .replace(/https?:\/\//g, '')
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 60) || null;
}

// Best-effort location formatting from Fantastic feed items.
function fmtLocationFromFantastic(item) {
  const workArr = item?.ai_work_arrangement || item?.aiWorkArrangement || '';
  const locationType = item?.location_type || item?.locationType || '';
  const remoteDerived = item?.remote_derived === true || item?.remoteDerived === true;

  if (remoteDerived || locationType === 'TELECOMMUTE' || /remote/i.test(workArr)) {
    const rl =
      item?.ai_remote_location?.[0] ||
      item?.ai_remote_location_derived?.[0] ||
      item?.aiRemoteLocation?.[0] ||
      item?.aiRemoteLocationDerived?.[0] ||
      null;

    if (rl && typeof rl === 'string') return `Remote (${rl})`;

    const c = item?.countries_derived?.[0] || item?.countriesDerived?.[0] || null;
    if (c) return `Remote (${c})`;

    return 'Remote';
  }

  const locObj = item?.locations_derived?.[0] || item?.locationsDerived?.[0] || null;
  if (locObj && typeof locObj === 'object') {
    const city = locObj.city || '';
    const admin = locObj.admin || locObj.region || '';
    const country = locObj.country || '';
    const parts = [city, admin, country].filter(Boolean);
    if (parts.length) return parts.join(', ');
  }

  const raw = item?.locations_raw?.[0] || item?.locations_alt_raw?.[0] || item?.locationsRaw?.[0] || null;
  if (raw) return typeof raw === 'string' ? raw : JSON.stringify(raw);

  // fallback
  return item?.location || item?.organization || null;
}

function fmtSalaryFromFantastic(item) {
  const cur = item?.ai_salary_currency || item?.aiSalaryCurrency || null;
  const unit = item?.ai_salary_unittext || item?.aiSalaryUnittext || null;
  const min = item?.ai_salary_minvalue ?? item?.aiSalaryMinvalue;
  const max = item?.ai_salary_maxvalue ?? item?.aiSalaryMaxvalue;
  const val = item?.ai_salary_value ?? item?.aiSalaryValue;

  const fmtNum = (n) => {
    if (typeof n !== 'number' || !isFinite(n)) return null;
    if (n >= 1000) return Math.round(n).toLocaleString('en-US');
    return n.toString();
  };

  const prefix = (cur === 'USD') ? '$' : (cur ? `${cur} ` : '');

  if (typeof min === 'number' && typeof max === 'number') {
    return `${prefix}${fmtNum(min)}–${prefix}${fmtNum(max)}${unit ? `/${unit.toLowerCase()}` : ''}`;
  }
  if (typeof val === 'number') {
    return `${prefix}${fmtNum(val)}${unit ? `/${unit.toLowerCase()}` : ''}`;
  }
  return '';
}

async function fetchFantasticFeed(cfg, manifest) {
  const actorId = cfg.actorId || 'fantastic-jobs/career-site-job-listing-feed';
  const limit = Math.max(200, Math.min(5000, Number(cfg.limit || 500)));

  const input = {
    limit,
    includeAi: cfg.includeAi !== false,
    includeLinkedIn: cfg.includeLinkedIn === true,
    titleSearch: cfg.titleSearch || ['Unity'],
    locationSearch: cfg.locationSearch || ['United States'],
    aiWorkArrangementFilter: cfg.aiWorkArrangementFilter || ['Remote OK', 'Remote Solely', 'Hybrid', 'On-site'],
    descriptionType: cfg.descriptionType || 'text'
  };

  manifest.push(`CALL actor ${actorId} input=${JSON.stringify(input)}`);

  // IMPORTANT: This uses Apify platform credentials (no APIFY_TOKEN required) but DOES require Full permissions.
  const run = await Actor.call(actorId, input);

  const datasetId = run?.defaultDatasetId;
  if (!datasetId) throw new Error(`Feed actor call succeeded but no defaultDatasetId returned (runId=${run?.id || 'unknown'})`);

  // Read up to "limit" items from dataset
  const client = Actor.apifyClient;
  const { items } = await client.dataset(datasetId).listItems({ limit });

  const normalized = (items || []).map(it => {
    const company = it.organization || it.company || it.domain_derived || it.domainDerived || it.source_domain || it.sourceDomain || 'Unknown';
    const slug = it.domain_derived ? slugify(it.domain_derived) : slugify(company);

    return {
      title: it.title || it.job_title || it.jobTitle || null,
      url: it.url || it.apply_url || it.applyUrl || it.job_url || it.jobUrl || null,
      location: fmtLocationFromFantastic(it),
      published: it.date_posted || it.datePosted || it.date_created || it.dateCreated || it.published_at || it.publishedAt || null,
      salary: fmtSalaryFromFantastic(it),
      raw: it,
      source: it.source || 'career-site-feed',
      company,
      company_slug: slug,
      whereFound: it.organization_url || it.organizationUrl || it.source_domain || it.sourceDomain || it.domain_derived || it.domainDerived || ''
    };
  });

  return {
    normalized,
    meta: {
      runId: run?.id || null,
      datasetId,
      requestedLimit: limit,
      returned: normalized.length
    }
  };
}

Actor.main(async () => {
  const kv = await Actor.openKeyValueStore('job-pipeline');
  const input = await Actor.getInput() || {};
  const fetchMode = safeLower(input.fetchMode || input.fetchModeOverride || 'fantastic_jobs_feed');

  const manifest = [];
  const records = [];
  const snapshot = {
    fetchMode,
    startedAt: new Date().toISOString(),
    companies_success: 0,
    companies_failed: 0,
    records_normalized: 0
  };

  if (fetchMode !== 'fantastic_jobs_feed') {
    throw new Error(`This v2.4 build expects fetchMode="fantastic_jobs_feed". Got: ${fetchMode}`);
  }

  const { normalized, meta } = await fetchFantasticFeed(input.fantastic || {}, manifest);
  records.push(...normalized);
  snapshot.fantastic = meta;

  // De-dupe within this run by URL
  const seen = new Set();
  const deduped = [];
  for (const r of records) {
    const u = (r.url || '').trim();
    if (!u) continue;
    if (seen.has(u)) continue;
    seen.add(u);
    deduped.push(r);
  }

  snapshot.records_normalized = deduped.length;
  snapshot.finishedAt = new Date().toISOString();

  await kv.setValue('manifest.log', manifest.join('\n'));
  await kv.setValue('merged.json', deduped);
  await kv.setValue('fetch_snapshot.json', snapshot);

  console.log(`Fetch complete. Normalized ${deduped.length} records.`);
  if (deduped.length >= 200) {
    await sleep(1); // no-op-ish; keeps log ordering stable
  }
});