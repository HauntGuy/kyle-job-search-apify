// 01a_collector_fantastic_feed/src/main.js â€” v0.1.0
// Calls Fantastic.jobs Career Site Job Listing Feed and writes normalized results to KV store.
//
// KV outputs (in kvStoreName, default "job-pipeline"):
// - outputKey (default "collector_fantastic_feed.json")
// - snapshotKey (default "snapshot_01a_fantastic.json")
// - manifest_01a_fantastic.log

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

  return item?.location || item?.organization || '';
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
    return `${prefix}${fmtNum(min)}â€“${prefix}${fmtNum(max)}${unit ? `/${unit.toLowerCase()}` : ''}`;
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

  const run = await Actor.call(actorId, input);
  const datasetId = run?.defaultDatasetId;
  if (!datasetId) throw new Error(`Feed actor call succeeded but no defaultDatasetId returned (runId=${run?.id || 'unknown'})`);

  const client = Actor.apifyClient;
  const { items } = await client.dataset(datasetId).listItems({ limit });

  const normalized = (items || []).map(it => {
    const company = it.organization || it.company || it.domain_derived || it.domainDerived || it.source_domain || it.sourceDomain || 'Unknown';
    const slug = it.domain_derived ? slugify(it.domain_derived) : slugify(company);

    return {
      title: it.title || it.job_title || it.jobTitle || '',
      url: it.url || it.apply_url || it.applyUrl || it.job_url || it.jobUrl || '',
      location: fmtLocationFromFantastic(it),
      published: it.date_posted || it.datePosted || it.date_created || it.dateCreated || it.published_at || it.publishedAt || '',
      salary: fmtSalaryFromFantastic(it),
      raw: it,
      source: 'fantastic_feed',
      company,
      company_slug: slug,
      whereFound: it.organization_url || it.organizationUrl || it.source_domain || it.sourceDomain || it.domain_derived || it.domainDerived || ''
    };
  });

  return { normalized, meta: { runId: run?.id || null, datasetId, requestedLimit: limit, returned: normalized.length } };
}

Actor.main(async () => {
  const input = await Actor.getInput() || {};
  const kvStoreName = (input.kvStoreName || 'job-pipeline').toString();
  const outputKey = (input.outputKey || 'collector_fantastic_feed.json').toString();
  const snapshotKey = (input.snapshotKey || 'snapshot_01a_fantastic.json').toString();
  const manifestKey = 'manifest_01a_fantastic.log';

  const kv = await Actor.openKeyValueStore(kvStoreName);

  const manifest = [];
  const snapshot = { startedAt: new Date().toISOString(), fetchMode: 'fantastic_jobs_feed', records_normalized: 0 };

  const { normalized, meta } = await fetchFantasticFeed(input.fantastic || {}, manifest);

  // dedup by URL
  const seen = new Set();
  const deduped = [];
  for (const r of normalized) {
    const u = (r.url || '').trim();
    if (!u) continue;
    if (seen.has(u)) continue;
    seen.add(u);
    deduped.push(r);
  }

  snapshot.records_normalized = deduped.length;
  snapshot.fantastic = meta;
  snapshot.finishedAt = new Date().toISOString();

  await kv.setValue(manifestKey, manifest.join('\n'));
  await kv.setValue(outputKey, deduped);
  await kv.setValue(snapshotKey, snapshot);

  console.log(`01a complete. Wrote ${deduped.length} -> ${kvStoreName}/${outputKey}`);
});
