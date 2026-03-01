// 03_fetch_ats/main.js — v2.3
// Fetch jobs either:
//  A) directly from discovered ATS tenants (legacy v2.2 behavior), OR
//  B) via the Apify Store actor "fantastic-jobs/career-site-job-listing-api" (recommended)
// Writes to KV store "job-pipeline":
//  - merged.json (normalized records)
//  - manifest.log (every fetch + status/errors)
//  - fetch_snapshot.json (counters)

import { Actor } from 'apify';
import fetch from 'node-fetch';
import { ApifyClient } from 'apify-client';
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function iso(d) {
  try { return new Date(d).toISOString(); } catch { return null; }
}

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

function fmtLocationFromFantastic(item) {
  // Prefer AI / derived location fields when present.
  const workArr = item?.ai_work_arrangement || '';
  const locationType = item?.location_type || '';
  const remoteDerived = item?.remote_derived === true;

  if (remoteDerived || locationType === 'TELECOMMUTE' || /remote/i.test(workArr)) {
    // If they restrict remote location, show that
    const rl = item?.ai_remote_location?.[0] || item?.ai_remote_location_derived?.[0] || null;
    if (rl && typeof rl === 'string') return `Remote (${rl})`;
    // If derived countries exist, show first
    const c = item?.countries_derived?.[0] || null;
    if (c) return `Remote (${c})`;
    return 'Remote';
  }

  const loc = item?.locations_derived?.[0] || null;
  if (loc && typeof loc === 'object') {
    const city = loc.city || '';
    const admin = loc.admin || '';
    const country = loc.country || '';
    const parts = [city, admin, country].filter(Boolean);
    if (parts.length) return parts.join(', ');
  }

  // Fallbacks
  const raw = item?.locations_raw?.[0] || item?.locations_alt_raw?.[0] || null;
  if (raw) return typeof raw === 'string' ? raw : JSON.stringify(raw);
  return item?.organization || null;
}

function fmtSalaryFromFantastic(item) {
  // Prefer AI enriched salary fields (min/max/value + currency + unit).
  const cur = item?.ai_salary_currency || null;
  const unit = item?.ai_salary_unittext || null;

  const min = item?.ai_salary_minvalue;
  const max = item?.ai_salary_maxvalue;
  const val = item?.ai_salary_value;

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

  // Raw schema (Google for Jobs) fallback
  const sr = item?.salary_raw;
  if (sr && typeof sr === 'object') {
    const rc = sr.currency || sr.currencyCode || null;
    const ru = sr.unitText || sr.unit || null;
    const rmin = sr.minValue || sr.min_value || null;
    const rmax = sr.maxValue || sr.max_value || null;
    const rval = sr.value || null;
    const rprefix = (rc === 'USD') ? '$' : (rc ? `${rc} ` : '');
    if (typeof rmin === 'number' && typeof rmax === 'number') {
      return `${rprefix}${fmtNum(rmin)}–${rprefix}${fmtNum(rmax)}${ru ? `/${ru.toLowerCase()}` : ''}`;
    }
    if (typeof rval === 'number') {
      return `${rprefix}${fmtNum(rval)}${ru ? `/${ru.toLowerCase()}` : ''}`;
    }
  }

  return '';
}

// -------------------- Legacy ATS fetchers (v2.2) --------------------

async function fetchAshby(slug, manifest) {
  const url = `https://jobs.ashbyhq.com/api/non-user-graphql?op=ApiJobBoardWithTeams`;
  const body = {
    operationName: "ApiJobBoardWithTeams",
    variables: { organizationHostedJobsPageName: slug },
    query: "query ApiJobBoardWithTeams($organizationHostedJobsPageName: String!) { jobBoardWithTeams(organizationHostedJobsPageName: $organizationHostedJobsPageName) { jobs { id title location { name } publishedAt jobUrl applyUrl descriptionHtml descriptionPlain } } }"
  };
  manifest.push(`GET (POST) ${url} [ashby:${slug}]`);
  const res = await fetch(url, { method:'POST', headers:{'content-type':'application/json'}, body: JSON.stringify(body) });
  const data = await res.json();
  const jobs = data?.data?.jobBoardWithTeams?.jobs || [];
  return jobs.map(j => ({
    title: j.title,
    url: j.jobUrl || j.applyUrl,
    location: j.location?.name || null,
    published: j.publishedAt || null,
    raw: j,
    source: 'ashby',
    company: slug,
    company_slug: slug,
    whereFound: `https://jobs.ashbyhq.com/${slug}`
  }));
}

async function fetchWorkable(slug, manifest) {
  const url = `https://apply.workable.com/api/v3/accounts/${slug}/jobs`;
  manifest.push(`GET ${url} [workable:${slug}]`);
  const res = await fetch(url);
  const data = await res.json();
  const jobs = data?.results || [];
  return jobs.map(j => ({
    title: j.title,
    url: j.url,
    location: j.location?.location_str || null,
    published: j.published_at || null,
    raw: j,
    source: 'workable',
    company: slug,
    company_slug: slug,
    whereFound: `https://apply.workable.com/${slug}`
  }));
}

async function fetchSmartRecruiters(slug, manifest) {
  const base = `https://api.smartrecruiters.com/v1/companies/${slug}/postings`;
  const out = [];
  for (let offset = 0; offset <= 400; offset += 100) {
    const url = `${base}?limit=100&offset=${offset}`;
    manifest.push(`GET ${url} [smartrecruiters:${slug}]`);
    const res = await fetch(url);
    const data = await res.json();
    const postings = data?.content || [];
    for (const p of postings) {
      out.push({
        title: p.name,
        url: p.ref?.applyUrl || p.ref?.url || p.ref || null,
        location: p.location?.city ? `${p.location.city}, ${p.location.country || ''}`.trim() : (p.location?.country || null),
        published: p.releasedDate || null,
        raw: p,
        source: 'smartrecruiters',
        company: slug,
        company_slug: slug,
        whereFound: `https://jobs.smartrecruiters.com/${slug}`
      });
    }
    if (postings.length < 100) break;
    await sleep(250);
  }
  return out;
}

async function fetchWorkdayLight(tenant, manifest) {
  const url = `https://${tenant}.myworkdayjobs.com/wday/cxs/${tenant}/External/jobs`;
  manifest.push(`GET ${url} [workday:${tenant}]`);
  const res = await fetch(url);
  const data = await res.json();
  const jobs = data?.jobPostings || [];
  return jobs.map(j => ({
    title: j.title,
    url: `https://${tenant}.myworkdayjobs.com/en-US/External/job/${j.externalPath || ''}`.replace(/\/+$/,''),
    location: j.locationsText || null,
    published: j.postedOn || null,
    raw: j,
    source: 'workday',
    company: tenant,
    company_slug: tenant,
    whereFound: `https://${tenant}.myworkdayjobs.com`
  }));
}

// -------------------- Fantastic.jobs Career Site Job Listing API --------------------

async function fetchFantasticJobs(cfg, kv, manifest) {
  const token = Actor.getEnv().token || process.env.APIFY_TOKEN;
  if (!token) throw new Error('Missing APIFY_TOKEN (required to call Apify Store actors).');

  // Auto-widen the time range to catch up if we missed a day.
  const now = Date.now();
  const last = await kv.getValue('fantastic_last_success.json'); // { finishedAtIso }
  let timeRange = cfg.timeRange || '24h';
  if (timeRange === '24h' && last?.finishedAtIso) {
    const ms = now - new Date(last.finishedAtIso).getTime();
    const hours = ms / 36e5;
    if (hours > 36) {
      timeRange = '7d';
      manifest.push(`NOTE: Auto-widened fantastic timeRange to 7d (last success ${hours.toFixed(1)}h ago).`);
    }
  }

  const actorId = cfg.actorId || 'fantastic-jobs/career-site-job-listing-api';
  const limit = Number(cfg.limit || 500);

  const input = {
    timeRange,
    limit,
    includeAi: cfg.includeAi !== false,
    includeLinkedIn: cfg.includeLinkedIn === true,
    titleSearch: cfg.titleSearch || ['Unity', 'Gameplay', 'Game:*'],
    descriptionSearch: cfg.descriptionSearch || ['Unity', 'C#', 'CSharp', 'Unity3D', 'Unity2D'],
    locationSearch: cfg.locationSearch || ['United States'],
    // Keep broad — LLM will disqualify on-site outside MA, backend-heavy, etc.
    aiWorkArrangementFilter: cfg.aiWorkArrangementFilter || ['Remote OK', 'Remote Solely', 'Hybrid', 'On-site'],
    aiHasSalary: cfg.aiHasSalary === true ? true : false,
    removeAgency: cfg.removeAgency === true ? true : false,
    descriptionType: cfg.descriptionType || 'text'
  };

  // Log the actor call (but never log tokens).
  manifest.push(`CALL actor ${actorId} input=${JSON.stringify({ ...input, titleSearch: input.titleSearch, descriptionSearch: input.descriptionSearch, locationSearch: input.locationSearch })}`);

  const client = new ApifyClient({ token });
  const run = await client.actor(actorId).call(input);
  const datasetId = run?.defaultDatasetId;
  if (!datasetId) throw new Error(`Fantastic actor call succeeded but no defaultDatasetId was returned (runId=${run?.id || 'unknown'})`);

  const { items } = await client.dataset(datasetId).listItems({ limit });
  const normalized = (items || []).map(it => {
    const company = it.organization || it.domain_derived || it.source_domain || 'Unknown';
    const slug = it.domain_derived ? slugify(it.domain_derived) : slugify(company);
    return {
      title: it.title || null,
      url: it.url || null,
      location: fmtLocationFromFantastic(it),
      published: it.date_posted || it.date_created || null,
      salary: fmtSalaryFromFantastic(it),
      raw: it,
      source: it.source || 'career-site',
      company,
      company_slug: slug,
      whereFound: it.organization_url || it.source_domain || it.domain_derived || ''
    };
  });

  await kv.setValue('fantastic_last_success.json', { finishedAtIso: new Date().toISOString(), runId: run?.id || null, datasetId, input });

  return { normalized, meta: { runId: run?.id || null, datasetId, requestedLimit: limit, returned: normalized.length, timeRange } };
}

// -------------------- Main --------------------

Actor.main(async () => {
  const kv = await Actor.openKeyValueStore('job-pipeline');
  const input = await Actor.getInput() || {};
  const fetchMode = safeLower(input.fetchMode || 'fantastic_jobs_api'); // direct_ats | fantastic_jobs_api | hybrid

  const manifest = [];
  const records = [];

  const snapshot = {
    fetchMode,
    startedAt: new Date().toISOString(),
    companies_success: 0,
    companies_failed: 0,
    records_normalized: 0
  };

  if (fetchMode === 'fantastic_jobs_api' || fetchMode === 'hybrid') {
    const { normalized, meta } = await fetchFantasticJobs(input.fantastic || {}, kv, manifest);
    records.push(...normalized);
    snapshot.fantastic = meta;
  }

  if (fetchMode === 'direct_ats' || fetchMode === 'hybrid') {
    const regCsv = await kv.getValue('companies_registry.csv');
    if (!regCsv) throw new Error('Missing companies_registry.csv in KV store "job-pipeline". Run 02_update_registry first (or switch fetchMode to fantastic_jobs_api).');
    const lines = regCsv.split('\n').map(s => s.trim()).filter(Boolean);
    const rows = lines.slice(1).map(l => {
      const [company, ats, slug] = l.split(',').map(s => (s || '').trim());
      return { company, ats, slug };
    }).filter(r => r.ats && r.slug);

    for (const r of rows) {
      try {
        let jobs = [];
        if (r.ats === 'ashby') jobs = await fetchAshby(r.slug, manifest);
        else if (r.ats === 'workable') jobs = await fetchWorkable(r.slug, manifest);
        else if (r.ats === 'smartrecruiters') jobs = await fetchSmartRecruiters(r.slug, manifest);
        else if (r.ats === 'workday') jobs = await fetchWorkdayLight(r.slug, manifest);
        else {
          manifest.push(`SKIP unsupported ATS ${r.ats} for ${r.company} (${r.slug})`);
          continue;
        }
        records.push(...jobs);
        snapshot.companies_success += 1;
      } catch (e) {
        snapshot.companies_failed += 1;
        manifest.push(`ERROR ${r.ats}:${r.slug} ${e?.message || e}`);
      }
      await sleep(250);
    }
  }

  // De-dupe within this run (by URL)
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
});
