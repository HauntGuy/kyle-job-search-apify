// 01_discover_serp/main.js — v2.2
import { Actor } from 'apify';
import fetch from 'node-fetch';
import crypto from 'crypto';

const DEFAULT_QUERIES = [
  'site:boards.greenhouse.io ("Unity" OR "C#") (Developer OR Engineer OR "Gameplay Programmer")',
  'site:boards.greenhouse.io ("Unity 2D" OR "2D") (Developer OR Engineer)',
  'site:boards.greenhouse.io ("Mobile" OR "iOS") (Unity OR "C#") (Developer OR Engineer)',
  'site:boards.greenhouse.io ("UI" OR "Tools") (Unity) (Engineer OR Developer)',
  'site:jobs.lever.co ("Unity" OR "C#") (Developer OR Engineer OR "Gameplay Programmer")',
  'site:jobs.lever.co ("Mobile" OR "iOS") (Unity) (Developer OR Engineer)',
  'site:jobs.lever.co ("UI" OR "Tools") (Unity) (Engineer OR Developer)',
  'site:jobs.ashbyhq.com ("Unity" OR "C#") (Developer OR Engineer OR "Gameplay Programmer")',
  'site:jobs.ashbyhq.com ("Mobile" OR "iOS") (Unity) (Developer OR Engineer)',
  'site:jobs.ashbyhq.com ("2D" OR "2-D") (Unity)',
  'site:apply.workable.com ("Unity" OR "C#") (Developer OR Engineer OR "Gameplay Programmer")',
  'site:apply.workable.com ("Mobile" OR "iOS") (Unity)',
  'site:apply.workable.com ("UI" OR "Tools") (Unity)',
  'site:jobs.smartrecruiters.com ("Unity" OR "C#") (Developer OR Engineer OR "Gameplay Programmer")',
  'site:jobs.smartrecruiters.com ("Mobile" OR "iOS") (Unity)',
  'site:jobs.smartrecruiters.com ("2D" OR "UI") (Unity)',
  'site:myworkdayjobs.com ("Unity" OR "C#") (Developer OR Engineer OR "Gameplay Programmer")',
  'site:myworkdayjobs.com ("Mobile" OR "iOS") (Unity)',
  'site:myworkdayjobs.com ("2D" OR "UI") (Unity)'
];

function kvKeyForQuery(q, tbs) {
  // Apify KV keys must be <= 63 chars. Use a stable SHA1 hex hash.
  const h = crypto.createHash('sha1').update(`${q}|${tbs || 'none'}`).digest('hex'); // 40 chars
  return `lastq_${h}`; // 6 + 40 = 46 chars
}

function extractSlug(url) {
  try {
    const u = new URL(url);
    const host = u.hostname;
    const parts = u.pathname.split('/').filter(Boolean);
    if (host === 'boards.greenhouse.io') return { ats: 'greenhouse', slug: parts[0] || null };
    if (host === 'jobs.lever.co') return { ats: 'lever', slug: parts[0] || null };
    if (host === 'jobs.ashbyhq.com') return { ats: 'ashby', slug: parts[0] || null };
    if (host === 'apply.workable.com') return { ats: 'workable', slug: parts[0] || null };
    if (host === 'jobs.smartrecruiters.com') return { ats: 'smartrecruiters', slug: parts[0] || null };
    if (host.endsWith('myworkdayjobs.com')) {
      const tenant = host.replace('.myworkdayjobs.com', '');
      return { ats: 'workday', slug: tenant || null };
    }
  } catch {}
  return { ats: null, slug: null };
}

async function serpSearch(SERPAPI_KEY, q, tbs) {
  const url = new URL('https://serpapi.com/search.json');
  url.searchParams.set('engine','google');
  url.searchParams.set('q', q);
  url.searchParams.set('num','100');
  url.searchParams.set('hl','en');
  url.searchParams.set('api_key', SERPAPI_KEY);
  if (tbs) url.searchParams.set('tbs', tbs);
  const res = await fetch(url.toString());
  const data = await res.json();
  return { url: url.toString(), data };
}

Actor.main(async () => {
  const SERPAPI_KEY = Actor.getEnv().SERPAPI_KEY;
  if (!SERPAPI_KEY) throw new Error('Missing SERPAPI_KEY');

  const input = await Actor.getInput() || {};
  const lookbackHours = Number(input.lookbackHours || 48);
  const queries = Array.isArray(input.queries) && input.queries.length ? input.queries : DEFAULT_QUERIES;

  const kv = await Actor.openKeyValueStore('job-pipeline');
  const ledger = (await kv.getValue('seen_slugs.json')) || {};
  const nowIso = new Date().toISOString();

  const discovered = [];

  for (const q of queries) {
    // Past day
    const a = await serpSearch(SERPAPI_KEY, q, 'qdr:d');
    await kv.setValue(kvKeyForQuery(q, 'qdr:d'), a.data); // hashed KV key
    for (const it of (a.data?.organic_results || [])) {
      const { ats, slug } = extractSlug(it.link || '');
      if (ats && slug) {
        const key = `${ats}:${slug}`.toLowerCase();
        if (!ledger[key]) {
          ledger[key] = nowIso;
          discovered.push({ ats, slug, companyHint: it.title || null, sourceLink: it.link, query: q, seenAt: nowIso });
        }
      }
    }

    // Past week (backfill; still deduped by ledger)
    const b = await serpSearch(SERPAPI_KEY, q, 'qdr:w');
    await kv.setValue(kvKeyForQuery(q, 'qdr:w'), b.data);
    for (const it of (b.data?.organic_results || [])) {
      const { ats, slug } = extractSlug(it.link || '');
      if (ats && slug) {
        const key = `${ats}:${slug}`.toLowerCase();
        if (!ledger[key]) {
          ledger[key] = nowIso;
          discovered.push({ ats, slug, companyHint: it.title || null, sourceLink: it.link, query: q, seenAt: nowIso });
        }
      }
    }
  }

  // Cross-task handoff via KV (not dataset)
  await kv.setValue('seen_slugs.json', ledger);
  await kv.setValue('discovered.json', discovered);
  await kv.setValue('discover_summary.json', { queries: queries.length, discovered: discovered.length, lookbackHours });

  console.log(`Discovery complete. Discovered ${discovered.length} new ATS slugs.`);
});
