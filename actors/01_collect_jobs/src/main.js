// actors/01_collect_jobs/src/main.js
// Collect jobs from multiple configured sources and write normalized records to a per-run dataset.

import { Actor, log } from 'apify';
import ExcelJS from 'exceljs';
import { gotScraping } from 'got-scraping';
import { withRetries, processWithRetries, fetchJsonRetryable } from './resilient-fetch.js';
import { load as cheerioLoad } from 'cheerio';

const BROWSER_UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36';

function nowIso() { return new Date().toISOString(); }
function safeRunId(runId) { if (!runId) return null; return String(runId).replace(/[^a-zA-Z0-9._-]/g, '-').slice(0, 80); }
function makeRunId() { return new Date().toISOString().replace(/[:.]/g, '-'); }
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
  const text = await fetchText(url, { ...headers, Accept: 'application/json' });
  try { return JSON.parse(text); }
  catch (e) { throw new Error(`Non-JSON response from ${url}: ${e?.message || e}\n${text.slice(0, 500)}`); }
}

async function fetchHtmlRetryable(url, headers = {}) {
  const res = await fetch(url, {
    method: 'GET',
    headers: { 'User-Agent': BROWSER_UA, ...headers },
  });
  const text = await res.text();
  if (!res.ok) {
    const err = new Error(`HTTP ${res.status} for ${url}: ${text.slice(0, 300)}`);
    err.status = res.status;
    const ra = res.headers.get('retry-after');
    if (ra) err.retryAfterMs = (parseInt(ra, 10) || 10) * 1000;
    throw err;
  }
  return text;
}

async function loadConfig(input) {
  if (input?.config && typeof input.config === 'object') return input.config;
  const configUrl = input?.configUrl || process.env.JOBSEARCH_CONFIG_URL || process.env.CONFIG_URL;
  if (!configUrl) throw new Error('Missing configUrl (set in task input, or JOBSEARCH_CONFIG_URL env var).');
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
  if (v == null) return [];
  return Array.isArray(v) ? v : [v];
}

function toIsoOrEmpty(v) {
  if (!v) return '';
  const s = String(v).trim();
  if (!s) return '';
  if (/\d{4}-\d{2}-\d{2}T/.test(s)) return s;
  const ms = Date.parse(s);
  if (!Number.isFinite(ms)) return s;
  return new Date(ms).toISOString();
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

function normalizeGeneric(sourceId, raw) {
  const title = firstString(raw.title, raw.job_title, raw.position, raw.role, raw.jobTitle);
  const company = firstString(raw.organization, raw.company, raw.companyName, raw.employer_name, raw.employerName, raw.company_name);
  const location = firstString(raw.location, raw.job_location, raw.jobLocation, raw.candidate_required_location, raw.city, raw.job_city, raw.job_state);
  const url = firstString(raw.url, raw.job_url, raw.jobUrl, raw.link, raw.job_google_link, raw.job_apply_link);
  const applyUrl = firstString(raw.apply_url, raw.applyUrl, raw.job_apply_link, raw.application_url, raw.apply_link);
  const description = firstString(raw.description_text, raw.description, raw.job_description, raw.descriptionText);
  const postedAt = firstString(raw.job_posted_at_datetime_utc, raw.publication_date, raw.date, raw.postedAt);
  const companyUrl = firstString(raw.company_url, raw.companyUrl, raw.employer_website, raw.organization_url, raw.company_website);
  const salary = firstString(raw.salary, raw.salary_range, raw.compensation);

  const out = {
    source: sourceId,
    fetchedAt: nowIso(),
    title,
    company,
    companyUrl: canonicalizeUrl(companyUrl),
    location,
    url: canonicalizeUrl(url),
    applyUrl: canonicalizeUrl(applyUrl || url),
    postedAt: toIsoOrEmpty(postedAt),
    description,
    salary,
    raw,
  };

  out.sourceJobId = firstString(raw.id, raw.job_id, raw.jobId, raw.guid);
  return out;
}

function normalizeFantasticFeed(sourceId, raw) {
  const title = firstString(raw.title);
  const company = firstString(raw.organization, raw.company, raw.company_name);
  const url = firstString(raw.url, raw.job_url);
  const applyUrl = firstString(raw.apply_url, raw.applyUrl, raw.apply_link, raw.application_url, url);
  // linkedin_org_url is the actual company website (from LinkedIn profiles);
  // organization_url can be a linkedin.com/company page for LinkedIn sources
  const companyUrl = firstString(raw.linkedin_org_url, raw.organization_url, raw.company_url, raw.company_website, raw.employer_website);

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
    companyUrl: canonicalizeUrl(companyUrl),
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

function coerceApifyActorInput(input) {
  const out = { ...(input || {}) };

  // Coerce to arrays if strings are provided
  for (const k of ['titleSearch', 'locationSearch', 'aiWorkArrangementFilter']) {
    if (!(k in out)) continue;

    const v = out[k];
    if (typeof v === 'string') {
      const s = v.trim();
      if (k === 'aiWorkArrangementFilter' && s.toLowerCase() === 'all') {
        // Treat "All" as "no filter"
        delete out[k];
      } else {
        out[k] = [s];
      }
    } else if (Array.isArray(v)) {
      // ok
    } else if (v == null) {
      delete out[k];
    } else {
      // Any other type: wrap
      out[k] = [v];
    }
  }

  return out;
}

async function runApifyActorSource(source, globalMaxItemsPerSource, remaining) {
  const actorId = String(source.actorId);
  const inputRaw = source.input || {};
  const input = coerceApifyActorInput(inputRaw);

  // Determine how many items to request from the upstream actor.
  // We avoid "waste" by never requesting more than we plan to consume.
  let requestedLimit =
    Number(input.limit ?? source.limit ?? globalMaxItemsPerSource ?? 0) || 0;

  // If limit isn't set, fall back to the global per-source cap (or a conservative default).
  if (!Number.isFinite(requestedLimit) || requestedLimit <= 0) {
    requestedLimit = Number(globalMaxItemsPerSource || 200) || 200;
  }

  requestedLimit = Math.floor(requestedLimit);

  // Apply global per-source cap.
  if (Number.isFinite(globalMaxItemsPerSource) && globalMaxItemsPerSource > 0) {
    requestedLimit = Math.min(requestedLimit, Math.floor(globalMaxItemsPerSource));
  }

  // Apply remaining cap (maxTotalItems) if provided.
  if (Number.isFinite(remaining) && remaining > 0) {
    requestedLimit = Math.min(requestedLimit, Math.floor(remaining));
  }

  // Apify Store actors have their own min/max constraints; enforce known mins for the Fantastic actors.
  // (This prevents accidental "limit too small" errors.)
  const actorLower = actorId.toLowerCase();
  if (actorLower.includes('fantastic-jobs/career-site-job-listing-feed')) {
    requestedLimit = Math.max(200, requestedLimit);
  }
  if (actorLower.includes('fantastic-jobs/advanced-linkedin-job-search-api')) {
    requestedLimit = Math.max(10, requestedLimit);
  }

  // Cap to a reasonable max to avoid giant accidental pulls.
  requestedLimit = Math.min(5000, requestedLimit);

  // Ensure we actually pass the upstream actor a limit.
  input.limit = requestedLimit;

  log.info(`[${source.id}] Calling Apify actor ${actorId} (limit=${requestedLimit})`);

  const run = await Actor.call(actorId, input);
  const status = run?.status || 'UNKNOWN';
  if (status !== 'SUCCEEDED') {
    throw new Error(`[${source.id}] Called actor did not succeed (status=${status}, runId=${run?.id || 'unknown'})`);
  }

  const datasetId = run?.defaultDatasetId;
  if (!datasetId) throw new Error(`[${source.id}] Missing defaultDatasetId in Actor.call result`);

  const rawItems = await listDatasetItems(datasetId, requestedLimit);
  log.info(`[${source.id}] Fetched ${rawItems.length} items from dataset ${datasetId}`);

  const adapter = String(source.adapter || 'generic');
  const jobs = rawItems.map((it) => {
    if (adapter === 'fantastic_feed' || adapter === 'linkedin_generic') return normalizeFantasticFeed(source.id, it);
    if (adapter === 'jsearch') return normalizeJSearch(source.id, it);
    return normalizeGeneric(source.id, it);
  });

  const hitLimitLikely = rawItems.length === requestedLimit;
  const hitCap = hitLimitLikely;

  return {
    jobs,
    meta: {
      actorId,
      runId: run?.id || null,
      datasetId,
      requestedLimit,
      returnedCount: rawItems.length,
      hitLimitLikely,
      hitCap,
      itemCount: jobs.length
    }
  };
}

async function runRemotive(source) {
  const q = String(source.query || '').trim();
  const limit = Number(source.limit || 200) || 200;

  const params = new URLSearchParams();
  if (q) params.set('search', q);
  params.set('limit', String(limit));

  const url = `https://remotive.com/api/remote-jobs?${params.toString()}`;
  log.info(`[${source.id}] GET ${url}`);

  const json = await fetchJson(url);
  const items = Array.isArray(json?.jobs) ? json.jobs : [];
  const jobs = items.slice(0, limit).map((it) => normalizeGeneric(source.id, it));
  const hitCap = items.length >= limit;
  return { jobs, meta: { itemCount: jobs.length, limit, returnedCount: items.length, hitCap } };
}

async function runRemoteOk(source) {
  const url = 'https://remoteok.com/api';
  log.info(`[${source.id}] GET ${url}`);

  const json = await fetchJson(url, { 'User-Agent': 'Mozilla/5.0 (jobsearch-bot)' });
  const items = Array.isArray(json) ? json : [];
  const jobItems = items.filter((it) => it && typeof it === 'object' && (it.position || it.company));
  const jobs = jobItems.map((it) => normalizeGeneric(source.id, it));
  return { jobs, meta: { itemCount: jobs.length } };
}

// --------------- GrackleHQ (gracklehq.com) ---------------

function normalizeGrackleHQ(sourceId, item) {
  // Parse age string ("3d", "1w", "2mo", "5h") into approximate postedAt
  let postedAt = '';
  if (item.age) {
    const age = String(item.age).trim().toLowerCase();
    let daysAgo = -1;
    const mH = age.match(/(\d+)\s*h/);
    const mD = age.match(/(\d+)\s*d/);
    const mW = age.match(/(\d+)\s*w/);
    const mM = age.match(/(\d+)\s*mo/);
    if (mH) daysAgo = 0;
    else if (mD) daysAgo = parseInt(mD[1], 10);
    else if (mW) daysAgo = parseInt(mW[1], 10) * 7;
    else if (mM) daysAgo = parseInt(mM[1], 10) * 30;
    if (daysAgo >= 0) {
      postedAt = new Date(Date.now() - daysAgo * 86400000).toISOString();
    }
  }

  // GrackleHQ often embeds city in the company field: "Roblox - San Mateo, CA, USA"
  // Split on " - " to separate company name from location when present.
  let company = item.company || '';
  let location = item.location || '';
  const dashIdx = company.indexOf(' - ');
  if (dashIdx > 0) {
    const afterDash = company.slice(dashIdx + 3).trim();
    // Looks like a location if it contains a comma (City, ST) or country name
    if (/,/.test(afterDash) || /\b(USA|United States|Canada|UK|Remote)\b/i.test(afterDash)) {
      const extractedLocation = afterDash;
      company = company.slice(0, dashIdx).trim();
      // Prefer the parsed location from the card text; use company-embedded location as fallback
      location = location || extractedLocation;
    }
  }

  return {
    source: sourceId,
    fetchedAt: nowIso(),
    title: item.title || '',
    company,
    companyUrl: '',
    location,
    url: canonicalizeUrl(item.url || ''),
    applyUrl: canonicalizeUrl(item.url || ''),
    postedAt,
    description: '',
    salary: '',
    sourceJobId: String(item.id || ''),
    raw: item,
  };
}

async function runGrackleHQ(source) {
  const country = source.country || 'USA';
  const departments = source.departments || ['Engineering', 'Design'];
  const maxPages = source.maxPages || 25;

  const allItems = [];
  const deptsCapped = [];

  for (const dept of departments) {
    let deptPagesFetched = 0;
    for (let page = 0; page < maxPages; page++) {
      const params = new URLSearchParams({ country, department: dept, pageidx: String(page) });
      const url = `https://gracklehq.com/jobs?${params.toString()}`;
      log.info(`[${source.id}] GET ${url} (${dept} p${page})`);

      let html;
      try {
        html = await withRetries(
          () => fetchHtmlRetryable(url),
          { retries: 2, baseMs: 2000, maxMs: 15000, label: `${source.id} ${dept}` },
        );
      } catch (err) {
        log.warning(`[${source.id}] Failed to fetch ${dept} page ${page}: ${err.message}. Stopping department.`);
        break;
      }

      const $ = cheerioLoad(html);
      const listings = $('.joblisting');

      if (listings.length === 0) {
        log.info(`[${source.id}] ${dept} page ${page}: no listings, stopping.`);
        break;
      }

      listings.each((_, el) => {
        const $el = $(el);
        const linkEl = $el.find('a[href*="/rd/"]').first();
        const title = linkEl.text().trim();
        const href = linkEl.attr('href') || '';

        // Extract GrackleHQ ID from href like "/rd/373904"
        const idMatch = href.match(/\/rd\/(\d+)/);
        const grackleId = idMatch ? idMatch[1] : '';

        const age = $el.find('.bottomright').text().trim();

        // Remove title and age from full text to isolate company + location
        const fullText = $el.text().trim();
        const titleIdx = fullText.indexOf(title);
        let afterTitle = titleIdx >= 0 ? fullText.slice(titleIdx + title.length) : fullText;
        afterTitle = afterTitle.replace(age, '').trim();

        // Split by newlines/tabs/multiple spaces to get company and location parts
        const parts = afterTitle.split(/[\n\t]+|\s{2,}/).map(s => s.trim()).filter(Boolean);
        const company = parts[0] || '';
        const location = parts.slice(1).join(', ').trim();

        if (title && grackleId) {
          allItems.push({
            id: grackleId, title, company, location, age,
            url: `https://gracklehq.com${href}`,
          });
        }
      });

      deptPagesFetched++;
      log.info(`[${source.id}] ${dept} p${page}: ${listings.length} listings (total ${allItems.length})`);

      // Small delay between pages
      if (page < maxPages - 1 && listings.length > 0) {
        await new Promise(r => setTimeout(r, 1000));
      }
    }
    if (deptPagesFetched >= maxPages) deptsCapped.push(dept);
  }

  // Deduplicate by ID (a job can appear in multiple departments)
  const seen = new Set();
  const unique = [];
  for (const item of allItems) {
    if (!seen.has(item.id)) { seen.add(item.id); unique.push(item); }
  }

  const jobs = unique.map(item => normalizeGrackleHQ(source.id, item));
  const hitCap = deptsCapped.length > 0;
  log.info(`[${source.id}] GrackleHQ: ${allItems.length} raw → ${unique.length} unique → ${jobs.length} normalized`);
  return { jobs, meta: { itemCount: jobs.length, rawCount: allItems.length, departments, maxPages, deptsCapped: hitCap ? deptsCapped : undefined, hitCap } };
}

// --------------- Built In (builtin.com) ---------------

function normalizeBuiltIn(sourceId, item) {
  return {
    source: sourceId,
    fetchedAt: nowIso(),
    title: item.title || '',
    company: item.company || '',
    companyUrl: canonicalizeUrl(item.companyUrl || ''),
    location: item.location || '',
    url: canonicalizeUrl(item.url || ''),
    applyUrl: canonicalizeUrl(item.url || ''),
    postedAt: toIsoOrEmpty(item.postedAt || ''),
    description: '',
    salary: item.salary || '',
    sourceJobId: String(item.id || ''),
    raw: item,
  };
}

async function runBuiltIn(source) {
  const query = String(source.query || '').trim();
  if (!query) throw new Error(`[${source.id}] Missing source.query`);

  const maxPages = source.maxPages || 5;
  const allLocations = source.allLocations !== false;
  const state = source.state || '';

  const allItems = [];
  let pagesFetched = 0;
  let lastPageHadCards = false;

  for (let page = 1; page <= maxPages; page++) {
    const params = new URLSearchParams({ search: query, page: String(page) });
    if (allLocations) params.set('allLocations', 'true');
    if (state) params.set('state', state);

    const url = `https://builtin.com/jobs?${params.toString()}`;
    log.info(`[${source.id}] GET ${url} (page ${page}/${maxPages})`);

    let html;
    try {
      html = await withRetries(
        () => fetchHtmlRetryable(url),
        { retries: 2, baseMs: 5000, maxMs: 30000, label: source.id },
      );
    } catch (err) {
      log.warning(`[${source.id}] Failed to fetch page ${page}: ${err.message}. Stopping.`);
      break;
    }

    const $ = cheerioLoad(html);
    const cards = $('[data-id="job-card"]');

    if (cards.length === 0) {
      log.info(`[${source.id}] Page ${page}: no cards, stopping.`);
      break;
    }

    cards.each((_, el) => {
      const $card = $(el);

      // Title and URL — data-id="job-card-title" is ON the <a> tag itself
      const titleEl = $card.find('[data-id="job-card-title"]').first();
      if (!titleEl.length) return;
      const title = titleEl.text().trim();
      const href = titleEl.attr('href') || '';
      const jobUrl = href.startsWith('http') ? href : (href ? `https://builtin.com${href}` : '');

      // Extract numeric ID from URL path like /job/slug/12345
      const idMatch = href.match(/\/(\d+)(?:\?|$)/);
      const jobId = idMatch ? idMatch[1] : '';

      // Company — data-id="company-title" is ON the <a> tag itself
      const companyEl = $card.find('[data-id="company-title"]').first();
      const company = companyEl.text().trim();
      const companyLink = companyEl.attr('href') || '';
      const companyUrl = companyLink.startsWith('http') ? companyLink
        : (companyLink ? `https://builtin.com${companyLink}` : '');

      // Extract location, work mode, and salary using Font Awesome icon markers.
      // Built In uses fa-house-building (work mode), fa-location-dot (city),
      // fa-sack-dollar (salary) as semantic anchors for each data field.
      let location = '';
      let workMode = '';
      let salary = '';

      // Work mode (Remote / Hybrid / In-Office)
      const houseIcon = $card.find('i.fa-house-building').first();
      if (houseIcon.length) {
        const container = houseIcon.closest('.d-flex.align-items-start');
        if (container.length) workMode = container.find('span').first().text().trim();
      }

      // City / geographic location
      const locIcon = $card.find('i.fa-location-dot').first();
      if (locIcon.length) {
        const container = locIcon.closest('.d-flex.align-items-start');
        if (container.length) {
          const locSpan = container.find('span').first();
          let locText = locSpan.text().trim();
          // Multi-location cards show "N Locations" with a tooltip listing individual cities
          if (/\d+\s+locations/i.test(locText)) {
            const tooltip = locSpan.attr('data-bs-title') || '';
            if (tooltip) {
              // Parse tooltip HTML: "<div class='text-truncate'>Austin, TX, USA</div>"
              const cities = [...tooltip.matchAll(/>([^<]+)</g)].map(m => m[1].trim()).filter(Boolean);
              if (cities.length) locText = cities.join('; ');
            }
          }
          location = locText;
        }
      }

      // Salary
      const salaryIcon = $card.find('i.fa-sack-dollar').first();
      if (salaryIcon.length) {
        const container = salaryIcon.closest('.d-flex.align-items-start');
        if (container.length) salary = container.find('span').first().text().trim();
      }

      // Combine work mode + location: "Remote | Boston, MA, USA" or just "In-Office | NYC"
      if (workMode && location) {
        location = `${workMode} | ${location}`;
      } else if (workMode && !location) {
        location = workMode; // Only work mode, no city
      }
      // If no icons found, fall back to span heuristics
      if (!location && !workMode) {
        $card.find('span').each((_, span) => {
          const text = $(span).text().trim();
          if (!text) return;
          if (!location && (/\b(Remote|Hybrid|On-?site)\b/i.test(text) || /,\s*[A-Z]{2}\b/.test(text))) {
            location = text;
          }
          if (!salary && /\$[\d,]+|\d+K\s*[-–]\s*\d+K/i.test(text)) {
            salary = text;
          }
        });
      }

      if (title) {
        allItems.push({ id: jobId, title, company, companyUrl, location, salary, url: jobUrl });
      }
    });

    pagesFetched++;
    lastPageHadCards = cards.length > 0;
    log.info(`[${source.id}] Page ${page}: ${cards.length} cards (total ${allItems.length})`);

    // Conservative delay to respect anti-bot measures
    if (page < maxPages && cards.length > 0) {
      await new Promise(r => setTimeout(r, 3000));
    }
  }

  const hitCap = pagesFetched >= maxPages && lastPageHadCards;
  const jobs = allItems.map(item => normalizeBuiltIn(source.id, item));
  return { jobs, meta: { itemCount: jobs.length, pagesFetched, maxPages, hitCap } };
}

// --------------- USAJobs API (data.usajobs.gov) ---------------

function normalizeUSAJobs(sourceId, item) {
  const desc = item.MatchedObjectDescriptor || {};
  const objectId = item.MatchedObjectId || '';
  const pos = Array.isArray(desc.PositionLocation) ? desc.PositionLocation[0] : {};
  const salary = Array.isArray(desc.PositionRemuneration) ? desc.PositionRemuneration[0] : {};
  const salaryMin = salary.MinimumRange || '';
  const salaryMax = salary.MaximumRange || '';
  const salaryInterval = salary.Description || salary.RateIntervalCode || '';
  const salaryStr = salaryMin && salaryMax
    ? `$${Number(salaryMin).toLocaleString('en-US')} - $${Number(salaryMax).toLocaleString('en-US')} ${salaryInterval}`.trim()
    : '';

  const title = desc.PositionTitle || '';
  const org = desc.OrganizationName || '';
  const dept = desc.DepartmentName || '';
  const company = org || dept;
  const location = desc.PositionLocationDisplay || pos?.LocationName || '';
  const url = desc.PositionURI || '';
  const applyUri = Array.isArray(desc.ApplyURI) ? desc.ApplyURI[0] : (desc.ApplyURI || '');

  // Build description from major duties + qualifications
  const duties = desc.UserArea?.Details?.MajorDuties || [];
  const qualSummary = desc.QualificationSummary || '';
  let description = '';
  if (duties.length > 0) {
    description = duties.join('\n\n');
    if (qualSummary) description += `\n\nQualifications: ${qualSummary}`;
  } else {
    description = qualSummary;
  }

  return {
    source: sourceId,
    fetchedAt: nowIso(),
    title,
    company,
    companyUrl: '',
    location,
    url: canonicalizeUrl(url),
    applyUrl: canonicalizeUrl(applyUri || url),
    postedAt: toIsoOrEmpty(desc.PublicationStartDate || ''),
    description: description.slice(0, 15000),
    salary: salaryStr,
    sourceJobId: String(objectId || desc.PositionID || ''),
    raw: item,
  };
}

async function runUSAJobs(source) {
  const apiKey = getEnvOrNull('USAJOBS_API_KEY');
  const email = getEnvOrNull('USAJOBS_EMAIL');
  if (!apiKey) throw new Error(`[${source.id}] Missing USAJOBS_API_KEY env var`);
  if (!email) throw new Error(`[${source.id}] Missing USAJOBS_EMAIL env var`);

  const keyword = String(source.keyword || '').trim();
  if (!keyword) throw new Error(`[${source.id}] Missing source.keyword`);

  const params = new URLSearchParams({ Keyword: keyword, ResultsPerPage: '500' });
  if (source.locationName) params.set('LocationName', source.locationName);
  if (source.datePosted) params.set('DatePosted', String(source.datePosted));
  if (source.jobCategoryCode) params.set('JobCategoryCode', source.jobCategoryCode);
  if (source.whoMayApply) params.set('WhoMayApply', source.whoMayApply);
  if (source.remoteIndicator) params.set('RemoteIndicator', source.remoteIndicator);

  const url = `https://data.usajobs.gov/api/search?${params.toString()}`;
  log.info(`[${source.id}] GET ${url}`);

  const headers = {
    'Authorization-Key': apiKey,
    'User-Agent': email,
  };

  const json = await withRetries(
    () => fetchJsonRetryable(url, headers),
    { retries: 3, baseMs: 2000, maxMs: 15000, label: source.id },
  );

  const items = json?.SearchResult?.SearchResultItems || [];
  const totalCount = json?.SearchResult?.SearchResultCount || 0;
  const jobs = items.map(item => normalizeUSAJobs(source.id, item));

  const hitCap = totalCount > items.length;
  log.info(`[${source.id}] USAJobs: ${items.length} items returned (total available: ${totalCount})`);
  return { jobs, meta: { itemCount: jobs.length, totalAvailable: totalCount, hitCap } };
}

// --------------- GameJobs.co (Atom feed + detail pages) ---------------

function normalizeGameJobsCo(sourceId, item) {
  return {
    source: sourceId,
    fetchedAt: nowIso(),
    title: item.title || '',
    company: item.company || '',
    companyUrl: canonicalizeUrl(item.companyUrl || ''),
    location: item.location || '',
    url: canonicalizeUrl(item.url || ''),
    applyUrl: canonicalizeUrl(item.applyUrl || item.url || ''),
    postedAt: toIsoOrEmpty(item.postedAt || ''),
    description: (item.description || '').slice(0, 15000),
    salary: item.salary || '',
    sourceJobId: String(item.id || ''),
    employmentType: item.employmentType || '',
    raw: item,
  };
}

/**
 * Parse a GameJobs.co detail page HTML to extract job metadata.
 * Tries JSON-LD first, falls back to HTML parsing.
 */
function parseGameJobsDetail(html, url) {
  const $ = cheerioLoad(html);
  const result = {};

  // --- Try JSON-LD first ---
  const ldScript = $('script[type="application/ld+json"]').first();
  if (ldScript.length) {
    try {
      const ld = JSON.parse(ldScript.html());
      if (ld['@type'] === 'JobPosting') {
        result.title = ld.title || '';
        result.company = ld.hiringOrganization?.name || '';
        result.location = typeof ld.jobLocation?.address === 'string'
          ? ld.jobLocation.address
          : (ld.jobLocation?.address?.addressLocality || ld.jobLocation?.address?.name || '');
        result.postedAt = ld.datePosted || '';
        // Description is HTML-encoded in JSON-LD; strip tags
        if (ld.description) {
          result.description = stripHtmlTags(ld.description);
        }
        if (ld.employmentType) {
          result.employmentType = Array.isArray(ld.employmentType)
            ? ld.employmentType[0] : String(ld.employmentType);
        }
        if (ld.baseSalary) {
          const bs = ld.baseSalary;
          const val = bs.value;
          if (val?.minValue && val?.maxValue) {
            result.salary = `$${Number(val.minValue).toLocaleString()}-$${Number(val.maxValue).toLocaleString()} ${val.unitText || ''}`.trim();
          } else if (val?.value) {
            result.salary = `$${Number(val.value).toLocaleString()} ${val.unitText || ''}`.trim();
          }
        }
      }
    } catch (e) {
      // JSON-LD parse failure, fall through to HTML
    }
  }

  // --- HTML fallback / supplement ---
  const article = $('article').first();
  if (article.length) {
    if (!result.title) {
      result.title = article.find('h1').first().text().trim();
    }
    if (!result.company) {
      const companyEl = article.find('a.c').first();
      result.company = companyEl.text().trim();
      result.companyUrl = companyEl.attr('href') || '';
      if (result.companyUrl && !result.companyUrl.startsWith('http')) {
        result.companyUrl = `https://gamejobs.co${result.companyUrl}`;
      }
    }
    if (!result.location) {
      const locEl = article.find('a.w').first();
      result.location = locEl.text().trim();
    }
    // Apply URL from the "Apply" button
    const applyBtn = article.find('a.btn[href]').first();
    if (applyBtn.length) {
      const href = applyBtn.attr('href') || '';
      if (href.startsWith('http')) result.applyUrl = href;
    }
    // Job ID from save form data-job-id attribute
    const saveForm = article.find('form.save-job').first();
    if (saveForm.length) {
      result.jobId = saveForm.attr('data-job-id') || '';
    }
    // Description from article paragraphs (if not from JSON-LD)
    if (!result.description) {
      const paragraphs = [];
      article.find('p, ul, ol').each((_, el) => {
        paragraphs.push($(el).text().trim());
      });
      result.description = paragraphs.filter(Boolean).join('\n\n');
    }
  }

  return result;
}

function stripHtmlTags(html) {
  if (!html) return '';
  return html
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/p>/gi, '\n\n')
    .replace(/<\/li>/gi, '\n')
    .replace(/<[^>]+>/g, '')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/\n{3,}/g, '\n\n')
    .trim();
}

const GAMEJOBS_CACHE_TTL_DAYS = 30;
const GAMEJOBS_CACHE_KEY = 'gamejobs_detail_cache.json';

async function runGameJobsCo(source, kv) {
  const query = String(source.query || '').trim();
  if (!query) throw new Error(`[${source.id}] Missing source.query`);

  const maxPages = source.maxPages || 3;
  const detailDelayMs = source.detailDelayMs || 6000;
  const fetchDetails = source.fetchDetails !== false;

  // Use Apify residential proxy to bypass Cloudflare
  let proxyConfig = null;
  try {
    proxyConfig = await Actor.createProxyConfiguration({ groups: ['RESIDENTIAL'] });
    log.info(`[${source.id}] Residential proxy enabled.`);
  } catch (err) {
    log.warning(`[${source.id}] Could not create residential proxy: ${err?.message || err}. Falling back to direct.`);
  }

  // Consistent browser fingerprint for this source run (avoids Chrome UA + Firefox TLS mismatch)
  const headerGenOpts = {
    browsers: [{ name: 'chrome', minVersion: 120 }],
    devices: ['desktop'],
    operatingSystems: ['windows'],
    locales: ['en-US'],
  };

  async function fetchWithProxy(url) {
    const opts = {
      url,
      responseType: 'text',
      timeout: { request: 30000 },
      headerGeneratorOptions: headerGenOpts,
    };
    if (proxyConfig) {
      opts.proxyUrl = await proxyConfig.newUrl();
    }
    const response = await gotScraping(opts);
    if (response.statusCode >= 400) {
      const err = new Error(`HTTP ${response.statusCode} for ${url}: ${response.body?.slice(0, 300)}`);
      err.status = response.statusCode;
      throw err;
    }
    return response.body;
  }

  // Randomized delay to avoid Cloudflare detecting fixed intervals (3-9 seconds)
  function randomDelay() {
    return 3000 + Math.floor(Math.random() * 6000);
  }

  // Collect entries from Atom feed
  const allEntries = [];
  let nextUrl = `https://gamejobs.co/search?q=${encodeURIComponent(query)}&format=atom`;
  let pagesFetched = 0;
  let lastPageHadEntries = false;

  for (let page = 1; page <= maxPages && nextUrl; page++) {
    log.info(`[${source.id}] GET ${nextUrl} (page ${page}/${maxPages})`);

    let xml;
    try {
      xml = await withRetries(
        () => fetchWithProxy(nextUrl),
        { retries: 2, baseMs: 5000, maxMs: 30000, label: source.id },
      );
    } catch (err) {
      log.warning(`[${source.id}] Failed to fetch feed page ${page}: ${err.message}. Stopping.`);
      break;
    }

    const $ = cheerioLoad(xml, { xmlMode: true });
    const entries = $('entry');

    if (entries.length === 0) {
      log.info(`[${source.id}] Page ${page}: no entries, stopping.`);
      break;
    }

    entries.each((_, el) => {
      const $e = $(el);
      const fullTitle = $e.find('title').first().text().trim();
      const link = $e.find('link[rel="alternate"]').attr('href')
        || $e.find('link').attr('href') || '';
      const updated = $e.find('updated').first().text().trim();

      // Title format: "Job Title at Company"
      let title = fullTitle;
      let company = '';
      const atIdx = fullTitle.lastIndexOf(' at ');
      if (atIdx > 0) {
        title = fullTitle.slice(0, atIdx).trim();
        company = fullTitle.slice(atIdx + 4).trim();
      }

      // Extract ID from URL: trailing number or full slug
      const idMatch = link.match(/\/([^/]+?)(?:-(\d+))?$/);
      const numericId = idMatch && idMatch[2] ? idMatch[2] : '';
      const slug = idMatch ? idMatch[1] + (idMatch[2] ? `-${idMatch[2]}` : '') : '';

      if (link) {
        allEntries.push({
          title,
          company,
          url: link.startsWith('http') ? link : `https://gamejobs.co${link}`,
          postedAt: updated,
          id: numericId || slug,
          slug,
        });
      }
    });

    pagesFetched++;
    lastPageHadEntries = entries.length > 0;
    log.info(`[${source.id}] Page ${page}: ${entries.length} entries (total ${allEntries.length})`);

    // Get next page URL
    const nextLink = $('link[rel="next"]').attr('href');
    nextUrl = nextLink || null;

    // Delay between feed pages (randomized to avoid bot detection)
    if (nextUrl && page < maxPages) {
      await new Promise(r => setTimeout(r, randomDelay()));
    }
  }

  log.info(`[${source.id}] Atom feed: ${allEntries.length} entries from ${pagesFetched} pages`);

  // Deduplicate entries by URL
  const seenUrls = new Set();
  const uniqueEntries = [];
  for (const entry of allEntries) {
    const key = entry.url.toLowerCase();
    if (!seenUrls.has(key)) {
      seenUrls.add(key);
      uniqueEntries.push(entry);
    }
  }
  if (uniqueEntries.length < allEntries.length) {
    log.info(`[${source.id}] Deduplicated: ${allEntries.length} → ${uniqueEntries.length}`);
  }

  // Load detail cache from KV store
  let detailCache = {};
  if (kv) {
    try {
      const rawCache = await kv.getValue(GAMEJOBS_CACHE_KEY);
      if (rawCache && typeof rawCache === 'object') {
        const cutoff = Date.now() - GAMEJOBS_CACHE_TTL_DAYS * 24 * 60 * 60 * 1000;
        for (const [url, entry] of Object.entries(rawCache)) {
          if (entry?.fetchedAt && new Date(entry.fetchedAt).getTime() >= cutoff) {
            detailCache[url] = entry;
          }
        }
        const pruned = Object.keys(rawCache).length - Object.keys(detailCache).length;
        if (pruned > 0) log.info(`[${source.id}] Detail cache: pruned ${pruned} expired entries (TTL ${GAMEJOBS_CACHE_TTL_DAYS}d).`);
        log.info(`[${source.id}] Detail cache loaded: ${Object.keys(detailCache).length} entries.`);
      }
    } catch (err) {
      log.warning(`[${source.id}] Failed to load detail cache: ${err?.message || err}`);
    }
  }

  // Fetch detail pages if enabled
  let detailsFetched = 0;
  let detailsFailed = 0;
  let detailsCached = 0;

  if (fetchDetails && uniqueEntries.length > 0) {
    log.info(`[${source.id}] Fetching ${uniqueEntries.length} detail pages (${detailDelayMs}ms delay)...`);

    // Helper: apply cached/fetched detail to an entry
    function applyDetail(entry, detail) {
      if (detail.title) entry.title = detail.title;
      if (detail.company) entry.company = detail.company;
      if (detail.companyUrl) entry.companyUrl = detail.companyUrl;
      if (detail.location) entry.location = detail.location;
      if (detail.applyUrl) entry.applyUrl = detail.applyUrl;
      if (detail.description) entry.description = detail.description;
      if (detail.salary) entry.salary = detail.salary;
      if (detail.employmentType) entry.employmentType = detail.employmentType;
      if (detail.postedAt) entry.postedAt = detail.postedAt;
      if (detail.jobId) entry.id = detail.jobId;
    }

    const failedEntries = []; // collect 403'd entries for retry

    for (let i = 0; i < uniqueEntries.length; i++) {
      const entry = uniqueEntries[i];
      const cacheKey = entry.url.toLowerCase();

      // Check cache first
      const cached = detailCache[cacheKey];
      if (cached) {
        applyDetail(entry, cached);
        detailsCached++;
        continue;
      }

      // Fetch from site
      try {
        const html = await withRetries(
          () => fetchWithProxy(entry.url),
          { retries: 1, baseMs: 5000, maxMs: 20000, label: `${source.id}:detail` },
        );
        const detail = parseGameJobsDetail(html, entry.url);
        applyDetail(entry, detail);

        // Save to cache
        detailCache[cacheKey] = { ...detail, fetchedAt: new Date().toISOString() };
        detailsFetched++;
      } catch (err) {
        log.warning(`[${source.id}] Detail page failed (${i + 1}/${uniqueEntries.length}): ${entry.url} — ${err.message}`);
        failedEntries.push(entry);
        detailsFailed++;
      }

      // Randomized delay between detail fetches (not after last one)
      if (i < uniqueEntries.length - 1) {
        await new Promise(r => setTimeout(r, randomDelay()));
      }

      // Progress log + incremental cache save every 25 pages
      if ((i + 1) % 25 === 0) {
        log.info(`[${source.id}] Detail progress: ${i + 1}/${uniqueEntries.length} (${detailsFetched} fetched, ${detailsCached} cached, ${detailsFailed} failed)`);
        // Save cache incrementally so progress survives timeouts
        if (kv && detailsFetched > 0) {
          try { await kv.setValue(GAMEJOBS_CACHE_KEY, detailCache); }
          catch { /* non-fatal */ }
        }
      }
    }

    log.info(`[${source.id}] Detail pages: ${detailsFetched} fetched, ${detailsCached} cached, ${detailsFailed} failed`);

    // Retry pass: re-attempt 403'd pages in shuffled order (different IP + timing)
    if (failedEntries.length > 0) {
      // Shuffle using Fisher-Yates
      for (let i = failedEntries.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [failedEntries[i], failedEntries[j]] = [failedEntries[j], failedEntries[i]];
      }

      log.info(`[${source.id}] Retry pass: ${failedEntries.length} failed pages (shuffled)...`);
      let retryOk = 0;
      let retryFail = 0;

      for (let i = 0; i < failedEntries.length; i++) {
        const entry = failedEntries[i];
        const cacheKey = entry.url.toLowerCase();

        try {
          const html = await fetchWithProxy(entry.url); // single attempt, no withRetries
          const detail = parseGameJobsDetail(html, entry.url);
          applyDetail(entry, detail);
          detailCache[cacheKey] = { ...detail, fetchedAt: new Date().toISOString() };
          retryOk++;
          detailsFetched++;
          detailsFailed--; // un-count the earlier failure
        } catch {
          retryFail++;
        }

        if (i < failedEntries.length - 1) {
          await new Promise(r => setTimeout(r, randomDelay()));
        }
      }

      log.info(`[${source.id}] Retry pass done: ${retryOk} recovered, ${retryFail} still failed.`);
    }
  }

  // Save detail cache back to KV store
  if (kv && (detailsFetched > 0 || Object.keys(detailCache).length > 0)) {
    try {
      await kv.setValue(GAMEJOBS_CACHE_KEY, detailCache);
      log.info(`[${source.id}] Detail cache saved: ${Object.keys(detailCache).length} entries.`);
    } catch (err) {
      log.warning(`[${source.id}] Failed to save detail cache: ${err?.message || err}`);
    }
  }

  const hitCap = pagesFetched >= maxPages && lastPageHadEntries;
  const jobs = uniqueEntries.map(item => normalizeGameJobsCo(source.id, item));
  return {
    jobs,
    meta: {
      itemCount: jobs.length,
      pagesFetched,
      maxPages,
      hitCap,
      detailsFetched,
      detailsCached,
      detailsFailed,
    },
  };
}

// --------------- Source dispatcher ---------------

async function runSource(source, config, remaining, kv) {
  const globalMax = Number(config?.run?.maxItemsPerSource || 300) || 300;
  const type = String(source.type || '').toLowerCase();

  if (type === 'apify_actor') return await runApifyActorSource(source, globalMax, remaining);
  if (type === 'remotive') return await runRemotive(source);
  if (type === 'remoteok') return await runRemoteOk(source);
  if (type === 'gracklehq') return await runGrackleHQ(source);
  if (type === 'builtin') return await runBuiltIn(source);
  if (type === 'usajobs') return await runUSAJobs(source);
  if (type === 'gamejobs_co') return await runGameJobsCo(source, kv);

  throw new Error(`[${source.id}] Unknown source.type=${source.type}`);
}

// --------------- collected.xlsx helpers ---------------

function collectedFriendlySource(sourceId) {
  if (!sourceId) return '';
  const s = String(sourceId);
  if (s.startsWith('fantastic_')) return 'Fantastic';
  if (s.startsWith('linkedin_')) return 'LinkedIn';
  if (s.startsWith('builtin_')) return 'BuiltIn';
  if (s.startsWith('usajobs_')) return 'USAJobs';
  if (s.startsWith('gamejobs_co')) return 'GameJobs';
  const map = {
    'fantastic_feed': 'Fantastic',
    'linkedin_jobs': 'LinkedIn',
    'remotive': 'Remotive',
    'remoteok': 'RemoteOK',
    'gracklehq': 'GrackleHQ',
  };
  return map[s] || s;
}

function jobIdPrefix(sourceId) {
  const s = String(sourceId || '');
  if (s.startsWith('fantastic_')) return 'F';
  if (s.startsWith('linkedin_')) return 'L';
  if (s.startsWith('builtin_')) return 'B';
  if (s.startsWith('usajobs_')) return 'U';
  if (s === 'gracklehq') return 'G';
  if (s.startsWith('gamejobs_co')) return 'J';
  return '?';
}

function ensureProtocol(url) {
  if (!url) return '';
  const s = String(url).trim();
  if (!s) return '';
  if (!/^https?:\/\//i.test(s)) return `https://${s}`;
  return s;
}

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

async function buildCollectedXlsx(jobs) {
  const workbook = new ExcelJS.Workbook();
  const ws = workbook.addWorksheet('Collected');

  ws.columns = [
    { header: 'Source',        width: 14 },
    { header: 'Company',       width: 26 },
    { header: 'Job Title',     width: 46 },
    { header: 'Location',      width: 36 },
    { header: 'Position Type', width: 14 },
    { header: 'Salary',        width: 22 },
    { header: 'Posted At',     width: 22 },
    { header: 'URL',           width: 50 },
    { header: 'Search Terms',  width: 40 },
    { header: 'Job IDs',       width: 30 },
  ];

  // Bold header row
  ws.getRow(1).font = { bold: true };

  // Freeze top row + first 2 columns
  ws.views = [{ state: 'frozen', xSplit: 2, ySplit: 1 }];

  for (const j of jobs) {
    const jobIdStr = j.sourceJobId ? `${jobIdPrefix(j.source)}:${j.sourceJobId}` : '';
    const row = ws.addRow([
      collectedFriendlySource(j.source),
      j.company || '',
      j.title || '',            // will become hyperlink
      j.location || '',
      j.employmentType || '',
      j.salary || '',
      j.postedAt || '',
      j.url || j.applyUrl || '',
      (j.searchTerms || []).join('; '),
      jobIdStr,
    ]);

    // Job Title hyperlink
    const jobUrl = j.applyUrl || j.url || '';
    setCellHyperlink(row.getCell(3), jobUrl, j.title || '');
  }

  return await workbook.xlsx.writeBuffer();
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
      const remaining = Math.max(0, maxTotal - allJobs.length);
      const { jobs, meta } = await runSource(source, config, remaining, kv);

      const remaining2 = Math.max(0, maxTotal - allJobs.length);
      const trimmed = remaining2 > 0 ? jobs.slice(0, remaining2) : [];

      // Stamp searchTerms from the source config so downstream actors can track provenance
      const searchTerms = source.input?.titleSearch || [];
      for (const job of trimmed) {
        job.searchTerms = searchTerms;
      }

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
      if (config?.run?.stopOnCollectorErrors) throw err;
    }
  }

  // Build cap warnings for sources that may have missed jobs
  const capWarnings = [];
  for (const s of report.sources) {
    if (s.status !== 'ok') continue;
    if (!s.meta?.hitCap) continue;
    const type = s.type;
    let detail = '';
    if (type === 'remotive') {
      detail = `returned ${s.meta.returnedCount} items (limit ${s.meta.limit}) — more may exist`;
    } else if (type === 'gracklehq') {
      detail = `departments hit page cap: ${(s.meta.deptsCapped || []).join(', ')}`;
    } else if (type === 'builtin') {
      detail = `fetched ${s.meta.pagesFetched}/${s.meta.maxPages} pages — last page still had results`;
    } else if (type === 'usajobs') {
      detail = `returned ${s.meta.itemCount} of ${s.meta.totalAvailable} available — ResultsPerPage cap`;
    } else if (s.meta?.hitLimitLikely) {
      detail = `returned exactly ${s.meta.returnedCount} items (limit ${s.meta.requestedLimit}) — may have more`;
    } else {
      detail = 'returned item count equals request cap';
    }
    capWarnings.push({ sourceId: s.id, type, detail });
  }
  if (allJobs.length >= maxTotal) {
    capWarnings.push({ sourceId: '_global', type: 'global', detail: `maxTotalItems=${maxTotal} reached — some sources may not have been fully collected` });
  }
  report.capWarnings = capWarnings;
  if (capWarnings.length > 0) {
    log.warning(`⚠️  Cap warnings (${capWarnings.length}): ${capWarnings.map(w => `${w.sourceId}: ${w.detail}`).join(' | ')}`);
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

  const datasetInfo = { id: rawDataset.getId?.() || null, name: rawDatasetName, itemCount: pushed };
  await kv.setValue('raw_dataset.json', datasetInfo);
  await kv.setValue('collect_report.json', report);

  // Build collected.xlsx for debugging (all raw jobs before merge/dedup)
  const collectedXlsx = await buildCollectedXlsx(allJobs);
  await kv.setValue('collected.xlsx', collectedXlsx, {
    contentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  });

  log.info(`Collection complete. Pushed ${pushed} jobs to dataset ${rawDatasetName}. collected.xlsx written to KV store.`);
});