// actors/01_collect_jobs/src/main.js
// Collect jobs from multiple configured sources and write normalized records to a per-run dataset.

import { Actor, log } from 'apify';
import ExcelJS from 'exceljs';
import { gotScraping } from 'got-scraping';
import { withRetries, processWithRetries, fetchJsonRetryable } from './resilient-fetch.js';
import { load as cheerioLoad } from 'cheerio';
import { createRequire } from 'module';

const _require = createRequire(import.meta.url);
const isoCountries = _require('i18n-iso-countries');
isoCountries.registerLocale(_require('i18n-iso-countries/langs/en.json'));
const { City, State, Country } = _require('country-state-city');

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
    // Handle arrays (e.g., LinkedIn employment_type: ['CONTRACTOR'])
    if (Array.isArray(v)) {
      for (const item of v) {
        if (typeof item === 'string' && item.trim()) return item.trim();
      }
    }
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

// --------------- Location Normalization ---------------
// Splits raw location strings into separate (workMode, location) fields.
// workMode: 'Remote' | 'Hybrid' | 'On-Site' | ''
// location: normalized geography (e.g., 'Boston MA', 'DEU', 'USA', '')

// Build US state lookups from country-state-city library
const _usStates = State.getStatesOfCountry('US');
const STATE_NAME_TO_ABBREV = {};
const US_STATE_ABBREVS = new Set();
for (const s of _usStates) {
  STATE_NAME_TO_ABBREV[s.name.toLowerCase()] = s.isoCode;
  US_STATE_ABBREVS.add(s.isoCode);
}

// Country name → ISO3 lookup using i18n-iso-countries library
function countryNameToIso3(name) {
  const code = isoCountries.getAlpha3Code(name, 'en');
  if (code && code !== 'USA') return code;
  // Handle special cases the library might miss
  const low = (name || '').toLowerCase();
  if (low === 'england' || low === 'scotland' || low === 'wales') return 'GBR';
  if (low === 'czechia') return 'CZE';
  // Inverted/alternate names from HRIS systems
  if (low === 'korea republic of' || low === 'republic of korea' || low === 'south korea') return 'KOR';
  return null;
}

// Direct library helpers (no pre-built tables needed)
function isIso3Foreign(code) {
  return code !== 'USA' && !!isoCountries.getName(code, 'en');
}
// Non-standard ISO-2 aliases (e.g., "UK" is commonly used but standard is "GB")
const ISO2_ALIASES = { UK: 'GB' };
function iso2ToIso3Foreign(code2) {
  const upper = code2.toUpperCase();
  const resolved = ISO2_ALIASES[upper] || upper;
  const code3 = isoCountries.alpha2ToAlpha3(resolved);
  return (code3 && code3 !== 'USA') ? code3 : null;
}

// City name → set of country ISO2 codes (for foreign detection)
// Normalize keys by stripping diacritics (e.g., İzmir → izmir) for reliable lookup
function _stripDiacritics(s) {
  return s.normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/\u0131/g, 'i').replace(/\u0130/g, 'I');
}
const _cityCountryMap = new Map();
for (const c of City.getAllCities()) {
  const key = _stripDiacritics(c.name).toLowerCase();
  if (!_cityCountryMap.has(key)) _cityCountryMap.set(key, new Set());
  _cityCountryMap.get(key).add(c.countryCode);
}
// Also add states/provinces (e.g., Istanbul is a state in Turkey, İzmir is a state in Turkey)
for (const country of Country.getAllCountries()) {
  for (const state of State.getStatesOfCountry(country.isoCode)) {
    const key = _stripDiacritics(state.name).toLowerCase();
    if (!_cityCountryMap.has(key)) _cityCountryMap.set(key, new Set());
    _cityCountryMap.get(key).add(country.isoCode);
  }
}

const US_DOMESTIC = new Set(['united states', 'us', 'usa', 'united states of america']);

const FOREIGN_REGIONS = new Set([
  'south america','latin america','europe','asia','africa','middle east',
  'southeast asia','east asia','south asia','central asia','central america','oceania',
  'eastern europe','western europe','emea','apac','latam',
]);

// Canadian province codes/names → CAN. Used to identify Canadian locations
// (e.g., "Montreal QC", "British Columbia") without a full province system.
const CANADIAN_PROVINCES = new Map([
  ['ab','CAN'],['bc','CAN'],['mb','CAN'],['nb','CAN'],['nl','CAN'],['ns','CAN'],
  ['nt','CAN'],['nu','CAN'],['on','CAN'],['pe','CAN'],['qc','CAN'],['sk','CAN'],['yt','CAN'],
  ['alberta','CAN'],['british columbia','CAN'],['manitoba','CAN'],['new brunswick','CAN'],
  ['newfoundland','CAN'],['nova scotia','CAN'],['ontario','CAN'],['quebec','CAN'],
  ['saskatchewan','CAN'],['prince edward island','CAN'],['northwest territories','CAN'],
  ['nunavut','CAN'],['yukon','CAN'],
]);

// --------------- Employment Type & Work Mode Maps ---------------
// Central lookup tables. Unknown values get logged for diagnostics (not silently defaulted).
const EMPLOYMENT_TYPE_MAP = {
  'full-time': 'Full-Time', 'full_time': 'Full-Time', 'fulltime': 'Full-Time',
  'full time': 'Full-Time', 'permanent': 'Full-Time', 'fte': 'Full-Time',
  'regular': 'Full-Time',
  'part-time': 'Part-Time', 'part_time': 'Part-Time', 'parttime': 'Part-Time',
  'part time': 'Part-Time',
  'contract': 'Freelance', 'contractor': 'Freelance', 'freelance': 'Freelance',
  'freelancer': 'Freelance', '1099': 'Freelance', 'self-employed': 'Freelance',
  'per_diem': 'Freelance', 'per diem': 'Freelance',
  'temporary': 'Temporary', 'temp': 'Temporary', 'seasonal': 'Temporary',
  'intermittent': 'Temporary',
  'intern': 'Internship', 'internship': 'Internship',
  'volunteer': 'Volunteer', 'volunteering': 'Volunteer',
};

const WORK_MODE_MAP = {
  'remote': 'Remote', 'telecommute': 'Remote', 'work from home': 'Remote',
  'wfh': 'Remote', 'fully remote': 'Remote',
  'hybrid': 'Hybrid', 'flexible': 'Hybrid',
  'on-site': 'On-Site', 'onsite': 'On-Site', 'on site': 'On-Site',
  'in-office': 'On-Site', 'in office': 'On-Site', 'in-person': 'On-Site',
};

// Track unknown values for diagnostics
const unknownEmploymentTypes = new Set();
const unknownWorkModes = new Set();

/**
 * Normalize an employmentType value from source data.
 * Source data sometimes puts work mode in employmentType (e.g., "Full-time, Remote").
 * Returns { employmentType, extractedWorkMode }.
 */
function normalizeEmploymentType(raw) {
  if (!raw || typeof raw !== 'string' || !raw.trim()) {
    return { employmentType: null, extractedWorkMode: null };
  }
  let extractedWorkMode = null;
  let remaining = raw.trim().toLowerCase();

  // Check if work mode is embedded (e.g., "Full-time, Remote" or just "Remote")
  for (const [key, mode] of Object.entries(WORK_MODE_MAP)) {
    if (remaining === key) {
      return { employmentType: null, extractedWorkMode: mode };
    }
    // Strip work mode from compound value: "Full-time, Remote" → "full-time"
    const re = new RegExp(`[,;/|\\s]+${key.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}\\b`, 'i');
    if (re.test(remaining)) {
      extractedWorkMode = mode;
      remaining = remaining.replace(re, '').trim();
    }
  }

  if (!remaining) return { employmentType: null, extractedWorkMode };

  const mapped = EMPLOYMENT_TYPE_MAP[remaining];
  if (mapped) return { employmentType: mapped, extractedWorkMode };

  // Unknown value — log for diagnostics
  unknownEmploymentTypes.add(raw.trim());
  return { employmentType: null, extractedWorkMode };
}

// --------------- Commutable Towns (within ~45 min of Lexington MA) ---------------
const COMMUTABLE_TOWNS = new Set([
  'acton', 'andover', 'arlington', 'ashland', 'ayer', 'bedford', 'belmont',
  'beverly', 'billerica', 'bolton', 'boston', 'boxborough', 'braintree',
  'brookline', 'burlington', 'cambridge', 'canton', 'carlisle', 'chelmsford',
  'chelsea', 'concord', 'danvers', 'dedham', 'dover', 'dracut', 'dunstable',
  'everett', 'foxborough', 'framingham', 'grafton', 'groton', 'harvard',
  'holliston', 'hopkinton', 'hudson', 'lawrence', 'lexington', 'lincoln',
  'littleton', 'lowell', 'lynn', 'lynnfield', 'malden', 'marlborough',
  'maynard', 'medfield', 'medford', 'medway', 'melrose', 'methuen', 'milford',
  'millis', 'milton', 'natick', 'needham', 'newton', 'north andover',
  'north reading', 'northborough', 'norwood', 'peabody', 'pepperell', 'quincy',
  'reading', 'revere', 'salem', 'saugus', 'sherborn', 'shirley', 'shrewsbury',
  'somerville', 'southborough', 'stoneham', 'stow', 'sudbury', 'tewksbury',
  'townsend', 'tyngsborough', 'wakefield', 'walpole', 'waltham', 'watertown',
  'wayland', 'wellesley', 'westborough', 'westford', 'weston', 'wilmington',
  'winchester', 'woburn', 'worcester',

]);

// Remote signal patterns for description scanning
const REMOTE_SIGNAL_PATTERNS = [
  /\bremote position\b/, /\bremote role\b/, /\bwork remotely\b/,
  /\bfully remote\b/, /\b100% remote\b/, /\bremote[\s-]?first\b/,
  /\bremote work\b/, /\bremote opportunity\b/,
];

function _detectWorkModeRaw(raw) {
  const low = raw.toLowerCase();
  if (/\bremote\b/.test(low)) return 'remote';
  if (/\bhybrid\b/.test(low)) return 'hybrid';
  if (/\b(on[- ]?site|in[- ]?office|in[- ]?person|onsite)\b/.test(low)) return 'onsite';
  return '';
}

function _stripWorkMode(raw) {
  let s = raw;
  // "In-Office or Remote | ..." compound prefix
  s = s.replace(/^(in[- ]?office\s+or\s+remote|remote\s+or\s+hybrid|remote\s+or\s+in[- ]?office)\s*[|/,\-:]\s*/i, '');
  // Simple work mode prefix
  s = s.replace(/^(remote|hybrid|on[- ]?site|in[- ]?office|in[- ]?person|onsite)\s*[|/,\-:]\s*/i, '');
  // Trailing "(Remote)", "(Onsite)", etc.
  s = s.replace(/\s*\((remote|hybrid|on[- ]?site|in[- ]?office|in[- ]?person|onsite)\)\s*/ig, '');
  // Standalone work mode words
  s = s.replace(/\b(remote|hybrid|on[- ]?site|in[- ]?office|in[- ]?person|onsite|flexible)\b/ig, '');
  // "Office" suffix (e.g., "Warsaw Office" -> "Warsaw")
  s = s.replace(/\s+[Oo]ffice\s*$/, '');
  // Strip parenthesized timezone hints (e.g., "(CET ±2h)", "(GMT+1)")
  s = s.replace(/\s*\((?:CET|GMT|EST|PST|UTC)[^)]*\)\s*/ig, '');
  // Smart parenthetical handling: check if parens contain a location keyword.
  // If so, extract the location content; otherwise strip as noise.
  // Examples: "Second Dinner (us )" → keep "us"; "Bengaluru India (zynga Office)" → strip parens.
  s = s.replace(/\s*\(([^)]*)\)\s*/g, (match, inner) => {
    const trimmed = inner.trim().toLowerCase();
    // Check for domestic signals
    if (US_DOMESTIC.has(trimmed)) return `, ${inner.trim()} `;
    // Check for US state name or abbreviation
    if (trimmed in STATE_NAME_TO_ABBREV || (trimmed.length === 2 && US_STATE_ABBREVS.has(trimmed.toUpperCase()))) {
      return `, ${inner.trim()} `;
    }
    // Check for "in <location>" pattern (e.g., "(in British Columbia)")
    const inMatch = trimmed.match(/^in\s+(.+)/);
    if (inMatch) return `, ${inMatch[1].trim()} `;
    // Check for country name
    const asCountry = countryNameToIso3(_titleCase(trimmed)) || countryNameToIso3(_titleCase(_stripDiacritics(trimmed)));
    if (asCountry) return `, ${inner.trim()} `;
    // No location keyword found — strip as noise
    return ' ';
  });
  // Strip zip/postal codes (e.g., "Seoul 06164" → "Seoul")
  s = s.replace(/\b\d{4,6}\b/g, '');
  // Clean up leftover delimiters and junk
  s = s.replace(/\s*[|/,\-:]\s*$/, '').replace(/^\s*[|/,\-:]\s*/, '');
  return s.trim();
}

/**
 * Classify-first location normalization.
 * Instead of branching on format (comma vs no-comma), we identify components
 * (country, state, city) from tokens, then assemble the output.
 *
 * Returns { workMode, location, commutable }
 *   workMode:   'Remote' | 'Hybrid' | 'On-Site' | ''
 *   location:   normalized display string (e.g., 'Boston MA', 'GBR', 'Izmir')
 *   commutable: true | false | null (within ~45 min of Lexington MA; null = unknown)
 */
function _titleCase(s) {
  return s.split(/\s+/).map(w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()).join(' ');
}

function _classifyToken(token) {
  const trimmed = token.trim();
  if (!trimmed) return null;
  const upper = trimmed.toUpperCase();
  const lower = trimmed.toLowerCase();

  // US domestic signal ("USA", "US", "United States", etc.)
  if (US_DOMESTIC.has(lower)) return { type: 'domestic' };

  // 3-letter ISO3 foreign code
  if (trimmed.length === 3 && isIso3Foreign(upper)) return { type: 'foreignCountry', iso3: upper };

  // 2-letter code: check if it's a US state OR foreign ISO2
  if (trimmed.length === 2) {
    const asUpper = upper;
    if (US_STATE_ABBREVS.has(asUpper)) return { type: 'usState', abbrev: asUpper };
    const iso3 = iso2ToIso3Foreign(asUpper);
    if (iso3) return { type: 'foreignCountry', iso3 };
  }

  // US state full name
  if (lower in STATE_NAME_TO_ABBREV) {
    // "Georgia" is ambiguous — US state or country
    if (lower === 'georgia') return { type: 'ambiguousState', abbrev: STATE_NAME_TO_ABBREV[lower] };
    return { type: 'usState', abbrev: STATE_NAME_TO_ABBREV[lower] };
  }

  // Canadian province code (e.g., "QC", "BC", "ON")
  if (CANADIAN_PROVINCES.has(lower)) return { type: 'foreignCountry', iso3: 'CAN' };

  // Foreign country name (single word) — try with and without diacritics (e.g., "México" → "Mexico")
  const asCountry = countryNameToIso3(_titleCase(trimmed)) || countryNameToIso3(_titleCase(_stripDiacritics(trimmed)));
  if (asCountry) return { type: 'foreignCountry', iso3: asCountry };

  // Foreign region name
  if (FOREIGN_REGIONS.has(lower)) return { type: 'foreignRegion', name: _titleCase(trimmed) };

  return null; // unclassified — potential city name
}

function _classifyMultiWordPhrase(words) {
  // Try 4-word, 3-word, then 2-word sliding windows for multi-word country/state names
  // (4-word needed for "United States of America")
  for (let windowSize = Math.min(4, words.length); windowSize >= 2; windowSize--) {
    for (let i = 0; i <= words.length - windowSize; i++) {
      const phrase = words.slice(i, i + windowSize).join(' ');
      const phraseLow = phrase.toLowerCase();

      if (US_DOMESTIC.has(phraseLow)) return { type: 'domestic', consumed: windowSize, startIdx: i };

      // Multi-word US state (e.g., "New York", "North Carolina")
      if (phraseLow in STATE_NAME_TO_ABBREV) {
        return { type: 'usState', abbrev: STATE_NAME_TO_ABBREV[phraseLow], consumed: windowSize, startIdx: i };
      }

      // Multi-word Canadian province (e.g., "British Columbia", "New Brunswick")
      if (CANADIAN_PROVINCES.has(phraseLow)) return { type: 'foreignCountry', iso3: 'CAN', consumed: windowSize, startIdx: i };

      // Multi-word country name (e.g., "United Kingdom", "South Korea", "Costa Rica")
      const iso3 = countryNameToIso3(_titleCase(phrase));
      if (iso3) return { type: 'foreignCountry', iso3, consumed: windowSize, startIdx: i };
    }
  }
  return null;
}

function _computeCommutable(stateAbbrev, cityName) {
  if (stateAbbrev !== 'MA') return false;
  if (!cityName) return true; // bare "MA" — assume commutable
  return COMMUTABLE_TOWNS.has(cityName.toLowerCase().trim());
}

function _resolveUSCity(cityName) {
  // Given a city name known to be in the US (via domestic signal), resolve state.
  // Only resolves when unambiguous (exactly 1 US city with that name).
  // Multiple matches (e.g., Boston GA/MA/NY) → leave as bare city, commutable: null.
  const cityLow = _stripDiacritics(cityName).toLowerCase().trim();
  const usCities = City.getCitiesOfCountry('US').filter(c => c.name.toLowerCase() === cityLow);
  if (usCities.length === 1) {
    const sc = usCities[0].stateCode;
    return { location: `${_titleCase(cityName)} ${sc}`, commutable: _computeCommutable(sc, cityName) };
  }
  // 0 matches or multiple US cities with same name — ambiguous
  return { location: _titleCase(cityName), commutable: null };
}

function _resolveCityOnly(cityName) {
  // Bare city with no state/country/domestic context. Three-tier resolution:
  //   Tier 1a: Full-string lookup in city database
  //     - Foreign-only city → ISO3, commutable: false
  //     - US-only city → try _resolveUSCity (resolves if unambiguous)
  //     - Ambiguous (US + foreign) → bare city, commutable: null (LLM decides)
  //   Tier 1b: Word-by-word lookup for multi-word strings not in database
  //     - Any word is a known city AND combined countries have NO US → foreign
  //     - Any word is a known city AND countries include US → ambiguous (LLM decides)
  //     - No words are known cities → blank location (LLM decides)
  //   Tier 1c: Single word not in database → blank location (LLM decides)
  const cityLow = _stripDiacritics(cityName).toLowerCase().trim();
  const countries = _cityCountryMap.get(cityLow);

  if (countries) {
    // Tier 1a: Full string found in city database
    const hasUS = countries.has('US');
    const foreignCodes = Array.from(countries).filter(c => c !== 'US');

    if (foreignCodes.length > 0 && !hasUS) {
      const iso3 = isoCountries.alpha2ToAlpha3(foreignCodes[0]);
      return { location: iso3 || _titleCase(cityName), commutable: false };
    }
    if (hasUS && foreignCodes.length === 0) {
      return _resolveUSCity(cityName);
    }
    // Ambiguous (exists in both US and foreign countries) — don't guess
    return { location: _titleCase(cityName), commutable: null };
  }

  // Tier 1b: Full string not found — try word-by-word for multi-word strings
  const words = cityName.replace(/-/g, ' ').split(/\s+/).filter(w => w.length > 1);
  if (words.length > 1) {
    const allCountries = new Set();
    let anyKnownCity = false;
    let firstKnownWord = null;

    for (const word of words) {
      const key = _stripDiacritics(word).toLowerCase();
      const wordCountries = _cityCountryMap.get(key);
      if (wordCountries) {
        anyKnownCity = true;
        if (!firstKnownWord) firstKnownWord = word;
        for (const c of wordCountries) allCountries.add(c);
      }
    }

    if (anyKnownCity) {
      const hasUS = allCountries.has('US');
      const foreignCodes = Array.from(allCountries).filter(c => c !== 'US');

      if (foreignCodes.length > 0 && !hasUS) {
        // All recognized city words are foreign-only → definitely not US
        const iso3 = isoCountries.alpha2ToAlpha3(foreignCodes[0]);
        return { location: iso3 || _titleCase(firstKnownWord), commutable: false };
      }
      if (hasUS && foreignCodes.length === 0) {
        return _resolveUSCity(firstKnownWord);
      }
      // Ambiguous — let LLM decide
      return { location: _titleCase(cityName), commutable: null };
    }

    // No words are known cities — probably not a real location (e.g., "Second Dinner").
    // Blank the location so the scorer's LLM can determine it from the description.
    return { location: '', commutable: null };
  }

  // Tier 1c: Single word not found in database (e.g., "München" native spelling).
  // Blank the location so the scorer's LLM can determine it from the description.
  return { location: '', commutable: null };
}

/**
 * Classify a comma-separated segment as a single geographical entity.
 * Returns a classification object or null if the segment is a city/place name.
 */
function _classifySegment(segment) {
  const words = segment.split(/\s+/).filter(Boolean);
  const cleaned = words.filter(w => !/^\d{5}(-\d{4})?$/.test(w));
  if (!cleaned.length) return null;

  // Try multi-word classification (must consume the entire segment)
  const multi = _classifyMultiWordPhrase(cleaned);
  if (multi && multi.consumed === cleaned.length) return multi;

  // Partial match: multi-word phrase found but doesn't consume the whole segment.
  // E.g., "Los Angeles United States of America" → "United States of America" matched.
  // Return the classification with leftover words as city parts.
  if (multi) {
    const leftover = [
      ...cleaned.slice(0, multi.startIdx),
      ...cleaned.slice(multi.startIdx + multi.consumed)
    ].join(' ').trim();
    return { ...multi, leftoverCity: leftover || null };
  }

  // Single word: try token classification
  if (cleaned.length === 1) return _classifyToken(cleaned[0]);

  // Multi-word segment not fully classified — treat as city/place name
  return null;
}

/**
 * Classify-first, comma-positional location normalization.
 *
 * Two modes:
 *   - Multi-segment (comma-separated): each segment is classified as a whole,
 *     right-to-left.  Commas act as structural boundaries between city / state /
 *     country.  No special cases needed for "New York, NY" because the comma
 *     tells us "New York" is the city and "NY" is the state.
 *   - Single segment (no commas): token-level classification within the segment,
 *     right-to-left.  Handles "Cambridge UK", "Jakarta IDN", "Boston", etc.
 *
 * Returns { workMode, location, commutable }
 */
function normalizeLocationFields(rawLocation, options) {
  if (!rawLocation || typeof rawLocation !== 'string' || !rawLocation.trim()) {
    return { workMode: '', location: '', commutable: null };
  }
  const raw = rawLocation.trim();
  const rawWm = _detectWorkModeRaw(raw);
  let geo = _stripWorkMode(raw);

  // Optional country hint (ISO2, e.g., "GE") from upstream source metadata.
  // When present and non-US, overrides false "United States" domestic signals
  // (e.g., Fantastic scraper geocoding Georgia-the-country as "Georgia, United States").
  const countryHint = options?.countryHint?.toUpperCase() || null;

  // Determine work mode
  let workMode = '';
  if (rawWm === 'remote') workMode = 'Remote';
  else if (rawWm === 'hybrid') workMode = 'Hybrid';
  else if (rawWm === 'onsite') workMode = 'On-Site';

  if (!geo) {
    return { workMode, location: '', commutable: null };
  }

  // Multi-location: pick first (e.g., "Boston; New York; Chicago", "Austin | Dallas")
  const multiLocParts = geo.split(/[;|/]/).map(s => s.trim()).filter(Boolean);
  if (multiLocParts.length > 1) geo = multiLocParts[0];

  // Pre-process hyphenated HRIS location codes from corporate job systems.
  // Examples: "Us-california-irvine", "Usa-tx-austin-12515researchbldg7",
  //           "Can-qc-montreal-rue St-hubert", "Intl-india-bengaluru"
  // Also handle "Us - Bellevue" (dash with spaces).
  // Strategy: if the first hyphen-segment is a recognizable country/domestic token,
  // split on hyphens (or " - ") and rejoin with commas, dropping address/junk suffixes.
  const dashParts = geo.split(/\s*-\s*/).filter(Boolean);
  if (dashParts.length >= 2) {
    const firstLow = dashParts[0].toLowerCase().trim();
    const isDomestic = ['us', 'usa'].includes(firstLow);
    const isKnownCountry = isDomestic || ['can', 'intl', 'international', 'irl', 'gbr', 'gb', 'deu', 'de', 'fra', 'fr', 'aus', 'jpn', 'ind', 'new'].includes(firstLow)
      || countryNameToIso3(_titleCase(firstLow))
      || (firstLow.length === 2 && iso2ToIso3Foreign(firstLow.toUpperCase()))
      || (firstLow.length === 3 && isIso3Foreign(firstLow.toUpperCase()));
    if (isKnownCountry) {
      // Drop segments that look like street addresses (start with digits, contain "bldg", "pkwy", etc.)
      const cleaned = dashParts.filter(p => !/^\d/.test(p.trim()) && !/(?:bldg|pkwy|ave|blvd|rue|str|plaza|via)\b/i.test(p));
      geo = cleaned.join(', ');
    }
  }

  // Normalize compressed comma patterns: "WA,US" → "WA, US", "Shanghai,CN" → "Shanghai, CN"
  // GameJobs sometimes omits spaces around commas in location codes.
  geo = geo.replace(/,(?!\s)/g, ', ');

  // Separate trailing 2-letter state/country codes from city names within segments.
  // "Renton WA" → "Renton, WA" when the last token is a known 2-letter code.
  // This ensures proper segment-level classification in multi-segment mode.
  geo = geo.replace(/\b([A-Za-z][\w\s]+?)\s+([A-Z]{2})(?=\s*,|$)/g, (match, city, code) => {
    const upper = code.toUpperCase();
    if (US_STATE_ABBREVS.has(upper) || CANADIAN_PROVINCES.has(code.toLowerCase())) {
      return `${city}, ${code}`;
    }
    return match;
  });

  // Deduplicate consecutive identical tokens (e.g., "US US" → "US", "Shanghai Shanghai" → "Shanghai")
  geo = geo.replace(/\b(\w+)(\s+\1)+\b/gi, '$1');

  // Strip trailing junk phrases from location strings (e.g., "No Mans Area", "Metropolitan Area")
  geo = geo.replace(/\s*\b(no mans area|metropolitan area|greater \w+ area)\b\s*/gi, '');

  // --- Split on commas for positional parsing ---
  let segments = geo.split(',').map(s => s.trim()).filter(Boolean);
  // Deduplicate consecutive identical segments (e.g., "Kyiv, Kyiv" → "Kyiv")
  segments = segments.filter((s, i) => i === 0 || s.toLowerCase() !== segments[i - 1].toLowerCase());

  let foundCountry = null;      // { iso3 } or null
  let foundState = null;        // { abbrev } or null
  let foundDomestic = false;    // "USA"/"US" signal
  let foundRegion = null;       // foreign region name
  let ambiguousState = null;    // "Georgia" — could be US state or country
  const cityParts = [];         // city segments (multi) or city tokens (single)

  if (segments.length >= 2) {
    // --- Multi-segment (comma-separated): positional parsing ---
    // Rightmost segments are most likely state/country/domestic signals.
    for (let i = segments.length - 1; i >= 0; i--) {
      const result = _classifySegment(segments[i]);
      if (result && result.type === 'foreignCountry' && !foundCountry) {
        foundCountry = result;
        if (result.leftoverCity) cityParts.unshift(result.leftoverCity);
      } else if (result && result.type === 'usState' && !foundState) {
        foundState = result;
        if (result.leftoverCity) cityParts.unshift(result.leftoverCity);
      } else if (result && result.type === 'domestic') {
        foundDomestic = true;
        if (result.leftoverCity) cityParts.unshift(result.leftoverCity);
      } else if (result && result.type === 'ambiguousState' && !ambiguousState) {
        ambiguousState = result;
        if (result.leftoverCity) cityParts.unshift(result.leftoverCity);
      } else if (result && result.type === 'foreignRegion' && !foundRegion) {
        foundRegion = result;
        if (result.leftoverCity) cityParts.unshift(result.leftoverCity);
      } else {
        cityParts.unshift(segments[i]); // preserve left-to-right order
      }
    }
  } else {
    // --- Single segment (no commas): token-level parsing ---
    const words = segments[0].split(/\s+/).filter(Boolean);
    const cleaned = words.filter(w => !/^\d{5}(-\d{4})?$/.test(w));
    if (!cleaned.length) return { workMode, location: '', commutable: null };

    // Try multi-word classification first (e.g., "South Korea", "United Kingdom")
    const multi = _classifyMultiWordPhrase(cleaned);
    if (multi) {
      if (multi.type === 'foreignCountry') foundCountry = { iso3: multi.iso3 };
      else if (multi.type === 'usState') foundState = { abbrev: multi.abbrev };
      else if (multi.type === 'domestic') foundDomestic = true;
      const remaining = [...cleaned.slice(0, multi.startIdx), ...cleaned.slice(multi.startIdx + multi.consumed)];
      for (const w of remaining) {
        const c = _classifyToken(w);
        if (!c) cityParts.push(w);
        else if (c.type === 'foreignCountry' && !foundCountry) foundCountry = c;
        else if (c.type === 'usState' && !foundState) foundState = c;
        else if (c.type === 'domestic') foundDomestic = true;
        else if (c.type === 'ambiguousState' && !ambiguousState) ambiguousState = c;
        else if (c.type === 'foreignRegion' && !foundRegion) foundRegion = c;
        else cityParts.push(w);
      }
    } else {
      // Classify individual tokens right-to-left
      const segCityTokens = [];
      for (let i = cleaned.length - 1; i >= 0; i--) {
        const c = _classifyToken(cleaned[i]);
        if (!c) {
          segCityTokens.push(cleaned[i]);
        } else if (c.type === 'foreignCountry' && !foundCountry) {
          foundCountry = c;
        } else if (c.type === 'usState') {
          if (!foundState) foundState = c;
          // Duplicate state — discard, don't add as city
        } else if (c.type === 'domestic') {
          foundDomestic = true;
        } else if (c.type === 'ambiguousState' && !ambiguousState) {
          ambiguousState = c;
        } else if (c.type === 'foreignRegion' && !foundRegion) {
          foundRegion = c;
        } else if (c.type === 'foreignCountry') {
          // Duplicate foreign country — discard
        } else {
          segCityTokens.push(cleaned[i]);
        }
      }
      segCityTokens.reverse();
      cityParts.push(...segCityTokens);
    }
  }

  // --- Resolve ---
  // Strip leading street address prefixes (e.g., "23 Odyssey Irvine" → "Irvine")
  // Pattern: digits followed by a word that isn't a city (street name), then real city
  let rawCity = cityParts.join(' ').trim();
  rawCity = rawCity.replace(/^\d+\s+\S+\s+/, (match) => {
    // Only strip if what remains has content (don't strip the whole thing)
    const remainder = rawCity.slice(match.length).trim();
    return remainder ? '' : match;
  });
  const cityStr = rawCity ? _titleCase(rawCity) : '';

  // Country hint override: if upstream metadata says the country is NOT the US,
  // don't let a false "United States" domestic signal promote an ambiguous state.
  // Example: Fantastic scraper geocodes Georgia (country) as "Georgia, United States".
  if (countryHint && countryHint !== 'US' && countryHint !== 'USA') {
    if (ambiguousState && foundDomestic) {
      // The domestic signal is wrong — resolve the ambiguous name as a foreign country.
      const iso3 = isoCountries.alpha2ToAlpha3(countryHint);
      if (iso3) {
        return { workMode, location: iso3, commutable: false };
      }
    }
    // Even without an ambiguous state, a non-US hint + domestic signal is contradictory.
    // Trust the hint: clear the domestic flag so downstream resolution doesn't assume US.
    foundDomestic = false;
  }

  // Promote ambiguous state when domestic signal confirms US (e.g., "Atlanta, Georgia, United States")
  if (ambiguousState && foundDomestic && !foundState) {
    foundState = { abbrev: ambiguousState.abbrev };
    ambiguousState = null;
  }

  // Validate state+city: if city doesn't exist in ANY US state but the 2-letter abbrev
  // is also a foreign ISO2 code, prefer the foreign interpretation (e.g., "Jakarta ID" → Indonesia)
  if (foundState && cityStr && !foundDomestic && !foundCountry) {
    const cityLow = _stripDiacritics(cityStr).toLowerCase();
    const cityCountries = _cityCountryMap.get(cityLow);
    if (cityCountries && !cityCountries.has('US')) {
      const iso3 = iso2ToIso3Foreign(foundState.abbrev);
      if (iso3) {
        foundCountry = { iso3 };
        foundState = null;
      }
    }
  }

  // Priority 1: Foreign country found
  if (foundCountry) {
    return { workMode, location: foundCountry.iso3, commutable: false };
  }

  // Priority 2: US state found
  if (foundState) {
    if (cityStr) {
      const commutable = _computeCommutable(foundState.abbrev, cityStr);
      return { workMode, location: `${cityStr} ${foundState.abbrev}`, commutable };
    }
    const commutable = foundState.abbrev === 'MA' ? true : false;
    return { workMode, location: foundState.abbrev, commutable };
  }

  // Priority 3: Domestic signal ("USA"/"US") with city — resolve state if unambiguous
  if (foundDomestic && cityStr) {
    const resolved = _resolveUSCity(cityStr);
    return { workMode, ...resolved };
  }

  // Priority 4: Bare domestic signal
  if (foundDomestic) {
    return { workMode, location: 'USA', commutable: null };
  }

  // Priority 5: Foreign region
  if (foundRegion) {
    return { workMode, location: foundRegion.name, commutable: false };
  }

  // Priority 6: Ambiguous state (e.g., "Georgia" — US state or country)
  if (ambiguousState && !cityStr) {
    return { workMode, location: _titleCase('Georgia'), commutable: null };
  }
  if (ambiguousState && cityStr) {
    // "Tbilisi, Georgia" — city lookup to disambiguate
    const cityCountries = _cityCountryMap.get(_stripDiacritics(cityStr).toLowerCase());
    if (cityCountries && cityCountries.has('GE')) {
      return { workMode, location: 'GEO', commutable: false };
    }
    // Default: assume US state Georgia
    const commutable = _computeCommutable(ambiguousState.abbrev, cityStr);
    return { workMode, location: `${cityStr} ${ambiguousState.abbrev}`, commutable };
  }

  // Priority 7: City-only (no state, country, or domestic signal)
  // _resolveCityOnly uses the city database for Tier 1 resolution, and blanks
  // unrecognized locations so the scorer's LLM can determine them (Tier 3).
  if (cityStr) {
    return { workMode, ..._resolveCityOnly(cityStr) };
  }

  // Priority 8: Nothing classifiable
  return { workMode, location: geo.trim(), commutable: null };
}

// --------------- Country Hint Extraction ---------------

/**
 * Extract a country hint (ISO2 code) from Fantastic/LinkedIn raw metadata.
 * Used to override false domestic signals (e.g., Georgia-the-country geocoded as "Georgia, United States").
 *
 * Signals checked (in priority order):
 * 1. source_domain: "ge.linkedin.com" → "GE"  (LinkedIn country subdomain)
 * 2. location_requirements_raw: @type "Country" with bare country name → country ISO2
 * 3. locations_raw[0].address.addressCountry: "US" or empty
 *
 * Returns ISO2 string (e.g., "GE", "US") or null if no signal found.
 */
function _extractCountryHint(raw) {
  // Signal 1: LinkedIn country-code subdomain (e.g., ge.linkedin.com, es.linkedin.com)
  const domain = String(raw.source_domain || raw.url || '');
  const subdomainMatch = domain.match(/^(?:https?:\/\/)?([a-z]{2})\.linkedin\.com/i);
  if (subdomainMatch) {
    const code = subdomainMatch[1].toUpperCase();
    // "www" is not a country code; standard LinkedIn domains (linkedin.com, www.linkedin.com) are not hints
    if (code !== 'WW' && code.length === 2) return code;
  }

  // Signal 2: location_requirements_raw with @type "Country" (Fantastic/Workable sources)
  const locReqs = asArray(raw.location_requirements_raw).filter(Boolean);
  for (const req of locReqs) {
    if (req?.['@type'] === 'Country' && req?.name) {
      const name = String(req.name).trim();
      // "Georgia, United States" → US (scraper thinks it's US); bare "Georgia" → GEO
      if (/united states/i.test(name)) return 'US';
      // Try to resolve the country name to ISO2
      const iso3 = countryNameToIso3(name);
      if (iso3) {
        const iso2 = isoCountries.alpha3ToAlpha2(iso3);
        if (iso2) return iso2;
      }
    }
  }

  // Signal 3: addressCountry from locations_raw
  const locsRaw = asArray(raw.locations_raw).filter(Boolean);
  for (const loc of locsRaw) {
    const ac = loc?.address?.addressCountry;
    if (ac && typeof ac === 'string' && ac.trim().length === 2) {
      return ac.trim().toUpperCase();
    }
  }

  return null;
}

// --------------- Source Normalizers ---------------

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

  // Extract country hint from raw metadata (source_domain, location_requirements_raw, addressCountry)
  const countryHint = _extractCountryHint(raw);

  // Check multiple sources for remote/hybrid status — remote_derived is unreliable for LinkedIn jobs.
  // LinkedIn's "Remote" bubble maps to location_type='TELECOMMUTE' in structured data,
  // but the scraper doesn't always populate remote_derived from it.
  const aiWA = String(raw.ai_work_arrangement || '').toLowerCase();
  const waD = String(raw.work_arrangement_derived || '').toLowerCase();
  const locType = String(raw.location_type || '').toLowerCase();
  const remote = !!raw.remote_derived || waD.includes('remote') || aiWA.includes('remote') || locType === 'telecommute';
  const hybrid = !!raw.hybrid_derived || waD.includes('hybrid') || aiWA.includes('hybrid');

  const locParts = [];
  if (remote) locParts.push('Remote');
  if (hybrid) locParts.push('Hybrid');
  if (locationsDerived.length) locParts.push(locationsDerived.join('; '));
  else if (locationsRaw.length) locParts.push(locationsRaw.join('; '));
  else if (raw.location) locParts.push(String(raw.location));

  const description = firstString(raw.description_text, raw.description, raw.description_html);
  const postedAt = firstString(raw.date_posted, raw.posted_at, raw.postedAt, raw.updated_at, raw.updatedAt);
  let salary = firstString(raw.salary_range_derived, raw.salary_range, raw.salary);
  // Fallback: build salary string from AI-derived min/max (LinkedIn enrichment)
  if (!salary && raw.ai_salary_minvalue && raw.ai_salary_maxvalue) {
    const unit = String(raw.ai_salary_unittext || '').toUpperCase();
    const cur = String(raw.ai_salary_currency || 'USD');
    const fmt = (n) => {
      if (n >= 1000) return `${cur === 'USD' ? '$' : cur + ' '}${Math.round(n / 1000)}K`;
      return `${cur === 'USD' ? '$' : cur + ' '}${n}`;
    };
    salary = `${fmt(raw.ai_salary_minvalue)} - ${fmt(raw.ai_salary_maxvalue)}${unit === 'HOUR' ? '/hr' : unit === 'YEAR' ? '/yr' : ''}`;
  }
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
    countryHint: countryHint || undefined,
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

const GAMEJOBS_CACHE_TTL_DAYS = 45;
const GAMEJOBS_CACHE_KEY = 'gamejobs_detail_cache.json';

async function runGameJobsCo(source, kv) {
  const query = String(source.query || '').trim();
  if (!query) throw new Error(`[${source.id}] Missing source.query`);

  const maxPages = source.maxPages || 3;
  const maxAgeDays = source.maxAgeDays || 45;
  const ageCutoff = Date.now() - maxAgeDays * 24 * 60 * 60 * 1000;
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
  let detailsSkippedAge = 0;

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

      // Check cache first (free — no network cost)
      const cached = detailCache[cacheKey];
      if (cached) {
        applyDetail(entry, cached);
        detailsCached++;
        continue;
      }

      // Skip detail fetching for entries older than maxAgeDays (postedAt from Atom feed)
      if (entry.postedAt) {
        const postedMs = new Date(entry.postedAt).getTime();
        if (!isNaN(postedMs) && postedMs < ageCutoff) {
          detailsSkippedAge++;
          continue;
        }
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

    log.info(`[${source.id}] Detail pages: ${detailsFetched} fetched, ${detailsCached} cached, ${detailsFailed} failed, ${detailsSkippedAge} skipped (>${maxAgeDays}d old)`);

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

  // Filter out entries older than maxAgeDays (GameJobs returns relevance-ranked,
  // not date-ordered, so old jobs are interleaved throughout all pages)
  const beforeFilter = uniqueEntries.length;
  const freshEntries = uniqueEntries.filter(e => {
    if (!e.postedAt) return true; // keep entries with unknown date
    const posted = new Date(e.postedAt).getTime();
    return !isNaN(posted) && posted >= ageCutoff;
  });
  const ageFiltered = beforeFilter - freshEntries.length;
  if (ageFiltered > 0) {
    log.info(`[${source.id}] Age filter: removed ${ageFiltered} entries older than ${maxAgeDays} days (${freshEntries.length} remaining).`);
  }

  const jobs = freshEntries.map(item => normalizeGameJobsCo(source.id, item));
  return {
    jobs,
    meta: {
      itemCount: jobs.length,
      pagesFetched,
      maxPages,
      maxAgeDays,
      hitCap,
      ageFiltered,
      detailsFetched,
      detailsCached,
      detailsFailed,
      detailsSkippedAge: detailsSkippedAge || 0,
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
    { header: 'Location',      width: 30 },
    { header: 'Work Mode',     width: 12 },
    { header: 'Commutable',    width: 12 },
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
      j.workMode || '',
      j.commutable === true ? 'Yes' : j.commutable === false ? 'No' : '',
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

        // Normalize location into separate workMode + location + commutable fields
        const { workMode, location, commutable } = normalizeLocationFields(job.location, { countryHint: job.countryHint });
        job.workMode = workMode;
        job.location = location;
        job.commutable = commutable;

        // Normalize employmentType (may extract work mode from mixed values like "Full-time, Remote")
        const { employmentType: normEt, extractedWorkMode } = normalizeEmploymentType(job.employmentType);
        if (normEt) job.employmentType = normEt;
        if (extractedWorkMode && !job.workMode) job.workMode = extractedWorkMode;

        // Scan description for remote signals (enrichRemoteStatus, absorbed into collector)
        if (!job.workMode || job.workMode !== 'Remote') {
          const desc = String(job.description || '').slice(0, 3000).toLowerCase();
          if (REMOTE_SIGNAL_PATTERNS.some(p => p.test(desc))) {
            job.workMode = 'Remote';
          }
        }
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

  // Log unknown employmentType/workMode values for diagnostics
  if (unknownEmploymentTypes.size > 0) {
    const vals = Array.from(unknownEmploymentTypes);
    log.warning(`Unknown employmentType values (${vals.length}): ${vals.join(', ')}`);
    report.unknownEmploymentTypes = vals;
  }
  if (unknownWorkModes.size > 0) {
    const vals = Array.from(unknownWorkModes);
    log.warning(`Unknown workMode values (${vals.length}): ${vals.join(', ')}`);
    report.unknownWorkModes = vals;
  }

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