// Analyze RAW source data through the new normalizeLocationFields + normalizeEmploymentType
// Simulates the collector's normalizeFantasticFeed/normalizeLinkedin → normalizeLocationFields pipeline
// Run: node analyze_raw.mjs
import { Country, State, City } from 'country-state-city';
import isoCountries from 'i18n-iso-countries';
import { createRequire, Module } from 'module';
import { readFileSync } from 'fs';
const require = createRequire(import.meta.url);
isoCountries.registerLocale(require('i18n-iso-countries/langs/en.json'));

// ========== Location normalization (exact copy from main.js) ==========
const _usStates = State.getStatesOfCountry('US');
const STATE_NAME_TO_ABBREV = {};
const US_STATE_ABBREVS = new Set();
for (const s of _usStates) { STATE_NAME_TO_ABBREV[s.name.toLowerCase()] = s.isoCode; US_STATE_ABBREVS.add(s.isoCode); }
const US_DOMESTIC = new Set(['united states', 'us', 'usa', 'united states of america']);
const FOREIGN_REGIONS = new Set(['south america','latin america','europe','asia','africa','middle east','southeast asia','east asia','south asia','central asia','central america','oceania','eastern europe','western europe','emea','apac','latam']);
const ISO2_ALIASES = { UK: 'GB' };
function _stripDiacritics(s) { return s.normalize('NFD').replace(/[\u0300-\u036f]/g, '').replace(/\u0131/g, 'i').replace(/\u0130/g, 'I'); }
const _cityCountryMap = new Map();
for (const c of City.getAllCities()) { const key = _stripDiacritics(c.name).toLowerCase(); if (!_cityCountryMap.has(key)) _cityCountryMap.set(key, new Set()); _cityCountryMap.get(key).add(c.countryCode); }
for (const country of Country.getAllCountries()) { for (const state of State.getStatesOfCountry(country.isoCode)) { const key = _stripDiacritics(state.name).toLowerCase(); if (!_cityCountryMap.has(key)) _cityCountryMap.set(key, new Set()); _cityCountryMap.get(key).add(country.isoCode); } }
const COMMUTABLE_TOWNS = new Set(['acton','andover','arlington','ashland','ayer','bedford','belmont','beverly','billerica','bolton','boston','boxborough','braintree','brookline','burlington','cambridge','canton','carlisle','chelmsford','chelsea','concord','danvers','dedham','dover','dracut','dunstable','everett','foxborough','framingham','grafton','groton','harvard','holliston','hopkinton','hudson','lawrence','lexington','lincoln','littleton','lowell','lynn','lynnfield','malden','marlborough','maynard','medfield','medford','medway','melrose','methuen','milford','millis','milton','natick','needham','newton','north andover','north reading','northborough','norwood','peabody','pepperell','quincy','reading','revere','salem','saugus','sherborn','shirley','shrewsbury','somerville','southborough','stoneham','stow','sudbury','tewksbury','townsend','tyngsborough','wakefield','walpole','waltham','watertown','wayland','wellesley','westborough','westford','weston','wilmington','winchester','woburn','worcester']);

function countryNameToIso3(name) { const code = isoCountries.getAlpha3Code(name, 'en'); if (code && code !== 'USA') return code; const low = (name || '').toLowerCase(); if (low === 'england' || low === 'scotland' || low === 'wales') return 'GBR'; if (low === 'czechia') return 'CZE'; return null; }
function isIso3Foreign(code) { return code !== 'USA' && !!isoCountries.getName(code, 'en'); }
function iso2ToIso3Foreign(code2) { const upper = code2.toUpperCase(); const resolved = ISO2_ALIASES[upper] || upper; const code3 = isoCountries.alpha2ToAlpha3(resolved); return (code3 && code3 !== 'USA') ? code3 : null; }
function _titleCase(s) { return s.split(/\s+/).map(w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()).join(' '); }

function _classifyToken(token) {
  const trimmed = token.trim(); if (!trimmed) return null;
  const upper = trimmed.toUpperCase(); const lower = trimmed.toLowerCase();
  if (US_DOMESTIC.has(lower)) return { type: 'domestic' };
  if (trimmed.length === 3 && isIso3Foreign(upper)) return { type: 'foreignCountry', iso3: upper };
  if (trimmed.length === 2) { if (US_STATE_ABBREVS.has(upper)) return { type: 'usState', abbrev: upper }; const iso3 = iso2ToIso3Foreign(upper); if (iso3) return { type: 'foreignCountry', iso3 }; }
  if (lower in STATE_NAME_TO_ABBREV) { if (lower === 'georgia') return { type: 'ambiguousState', abbrev: STATE_NAME_TO_ABBREV[lower] }; return { type: 'usState', abbrev: STATE_NAME_TO_ABBREV[lower] }; }
  const asCountry = countryNameToIso3(_titleCase(trimmed)); if (asCountry) return { type: 'foreignCountry', iso3: asCountry };
  if (FOREIGN_REGIONS.has(lower)) return { type: 'foreignRegion', name: _titleCase(trimmed) };
  return null;
}
function _classifyMultiWordPhrase(words) {
  for (let windowSize = Math.min(3, words.length); windowSize >= 2; windowSize--) {
    for (let i = 0; i <= words.length - windowSize; i++) {
      const phrase = words.slice(i, i + windowSize).join(' '); const phraseLow = phrase.toLowerCase();
      if (US_DOMESTIC.has(phraseLow)) return { type: 'domestic', consumed: windowSize, startIdx: i };
      if (phraseLow in STATE_NAME_TO_ABBREV) return { type: 'usState', abbrev: STATE_NAME_TO_ABBREV[phraseLow], consumed: windowSize, startIdx: i };
      const iso3 = countryNameToIso3(_titleCase(phrase)); if (iso3) return { type: 'foreignCountry', iso3, consumed: windowSize, startIdx: i };
    }
  }
  return null;
}
function _computeCommutable(st, city) { if (st !== 'MA') return false; if (!city) return true; return COMMUTABLE_TOWNS.has(city.toLowerCase().trim()); }
function _resolveUSCity(cityName) {
  const cityLow = _stripDiacritics(cityName).toLowerCase().trim();
  const usCities = City.getCitiesOfCountry('US').filter(c => c.name.toLowerCase() === cityLow);
  if (usCities.length === 1) { const sc = usCities[0].stateCode; return { location: `${_titleCase(cityName)} ${sc}`, commutable: _computeCommutable(sc, cityName) }; }
  return { location: _titleCase(cityName), commutable: null };
}
function _resolveCityOnly(cityName) {
  const cityLow = _stripDiacritics(cityName).toLowerCase().trim();
  const countries = _cityCountryMap.get(cityLow); if (!countries) return null;
  const hasUS = countries.has('US'); const foreignCodes = Array.from(countries).filter(c => c !== 'US');
  if (foreignCodes.length > 0 && !hasUS) { const iso3 = isoCountries.alpha2ToAlpha3(foreignCodes[0]); return { location: iso3 || _titleCase(cityName), commutable: false }; }
  if (hasUS && foreignCodes.length === 0) { return _resolveUSCity(cityName); }
  return { location: _titleCase(cityName), commutable: null };
}
function _classifySegment(segment) {
  const words = segment.split(/\s+/).filter(Boolean);
  const cleaned = words.filter(w => !/^\d{5}(-\d{4})?$/.test(w));
  if (!cleaned.length) return null;
  const multi = _classifyMultiWordPhrase(cleaned);
  if (multi && multi.consumed === cleaned.length) return multi;
  if (cleaned.length === 1) return _classifyToken(cleaned[0]);
  return null;
}
function _detectWorkModeRaw(raw) { const low = raw.toLowerCase(); if (/\bremote\b/.test(low)) return 'remote'; if (/\bhybrid\b/.test(low)) return 'hybrid'; if (/\b(on[- ]?site|in[- ]?office|in[- ]?person|onsite)\b/.test(low)) return 'onsite'; return ''; }
function _stripWorkMode(raw) { let s = raw; s = s.replace(/^(in[- ]?office\s+or\s+remote|remote\s+or\s+hybrid|remote\s+or\s+in[- ]?office)\s*[|/,\-:]\s*/i, ''); s = s.replace(/^(remote|hybrid|on[- ]?site|in[- ]?office|in[- ]?person|onsite)\s*[|/,\-:]\s*/i, ''); s = s.replace(/\s*\((remote|hybrid|on[- ]?site|in[- ]?office|in[- ]?person|onsite)\)\s*/ig, ''); s = s.replace(/\b(remote|hybrid|on[- ]?site|in[- ]?office|in[- ]?person|onsite)\b/ig, ''); s = s.replace(/\s+[Oo]ffice\s*$/, ''); s = s.replace(/\s*\((?:CET|GMT|EST|PST|UTC)[^)]*\)\s*/ig, ''); s = s.replace(/\s*[|/,\-:]\s*$/, '').replace(/^\s*[|/,\-:]\s*/, ''); return s.trim(); }

function normalizeLocationFields(rawLocation) {
  if (!rawLocation || typeof rawLocation !== 'string' || !rawLocation.trim()) return { workMode: '', location: '', commutable: null };
  const raw = rawLocation.trim();
  const rawWm = _detectWorkModeRaw(raw);
  let geo = _stripWorkMode(raw);
  let workMode = ''; if (rawWm === 'remote') workMode = 'Remote'; else if (rawWm === 'hybrid') workMode = 'Hybrid'; else if (rawWm === 'onsite') workMode = 'On-Site';
  if (!geo) return { workMode, location: '', commutable: null };
  const multiLocParts = geo.split(/[;|/]/).map(s => s.trim()).filter(Boolean);
  if (multiLocParts.length > 1) geo = multiLocParts[0];
  const segments = geo.split(',').map(s => s.trim()).filter(Boolean);
  let foundCountry = null, foundState = null, foundDomestic = false, foundRegion = null, ambiguousState = null;
  const cityParts = [];
  if (segments.length >= 2) {
    for (let i = segments.length - 1; i >= 0; i--) {
      const result = _classifySegment(segments[i]);
      if (result && result.type === 'foreignCountry' && !foundCountry) foundCountry = result;
      else if (result && result.type === 'usState' && !foundState) foundState = result;
      else if (result && result.type === 'domestic') foundDomestic = true;
      else if (result && result.type === 'ambiguousState' && !ambiguousState) ambiguousState = result;
      else if (result && result.type === 'foreignRegion' && !foundRegion) foundRegion = result;
      else cityParts.unshift(segments[i]);
    }
  } else {
    const words = segments[0].split(/\s+/).filter(Boolean);
    const cleaned = words.filter(w => !/^\d{5}(-\d{4})?$/.test(w));
    if (!cleaned.length) return { workMode, location: '', commutable: null };
    const multi = _classifyMultiWordPhrase(cleaned);
    if (multi) {
      if (multi.type === 'foreignCountry') foundCountry = { iso3: multi.iso3 };
      else if (multi.type === 'usState') foundState = { abbrev: multi.abbrev };
      else if (multi.type === 'domestic') foundDomestic = true;
      const remaining = [...cleaned.slice(0, multi.startIdx), ...cleaned.slice(multi.startIdx + multi.consumed)];
      for (const w of remaining) { const c = _classifyToken(w); if (!c) cityParts.push(w); else if (c.type === 'foreignCountry' && !foundCountry) foundCountry = c; else if (c.type === 'usState' && !foundState) foundState = c; else if (c.type === 'domestic') foundDomestic = true; else if (c.type === 'ambiguousState' && !ambiguousState) ambiguousState = c; else if (c.type === 'foreignRegion' && !foundRegion) foundRegion = c; else cityParts.push(w); }
    } else {
      const segCityTokens = [];
      for (let i = cleaned.length - 1; i >= 0; i--) { const c = _classifyToken(cleaned[i]); if (!c) segCityTokens.push(cleaned[i]); else if (c.type === 'foreignCountry' && !foundCountry) foundCountry = c; else if (c.type === 'usState') { if (!foundState) foundState = c; } else if (c.type === 'domestic') foundDomestic = true; else if (c.type === 'ambiguousState' && !ambiguousState) ambiguousState = c; else if (c.type === 'foreignRegion' && !foundRegion) foundRegion = c; else if (c.type === 'foreignCountry') {} else segCityTokens.push(cleaned[i]); }
      segCityTokens.reverse();
      cityParts.push(...segCityTokens);
    }
  }
  const cityStr = cityParts.length ? _titleCase(cityParts.join(' ').trim()) : '';
  if (foundCountry) return { workMode, location: foundCountry.iso3, commutable: false };
  if (foundState) { if (cityStr) return { workMode, location: `${cityStr} ${foundState.abbrev}`, commutable: _computeCommutable(foundState.abbrev, cityStr) }; return { workMode, location: foundState.abbrev, commutable: foundState.abbrev === 'MA' ? true : false }; }
  if (foundDomestic && cityStr) { const r = _resolveUSCity(cityStr); return { workMode, ...r }; }
  if (foundDomestic) return { workMode, location: 'USA', commutable: null };
  if (foundRegion) return { workMode, location: foundRegion.name, commutable: false };
  if (ambiguousState && !cityStr) return { workMode, location: _titleCase('Georgia'), commutable: null };
  if (ambiguousState && cityStr) { const cc = _cityCountryMap.get(_stripDiacritics(cityStr).toLowerCase()); if (cc && cc.has('GE')) return { workMode, location: 'GEO', commutable: false }; return { workMode, location: `${cityStr} ${ambiguousState.abbrev}`, commutable: _computeCommutable(ambiguousState.abbrev, cityStr) }; }
  if (cityStr) { const r = _resolveCityOnly(cityStr); if (r) return { workMode, ...r }; return { workMode, location: cityStr, commutable: null }; }
  return { workMode, location: geo.trim(), commutable: null };
}

// ========== Employment Type normalization ==========
const EMPLOYMENT_TYPE_MAP = {
  'full_time': 'Full-Time', 'full-time': 'Full-Time', 'full time': 'Full-Time', 'fulltime': 'Full-Time',
  'ft': 'Full-Time', 'salaried_ft': 'Full-Time', 'permanent': 'Full-Time', 'regular': 'Full-Time',
  'part_time': 'Part-Time', 'part-time': 'Part-Time', 'part time': 'Part-Time', 'parttime': 'Part-Time', 'pt': 'Part-Time',
  'freelance': 'Freelance', 'contract': 'Freelance', 'contractor': 'Freelance', 'per_diem': 'Freelance', 'per diem': 'Freelance', '1099': 'Freelance',
  'temporary': 'Temporary', 'temp': 'Temporary', 'seasonal': 'Temporary',
  'internship': 'Internship', 'intern': 'Internship',
  'volunteer': 'Volunteer',
};

function normalizeEmploymentType(raw) {
  if (!raw || typeof raw !== 'string') return { employmentType: null, extractedWorkMode: null };
  const low = raw.trim().toLowerCase();
  if (low === 'other') return { employmentType: null, extractedWorkMode: null };
  // Try direct match
  if (EMPLOYMENT_TYPE_MAP[low]) return { employmentType: EMPLOYMENT_TYPE_MAP[low], extractedWorkMode: null };
  // Strip common suffixes and try again
  const cleaned = low.replace(/\s*(exempt|regular|employee|salaried)\s*/g, ' ').trim();
  if (EMPLOYMENT_TYPE_MAP[cleaned]) return { employmentType: EMPLOYMENT_TYPE_MAP[cleaned], extractedWorkMode: null };
  // Try prefix match (e.g., "full-time remote")
  for (const [key, val] of Object.entries(EMPLOYMENT_TYPE_MAP)) {
    if (low.startsWith(key)) {
      let extractedWorkMode = null;
      const rest = low.slice(key.length).trim();
      if (/remote/.test(rest)) extractedWorkMode = 'Remote';
      else if (/hybrid/.test(rest)) extractedWorkMode = 'Hybrid';
      return { employmentType: val, extractedWorkMode };
    }
  }
  return { employmentType: null, extractedWorkMode: null };
}

// ========== Simulate collector pipeline ==========
function firstString(...args) { for (const a of args) { if (Array.isArray(a)) { for (const v of a) if (typeof v === 'string' && v.trim()) return v.trim(); } else if (typeof a === 'string' && a.trim()) return a.trim(); } return ''; }
function asArray(v) { return Array.isArray(v) ? v : v != null ? [v] : []; }

function buildLocationString(raw) {
  const locationsDerived = asArray(raw.locations_derived).filter(Boolean);
  const aiWA = String(raw.ai_work_arrangement || '').toLowerCase();
  const waD = String(raw.work_arrangement_derived || '').toLowerCase();
  const locType = String(raw.location_type || '').toLowerCase();
  const remote = !!raw.remote_derived || waD.includes('remote') || aiWA.includes('remote') || locType === 'telecommute';
  const hybrid = !!raw.hybrid_derived || waD.includes('hybrid') || aiWA.includes('hybrid');
  const locParts = [];
  if (remote) locParts.push('Remote');
  if (hybrid) locParts.push('Hybrid');
  if (locationsDerived.length) locParts.push(locationsDerived.join('; '));
  else if (raw.location) locParts.push(String(raw.location));
  return locParts.filter(Boolean).join(' | ');
}

// ========== Load and process ==========
const TMP = 'C:/Users/Randy/AppData/Local/Temp';
const sources = [
  { file: `${TMP}/raw_fantastic_nw.json`, name: 'Fantastic (nationwide)' },
  { file: `${TMP}/raw_fantastic_ma.json`, name: 'Fantastic (MA)' },
  { file: `${TMP}/raw_linkedin_nw.json`, name: 'LinkedIn (nationwide)' },
  { file: `${TMP}/raw_linkedin_ma.json`, name: 'LinkedIn (MA)' },
];

const allResults = [];
const issues = [];

for (const src of sources) {
  const items = JSON.parse(readFileSync(src.file, 'utf8'));
  for (const raw of items) {
    const locStr = buildLocationString(raw);
    const result = normalizeLocationFields(locStr);
    const rawEt = firstString(raw.employment_type_derived, raw.employment_type);
    const etResult = normalizeEmploymentType(rawEt);

    allResults.push({
      source: src.name,
      company: raw.organization || '',
      title: raw.title || '',
      rawLocation: locStr,
      location: result.location,
      workMode: result.workMode,
      commutable: result.commutable,
      rawEmploymentType: rawEt,
      employmentType: etResult.employmentType,
      extractedWorkMode: etResult.extractedWorkMode,
    });
  }
}

// ========== Report ==========
console.log(`Processed ${allResults.length} jobs from ${sources.length} sources\n`);

// Unique location outputs
const locCounts = {};
for (const r of allResults) {
  const key = `${r.workMode ? r.workMode + ' | ' : ''}${r.location}`;
  if (!locCounts[key]) locCounts[key] = { count: 0, commutable: r.commutable, examples: [] };
  locCounts[key].count++;
  if (locCounts[key].examples.length < 2) locCounts[key].examples.push(r.rawLocation);
}

const locEntries = Object.entries(locCounts).sort((a, b) => b[1].count - a[1].count);
console.log('=' .repeat(90));
console.log('LOCATION OUTPUT (normalized) — sorted by count');
console.log('=' .repeat(90));
for (const [display, info] of locEntries) {
  const comm = info.commutable === true ? 'YES' : info.commutable === false ? 'no' : '?';
  const rawEx = info.examples[0] !== display ? ` ← "${info.examples[0]}"` : '';
  console.log(`  ${info.count.toString().padStart(4)} × "${display}" (comm=${comm})${rawEx}`);
}

// Commutable summary
const comm = { yes: 0, no: 0, unk: 0 };
for (const r of allResults) { if (r.commutable === true) comm.yes++; else if (r.commutable === false) comm.no++; else comm.unk++; }
console.log(`\nCommutable: ${comm.yes} yes, ${comm.no} no, ${comm.unk} unknown\n`);

// Employment type
const etCounts = {};
for (const r of allResults) {
  const key = `${r.rawEmploymentType || '(empty)'} → ${r.employmentType || '(null)'}`;
  if (!etCounts[key]) etCounts[key] = 0;
  etCounts[key]++;
}
console.log('=' .repeat(90));
console.log('EMPLOYMENT TYPE (raw → normalized)');
console.log('=' .repeat(90));
for (const [key, count] of Object.entries(etCounts).sort((a, b) => b - a)) {
  const wm = allResults.find(r => `${r.rawEmploymentType || '(empty)'} → ${r.employmentType || '(null)'}` === key && r.extractedWorkMode);
  const wmNote = wm ? ` [+wm=${wm.extractedWorkMode}]` : '';
  console.log(`  ${count.toString().padStart(4)} × ${key}${wmNote}`);
}

// Suspicious/interesting cases
console.log('\n' + '=' .repeat(90));
console.log('INTERESTING CASES (potential issues to review)');
console.log('=' .repeat(90));
for (const r of allResults) {
  // Flag: commutable=null with a specific city
  if (r.commutable === null && r.location && r.location !== 'USA' && r.location !== '' && !r.location.includes(' ')) {
    // Bare city name — ambiguous
    issues.push({ type: 'ambiguous_city', ...r });
  }
  // Flag: location seems wrong
  if (r.location && r.rawLocation && r.location.length <= 3 && r.rawLocation.length > 20) {
    issues.push({ type: 'collapsed_to_code', ...r });
  }
}
// Deduplicate issues by type+location
const seenIssues = new Set();
for (const iss of issues) {
  const key = `${iss.type}|${iss.location}|${iss.rawLocation}`;
  if (seenIssues.has(key)) continue;
  seenIssues.add(key);
  console.log(`  [${iss.type}] "${iss.rawLocation}" → loc="${iss.location}" wm="${iss.workMode}" comm=${iss.commutable === true ? 'YES' : iss.commutable === false ? 'no' : '?'}`);
  console.log(`    ${iss.company} — ${iss.title}`);
}
