// Analyze collected.xlsx locations through the new normalizeLocationFields
// and position types through normalizeEmploymentType.
// Run: node analyze_collected.mjs
import { Country, State, City } from 'country-state-city';
import isoCountries from 'i18n-iso-countries';
import { createRequire } from 'module';
import { readFileSync } from 'fs';
const require = createRequire(import.meta.url);
isoCountries.registerLocale(require('i18n-iso-countries/langs/en.json'));

// --- Location normalization infrastructure (matches main.js) ---
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
  if (usCities.length === 1) {
    const sc = usCities[0].stateCode;
    return { location: `${_titleCase(cityName)} ${sc}`, commutable: _computeCommutable(sc, cityName) };
  }
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

// --- Employment Type normalization (from main.js) ---
const EMPLOYMENT_TYPE_MAP = {
  'full_time': 'Full-Time', 'full-time': 'Full-Time', 'full time': 'Full-Time', 'fulltime': 'Full-Time',
  'ft': 'Full-Time', 'salaried_ft': 'Full-Time', 'permanent': 'Full-Time', 'regular': 'Full-Time',
  'part_time': 'Part-Time', 'part-time': 'Part-Time', 'part time': 'Part-Time', 'parttime': 'Part-Time', 'pt': 'Part-Time',
  'freelance': 'Freelance', 'contract': 'Freelance', 'contractor': 'Freelance', 'per_diem': 'Freelance', 'per diem': 'Freelance', '1099': 'Freelance',
  'temporary': 'Temporary', 'temp': 'Temporary', 'seasonal': 'Temporary',
  'internship': 'Internship', 'intern': 'Internship',
  'volunteer': 'Volunteer',
};
const WORK_MODE_MAP = {
  'remote': 'Remote', 'hybrid': 'Hybrid', 'on-site': 'On-Site', 'onsite': 'On-Site',
  'on site': 'On-Site', 'in-office': 'On-Site', 'in office': 'On-Site', 'in-person': 'On-Site', 'in person': 'On-Site',
  'telecommute': 'Remote', 'work from home': 'Remote', 'wfh': 'Remote',
};

function normalizeEmploymentType(raw) {
  if (!raw || typeof raw !== 'string') return { employmentType: null, extractedWorkMode: null };
  const parts = raw.split(/[,;/]+/).map(s => s.trim().toLowerCase()).filter(Boolean);
  let employmentType = null;
  let extractedWorkMode = null;
  for (const part of parts) {
    // Check work mode first
    if (WORK_MODE_MAP[part]) { extractedWorkMode = WORK_MODE_MAP[part]; continue; }
    // Strip common prefixes/suffixes
    const cleaned = part.replace(/\s*(exempt|regular|employee|salaried)\s*/g, ' ').trim();
    if (EMPLOYMENT_TYPE_MAP[cleaned] && !employmentType) { employmentType = EMPLOYMENT_TYPE_MAP[cleaned]; continue; }
    if (EMPLOYMENT_TYPE_MAP[part] && !employmentType) { employmentType = EMPLOYMENT_TYPE_MAP[part]; continue; }
    // Try prefix match (e.g., "full-time remote" → Full-Time + Remote)
    for (const [key, val] of Object.entries(EMPLOYMENT_TYPE_MAP)) {
      if (part.startsWith(key) && !employmentType) { employmentType = val; break; }
    }
    for (const [key, val] of Object.entries(WORK_MODE_MAP)) {
      if (part.includes(key) && !extractedWorkMode) { extractedWorkMode = val; break; }
    }
  }
  if (!employmentType && raw.trim().toLowerCase() === 'other') employmentType = null; // 'OTHER' → null
  return { employmentType, extractedWorkMode };
}

// --- Load data and analyze ---
const data = JSON.parse(readFileSync('C:/Users/Randy/AppData/Local/Temp/collected_analysis.json', 'utf8'));

// 1. Location analysis
console.log('=' .repeat(80));
console.log('LOCATION ANALYSIS: Old values → New normalizeLocationFields output');
console.log('=' .repeat(80));
console.log();

const locationChanges = [];
const locationSame = [];
const locationByNew = {};

for (const [oldLoc, count] of Object.entries(data.locations)) {
  const result = normalizeLocationFields(oldLoc);
  const newLoc = result.location;
  const commStr = result.commutable === true ? 'YES' : result.commutable === false ? 'no' : '?';

  if (oldLoc !== newLoc || result.workMode) {
    locationChanges.push({ oldLoc, newLoc, commutable: commStr, workMode: result.workMode, count });
  } else {
    locationSame.push({ loc: oldLoc, commutable: commStr, count });
  }
}

// Sort changes by count descending
locationChanges.sort((a, b) => b.count - a.count);

console.log(`CHANGED (${locationChanges.length} unique values):`);
console.log('-'.repeat(80));
for (const ch of locationChanges) {
  const wmNote = ch.workMode ? ` [wm=${ch.workMode}]` : '';
  console.log(`  ${ch.count.toString().padStart(4)} × "${ch.oldLoc}" → "${ch.newLoc}" (comm=${ch.commStr})${wmNote}`);
}
console.log();

console.log(`UNCHANGED (${locationSame.length} unique values, showing commutable):`);
console.log('-'.repeat(80));
locationSame.sort((a, b) => b.count - a.count);
for (const s of locationSame.slice(0, 40)) {
  console.log(`  ${s.count.toString().padStart(4)} × "${s.loc}" (comm=${s.commStr})`);
}
if (locationSame.length > 40) console.log(`  ... and ${locationSame.length - 40} more`);
console.log();

// 2. Commutable summary
const commCounts = { yes: 0, no: 0, unknown: 0 };
for (const [oldLoc, count] of Object.entries(data.locations)) {
  const result = normalizeLocationFields(oldLoc);
  if (result.commutable === true) commCounts.yes += count;
  else if (result.commutable === false) commCounts.no += count;
  else commCounts.unknown += count;
}
console.log('COMMUTABLE SUMMARY:');
console.log(`  Commutable (YES):  ${commCounts.yes}`);
console.log(`  Not commutable:    ${commCounts.no}`);
console.log(`  Unknown (?):       ${commCounts.unknown}`);
console.log();

// 3. Position Type / Employment Type analysis
console.log('=' .repeat(80));
console.log('EMPLOYMENT TYPE ANALYSIS: Old Position Type → New normalizeEmploymentType');
console.log('=' .repeat(80));
console.log();

for (const [oldPt, count] of Object.entries(data.positionTypes).sort((a, b) => b[1] - a[1])) {
  const { employmentType, extractedWorkMode } = normalizeEmploymentType(oldPt);
  const etStr = employmentType || '(null)';
  const wmStr = extractedWorkMode ? ` [wm=${extractedWorkMode}]` : '';
  const changed = (oldPt || '(empty)') !== (employmentType || '(empty)');
  const marker = changed ? ' ← CHANGED' : '';
  console.log(`  ${count.toString().padStart(4)} × "${oldPt || '(empty)'}" → "${etStr}"${wmStr}${marker}`);
}
console.log();

// 4. Interesting cases: bare city names that should resolve
console.log('=' .repeat(80));
console.log('BARE CITY NAMES (old code left unresolved, new code resolves):');
console.log('=' .repeat(80));
console.log();
const bareCities = locationChanges.filter(ch => !ch.workMode && ch.oldLoc !== ch.newLoc && !ch.oldLoc.includes(' '));
for (const ch of bareCities) {
  const countries = _cityCountryMap.get(_stripDiacritics(ch.oldLoc).toLowerCase());
  const countryList = countries ? Array.from(countries).join(',') : 'NOT FOUND';
  console.log(`  ${ch.count.toString().padStart(4)} × "${ch.oldLoc}" → "${ch.newLoc}" (countries in DB: ${countryList})`);
}
