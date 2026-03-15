// Quick test of normalizeLocationFields - run from actors/01_collect_jobs/
// node test_location.mjs
import { Country, State, City } from 'country-state-city';
import isoCountries from 'i18n-iso-countries';
import { createRequire } from 'module';
const require = createRequire(import.meta.url);
isoCountries.registerLocale(require('i18n-iso-countries/langs/en.json'));

// --- Copy key infrastructure from main.js ---
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
  if (usCities.length === 0) return null;
  let best = usCities[0];
  if (usCities.length > 1) { const cm = usCities.find(c => c.stateCode === 'MA'); if (cm) best = cm; }
  const sc = best.stateCode; return { location: `${_titleCase(cityName)} ${sc}`, commutable: _computeCommutable(sc, cityName) };
}
function _resolveCityOnly(cityName) {
  const cityLow = _stripDiacritics(cityName).toLowerCase().trim();
  const countries = _cityCountryMap.get(cityLow); if (!countries) return null;
  const hasUS = countries.has('US'); const foreignCodes = Array.from(countries).filter(c => c !== 'US'); const hasForeign = foreignCodes.length > 0;
  if (hasForeign && !hasUS) { const iso3 = isoCountries.alpha2ToAlpha3(foreignCodes[0]); return { location: iso3 || _titleCase(cityName), commutable: false }; }
  if (hasUS && !hasForeign) { const resolved = _resolveUSCity(cityName); if (resolved) return resolved; return { location: _titleCase(cityName), commutable: null }; }
  if (hasUS) { const resolved = _resolveUSCity(cityName); if (resolved) return resolved; }
  return { location: _titleCase(cityName), commutable: null };
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
  if (geo.includes(';')) { const parts = geo.split(';').map(s => s.trim()).filter(Boolean); if (parts.length > 1) geo = parts[0]; }

  const segments = geo.split(/[,|;:/]+/).map(s => s.trim()).filter(Boolean);
  let foundCountry = null, foundState = null, foundDomestic = false, foundRegion = null, ambiguousState = null;
  const cityTokens = [];

  for (const segment of segments) {
    const words = segment.split(/\s+/).filter(Boolean); if (!words.length) continue;
    const cleaned = words.filter(w => !/^\d{5}(-\d{4})?$/.test(w)); if (!cleaned.length) continue;
    const multiResult = _classifyMultiWordPhrase(cleaned);
    if (multiResult) {
      if (multiResult.type === 'foreignCountry') foundCountry = { iso3: multiResult.iso3 };
      else if (multiResult.type === 'usState') foundState = { abbrev: multiResult.abbrev };
      else if (multiResult.type === 'domestic') foundDomestic = true;
      const remaining = [...cleaned.slice(0, multiResult.startIdx), ...cleaned.slice(multiResult.startIdx + multiResult.consumed)];
      if (multiResult.type === 'usState' && remaining.length === 0 && multiResult.consumed === cleaned.length) {
        cityTokens.push(...cleaned.slice(multiResult.startIdx, multiResult.startIdx + multiResult.consumed));
      }
      for (const w of remaining) { const c = _classifyToken(w); if (!c) cityTokens.push(w); else if (c.type === 'foreignCountry' && !foundCountry) foundCountry = c; else if (c.type === 'usState' && !foundState) foundState = c; else if (c.type === 'domestic') foundDomestic = true; else if (c.type === 'ambiguousState' && !ambiguousState) ambiguousState = c; else if (c.type === 'foreignRegion' && !foundRegion) foundRegion = c; else cityTokens.push(w); }
      continue;
    }
    const segCityTokens = [];
    for (let i = cleaned.length - 1; i >= 0; i--) {
      const c = _classifyToken(cleaned[i]);
      if (!c) segCityTokens.push(cleaned[i]);
      else if (c.type === 'foreignCountry' && !foundCountry) foundCountry = c;
      else if (c.type === 'usState') { if (!foundState) foundState = c; }
      else if (c.type === 'domestic') foundDomestic = true;
      else if (c.type === 'ambiguousState' && !ambiguousState) ambiguousState = c;
      else if (c.type === 'foreignRegion' && !foundRegion) foundRegion = c;
      else if (c.type === 'foreignCountry') { /* dup */ }
      else segCityTokens.push(cleaned[i]);
    }
    segCityTokens.reverse();
    cityTokens.push(...segCityTokens);
  }

  if (!foundCountry && /\bCET\b|\bGMT[+-]\d/.test(geo)) foundRegion = { name: geo.trim() };
  const cityStr = cityTokens.length ? cityTokens.map(w => w.charAt(0).toUpperCase() + w.slice(1).toLowerCase()).join(' ') : '';

  if (foundCountry) return { workMode, location: foundCountry.iso3, commutable: false };
  if (foundState) {
    if (cityStr) return { workMode, location: `${cityStr} ${foundState.abbrev}`, commutable: _computeCommutable(foundState.abbrev, cityStr) };
    return { workMode, location: foundState.abbrev, commutable: foundState.abbrev === 'MA' ? true : false };
  }
  if (foundDomestic && cityStr) { const r = _resolveCityOnly(cityStr); if (r) return { workMode, ...r }; return { workMode, location: cityStr, commutable: null }; }
  if (foundDomestic) return { workMode, location: 'USA', commutable: null };
  if (foundRegion) return { workMode, location: foundRegion.name, commutable: false };
  if (ambiguousState && !cityStr) return { workMode, location: _titleCase('Georgia'), commutable: null };
  if (ambiguousState && cityStr) {
    const cc = _cityCountryMap.get(_stripDiacritics(cityStr).toLowerCase());
    if (cc && cc.has('GE')) return { workMode, location: 'GEO', commutable: false };
    return { workMode, location: `${cityStr} ${ambiguousState.abbrev}`, commutable: _computeCommutable(ambiguousState.abbrev, cityStr) };
  }
  if (cityStr) { const r = _resolveCityOnly(cityStr); if (r) return { workMode, ...r }; return { workMode, location: cityStr, commutable: null }; }
  return { workMode, location: geo.trim(), commutable: null };
}

// --- Tests ---
const tests = [
  ['Cambridge, UK',              'GBR',             false, ''],
  ['Cambridge UK',               'GBR',             false, ''],
  ['London, UK',                 'GBR',             false, ''],
  ['London UK',                  'GBR',             false, ''],
  ['Berlin, DEU',                'DEU',             false, ''],
  ['Jakarta IDN',                'IDN',             false, ''],
  ['Boston, MA, USA',            'Boston MA',       true,  ''],
  ['Remote | Boston, MA',        'Boston MA',       true,  'Remote'],
  ['Hybrid | Cambridge, MA',     'Cambridge MA',    true,  'Hybrid'],
  ['Vancouver',                  'Vancouver WA',    false,  ''],
  ['Izmir',                      'TUR',             false, ''],
  ['Remote',                     '',                null,  'Remote'],
  ['USA',                        'USA',             null,  ''],
  ['San Francisco, CA',          'San Francisco CA', false, ''],
  ['Lexington, MA',              'Lexington MA',    true,  ''],
  ['Worcester, MA',              'Worcester MA',    true,  ''],
  ['Louisville, KY',             'Louisville KY',   false, ''],
  ['Georgia',                    'Georgia',         null,  ''],
  ['Tbilisi, Georgia',           'GEO',             false, ''],
  ['Atlanta, Georgia',           'Atlanta GA',      false, ''],
  ['Remote | Istanbul, Turkey',  'TUR',             false, 'Remote'],
  ['In-Office | New York, NY',   'New York NY',     false, 'On-Site'],
  ['Remote (CET ±2h)',           '',                null,  'Remote'],  // Voodoo-style
  ['Boston',                     'Boston MA',       true,  ''],
  ['United Kingdom',             'GBR',             false, ''],
  ['South Korea',                'KOR',             false, ''],
  ['Vancouver, USA',             'Vancouver WA',    false, ''],
];

let passed = 0, failed = 0;
for (const [input, expLoc, expComm, expWm] of tests) {
  const r = normalizeLocationFields(input);
  const ok = r.location === expLoc && r.commutable === expComm && r.workMode === expWm;
  if (ok) { passed++; console.log(`  PASS: "${input}" => ${JSON.stringify(r)}`); }
  else { failed++; console.log(`  FAIL: "${input}"\n    Got:      ${JSON.stringify(r)}\n    Expected: loc=${expLoc}, comm=${expComm}, wm=${expWm}`); }
}
console.log(`\n${passed} passed, ${failed} failed`);
