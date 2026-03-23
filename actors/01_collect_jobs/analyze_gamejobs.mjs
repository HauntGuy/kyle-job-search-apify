// Analyze GameJobs.co locations through normalizeLocationFields
// Run: node analyze_gamejobs.mjs
import { Country, State, City } from 'country-state-city';
import isoCountries from 'i18n-iso-countries';
import { createRequire } from 'module';
import { readFileSync } from 'fs';
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
  for (let windowSize = Math.min(4, words.length); windowSize >= 2; windowSize--) {
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
  if (hasUS && foreignCodes.length === 0) return _resolveUSCity(cityName);
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
      cityParts.push(...segCityTokens);
    }
  }
  const cityStr = cityParts.length ? _titleCase(cityParts.join(' ').trim()) : '';
  if (ambiguousState && foundDomestic && !foundState) { foundState = { abbrev: ambiguousState.abbrev }; ambiguousState = null; }
  // Validate state+city: if city doesn't exist in ANY US state but the 2-letter abbrev
  // is also a foreign ISO2 code, prefer the foreign interpretation (e.g., "Jakarta ID" → Indonesia)
  if (foundState && cityStr && !foundDomestic && !foundCountry) {
    const cityLow = _stripDiacritics(cityStr).toLowerCase();
    const cityCountries = _cityCountryMap.get(cityLow);
    if (cityCountries && !cityCountries.has('US')) {
      const iso3 = iso2ToIso3Foreign(foundState.abbrev);
      if (iso3) { foundCountry = { iso3 }; foundState = null; }
    }
  }
  if (foundCountry) return { workMode, location: foundCountry.iso3, commutable: false };
  if (foundState) {
    if (cityStr) return { workMode, location: `${cityStr} ${foundState.abbrev}`, commutable: _computeCommutable(foundState.abbrev, cityStr) };
    return { workMode, location: foundState.abbrev, commutable: foundState.abbrev === 'MA' ? true : false };
  }
  if (foundDomestic && cityStr) { const r = _resolveUSCity(cityStr); return { workMode, ...r }; }
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

// --- Load and process GameJobs detail cache ---
const cache = JSON.parse(readFileSync('C:/Users/Randy/AppData/Local/Temp/gamejobs_cache.json', 'utf8'));
const entries = Object.entries(cache);

const results = { foreign: [], domestic: [], unknown: [], empty: [] };
const issues = []; // suspicious results

for (const [url, detail] of entries) {
  const rawLoc = detail.location || '';
  if (!rawLoc) { results.empty.push({ url, rawLoc }); continue; }

  const r = normalizeLocationFields(rawLoc);
  const entry = { rawLoc, ...r };

  if (r.commutable === false) results.foreign.push(entry);
  else if (r.commutable === true) results.domestic.push(entry);
  else results.unknown.push(entry);
}

console.log(`\n=== GameJobs.co Location Analysis (${entries.length} cached entries) ===`);
console.log(`Foreign (commutable=false): ${results.foreign.length}`);
console.log(`Domestic (commutable=true): ${results.domestic.length}`);
console.log(`Unknown (commutable=null):  ${results.unknown.length}`);
console.log(`Empty location:             ${results.empty.length}`);

// Show domestic results (should be US locations near MA)
console.log(`\n--- Domestic (commutable=true) ---`);
for (const e of results.domestic.slice(0, 30)) {
  console.log(`  "${e.rawLoc}" => loc="${e.location}" wm="${e.workMode}"`);
}

// Show unknown results (need manual review)
console.log(`\n--- Unknown (commutable=null) --- (showing first 50)`);
for (const e of results.unknown.slice(0, 50)) {
  console.log(`  "${e.rawLoc}" => loc="${e.location}" wm="${e.workMode}"`);
}

// Show some foreign results
console.log(`\n--- Foreign (commutable=false, showing first 50) ---`);
for (const e of results.foreign.slice(0, 50)) {
  console.log(`  "${e.rawLoc}" => loc="${e.location}" wm="${e.workMode}"`);
}

// Check for potential misclassifications: US locations classified as foreign
const suspectForeign = results.foreign.filter(e => {
  const low = e.rawLoc.toLowerCase();
  return low.includes('united states') || low.includes('usa') || /\bus\b/.test(low);
});
if (suspectForeign.length) {
  console.log(`\n--- SUSPECT: Foreign but mentions US/USA ---`);
  for (const e of suspectForeign) console.log(`  "${e.rawLoc}" => loc="${e.location}"`);
}

// Check for potential misclassifications: foreign locations classified as domestic
const suspectDomestic = results.domestic.filter(e => {
  const low = e.rawLoc.toLowerCase();
  return low.includes('canada') || low.includes('japan') || low.includes('uk') || low.includes('sweden') || low.includes('germany');
});
if (suspectDomestic.length) {
  console.log(`\n--- SUSPECT: Domestic but mentions foreign country ---`);
  for (const e of suspectDomestic) console.log(`  "${e.rawLoc}" => loc="${e.location}"`);
}
