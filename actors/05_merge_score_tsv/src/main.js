// 05_merge_score_tsv/main.js — v2.4 (env var fix + location/visa robust)
// Reads merged.json from KV store "job-pipeline", runs LLM evaluation for Kyle fit,
// and writes accepted.csv + scored.csv (Google Sheets friendly).
//
// Key behavior:
// - Location: LLM reasoning + remote override safety net.
// - Visa targeting: disqualify postings clearly aimed at OPT/CPT/H1B-only pipelines.
// - Acceptance: NOT gated on location_ok for now (debug phase).
// - Outputs: accepted.csv, scored.csv, accepted_debug.json, run_report.json

import { Actor } from 'apify';
import fetch from 'node-fetch';
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const DEFAULT_THRESHOLD = 0.60;
const DEFAULT_MODEL = 'gpt-4o-mini';

function nowIso() { return new Date().toISOString(); }

function escapeFormulaText(s) {
  const t = (s || '').toString().replace(/"/g,'""');
  if (/^[=+\-@]/.test(t)) return `'${t}`;
  return t;
}

function csvEscape(value) {
  const s = (value ?? '').toString();
  const needs = /[",\n]/.test(s);
  const out = s.replace(/"/g, '""');
  return needs ? `"${out}"` : out;
}

function toDaysAgo(published) {
  try {
    const d = new Date(published);
    if (!isFinite(d.getTime())) return '';
    const days = Math.floor((Date.now() - d.getTime()) / 86400000);
    return String(Math.max(0, days));
  } catch { return ''; }
}

function getDescription(rec) {
  const raw = rec.raw || {};
  if (raw.descriptionPlain) return raw.descriptionPlain;
  if (raw.descriptionText) return raw.descriptionText;
  if (raw.description_text) return raw.description_text;
  if (raw.description_html) return raw.description_html;
  return '';
}

function getSalaryHint(rec) {
  if (rec.salary) return rec.salary;
  const raw = rec.raw || {};
  const cur = raw.ai_salary_currency;
  const unit = raw.ai_salary_unittext;
  const min = raw.ai_salary_minvalue;
  const max = raw.ai_salary_maxvalue;
  const val = raw.ai_salary_value;
  const prefix = (cur === 'USD') ? '$' : (cur ? `${cur} ` : '');
  const fmt = (n) => (typeof n === 'number' && isFinite(n)) ? Math.round(n).toLocaleString('en-US') : null;
  if (typeof min === 'number' && typeof max === 'number') return `${prefix}${fmt(min)}–${prefix}${fmt(max)}${unit ? `/${unit.toLowerCase()}` : ''}`;
  if (typeof val === 'number') return `${prefix}${fmt(val)}${unit ? `/${unit.toLowerCase()}` : ''}`;
  return '';
}

function stripCodeFences(s) {
  if (!s) return '';
  return s.replace(/^\s*```(?:json)?\s*/i,'').replace(/\s*```\s*$/,'').trim();
}

function normalizeLocation(loc) {
  if (!loc) return '';
  const s = String(loc).trim();
  if (!s) return '';
  if (!s.startsWith('{')) return s;

  try {
    const obj = JSON.parse(s);
    const parts = [];
    if (obj.name) parts.push(obj.name);

    const addr = obj.address;
    if (addr && typeof addr === 'object') {
      const city = addr.addressLocality;
      const region = addr.addressRegion;
      const country = addr.addressCountry;
      const addrParts = [city, region, country].filter(Boolean);
      if (addrParts.length) parts.push(addrParts.join(', '));
    }
    return parts.length ? parts.join(' — ') : s;
  } catch {
    return s;
  }
}

function isProbablyRemoteText(s) {
  const t = (s || '').toString().toLowerCase();
  return /(remote|work from home|wfh|telecommut|distributed|anywhere)/i.test(t);
}

function isLocationDisqualifierReason(reason) {
  const r = (reason || '').toString().toLowerCase();
  return (
    r.includes('on-site') ||
    r.includes('onsite') ||
    r.includes('hybrid') ||
    r.includes('outside massachusetts') ||
    r.includes('not in massachusetts') ||
    r.includes('location')
  );
}

function applyRemoteOverride(evalObj, locRaw, locNorm) {
  const out = { ...evalObj };

  const workMode = (out.work_mode || '').toString().toLowerCase();
  const remoteByMode = workMode === 'remote';
  const remoteByText = isProbablyRemoteText(locRaw) || isProbablyRemoteText(locNorm) || isProbablyRemoteText(out.location_interpreted) || isProbablyRemoteText(out.notes);

  const isRemote = remoteByMode || remoteByText;

  if (isRemote) {
    out.work_mode = 'Remote';
    out.location_ok = true;

    const reasons = Array.isArray(out.disqualify_reasons) ? out.disqualify_reasons : [];
    const nonLocReasons = reasons.filter(r => !isLocationDisqualifierReason(r));
    const hadOnlyLoc = reasons.length > 0 && nonLocReasons.length === 0;

    out.disqualify_reasons = nonLocReasons;

    if (out.disqualify === true && hadOnlyLoc) {
      out.disqualify = false;
      out.notes = `${(out.notes || '').toString().slice(0, 260)} [Auto-fix: Remote override cleared location-only disqualifier]`.trim();
    }
  }

  if (typeof out.location_interpreted !== 'string') out.location_interpreted = '';
  if (!['high','medium','low'].includes((out.location_confidence || '').toString().toLowerCase())) out.location_confidence = 'low';

  return out;
}

async function llmEvaluate({ apiKey, model, job }) {
  const sys = `You are a strict job-fit evaluator for a specific candidate. You must follow the rubric exactly and output ONLY JSON.`;

  const user = `
Candidate: Kyle Forgaard
Profile:
- 7 years professional Unity/C# (mobile/casual), some C++/Lua, some React experience (not React expert).
- Strong in client-side gameplay/UI/tools in Unity. Unity 2D preferred; Unity 3D OK if not lead/architect.

Hard disqualifiers (set disqualify=true ONLY when clearly true):
- Not primarily a software engineering / programming role (e.g., mechanical engineer, data scientist, marketing, sales, HR, finance, PM).
- VR/XR/AR focus REQUIRED (optional/minor mention is OK).
- Backend-heavy or full-stack-primary (Unity as a small part is NOT OK).
- On-site or Hybrid REQUIRED outside Massachusetts (MA). (Remote anywhere in the United States is OK.)
- Salary explicitly max < 90000 USD/year AND no explicit equity/rev-share/commission upside.
- Visa targeting / sponsorship pipeline ONLY (disqualify=true): postings that explicitly target OPT/CPT/H4 EAD/TN/E3-only, H1B-only/transfer-only, or "non-immigrant visa people" as the intended audience.
  (If the posting merely says "no sponsorship" or "sponsorship available", do NOT disqualify.)

Location interpretation rules (use reasoning):
- The “Location” field may be ambiguous, partial, marketing language, or JSON-like. Interpret it.
- Remote anywhere in the United States is OK. If the job is Remote (or Remote OK), set location_ok=true.
- Treat any of these as Remote: strings containing "remote", "work from home", "wfh", "anywhere", "distributed", "telecommute", "remote ok".
- Treat “Hybrid” as OK only if you can reasonably infer Massachusetts (e.g., Boston/Cambridge/Waltham/Lexington/MA).
- Treat “On-site” as OK only if you can reasonably infer Massachusetts.
- If the location is ambiguous (e.g., "Lexington" with no state), DO NOT fail it—set location_ok=true and location_confidence="low", and explain in notes.
- Only set location_ok=false when it is clearly on-site/hybrid required AND clearly outside Massachusetts.
- Fill location_interpreted with your best guess, e.g., "Remote (US)", "Boston, MA", "Lexington (state unknown)", etc.

Scoring:
- score in [0,1]. 0.60+ means "worth applying" IF not disqualified.
- If missing critical info (salary/location), do NOT auto-disqualify; lower score unless clearly disqualifying.

Return JSON with EXACT keys:
{
  "score": number,
  "disqualify": boolean,
  "disqualify_reasons": string[],
  "work_mode": "Remote"|"Hybrid"|"On-site"|"Unclear",
  "location_ok": boolean,
  "location_interpreted": string,
  "location_confidence": "high"|"medium"|"low",
  "salary_text": string,
  "notes": string
}

Job:
Title: ${job.title || ''}
Company: ${job.company || ''}

Location (raw): ${job.locationRaw || ''}
Location (normalized): ${job.locationNorm || ''}

Source salary hint (may be empty): ${job.salaryHint || ''}

Description (may be truncated):
${job.description || ''}

Output ONLY the JSON object.`;

  const payload = {
    model,
    input: [
      { role: 'system', content: [{ type: 'input_text', text: sys }] },
      { role: 'user', content: [{ type: 'input_text', text: user }] }
    ],
    temperature: 0,
    text: { format: { type: "json_object" } }
  };

  const res = await fetch('https://api.openai.com/v1/responses', {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      'Authorization': `Bearer ${apiKey}`
    },
    body: JSON.stringify(payload)
  });

  const text = await res.text();
  if (res.status >= 300) throw new Error(`OpenAI error ${res.status}: ${text}`);

  const data = JSON.parse(text);

  const outText =
    (typeof data.output_text === 'string' && data.output_text.trim())
      ? data.output_text.trim()
      : (
          Array.isArray(data.output)
            ? data.output
                .flatMap(o => Array.isArray(o.content) ? o.content : [])
                .map(c => (c && typeof c.text === 'string') ? c.text : '')
                .filter(Boolean)
                .join('')
                .trim()
            : ''
        );

  if (!outText) throw new Error(`Empty LLM output. keys=${Object.keys(data || {}).join(',')}`);

  const cleaned = stripCodeFences(outText);
  return JSON.parse(cleaned);
}

Actor.main(async () => {
  const kv = await Actor.openKeyValueStore('job-pipeline');
  const input = await Actor.getInput() || {};
  const threshold = Number(input.threshold ?? DEFAULT_THRESHOLD);
  const prefilterMode = (input.prefilterMode || 'none').toString();
  const model = (input.openaiModel || DEFAULT_MODEL).toString();

  // ✅ Correct: custom env vars come from process.env
  const OPENAI_API_KEY = (process.env.OPENAI_API_KEY || '').trim();

  const merged = await kv.getValue('merged.json') || [];
  const total = Array.isArray(merged) ? merged.length : 0;

  const report = {
    startedAt: nowIso(),
    total_records: total,
    prefilterMode,
    threshold,
    model,
    scored: 0,
    accepted: 0,
    errors: 0,
    note: ''
  };

  let candidates = Array.isArray(merged) ? merged : [];
  if (prefilterMode === 'light') {
    candidates = candidates.filter(r => ((r.title || '').toLowerCase().includes('unity')));
  }

  if (!OPENAI_API_KEY) {
    report.note = 'Missing OPENAI_API_KEY in Actor environment variables; scoring skipped and accepted list is empty.';
    await kv.setValue('accepted.csv', 'Company,Job Title,Salary,Where,Score,Age (days),Where Found\n', { contentType: 'text/csv' });
    await kv.setValue('accepted_debug.json', []);
    await kv.setValue('scored.csv', 'Company,Job Title,Salary,Where,Score,Disqualified,Location OK,Location Interpreted,Location Confidence,Disqualify Reasons,Notes,Age (days),Where Found\n', { contentType: 'text/csv' });
    report.finishedAt = nowIso();
    await kv.setValue('run_report.json', report);
    console.log(report.note);
    return;
  }

  const accepted = [];
  const acceptedDebug = [];
  const scored = [];

  for (const r of candidates) {
    const desc = (getDescription(r) || '').toString();
    const descTrunc = desc.length > 4000 ? desc.slice(0, 4000) + '…' : desc;

    const locRaw = (r.location || '').toString();
    const locNorm = normalizeLocation(locRaw);

    const job = {
      title: r.title || '',
      company: r.company || '',
      locationRaw: locRaw,
      locationNorm: locNorm,
      salaryHint: getSalaryHint(r),
      description: descTrunc
    };

    let evalObj;
    try {
      evalObj = await llmEvaluate({ apiKey: OPENAI_API_KEY, model, job });
    } catch (e) {
      report.errors += 1;
      const msg = String(e?.message || e);
      if (!report.note) report.note = `First LLM error: ${msg}`.slice(0, 900);
      console.log(`LLM error: ${msg}`);

      evalObj = {
        score: 0,
        disqualify: true,
        disqualify_reasons: ['LLM_ERROR'],
        work_mode: 'Unclear',
        location_ok: false,
        location_interpreted: '',
        location_confidence: 'low',
        salary_text: '',
        notes: msg
      };
    }

    evalObj = applyRemoteOverride(evalObj, locRaw, locNorm);

    report.scored += 1;

    const score = Number(evalObj.score || 0);
    const disq = evalObj.disqualify === true;

    const where = locNorm || locRaw || '';
    const ageDays = toDaysAgo(r.published);
    const salary = (evalObj.salary_text || getSalaryHint(r) || '').toString();

    const companyLink = `=HYPERLINK("${(r.whereFound || r.url || '').replace(/"/g,'""')}","${escapeFormulaText(r.company || '')}")`;
    const titleLink = `=HYPERLINK("${(r.url || '').replace(/"/g,'""')}","${escapeFormulaText(r.title || '')}")`;

    const reasons = Array.isArray(evalObj.disqualify_reasons) ? evalObj.disqualify_reasons.join('; ') : '';
    const notes = (evalObj.notes || '').toString().slice(0, 300);
    const locInterp = (evalObj.location_interpreted || '').toString().slice(0, 160);
    const locConf = (evalObj.location_confidence || '').toString().slice(0, 16);

    scored.push([
      companyLink,
      titleLink,
      salary,
      where,
      isFinite(score) ? score.toFixed(2) : '',
      disq ? 'YES' : 'NO',
      evalObj.location_ok === true ? 'YES' : 'NO',
      locInterp,
      locConf,
      reasons,
      notes,
      ageDays,
      (r.whereFound || '')
    ]);

    // Debug phase: don't gate on location_ok
    if (!disq && isFinite(score) && score >= threshold) {
      accepted.push([
        companyLink,
        titleLink,
        salary,
        where,
        score.toFixed(2),
        ageDays,
        (r.whereFound || '')
      ]);
      acceptedDebug.push({ record: r, eval: evalObj });
    }

    if (report.scored % 20 === 0) await sleep(30);
  }

  report.accepted = accepted.length;
  report.finishedAt = nowIso();

  const header = ['Company','Job Title','Salary','Where','Score','Age (days)','Where Found'];
  const lines = [header.map(csvEscape).join(',')];
  for (const row of accepted) lines.push(row.map(csvEscape).join(','));
  await kv.setValue('accepted.csv', lines.join('\n') + '\n', { contentType: 'text/csv' });
  await kv.setValue('accepted_debug.json', acceptedDebug);

  const scoredHeader = [
    'Company','Job Title','Salary','Where','Score',
    'Disqualified','Location OK','Location Interpreted','Location Confidence',
    'Disqualify Reasons','Notes','Age (days)','Where Found'
  ];
  const scoredLines = [scoredHeader.map(csvEscape).join(',')];
  for (const row of scored) scoredLines.push(row.map(csvEscape).join(','));
  await kv.setValue('scored.csv', scoredLines.join('\n') + '\n', { contentType: 'text/csv' });

  await kv.setValue('run_report.json', report);

  console.log(`Scoring complete. Accepted ${accepted.length}/${report.scored} scored (threshold=${threshold}).`);
});