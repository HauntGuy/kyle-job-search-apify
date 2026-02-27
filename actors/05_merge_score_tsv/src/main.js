// 05_merge_score_tsv/main.js — v2.3
// Reads merged.json from KV store "job-pipeline", runs LLM evaluation for Kyle fit,
// and writes accepted.csv (Google Sheets friendly).
//
// Output keys in KV store:
//  - accepted.csv
//  - accepted_debug.json (accepted rows + LLM eval)
//  - run_report.json

import { Actor } from 'apify';
import fetch from 'node-fetch';
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

const DEFAULT_THRESHOLD = 0.60;
const DEFAULT_MODEL = 'gpt-4o-mini'; // override with input.openaiModel

function nowIso() { return new Date().toISOString(); }

function escapeFormulaText(s) {
  // Protect against formulas in the visible label.
  const t = (s || '').toString().replace(/"/g,'""');
  // If it starts with =, +, -, @, prefix with apostrophe.
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
  // Ashby legacy
  if (raw.descriptionPlain) return raw.descriptionPlain;
  if (raw.descriptionText) return raw.descriptionText;
  // Fantastic.jobs output schema
  if (raw.description_text) return raw.description_text;
  if (raw.description_html) return raw.description_html;
  // Fallback
  return '';
}

function getSalaryHint(rec) {
  if (rec.salary) return rec.salary;
  const raw = rec.raw || {};
  // Fantastic.jobs AI fields
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

async function llmEvaluate({ apiKey, model, job }) {
  const sys = `You are a strict job-fit evaluator for a specific candidate. You must follow the rubric exactly and output ONLY JSON.`;
  const user = `
Candidate: Kyle Forgaard
Profile:
- 7 years professional Unity/C# (mobile/casual), some C++/Lua, some React experience (not React expert).
- Strong in client-side gameplay/UI/tools in Unity. Unity 2D preferred; Unity 3D OK if not lead/architect.
Hard disqualifiers (set disqualify=true):
- Not primarily a software engineering / programming role (e.g., mechanical engineer, data scientist, marketing, sales, HR, finance, PM).
- VR/XR/AR focus REQUIRED (optional/minor mention is OK).
- Backend-heavy or full-stack-primary (Unity as a small part is NOT OK).
- On-site REQUIRED outside Massachusetts (MA). Hybrid is OK only if the office is in Massachusetts. Remote is OK.
- Salary explicitly max < 90000 USD/year AND no explicit equity/rev-share/commission upside.
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
  "salary_text": string,
  "notes": string
}

Job:
Title: ${job.title || ''}
Company: ${job.company || ''}
Location (as listed): ${job.location || ''}
Source salary hint (may be empty): ${job.salaryHint || ''}

Description (may be truncated):
${job.description || ''}

Output ONLY the JSON object.`;

  const payload = {
    model,
    input: [
      { role: 'system', content: [{ type: 'text', text: sys }] },
      { role: 'user', content: [{ type: 'text', text: user }] }
    ],
    temperature: 0
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
  const outText = data.output_text || '';
  const cleaned = stripCodeFences(outText);
  const obj = JSON.parse(cleaned);
  return obj;
}

Actor.main(async () => {
  const kv = await Actor.openKeyValueStore('job-pipeline');
  const input = await Actor.getInput() || {};
  const threshold = Number(input.threshold ?? DEFAULT_THRESHOLD);
  const prefilterMode = (input.prefilterMode || 'none').toString();
  const model = (input.openaiModel || DEFAULT_MODEL).toString();

  const OPENAI_API_KEY = Actor.getEnv().OPENAI_API_KEY || input.OPENAI_API_KEY || '';

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

  // Prefilter (optional; keep broad)
  let candidates = Array.isArray(merged) ? merged : [];
  if (prefilterMode === 'light') {
    candidates = candidates.filter(r => {
      const t = (r.title || '').toLowerCase();
      const d = (getDescription(r) || '').toLowerCase();
      const hay = `${t}\n${d}`;
      // broad: allow Unity OR gameplay/game OR C# with "game" context
      const hasUnity = hay.includes('unity');
      const hasGameplay = hay.includes('gameplay') || hay.includes('game developer') || hay.includes('game programmer') || hay.includes('game engineer');
      const hasCSharp = hay.includes('c#') || hay.includes('csharp');
      const hasGame = hay.includes(' game ') || hay.includes(' games ') || hay.includes('gaming');
      return hasUnity || hasGameplay || (hasCSharp && hasGame);
    });
  }

  if (!OPENAI_API_KEY) {
    report.note = 'Missing OPENAI_API_KEY; scoring skipped and accepted list is empty.';
    await kv.setValue('accepted.csv', 'Company,Job Title,Salary,Where,Score,Age (days),Where Found\n');
    await kv.setValue('accepted_debug.json', []);
    report.finishedAt = nowIso();
    await kv.setValue('run_report.json', report);
    console.log(report.note);
    return;
  }

  const accepted = [];
  const acceptedDebug = [];

  for (const r of candidates) {
    const desc = (getDescription(r) || '').toString();
    const descTrunc = desc.length > 4000 ? desc.slice(0, 4000) + '…' : desc;

    const job = {
      title: r.title || '',
      company: r.company || '',
      location: r.location || '',
      salaryHint: getSalaryHint(r),
      description: descTrunc
    };

    let evalObj;
    try {
      evalObj = await llmEvaluate({ apiKey: OPENAI_API_KEY, model, job });
    } catch (e) {
      report.errors += 1;
      // Fail closed
      evalObj = { score: 0, disqualify: true, disqualify_reasons: ['LLM_ERROR'], work_mode: 'Unclear', location_ok: false, salary_text: '', notes: String(e?.message || e) };
    }

    report.scored += 1;

    const score = Number(evalObj.score || 0);
    const disq = evalObj.disqualify === true;

    if (!disq && evalObj.location_ok === true && isFinite(score) && score >= threshold) {
      const where = r.location || '';
      const ageDays = toDaysAgo(r.published);
      const salary = (evalObj.salary_text || getSalaryHint(r) || '').toString();

      const companyLink = `=HYPERLINK("${(r.whereFound || r.url || '').replace(/"/g,'""')}","${escapeFormulaText(r.company || '')}")`;
      const titleLink = `=HYPERLINK("${(r.url || '').replace(/"/g,'""')}","${escapeFormulaText(r.title || '')}")`;

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

    if (report.scored % 20 === 0) await sleep(30); // gentle pacing
  }

  report.accepted = accepted.length;
  report.finishedAt = nowIso();

  // Write CSV
  const header = ['Company','Job Title','Salary','Where','Score','Age (days)','Where Found'];
  const lines = [header.map(csvEscape).join(',')];
  for (const row of accepted) lines.push(row.map(csvEscape).join(','));
  await kv.setValue('accepted.csv', lines.join('\n') + '\n');
  await kv.setValue('accepted_debug.json', acceptedDebug);
  await kv.setValue('run_report.json', report);

  console.log(`Scoring complete. Accepted ${accepted.length}/${report.scored} scored (threshold=${threshold}).`);
});
