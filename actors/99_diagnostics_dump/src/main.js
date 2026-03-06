// actors/99_diagnostics_dump/src/main.js
// v3 — Diagnostics to GitHub Gist (HTTPS)
//
// Writes job_search_log.html into a secret/unlisted GitHub Gist so it is readable via HTTPS.
//
// Required env vars on this Apify actor:
// - JOBSEARCH_CONFIG_URL  (or configUrl in task input)
// - GIST_ID
// - GITHUB_TOKEN
// - (optional) GIST_FILENAME  (defaults to "job_search_log.html")
//
// Optional task input:
// - smokeTest: true (uploads a tiny page to prove gist update works)

import { Actor, log } from 'apify';
import fetch from 'node-fetch';

function nowIso() { return new Date().toISOString(); }

function escHtml(s) {
  return String(s ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
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
  catch (e) { throw new Error(`Config at ${url} is not valid JSON: ${e?.message || e}`); }
}

async function loadConfig(input) {
  if (input?.config && typeof input.config === 'object') return input.config;
  const configUrl = input?.configUrl || process.env.JOBSEARCH_CONFIG_URL || process.env.CONFIG_URL;
  if (!configUrl) throw new Error('Missing configUrl (set in task input, or JOBSEARCH_CONFIG_URL env var).');
  return await fetchJson(configUrl);
}

async function updateGist({ gistId, token, filename, content, description }) {
  const url = `https://api.github.com/gists/${encodeURIComponent(gistId)}`;
  const body = {
    description: description || 'Job search diagnostics (auto-updated)',
    files: { [filename]: { content } }
  };

  const res = await fetch(url, {
    method: 'PATCH',
    headers: {
      Accept: 'application/vnd.github+json',
      Authorization: `Bearer ${token}`,
      'X-GitHub-Api-Version': '2022-11-28',
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(body)
  });

  const text = await res.text();
  if (!res.ok) throw new Error(`GitHub gist update failed (HTTP ${res.status}): ${text.slice(0, 800)}`);
  return text;
}

Actor.main(async () => {
  const input = (await Actor.getInput()) || {};
  const config = await loadConfig(input);

  const kvStoreName = (input.kvStoreName || config.kvStoreName || 'job-pipeline-v3').toString();
  const kv = await Actor.openKeyValueStore(kvStoreName);

  const gistId = (process.env.GIST_ID || input.gistId || '').toString().trim();
  const ghToken = (process.env.GITHUB_TOKEN || input.githubToken || '').toString().trim();
  const filename = (process.env.GIST_FILENAME || input.gistFilename || 'job_search_log.html').toString().trim();

  if (!gistId) throw new Error('Missing GIST_ID env var on 99_diagnostics_dump actor.');
  if (!ghToken) throw new Error('Missing GITHUB_TOKEN env var on 99_diagnostics_dump actor.');

  const smokeTest = !!input.smokeTest;

  if (smokeTest) {
    const ts = nowIso();
    const html = `<!doctype html>
<html><head><meta charset="utf-8"><title>Job Search Log (Smoke Test)</title></head>
<body>
<h1>Job Search Log (Smoke Test)</h1>
<p>Updated: ${escHtml(ts)}</p>
<p>If you can see this, gist updates are working.</p>
</body></html>`;

    await updateGist({
      gistId,
      token: ghToken,
      filename,
      content: html,
      description: 'Job search log (smoke test)'
    });

    const rawUrl = `https://gist.githubusercontent.com/HauntGuy/${gistId}/raw/${filename}`;
    await kv.setValue('diagnostics_report.json', { status: 'smoke_test_uploaded', at: ts, gistId, filename, rawUrl });
    log.info(`Smoke test uploaded. Raw URL: ${rawUrl}`);
    return;
  }

  // Pull key artifacts (best-effort; missing keys are OK)
  const keys = [
    'run_meta.json',
    'pipeline_report.json',
    'collect_report.json',
    'merge_report.json',
    'scoring_report.json',
    'notify_report.json',
    'raw_dataset.json',
    'merged_dataset.json',
    'scored_dataset.json',
    'accepted_dataset.json'
  ];

  const data = {};
  for (const k of keys) {
    try { data[k] = await kv.getValue(k); }
    catch (e) { data[k] = { _error: String(e?.message || e) }; }
  }

  let acceptedCsvPreview = '';
  try {
    const csv = await kv.getValue('accepted.csv');
    if (csv) acceptedCsvPreview = String(csv).split('\n').slice(0, 40).join('\n');
  } catch (e) {
    acceptedCsvPreview = `Error reading accepted.csv: ${String(e?.message || e)}`;
  }

  const ts = nowIso();
  const runId = input.runId || data['run_meta.json']?.runId || '(unknown runId)';

  const envCheck = {
    GIST_ID: gistId ? 'set' : 'missing',
    GITHUB_TOKEN: ghToken ? 'set' : 'missing',
    GIST_FILENAME: filename || '(default)',
    JOBSEARCH_CONFIG_URL: process.env.JOBSEARCH_CONFIG_URL ? 'set' : 'missing',
    CONFIG_URL: process.env.CONFIG_URL ? 'set' : 'missing'
  };

  const jsonPre = (obj) => `\n<pre>${escHtml(JSON.stringify(obj ?? null, null, 2))}</pre>\n`;

  const html = `<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Job Search Log</title>
  <style>
    body { font-family: Arial, sans-serif; padding: 16px; }
    .ts { font-size: 14px; color: #444; margin-bottom: 12px; }
    pre { background: #f5f5f5; padding: 12px; border-radius: 8px; overflow-x: auto; }
  </style>
</head>
<body>
  <div class="ts">Updated: ${escHtml(ts)} | Run ID: ${escHtml(runId)}</div>

  <h2>Diagnostics actor environment (99_diagnostics_dump only)</h2>
  <p><i>Note: OPENAI_API_KEY is checked in 03_score_jobs, not here.</i></p>
  ${jsonPre(envCheck)}

  <h2>Reports</h2>
  <h3>pipeline_report.json</h3>${jsonPre(data['pipeline_report.json'])}
  <h3>collect_report.json</h3>${jsonPre(data['collect_report.json'])}
  <h3>merge_report.json</h3>${jsonPre(data['merge_report.json'])}
  <h3>scoring_report.json</h3>${jsonPre(data['scoring_report.json'])}
  <h3>notify_report.json</h3>${jsonPre(data['notify_report.json'])}

  <h2>Datasets</h2>
  <h3>raw_dataset.json</h3>${jsonPre(data['raw_dataset.json'])}
  <h3>merged_dataset.json</h3>${jsonPre(data['merged_dataset.json'])}
  <h3>scored_dataset.json</h3>${jsonPre(data['scored_dataset.json'])}
  <h3>accepted_dataset.json</h3>${jsonPre(data['accepted_dataset.json'])}

  <h2>accepted.csv (preview)</h2>
  <pre>${escHtml(acceptedCsvPreview || '(none)')}</pre>
</body>
</html>`;

  log.info(`Updating gist ${gistId}/${filename} ...`);
  await updateGist({ gistId, token: ghToken, filename, content: html });

  const rawUrl = `https://gist.githubusercontent.com/HauntGuy/${gistId}/raw/${filename}`;
  await kv.setValue('diagnostics_report.json', { status: 'uploaded', at: ts, gistId, filename, rawUrl });

  log.info(`Job search log updated. Raw URL: ${rawUrl}`);
});