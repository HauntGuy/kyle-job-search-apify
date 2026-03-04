// actors/99_diagnostics_dump/src/main.js
// Builds an HTML diagnostics page from KV store artifacts and uploads it to your website via diag_upload.php.

import { Actor, log } from 'apify';

function nowIso() {
  return new Date().toISOString();
}

async function fetchText(url, headers = {}) {
  const u = url.includes('?') ? `${url}&cb=${Date.now()}` : `${url}?cb=${Date.now()}`;
  const res = await fetch(u, { method: 'GET', headers });
  const text = await res.text();
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}: ${text.slice(0, 500)}`);
  return text;
}

async function fetchJson(url, headers = {}) {
  const text = await fetchText(url, { ...headers, 'Accept': 'application/json' });
  try {
    return JSON.parse(text);
  } catch (e) {
    throw new Error(`Config at ${url} is not valid JSON: ${e?.message || e}`);
  }
}

async function loadConfig(input) {
  if (input?.config && typeof input.config === 'object') return input.config;

  const configUrl =
    input?.configUrl ||
    process.env.JOBSEARCH_CONFIG_URL ||
    process.env.CONFIG_URL;

  if (!configUrl) {
    throw new Error('Missing configUrl (set in task input, or JOBSEARCH_CONFIG_URL env var).');
  }
  return await fetchJson(configUrl);
}

function escHtml(s) {
  return String(s ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

function jsonPre(obj) {
  return `<pre>${escHtml(JSON.stringify(obj ?? null, null, 2))}</pre>`;
}

async function uploadHtml({ uploadUrl, token, html }) {
  const body = new URLSearchParams({ token, content: html }).toString();
  const res = await fetch(uploadUrl, {
    method: 'POST',
    headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
    body,
  });
  const text = await res.text();
  if (!res.ok) {
    throw new Error(`Upload failed (HTTP ${res.status}): ${text.slice(0, 500)}`);
  }
  return text;
}

Actor.main(async () => {
  const input = (await Actor.getInput()) || {};
  const config = await loadConfig(input);

  const kvStoreName = input.kvStoreName || config.kvStoreName || 'job-pipeline-v3';
  const kv = await Actor.openKeyValueStore(kvStoreName);

  const diagCfg = config?.diagnostics || {};
  const uploadUrl =
    input.uploadUrl ||
    process.env[diagCfg.uploadUrlEnv || 'DIAG_UPLOAD_URL'] ||
    process.env.DIAG_UPLOAD_URL;

  const token =
    input.token ||
    process.env[diagCfg.tokenEnv || 'DIAG_UPLOAD_TOKEN'] ||
    process.env.DIAG_UPLOAD_TOKEN;

  if (!uploadUrl) throw new Error('Missing diagnostics upload URL (set DIAG_UPLOAD_URL env var or config.diagnostics.uploadUrlEnv).');
  if (!token) throw new Error('Missing diagnostics upload token (set DIAG_UPLOAD_TOKEN env var or config.diagnostics.tokenEnv).');

  // Smoke test mode: upload a minimal page without depending on KV contents.
  const smokeTest = !!input.smokeTest;
  if (smokeTest) {
    const html = `<!doctype html>
<html><head><meta charset="utf-8"><title>Diagnostics Smoke Test</title></head>
<body style="font-family: Arial, sans-serif;">
<h2>Diagnostics Smoke Test</h2>
<p>Uploaded at ${escHtml(nowIso())}</p>
<p>If you can see this page, your <code>diag_upload.php</code> endpoint is working.</p>
</body></html>`;
    const respText = await uploadHtml({ uploadUrl, token, html });
    await kv.setValue('diagnostics_report.json', { status: 'smoke_test_uploaded', at: nowIso(), uploadUrl, response: respText.slice(0, 500) });
    log.info('Smoke test diagnostics uploaded.');
    return;
  }

  // Gather artifacts (best-effort)
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
    'accepted_dataset.json',
  ];

  const data = {};
  for (const k of keys) {
    try {
      data[k] = await kv.getValue(k);
    } catch (e) {
      data[k] = { _error: String(e?.message || e) };
    }
  }

  let acceptedCsvPreview = '';
  try {
    const csv = await kv.getValue('accepted.csv');
    if (csv) acceptedCsvPreview = String(csv).split('\n').slice(0, 30).join('\n');
  } catch (e) {
    acceptedCsvPreview = `Error reading accepted.csv: ${String(e?.message || e)}`;
  }

  const runId = input.runId || data['run_meta.json']?.runId || '(unknown runId)';

  const envCheck = {
    OPENAI_API_KEY: process.env.OPENAI_API_KEY ? 'set' : 'missing',
    DIAG_UPLOAD_URL: uploadUrl ? 'set' : 'missing',
    DIAG_UPLOAD_TOKEN: token ? 'set' : 'missing',
    RAPIDAPI_KEY: process.env.RAPIDAPI_KEY ? 'set' : 'missing',
  };

  const html = `<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Job Pipeline Diagnostics</title>
  <style>
    body { font-family: Arial, sans-serif; margin: 24px; }
    code, pre { background: #f6f8fa; padding: 2px 4px; border-radius: 4px; }
    pre { padding: 12px; overflow-x: auto; }
    h2 { margin-top: 28px; }
    .muted { color: #666; font-size: 12px; }
  </style>
</head>
<body>
  <h1>Job Pipeline Diagnostics</h1>
  <p><b>Run ID:</b> ${escHtml(runId)}</p>
  <p class="muted">Generated at ${escHtml(nowIso())}</p>

  <h2>Environment</h2>
  ${jsonPre(envCheck)}

  <h2>Reports</h2>
  <h3>pipeline_report.json</h3>
  ${jsonPre(data['pipeline_report.json'])}
  <h3>collect_report.json</h3>
  ${jsonPre(data['collect_report.json'])}
  <h3>merge_report.json</h3>
  ${jsonPre(data['merge_report.json'])}
  <h3>scoring_report.json</h3>
  ${jsonPre(data['scoring_report.json'])}
  <h3>notify_report.json</h3>
  ${jsonPre(data['notify_report.json'])}

  <h2>Datasets</h2>
  <h3>raw_dataset.json</h3>
  ${jsonPre(data['raw_dataset.json'])}
  <h3>merged_dataset.json</h3>
  ${jsonPre(data['merged_dataset.json'])}
  <h3>scored_dataset.json</h3>
  ${jsonPre(data['scored_dataset.json'])}
  <h3>accepted_dataset.json</h3>
  ${jsonPre(data['accepted_dataset.json'])}

  <h2>accepted.csv (preview)</h2>
  <pre>${escHtml(acceptedCsvPreview || '(none)')}</pre>
</body>
</html>`;

  log.info(`Uploading diagnostics HTML to ${uploadUrl} ...`);
  const respText = await uploadHtml({ uploadUrl, token, html });

  const report = {
    status: 'uploaded',
    at: nowIso(),
    uploadUrl,
    responsePreview: respText.slice(0, 500),
  };
  await kv.setValue('diagnostics_report.json', report);

  log.info('Diagnostics uploaded.');
});
