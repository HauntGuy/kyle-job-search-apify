// 99_diagnostics_dump/src/main.js — v0.1.1
// Builds a single HTML diagnostics report and uploads it to your GoDaddy site via jobsearch/diag_upload.php.
//
// Requires env vars on this actor:
// - DIAG_UPLOAD_URL (optional; overrides task input uploadUrl)
// - DIAG_UPLOAD_TOKEN (REQUIRED; must match token in diag_upload.php)

import { Actor } from 'apify';
import fetch from 'node-fetch';

function escHtml(s) {
  return (s ?? '').toString()
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;');
}

Actor.main(async () => {
  const input = (await Actor.getInput()) || {};
  const kvStoreName = (input.kvStoreName || 'job-pipeline').toString();
  const kv = await Actor.openKeyValueStore(kvStoreName);

  const uploadUrl = (process.env.DIAG_UPLOAD_URL || input.uploadUrl || '').toString();
  const token = (process.env.DIAG_UPLOAD_TOKEN || '').toString();

  if (!uploadUrl) throw new Error('Missing uploadUrl (set DIAG_UPLOAD_URL env var or task input uploadUrl).');
  if (!token) throw new Error('Missing DIAG_UPLOAD_TOKEN env var on 99_diagnostics_dump actor.');

  const keys = [
    'snapshot_01a_fantastic.json',
    'snapshot_01b_linkedin.json',
    'merge_report.json',
    'run_report.json',
    'fetch_snapshot.json',
    'manifest_01a_fantastic.log',
    'manifest_01b_linkedin.log',
  ];

  const data = {};
  for (const k of keys) {
    try {
      data[k] = await kv.getValue(k);
    } catch (e) {
      data[k] = { _error: String(e?.message || e) };
    }
  }

  const ts = new Date().toISOString();
  const jsonPretty = JSON.stringify({ updatedAt: ts, kvStoreName, data }, null, 2);

  const html = `<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <title>Job Search Diagnostics</title>
  <style>
    body { font-family: Arial, sans-serif; padding: 16px; }
    .ts { font-size: 14px; color: #444; margin-bottom: 12px; }
    pre { background: #f5f5f5; padding: 12px; border-radius: 8px; overflow-x: auto; }
  </style>
</head>
<body>
  <div class="ts">Updated: ${escHtml(ts)}</div>
  <pre>${escHtml(jsonPretty)}</pre>
</body>
</html>`;

  const res = await fetch(uploadUrl, {
    method: 'POST',
    headers: {
      'Content-Type': 'text/html; charset=utf-8',
      'X-Diag-Token': token,
    },
    body: html,
  });

  const text = await res.text();
  if (res.status >= 300) {
    throw new Error(`Upload failed ${res.status}: ${text}`);
  }

  console.log(`Diagnostics uploaded OK: ${uploadUrl} -> (public) ${input.publicUrl || ''}`);
});