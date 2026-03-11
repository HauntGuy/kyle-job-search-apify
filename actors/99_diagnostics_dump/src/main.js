// actors/99_diagnostics_dump/src/main.js
// v3 — Diagnostics to GitHub Gist (HTTPS)
//
// Writes job_search_log.html into a secret/unlisted GitHub Gist so it is readable via HTTPS.
//
// Required env vars on this Apify actor:
// - GIST_ID
// - GITHUB_TOKEN
// - (optional) GIST_FILENAME  (defaults to "job_search_log.html")
//
// Optional runtime sources for kvStoreName:
// - input.kvStoreName
// - JOBSEARCH_KV_STORE_NAME
// - fallback: "job-pipeline-v3"
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

function jsonPre(obj) {
  return `\n<pre>${escHtml(JSON.stringify(obj ?? null, null, 2))}</pre>\n`;
}

Actor.main(async () => {
  const input = (await Actor.getInput()) || {};

  const kvStoreName = (
    input.kvStoreName ||
    process.env.JOBSEARCH_KV_STORE_NAME ||
    'job-pipeline-v3'
  ).toString();

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
    await kv.setValue('diagnostics_report.json', {
      status: 'smoke_test_uploaded',
      at: ts,
      gistId,
      filename,
      rawUrl,
      kvStoreName
    });

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

  const collectReport = data['collect_report.json'] || {};
  const mergeReport = data['merge_report.json'] || {};
  const scoringReport = data['scoring_report.json'] || {};
  const notifyReport = data['notify_report.json'] || {};

  const collected = Number(collectReport?.totals?.pushed ?? collectReport?.totals?.collected ?? 0);
  const merged = Number(mergeReport?.outputTotal ?? mergeReport?.merged ?? 0);
  const scored = Number(scoringReport?.totalScored ?? 0);
  const accepted = Number(scoringReport?.accepted ?? 0);

  const sourceSummaryLines = [];
  const collectionWarnings = [];

  if (Array.isArray(collectReport.sources)) {
    for (const s of collectReport.sources) {
      if (!s) continue;
      const meta = s.meta || {};
      const id = s.id || '(unknown source)';
      const returnedCount = meta.returnedCount;
      const requestedLimit = meta.requestedLimit;

      if (requestedLimit != null && returnedCount != null) {
        sourceSummaryLines.push(`${id}: returned ${returnedCount} / limit ${requestedLimit}`);
      } else if (s.itemCount != null) {
        sourceSummaryLines.push(`${id}: returned ${s.itemCount}`);
      }

      // Source-level errors (entire source failed — 0 jobs collected from it)
      if (s.status === 'error') {
        collectionWarnings.push(
          `${id}: source failed entirely — ${s.error || 'unknown error'}. No jobs were collected from this source.`
        );
      }

      // Mantiks partial search (some pages succeeded, then a page failed after retries)
      if (meta.searchError) {
        collectionWarnings.push(
          `${id}: ${meta.searchError}. Results are partial (${meta.searchPagesFetched || '?'} pages succeeded).`
        );
      }

      // Mantiks detail failures (search succeeded but some detail calls failed after retries)
      const detailFails = Number(meta.detailFailures || 0);
      if (detailFails > 0) {
        collectionWarnings.push(
          `${id}: ${detailFails} job detail call${detailFails === 1 ? '' : 's'} failed after retries. Those jobs have no description (search-level data only).`
        );
      }
    }
  }

  const rateLimit429 = Number(scoringReport?.openai?.rateLimit429 || 0);
  const retryCount = Number(scoringReport?.openai?.retries || 0);

  const summaryItems = [
    `<li><b>Collected:</b> ${collected}</li>`,
    `<li><b>Merged:</b> ${merged}</li>`,
    `<li><b>Scored:</b> ${scored}</li>`,
    `<li><b>Accepted:</b> ${accepted}</li>`
  ];

  if (sourceSummaryLines.length) {
    summaryItems.push(
      `<li><b>Per-source results:</b><br/>${sourceSummaryLines.map(escHtml).join('<br/>')}</li>`
    );
  }

  if (rateLimit429 > 0) {
    summaryItems.push(
      `<li><b>OpenAI rate limiting occurred:</b> ${rateLimit429} time(s). ` +
      `The scorer retried with exponential backoff (retry attempts: ${retryCount}). ` +
      `If this becomes common, consider lowering <code>scoring.concurrency</code> or requesting higher rate limits from OpenAI.</li>`
    );
  }

  const unscoredCount = Number(scoringReport?.unscoredCount || 0);

  // Build warning banners for issues that need attention (shown at very top of gist)
  const warningBanners = [];
  if (unscoredCount > 0) {
    warningBanners.push(
      `⚠️ ${unscoredCount} job${unscoredCount === 1 ? '' : 's'} remain${unscoredCount === 1 ? 's' : ''} unscored, due to rate limits`
    );
  }
  for (const w of collectionWarnings) {
    warningBanners.push(`⚠️ ${w}`);
  }

  // Cap warnings — sources that may have more results than we retrieved
  const capWarnings = Array.isArray(collectReport.capWarnings) ? collectReport.capWarnings : [];
  for (const cw of capWarnings) {
    warningBanners.push(`⚠️ ${cw.sourceId}: ${cw.detail}`);
  }
  const bannersHtml = warningBanners.map(
    (msg) => `<div style="background:#fff3cd;border:2px solid #ffc107;border-radius:8px;padding:12px 16px;margin-bottom:8px;font-size:16px;"><b>${escHtml(msg)}</b></div>`
  ).join('\n');

  const summarySection = `${bannersHtml}<h2>Summary</h2><ul>${summaryItems.join('')}</ul>`;

  const envCheck = {
    GIST_ID: gistId ? 'set' : 'missing',
    GITHUB_TOKEN: ghToken ? 'set' : 'missing',
    GIST_FILENAME: filename || '(default)',
    JOBSEARCH_KV_STORE_NAME: process.env.JOBSEARCH_KV_STORE_NAME ? 'set' : 'missing'
  };

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

  ${summarySection}

  <h2>Diagnostics actor environment (99_diagnostics_dump only)</h2>
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
  await kv.setValue('diagnostics_report.json', {
    status: 'uploaded',
    at: ts,
    gistId,
    filename,
    rawUrl,
    kvStoreName
  });

  log.info(`Job search log updated. Raw URL: ${rawUrl}`);
});