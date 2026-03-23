// actors/04_notify_email/src/main.js
// Sends an email summary (and attaches XLSX spreadsheets) using apify/send-mail.

import { Actor, log } from 'apify';

// KEEP IN SYNC with: actors/00_run_pipeline/src/main.js, actors/01_collect_jobs/src/main.js, actors/02_merge_dedup/src/main.js, actors/03_score_jobs/src/main.js, actors/99_diagnostics_dump/src/main.js
function nowIso() {
  return new Date().toISOString();
}

// KEEP IN SYNC with: actors/01_collect_jobs/src/main.js, actors/02_merge_dedup/src/main.js, actors/03_score_jobs/src/main.js
async function fetchText(url, headers = {}) {
  const u = url.includes('?') ? `${url}&cb=${Date.now()}` : `${url}?cb=${Date.now()}`;
  const res = await fetch(u, { method: 'GET', headers });
  const text = await res.text();
  if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}: ${text.slice(0, 500)}`);
  return text;
}

// KEEP IN SYNC with: actors/00_run_pipeline/src/main.js, actors/01_collect_jobs/src/main.js, actors/02_merge_dedup/src/main.js, actors/03_score_jobs/src/main.js
async function fetchJson(url, headers = {}) {
  const text = await fetchText(url, { ...headers, 'Accept': 'application/json' });
  try {
    return JSON.parse(text);
  } catch (e) {
    throw new Error(`Config at ${url} is not valid JSON: ${e?.message || e}`);
  }
}

// KEEP IN SYNC with: actors/00_run_pipeline/src/main.js, actors/01_collect_jobs/src/main.js, actors/02_merge_dedup/src/main.js, actors/03_score_jobs/src/main.js
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

function b64(s) {
  return Buffer.from(String(s ?? ''), 'utf-8').toString('base64');
}

Actor.main(async () => {
  const input = (await Actor.getInput()) || {};
  const config = await loadConfig(input);

  const notifyCfg = config?.notify || {};
  if (notifyCfg.enabled === false) {
    log.warning('Notify disabled by config.notify.enabled=false');
    return;
  }

  const kvStoreName = input.kvStoreName || config.kvStoreName || 'job-pipeline-v3';
  const kv = await Actor.openKeyValueStore(kvStoreName);

  const acceptedXlsx = await kv.getValue('accepted.xlsx');
  const scoredXlsx = await kv.getValue('scored.xlsx');
  const collectedXlsx = await kv.getValue('collected.xlsx');
  const scoringReport = await kv.getValue('scoring_report.json');
  const pipelineReport = await kv.getValue('pipeline_report.json');

  const toEmail = String(notifyCfg.toEmail || '').trim();
  if (!toEmail) throw new Error('Missing config.notify.toEmail');

  const subjectPrefix = String(notifyCfg.subjectPrefix || '[Job Search]').trim();
  const acceptedCount = Number(scoringReport?.accepted ?? 0) || 0;
  const totalScored = Number(scoringReport?.totalScored ?? 0) || 0;

  // Read run number from KV store (set by orchestrator)
  let runNumber = null;
  try {
    const runMeta = await kv.getValue('run_meta.json');
    if (runMeta?.runNumber) runNumber = runMeta.runNumber;
  } catch { /* run number is optional */ }

  const sendEvenIfEmpty = notifyCfg.sendEvenIfEmpty !== false;

  if (!acceptedXlsx && !sendEvenIfEmpty) {
    log.info('No accepted.xlsx found and sendEvenIfEmpty=false; skipping email.');
    await kv.setValue('notify_report.json', { status: 'skipped_no_results', at: nowIso() });
    return;
  }

  const runLabel = runNumber != null ? ` Run ${runNumber},` : '';
  const subject = `${subjectPrefix}${runLabel} Accepted: ${acceptedCount} (Scored: ${totalScored})`;

  const diagnosticsUrl = config?.diagnostics?.publicDiagnosticsUrl || '';
  const unscoredCount = Number(scoringReport?.unscoredCount || 0);
  const unscoredWarning = unscoredCount > 0
    ? `<div style="background:#fff3cd;border:2px solid #ffc107;border-radius:8px;padding:12px 16px;margin-bottom:16px;font-size:16px;"><b>⚠️ ${unscoredCount} job${unscoredCount === 1 ? '' : 's'} remain${unscoredCount === 1 ? 's' : ''} unscored, due to rate limits</b></div>`
    : '';

  // Cap warnings from collector — sources that may have more results
  let capWarningsHtml = '';
  try {
    const collectReport = await kv.getValue('collect_report.json');
    const capWarnings = Array.isArray(collectReport?.capWarnings) ? collectReport.capWarnings : [];
    if (capWarnings.length > 0) {
      const lines = capWarnings.map(w => `• ${w.sourceId}: ${w.detail}`).join('<br/>');
      capWarningsHtml = `<div style="background:#fff3cd;border:2px solid #ffc107;border-radius:8px;padding:12px 16px;margin-bottom:16px;font-size:14px;"><b>⚠️ Some sources may have more results than we retrieved:</b><br/>${lines}</div>`;
    }
  } catch { /* collect_report may not exist */ }

  // Pipeline duration from orchestrator report
  let durationLine = '';
  if (pipelineReport?.startedAt && pipelineReport?.finishedAt) {
    const durMs = new Date(pipelineReport.finishedAt) - new Date(pipelineReport.startedAt);
    const durMin = (durMs / 60000).toFixed(1);
    const status = pipelineReport.status || 'unknown';
    durationLine = `<b>Duration:</b> ${durMin} min | <b>Status:</b> ${status}`;
  }

  // Salary notes from scorer (garbage removed, non-USD detected, etc.)
  let salaryNotesHtml = '';
  const salaryNotes = Array.isArray(scoringReport?.salaryNotes) ? scoringReport.salaryNotes : [];
  if (salaryNotes.length > 0) {
    const lines = salaryNotes.map(n => `• ${n}`).join('<br/>');
    salaryNotesHtml = `<div style="background:#e8f4fd;border:2px solid #2196F3;border-radius:8px;padding:12px 16px;margin-bottom:16px;font-size:14px;"><b>ℹ️ Salary notes:</b><br/>${lines}</div>`;
  }

  const html = `
    <div style="font-family: Arial, sans-serif;">
      ${unscoredWarning}
      ${capWarningsHtml}
      ${salaryNotesHtml}
      <h2>Job Search Results</h2>
      <p><b>Accepted:</b> ${acceptedCount}<br/>
         <b>Scored:</b> ${totalScored}
         ${durationLine ? `<br/>${durationLine}` : ''}</p>
      ${diagnosticsUrl ? `<p>Diagnostics: <a href="${diagnosticsUrl}">${diagnosticsUrl}</a></p>` : ''}
      <p>See attached spreadsheets: <code>accepted.xlsx</code> (top picks), <code>scored.xlsx</code> (all scored jobs), <code>collected.xlsx</code> (raw collected).</p>
      <hr/>
      <p style="color:#666;font-size:12px;">Sent at ${nowIso()}</p>
    </div>
  `.trim();

  // apify/send-mail expects base64 data for attachments
  // XLSX files from KV store are already Buffer objects
  function xlsxToBase64(buf) {
    if (!buf) return '';
    if (Buffer.isBuffer(buf)) return buf.toString('base64');
    if (buf instanceof ArrayBuffer) return Buffer.from(buf).toString('base64');
    if (typeof buf === 'string') return Buffer.from(buf, 'utf-8').toString('base64');
    // Could be Uint8Array or similar
    return Buffer.from(buf).toString('base64');
  }

  const attachments = [];
  if (acceptedXlsx) {
    attachments.push({ filename: 'accepted.xlsx', data: xlsxToBase64(acceptedXlsx) });
  }
  if (scoredXlsx) {
    attachments.push({ filename: 'scored.xlsx', data: xlsxToBase64(scoredXlsx) });
  }
  if (collectedXlsx) {
    attachments.push({ filename: 'collected.xlsx', data: xlsxToBase64(collectedXlsx) });
  }

  const mailInput = {
    to: toEmail,
    subject,
    html,
    attachments,
  };

  log.info(`Sending email to ${toEmail} via apify/send-mail...`);
  const run = await Actor.call('apify/send-mail', mailInput);

  const status = run?.status || 'UNKNOWN';
  if (status !== 'SUCCEEDED') {
    throw new Error(`apify/send-mail failed (status=${status}, runId=${run?.id || 'unknown'})`);
  }

  const report = {
    status: 'sent',
    at: nowIso(),
    to: toEmail,
    subject,
    acceptedCount,
    totalScored,
    sendMailRunId: run?.id || null,
  };

  await kv.setValue('notify_report.json', report);
  log.info('Email sent.');
});
