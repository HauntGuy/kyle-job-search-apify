// actors/04_notify_email/src/main.js
// Sends an email summary (and attaches accepted.csv) using apify/send-mail.

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

  const acceptedCsv = await kv.getValue('accepted.csv');
  const scoredCsv = await kv.getValue('scored.csv');
  const collectedCsv = await kv.getValue('collected.csv');
  const scoringReport = await kv.getValue('scoring_report.json');

  const toEmail = String(notifyCfg.toEmail || '').trim();
  if (!toEmail) throw new Error('Missing config.notify.toEmail');

  const subjectPrefix = String(notifyCfg.subjectPrefix || '[Job Search]').trim();
  const acceptedCount = Number(scoringReport?.accepted ?? 0) || 0;
  const totalScored = Number(scoringReport?.totalScored ?? 0) || 0;

  const sendEvenIfEmpty = notifyCfg.sendEvenIfEmpty !== false;

  if (!acceptedCsv && !sendEvenIfEmpty) {
    log.info('No accepted.csv found and sendEvenIfEmpty=false; skipping email.');
    await kv.setValue('notify_report.json', { status: 'skipped_no_results', at: nowIso() });
    return;
  }

  const subject = `${subjectPrefix} Accepted: ${acceptedCount} (Scored: ${totalScored})`;

  const diagnosticsUrl = config?.diagnostics?.publicDiagnosticsUrl || '';
  const html = `
    <div style="font-family: Arial, sans-serif;">
      <h2>Job Search Results</h2>
      <p><b>Accepted:</b> ${acceptedCount}<br/>
         <b>Scored:</b> ${totalScored}</p>
      ${diagnosticsUrl ? `<p>Diagnostics: <a href="${diagnosticsUrl}">${diagnosticsUrl}</a></p>` : ''}
      <p>See attached CSVs: <code>accepted.csv</code> (top picks), <code>scored.csv</code> (all scored jobs), <code>collected.csv</code> (raw collected).</p>
      <hr/>
      <p style="color:#666;font-size:12px;">Sent at ${nowIso()}</p>
    </div>
  `.trim();

  // apify/send-mail expects base64 data for attachments
  const emptyHeader = 'Company,Job Title,Salary,Where,Score,Age (days),Where Found,Sources,Reason,Tags,Red Flags\n';
  const emptyCollectedHeader = 'Source,Company,Job Title,Location,Salary,Posted At,URL\n';

  const attachments = [
    {
      filename: 'accepted.csv',
      data: b64(acceptedCsv || emptyHeader),
    },
    {
      filename: 'scored.csv',
      data: b64(scoredCsv || emptyHeader),
    },
    {
      filename: 'collected.csv',
      data: b64(collectedCsv || emptyCollectedHeader),
    },
  ];

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
