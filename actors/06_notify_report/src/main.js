// 06_notify_report/main.js — v2.2 (prefixed; defaults to randy@forgaard.com)
import { Actor } from 'apify';
import fetch from 'node-fetch';

const SUBJECT_PREFIX = '[Kyle Job Search Bot] ';

async function sendEmail(sendgridKey, fromEmail, toEmail, subject, text) {
  const payload = {
    personalizations: [{ to: [{ email: toEmail }] }],
    from: { email: fromEmail, name: 'Kyle Job Search Bot' },
    subject,
    content: [{ type: 'text/plain', value: text }],
    reply_to: { email: toEmail }
  };

  const res = await fetch('https://api.sendgrid.com/v3/mail/send', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${sendgridKey}` },
    body: JSON.stringify(payload)
  });

  if (res.status >= 300) {
    const body = await res.text();
    throw new Error(`SendGrid error ${res.status}: ${body}`);
  }
}

Actor.main(async () => {
  const kv = await Actor.openKeyValueStore('job-pipeline');
  const SENDGRID_API_KEY = Actor.getEnv().SENDGRID_API_KEY;
  const input = await Actor.getInput() || {};

  const TO_EMAIL = Actor.getEnv().TO_EMAIL || input.toEmail || 'randy@forgaard.com';
  const FROM_EMAIL = Actor.getEnv().FROM_EMAIL || input.fromEmail || 'randy@forgaard.com';

  if (!SENDGRID_API_KEY) {
    console.log('Missing SENDGRID_API_KEY; skipping email.');
    return;
  }

  const discover = await kv.getValue('discover_summary.json') || {};
  const registry = await kv.getValue('registry_summary.json') || {};
  const fetchSnap = await kv.getValue('fetch_snapshot.json') || {};
  const runReport = await kv.getValue('run_report.json') || {};

  const lines = [];
  lines.push('Nightly Job Pipeline Status');
  lines.push('--------------------------------');
  lines.push(`Discovery: ${discover.discovered ?? 0} discovered (queries=${discover.queries ?? 0}, lookback=${discover.lookbackHours ?? 48}h)`);
  lines.push(`Registry: total companies ${registry.total ?? 0}, added ${registry.added ?? 0}`);
  lines.push(`Fetch: normalized records ${fetchSnap.records_normalized ?? 0} (success ${fetchSnap.companies_success ?? 0} / fail ${fetchSnap.companies_failed ?? 0})`);
  lines.push(`LLM: accepted ${runReport.accepted ?? 0} (prefilter=${runReport.prefilterMode ?? 'none'}, threshold=${runReport.threshold ?? 0.60})`);
  lines.push('Artifacts in KV store "job-pipeline":');
  lines.push(' - accepted.csv');
  lines.push(' - run_report.json');
  lines.push(' - manifest.log');
  lines.push(' - fetch_snapshot.json');

  const subject = SUBJECT_PREFIX + `Nightly Job Pipeline: ${runReport.accepted ?? 0} accepted`;
  await sendEmail(SENDGRID_API_KEY, FROM_EMAIL, TO_EMAIL, subject, lines.join('\n'));
  console.log('Status email sent.');
});
