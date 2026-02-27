// 06_notify_report/main.js — v2.3.1
// Sends nightly email via Apify's official actor: apify/send-mail
// Attaches accepted.csv from KV store job-pipeline.

import { Actor } from 'apify';

const SUBJECT_PREFIX_DEFAULT = '[Kyle Job Search Bot] ';

function toBase64Utf8(str) {
  return Buffer.from(str ?? '', 'utf8').toString('base64');
}

Actor.main(async () => {
  const kv = await Actor.openKeyValueStore('job-pipeline');
  const input = (await Actor.getInput()) || {};

  const TO_EMAIL = Actor.getEnv().TO_EMAIL || input.toEmail || 'randy@forgaard.com';
  const SUBJECT_PREFIX = Actor.getEnv().SUBJECT_PREFIX || input.subjectPrefix || SUBJECT_PREFIX_DEFAULT;

  const discover = (await kv.getValue('discover_summary.json')) || {};
  const registry = (await kv.getValue('registry_summary.json')) || {};
  const fetchSnap = (await kv.getValue('fetch_snapshot.json')) || {};
  const runReport = (await kv.getValue('run_report.json')) || {};

  const acceptedCsv = (await kv.getValue('accepted.csv')) || '';
  const acceptedCount = runReport.accepted ?? 0;

  const lines = [];
  lines.push('Nightly Job Pipeline Status');
  lines.push('--------------------------------');

  // These two may be empty if you're using Fantastic mode and skipping 01/02.
  if (Object.keys(discover).length) {
    lines.push(`Discovery: ${discover.discovered ?? 0} discovered (queries=${discover.queries ?? 0}, lookback=${discover.lookbackHours ?? 48}h)`);
  }
  if (Object.keys(registry).length) {
    lines.push(`Registry: total companies ${registry.total ?? 0}, added ${registry.added ?? 0}`);
  }

  lines.push(`Fetch: normalized records ${fetchSnap.records_normalized ?? 0} (success ${fetchSnap.companies_success ?? 0} / fail ${fetchSnap.companies_failed ?? 0})`);
  lines.push(`LLM: accepted ${acceptedCount} (prefilter=${runReport.prefilterMode ?? 'none'}, threshold=${runReport.threshold ?? 0.60}, model=${runReport.model ?? 'n/a'})`);
  lines.push('');
  lines.push('Attached: accepted.csv');

  const subject = `${SUBJECT_PREFIX}Nightly Job Pipeline: ${acceptedCount} accepted`;

  const attachments = [{
    filename: 'accepted.csv',
    data: toBase64Utf8(acceptedCsv),
  }];

  // Requires "Full permissions" in the Task's Run options (so it can call another actor).
  await Actor.call('apify/send-mail', {
    to: TO_EMAIL,
    subject,
    text: lines.join('\n'),
    attachments,
  });

  console.log(`Email sent to ${TO_EMAIL} with accepted.csv attached.`);
});