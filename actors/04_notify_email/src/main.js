// 04_notify_email/src/main.js â€” v0.1.0
// Emails accepted.csv via apify/send-mail

import { Actor } from 'apify';

const SUBJECT_PREFIX_DEFAULT = '[Kyle Job Search Bot] ';

function toBase64Utf8(value) {
  if (value == null) return '';
  if (Buffer.isBuffer(value)) return value.toString('base64');
  return Buffer.from(String(value), 'utf8').toString('base64');
}

Actor.main(async () => {
  const input = (await Actor.getInput()) || {};
  const kvStoreName = (input.kvStoreName || 'job-pipeline').toString();
  const kv = await Actor.openKeyValueStore(kvStoreName);

  const TO_EMAIL = process.env.TO_EMAIL || input.toEmail || 'randy@forgaard.com';
  const SUBJECT_PREFIX = process.env.SUBJECT_PREFIX || input.subjectPrefix || SUBJECT_PREFIX_DEFAULT;

  const runReport = (await kv.getValue('run_report.json')) || {};
  const acceptedCsv = (await kv.getValue('accepted.csv')) || '';

  const acceptedCount = runReport.accepted ?? 0;

  const subject = `${SUBJECT_PREFIX}Nightly Job Pipeline: ${acceptedCount} accepted`;

  const body = [
    'Nightly Job Pipeline Status',
    '--------------------------------',
    `LLM: accepted ${acceptedCount} (prefilter=${runReport.prefilterMode ?? 'none'}, threshold=${runReport.threshold ?? 'n/a'}, model=${runReport.model ?? 'n/a'})`,
    '',
    'Attached: accepted.csv'
  ].join('\n');

  await Actor.call('apify/send-mail', {
    to: TO_EMAIL,
    subject,
    text: body,
    attachments: [{ filename: 'accepted.csv', data: toBase64Utf8(acceptedCsv) }]
  });

  console.log(`Email sent to ${TO_EMAIL} with accepted.csv attached.`);
});
