// 02_merge_dedup/src/main.js â€” v0.1.0
// Reads multiple collector outputs from KV store and writes merged.json (deduped).

import { Actor } from 'apify';

function normStr(s) {
  return (s || '').toString().trim().toLowerCase().replace(/\s+/g, ' ');
}

function dedupKey(rec) {
  const c = normStr(rec.company);
  const t = normStr(rec.title);
  const l = normStr(rec.location);
  return `${c}||${t}||${l}`;
}

Actor.main(async () => {
  const input = await Actor.getInput() || {};
  const kvStoreName = (input.kvStoreName || 'job-pipeline').toString();
  const inputs = Array.isArray(input.inputs) ? input.inputs : ['collector_fantastic_feed.json', 'collector_linkedin_jobs.json'];
  const outputKey = (input.outputKey || 'merged.json').toString();
  const reportKey = (input.reportKey || 'merge_report.json').toString();

  const kv = await Actor.openKeyValueStore(kvStoreName);

  const loaded = [];
  const perKeyCounts = {};

  for (const k of inputs) {
    const arr = await kv.getValue(k);
    const n = Array.isArray(arr) ? arr.length : 0;
    perKeyCounts[k] = n;
    if (Array.isArray(arr)) loaded.push(...arr);
  }

  const urlSeen = new Set();
  const keySeen = new Set();
  const merged = [];

  let droppedUrlDup = 0;
  let droppedKeyDup = 0;

  for (const r of loaded) {
    const url = (r.url || '').toString().trim();
    if (url) {
      if (urlSeen.has(url)) { droppedUrlDup += 1; continue; }
      urlSeen.add(url);
      merged.push(r);
      continue;
    }
    const k = dedupKey(r);
    if (keySeen.has(k)) { droppedKeyDup += 1; continue; }
    keySeen.add(k);
    merged.push(r);
  }

  const report = {
    startedAt: new Date().toISOString(),
    kvStoreName,
    inputs,
    perKeyCounts,
    loadedTotal: loaded.length,
    outputTotal: merged.length,
    droppedUrlDup,
    droppedKeyDup,
    finishedAt: new Date().toISOString()
  };

  await kv.setValue(outputKey, merged);
  await kv.setValue(reportKey, report);

  console.log(`02 complete. Loaded ${loaded.length}; wrote ${merged.length} -> ${kvStoreName}/${outputKey}`);
});
