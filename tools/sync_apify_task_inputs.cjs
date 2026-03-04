#!/usr/bin/env node
/**
 * tools/sync_apify_task_inputs.cjs
 *
 * Syncs each actors/<name>/task_input.json into the corresponding Apify Task input.
 *
 * Usage:
 *   node tools/sync_apify_task_inputs.cjs tools/apify_tasks_map.json
 *
 * Environment:
 *   APIFY_TOKEN  (GitHub secret recommended)
 */

const fs = require('fs');
const path = require('path');

function readJson(p) {
  return JSON.parse(fs.readFileSync(p, 'utf8'));
}

async function putJson(url, token, bodyObj) {
  const u = url.includes('?') ? `${url}&token=${encodeURIComponent(token)}` : `${url}?token=${encodeURIComponent(token)}`;
  const res = await fetch(u, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(bodyObj),
  });
  const text = await res.text();
  if (!res.ok) {
    throw new Error(`HTTP ${res.status}: ${text.slice(0, 1000)}`);
  }
  return text;
}

async function main() {
  const token = process.env.APIFY_TOKEN;
  if (!token) {
    console.error('Missing APIFY_TOKEN env var.');
    process.exit(1);
  }

  const mapPath = process.argv[2] || path.join('tools', 'apify_tasks_map.json');

  if (!fs.existsSync(mapPath)) {
    console.log(`No task map found at ${mapPath}. Skipping sync (this is OK until you create it).`);
    return;
  }

  const taskMap = readJson(mapPath);

  const entries = Object.entries(taskMap);
  if (entries.length === 0) {
    console.log('Task map is empty. Nothing to sync.');
    return;
  }

  let ok = 0;
  let fail = 0;

  for (const [actorName, info] of entries) {
    const taskId = info && (info.taskId || info.id);
    if (!taskId) {
      console.warn(`Skipping ${actorName}: missing taskId in map.`);
      continue;
    }

    const inputPath = path.join('actors', actorName, 'task_input.json');
    if (!fs.existsSync(inputPath)) {
      console.warn(`Skipping ${actorName}: missing ${inputPath}.`);
      continue;
    }

    const inputJson = readJson(inputPath);

    const url = `https://api.apify.com/v2/actor-tasks/${encodeURIComponent(taskId)}/input`;
    process.stdout.write(`Syncing ${actorName} -> task ${taskId} ... `);

    try {
      await putJson(url, token, inputJson);
      ok += 1;
      console.log('OK');
    } catch (e) {
      fail += 1;
      console.log('FAIL');
      console.error(`  ${e.message || e}`);
    }
  }

  console.log(`Sync complete: ok=${ok}, fail=${fail}`);
  if (fail > 0) process.exit(1);
}

main().catch((e) => {
  console.error(e.stack || e);
  process.exit(1);
});
