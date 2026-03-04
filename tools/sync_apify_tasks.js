// tools/sync_apify_tasks.js
// Syncs actors/*/task_input.json into Apify Saved Task inputs.
// Requires env var: APIFY_TOKEN (as GitHub Secret).
//
// Usage: node tools/sync_apify_tasks.js tools/apify_tasks_map.json

import fs from 'fs';
import path from 'path';
import fetch from 'node-fetch';

function readJson(p) {
  return JSON.parse(fs.readFileSync(p, 'utf8'));
}

function listActorTaskInputs() {
  const actorsDir = path.join(process.cwd(), 'actors');
  const result = {};
  for (const name of fs.readdirSync(actorsDir)) {
    const p = path.join(actorsDir, name, 'task_input.json');
    if (fs.existsSync(p)) {
      result[name] = readJson(p);
    }
  }
  return result;
}

async function updateTaskInput({ baseUrl, token, taskId, inputObj }) {
  // Apify API: Update task (input)
  // Endpoint: PUT /v2/actor-tasks/{taskId}
  const url = `${baseUrl.replace(/\/$/, '')}/v2/actor-tasks/${taskId}?token=${encodeURIComponent(token)}`;
  const body = { input: inputObj };

  const res = await fetch(url, {
    method: 'PUT',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });

  const text = await res.text();
  if (res.status >= 300) {
    throw new Error(`Update task ${taskId} failed ${res.status}: ${text}`);
  }
  return text;
}

async function main() {
  const token = process.env.APIFY_TOKEN;
  if (!token) throw new Error('Missing APIFY_TOKEN env var (set as GitHub Secret).');

  const mapPath = process.argv[2] || 'tools/apify_tasks_map.json';
  const map = readJson(mapPath);

  const baseUrl = map.APIFY_BASE_URL || 'https://api.apify.com';
  const inputs = listActorTaskInputs();

  const tasks = map.TASKS || {};
  const names = Object.keys(tasks);

  for (const name of names) {
    const taskId = tasks[name]?.apifyTaskId;
    if (!taskId || taskId === 'REPLACE_ME') {
      console.log(`Skipping ${name}: apifyTaskId not set`);
      continue;
    }
    const inputObj = inputs[name];
    if (!inputObj) {
      console.log(`Skipping ${name}: no actors/${name}/task_input.json`);
      continue;
    }
    console.log(`Updating Apify task for ${name} (taskId=${taskId})...`);
    await updateTaskInput({ baseUrl, token, taskId, inputObj });
    console.log(`OK: ${name}`);
  }
}

main().catch(err => {
  console.error(err);
  process.exit(1);
});
