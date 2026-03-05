// actors/00_run_pipeline/src/main.js
// Orchestrates the pipeline by calling the other actors in order.

import { Actor, log } from 'apify';

function nowIso() {
  return new Date().toISOString();
}

function makeRunId() {
  // Example: 2026-03-04T05-00-00Z
  return new Date().toISOString()
    .replace(/[:.]/g, '-')
    .replace('T', 'T')
    .replace('Z', 'Z');
}

async function fetchJson(url) {
  const u = url.includes('?') ? `${url}&cb=${Date.now()}` : `${url}?cb=${Date.now()}`;
  const res = await fetch(u, { method: 'GET', headers: { 'Accept': 'application/json' } });
  const text = await res.text();
  if (!res.ok) {
    throw new Error(`Failed to load JSON from ${url} (HTTP ${res.status}): ${text.slice(0, 500)}`);
  }
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

function resolveActorId({ config, actorUser, step }) {
  // Allow full override from config
  if (config?.actors && typeof config.actors === 'object' && config.actors[step]) {
    return String(config.actors[step]);
  }

  if (!actorUser) {
    throw new Error(`Missing actorUser for resolving actor IDs (step=${step}). Set 00_run_pipeline task input "actorUser".`);
  }

  const defaults = {
  collect: '01-collect-jobs',
  merge: '02-merge-dedup',
  score: '03-score-jobs',
  notify: '04-notify-email',
  diagnostics: '99-diagnostics-dump',
};

  const suffix = defaults[step];
  if (!suffix) throw new Error(`Unknown step "${step}"`);
  return `${actorUser}/${suffix}`;
}

async function safeCallActor(actorId, input, label) {
  log.info(`Calling ${label}: ${actorId}`);
  const started = Date.now();
  const run = await Actor.call(actorId, input);
  const ms = Date.now() - started;

  // Actor.call returns the run object (status may be SUCCEEDED / FAILED / etc.)
  const status = run?.status || 'UNKNOWN';
  log.info(`${label} finished with status=${status} in ${(ms / 1000).toFixed(1)}s`);

  if (status !== 'SUCCEEDED') {
    throw new Error(`${label} failed (status=${status}, runId=${run?.id || 'unknown'})`);
  }
  return run;
}

Actor.main(async () => {
  const input = (await Actor.getInput()) || {};
  const runId = input.runId || makeRunId();

  const config = await loadConfig(input);

  const kvStoreName = input.kvStoreName || config.kvStoreName || 'job-pipeline-v3';
  const datasetPrefix = input.datasetPrefix || config.datasetPrefix || 'jobsearch-v3';
  const actorUser = input.actorUser || config.actorUser || null;

  const kv = await Actor.openKeyValueStore(kvStoreName);

  const pipelineMeta = {
    runId,
    startedAt: nowIso(),
    kvStoreName,
    datasetPrefix,
    configUrl: input.configUrl || process.env.JOBSEARCH_CONFIG_URL || process.env.CONFIG_URL || null,
  };

  await kv.setValue('run_meta.json', pipelineMeta);

  let overallStatus = 'SUCCEEDED';
  const stepRuns = {};

  try {
    // 1) Collect
    const collectActor = resolveActorId({ config, actorUser, step: 'collect' });
    stepRuns.collect = await safeCallActor(collectActor, { config, kvStoreName, datasetPrefix, runId }, 'collect');

    // 2) Merge + dedup
    const mergeActor = resolveActorId({ config, actorUser, step: 'merge' });
    stepRuns.merge = await safeCallActor(mergeActor, { config, kvStoreName, datasetPrefix, runId }, 'merge');

    // 3) Score
    if (config?.scoring?.enabled !== false) {
      const scoreActor = resolveActorId({ config, actorUser, step: 'score' });
      stepRuns.score = await safeCallActor(scoreActor, { config, kvStoreName, datasetPrefix, runId }, 'score');
    } else {
      log.warning('Scoring disabled by config.scoring.enabled=false');
    }

    // 4) Notify
    if (config?.notify?.enabled !== false) {
      const notifyActor = resolveActorId({ config, actorUser, step: 'notify' });
      stepRuns.notify = await safeCallActor(notifyActor, { config, kvStoreName, datasetPrefix, runId }, 'notify');
    } else {
      log.warning('Notify disabled by config.notify.enabled=false');
    }

    // 5) Diagnostics (optional)
    if (config?.diagnostics?.enabled) {
      const diagActor = resolveActorId({ config, actorUser, step: 'diagnostics' });
      stepRuns.diagnostics = await safeCallActor(diagActor, { config, kvStoreName, datasetPrefix, runId }, 'diagnostics');
    } else {
      log.info('Diagnostics upload disabled by config.diagnostics.enabled=false');
    }
  } catch (err) {
    overallStatus = 'FAILED';
    log.error(`Pipeline failed: ${err?.stack || err}`);

    // Best-effort diagnostics on failure
    try {
      if (config?.diagnostics?.enabled) {
        const diagActor = resolveActorId({ config, actorUser, step: 'diagnostics' });
        await Actor.call(diagActor, {
          config,
          kvStoreName,
          datasetPrefix,
          runId,
          mode: 'failure',
          errorMessage: String(err?.message || err),
          errorStack: String(err?.stack || ''),
        });
      }
    } catch (diagErr) {
      log.error(`Diagnostics call also failed: ${diagErr?.stack || diagErr}`);
    }

    throw err;
  } finally {
    const finishedAt = nowIso();
    const report = {
      ...pipelineMeta,
      finishedAt,
      status: overallStatus,
      stepRuns: Object.fromEntries(
        Object.entries(stepRuns).map(([k, r]) => [k, { id: r?.id || null, status: r?.status || null }])
      ),
    };
    await kv.setValue('pipeline_report.json', report);
  }
});
