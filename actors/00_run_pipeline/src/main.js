// actors/00_run_pipeline/src/main.js
// Orchestrates the pipeline by calling the other actors in order.

import { Actor, log } from 'apify';
import http from 'node:http';
import https from 'node:https';

// ── Transient-error resilience (mirrors 03_score_jobs) ──────────────
// Disable HTTP keep-alive globally.  The orchestrator sits in Actor.call()
// for 20-40 minutes while the scorer runs, polling the Apify API.  During
// that time idle keep-alive sockets can be closed server-side, causing
// ECONNRESET crashes.  Fresh connections per request avoids this.
http.globalAgent = new http.Agent({ keepAlive: false });
https.globalAgent = new https.Agent({ keepAlive: false });

// Prevent ECONNRESET / socket-close errors from crashing the process.
// These are transient network issues; use console directly (not `log`)
// because the Apify logger may not be initialized when these fire.
const TRANSIENT_CODES = new Set(['ECONNRESET', 'ETIMEDOUT', 'EPIPE', 'ECONNABORTED', 'UND_ERR_SOCKET']);
function isTransientError(err) {
  if (!err) return false;
  if (TRANSIENT_CODES.has(err.code)) return true;
  const msg = String(err.message || '').toLowerCase();
  return msg.includes('aborted') || msg.includes('socket hang up') || msg.includes('econnreset');
}

process.on('uncaughtException', (err, origin) => {
  if (isTransientError(err)) {
    console.warn(`[CAUGHT] Transient ${origin}: ${err.code || err.message}`);
    return; // swallow — the Apify SDK's internal polling will retry
  }
  console.error(`[FATAL] Uncaught exception:`, err?.stack || err);
  process.exit(1);
});

process.on('unhandledRejection', (reason) => {
  if (isTransientError(reason)) {
    console.warn(`[CAUGHT] Transient unhandledRejection: ${reason?.code || reason?.message}`);
    return; // swallow
  }
  console.error(`[FATAL] Unhandled rejection:`, reason?.stack || reason);
  process.exit(1);
});

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

// Start an actor and poll for completion.  Uses Actor.start() + polling
// instead of Actor.call() to avoid ECONNRESET during long-running actors.
// Actor.call() holds a single HTTP connection open for the entire run
// (8+ minutes for the scorer), and that idle connection gets killed by the
// server.  With start+poll, each status check is a fresh short-lived request.
// If any individual poll fails, we retry just that poll — never restart the actor.
async function safeCallActor(actorId, input, label) {
  const client = Actor.apifyClient;
  const started = Date.now();

  // 1) Start the actor (returns immediately — no long-lived connection)
  let runId;
  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      log.info(`Starting ${label}: ${actorId}${attempt > 1 ? ` (start retry ${attempt}/3)` : ''}`);
      const run = await client.actor(actorId).start(input);
      runId = run.id;
      break;
    } catch (err) {
      if (isTransientError(err) && attempt < 3) {
        log.warning(`${label} start failed: ${err.code || err.message}. Retrying in 3s...`);
        await new Promise(r => setTimeout(r, 3000));
        continue;
      }
      throw err;
    }
  }

  log.info(`${label} running: runId=${runId}. Polling every 10s...`);

  // 2) Poll run status until done.  Each poll is a short request; if it
  //    fails with ECONNRESET we retry just that poll, not the whole actor.
  const POLL_INTERVAL_MS = 10000;
  const MAX_POLL_RETRIES = 5;

  while (true) {
    await new Promise(r => setTimeout(r, POLL_INTERVAL_MS));

    let runInfo;
    for (let attempt = 1; attempt <= MAX_POLL_RETRIES; attempt++) {
      try {
        runInfo = await client.run(runId).get();
        break;
      } catch (err) {
        if (isTransientError(err) && attempt < MAX_POLL_RETRIES) {
          log.warning(`${label} poll ${attempt}/${MAX_POLL_RETRIES} failed: ${err.code || err.message}. Retrying in 5s...`);
          await new Promise(r => setTimeout(r, 5000));
          continue;
        }
        throw err;
      }
    }

    const status = runInfo?.status;
    if (status === 'SUCCEEDED') {
      const ms = Date.now() - started;
      log.info(`${label} finished with status=SUCCEEDED in ${(ms / 1000).toFixed(1)}s (runId=${runId})`);
      return runInfo;
    }

    if (status === 'FAILED' || status === 'ABORTED' || status === 'TIMED-OUT') {
      const ms = Date.now() - started;
      throw new Error(`${label} failed (status=${status}, runId=${runId}) after ${(ms / 1000).toFixed(1)}s`);
    }

    // Still RUNNING or READY — continue polling
  }
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

    // Safety check: if any source hit its limit, abort before spending money
    // on scoring.  This means we may be missing results and should investigate
    // (raise the limit or split the source).
    const collectReport = await kv.getValue('collect_report.json');
    if (collectReport?.sources) {
      const hitLimit = collectReport.sources.filter(
        (s) => s.status === 'ok' && s.meta?.hitLimitLikely
      );
      if (hitLimit.length > 0) {
        const names = hitLimit.map((s) => `${s.id} (${s.meta?.itemCount}/${s.meta?.requestedLimit || '?'})`).join(', ');
        const msg = `ABORTING: ${hitLimit.length} source(s) hit their limit — results may be incomplete. ` +
          `Raise the limit or split the source before re-running. Sources: ${names}`;
        log.error(msg);
        // Write a report so the notification actor can send an alert
        await kv.setValue('pipeline_report.json', {
          ...pipelineMeta,
          finishedAt: nowIso(),
          status: 'ABORTED_HIT_LIMIT',
          errorMessage: msg,
          hitLimitSources: hitLimit.map((s) => s.id),
          stepRuns: Object.fromEntries(
            Object.entries(stepRuns).map(([k, r]) => [k, { id: r?.id || null, status: r?.status || null }])
          ),
        });
        throw new Error(msg);
      }
    }

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

  } catch (err) {
    overallStatus = 'FAILED';
    log.error(`Pipeline failed: ${err?.stack || err}`);

    // Write pipeline_report.json BEFORE best-effort diagnostics so the
    // diagnostics actor reads the current run's report, not a stale one.
    const failReport = {
      ...pipelineMeta,
      finishedAt: nowIso(),
      status: overallStatus,
      errorMessage: String(err?.message || err),
      stepRuns: Object.fromEntries(
        Object.entries(stepRuns).map(([k, r]) => [k, { id: r?.id || null, status: r?.status || null }])
      ),
    };
    await kv.setValue('pipeline_report.json', failReport);

    // Best-effort diagnostics on failure
    try {
      if (config?.diagnostics?.enabled) {
        const diagActor = resolveActorId({ config, actorUser, step: 'diagnostics' });
        await safeCallActor(diagActor, {
          config,
          kvStoreName,
          datasetPrefix,
          runId,
          mode: 'failure',
          errorMessage: String(err?.message || err),
          errorStack: String(err?.stack || ''),
        }, 'diagnostics (failure)');
      }
    } catch (diagErr) {
      log.error(`Diagnostics call also failed: ${diagErr?.stack || diagErr}`);
    }

    throw err;
  }

  // Write pipeline_report.json BEFORE calling diagnostics so it reads the
  // current run's report, not a stale one from a previous run.
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

  // 5) Diagnostics (optional) — runs AFTER pipeline_report.json is written
  if (config?.diagnostics?.enabled) {
    const diagActor = resolveActorId({ config, actorUser, step: 'diagnostics' });
    stepRuns.diagnostics = await safeCallActor(diagActor, { config, kvStoreName, datasetPrefix, runId }, 'diagnostics');
  } else {
    log.info('Diagnostics upload disabled by config.diagnostics.enabled=false');
  }
});
