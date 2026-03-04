// 00_run_pipeline/src/main.js â€” v0.1.0
// Orchestrator: runs all pipeline steps sequentially using Actor.call, stops on first error.
// If a step fails and onFailure.actorId is provided, it runs that actor as a best-effort diagnostics step.

import { Actor } from 'apify';

Actor.main(async () => {
  const input = await Actor.getInput() || {};
  const steps = Array.isArray(input.steps) ? input.steps : [];
  const onFailure = input.onFailure || null;

  if (!steps.length) throw new Error('No steps provided. Provide input.steps[] with actorId and input.');

  const results = [];
  try {
    for (let i = 0; i < steps.length; i++) {
      const step = steps[i] || {};
      const actorId = step.actorId;
      const stepInput = step.input || {};
      if (!actorId) throw new Error(`Step ${i} missing actorId.`);

      console.log(`Running step ${i + 1}/${steps.length}: ${actorId}`);
      const run = await Actor.call(actorId, stepInput);
      results.push({ actorId, runId: run?.id || null, status: run?.status || null });
      console.log(`Step ${i + 1} complete: ${actorId} (runId=${run?.id || 'n/a'})`);
    }

    console.log('Pipeline complete.');
    console.log(JSON.stringify(results, null, 2));
  } catch (err) {
    console.log('Pipeline failed:', String(err?.message || err));
    console.log(JSON.stringify(results, null, 2));

    if (onFailure && onFailure.actorId) {
      try {
        console.log(`Running onFailure diagnostics actor: ${onFailure.actorId}`);
        await Actor.call(onFailure.actorId, onFailure.input || {});
      } catch (e) {
        console.log('onFailure diagnostics actor failed:', String(e?.message || e));
      }
    }

    throw err;
  }
});
