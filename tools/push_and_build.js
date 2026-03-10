// Trigger Apify builds for actors that use GitHub integration.
// Usage: node tools/push_and_build.js [actor_name ...]
// Token is read from $APIFY_TOKEN env var.
// If no actor names given, builds all actors.

const token = (process.env.APIFY_TOKEN || '').replace(/[\r\n]/g, '');
if (!token) {
  console.error('Error: APIFY_TOKEN environment variable is not set.');
  process.exit(1);
}

const actors = [
  { id: 'geG3B8lxaePXguzP5', name: '00_run_pipeline' },
  { id: 'SGTmGMEk3jQYL1gyt', name: '01_collect_jobs' },
  { id: '9NCG9BRS1FK8wHXyv', name: '02_merge_dedup' },
  { id: 'CCrFLcQe6Gg25Ye8p', name: '03_score_jobs' },
  { id: 'BzKpKoXJ2AYg98U62', name: '04_notify_email' },
  { id: 'FxD46vUSatdzoVm97', name: '99_diagnostics_dump' },
];

const filterNames = process.argv.slice(2);

async function buildActor(actorId, actorName) {
  const res = await fetch(
    `https://api.apify.com/v2/acts/${actorId}/builds?token=${token}&version=0.0&useGitHubIntegration=true`,
    { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' }
  );
  const json = await res.json();
  if (res.status >= 400) {
    console.error(`${actorName} build FAILED:`, JSON.stringify(json).slice(0, 500));
    return null;
  }
  console.log(`${actorName} build started: id=${json.data?.id} status=${json.data?.status}`);
  return json.data?.id;
}

(async () => {
  const toBuild = filterNames.length > 0
    ? actors.filter((a) => filterNames.includes(a.name))
    : actors;

  if (toBuild.length === 0) {
    console.error('No matching actors. Available:', actors.map((a) => a.name).join(', '));
    process.exit(1);
  }

  const results = {};
  for (const a of toBuild) {
    results[a.name] = await buildActor(a.id, a.name);
  }
  console.log('Build IDs:', JSON.stringify(results, null, 2));
})();
