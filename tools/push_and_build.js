// Temporary script to push source code and trigger builds on Apify
const fs = require('fs');

const token = process.argv[2];
if (!token) { console.error('Usage: node push_and_build.js <APIFY_TOKEN>'); process.exit(1); }

// Actor IDs can be passed as additional args, or defaults to all actors
const actors = [
  { id: 'geG3B8lxaePXguzP5', dir: 'actors/00_run_pipeline', name: '00_run_pipeline' },
  { id: 'SGTmGMEk3jQYL1gyt', dir: 'actors/01_collect_jobs', name: '01_collect_jobs' },
  { id: '9NCG9BRS1FK8wHXyv', dir: 'actors/02_merge_dedup',  name: '02_merge_dedup' },
  { id: 'CCrFLcQe6Gg25Ye8p', dir: 'actors/03_score_jobs',   name: '03_score_jobs' },
  { id: 'BzKpKoXJ2AYg98U62', dir: 'actors/04_notify_email', name: '04_notify_email' },
  { id: 'FxD46vUSatdzoVm97', dir: 'actors/99_diagnostics_dump', name: '99_diagnostics_dump' },
];

// If specific actor names are passed, only build those
const filterNames = process.argv.slice(3);

async function pushActor(actorId, actorDir, actorName) {
  const mainJs = fs.readFileSync(actorDir + '/src/main.js', 'utf8');
  const pkgJson = fs.readFileSync(actorDir + '/package.json', 'utf8');

  const sourceFiles = [
    { name: 'src/main.js', format: 'TEXT', content: mainJs },
    { name: 'package.json', format: 'TEXT', content: pkgJson },
  ];

  // Update actor version source files
  const updateRes = await fetch(
    `https://api.apify.com/v2/acts/${actorId}/versions/0.0?token=${token}`,
    {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        versionNumber: '0.0',
        buildTag: 'latest',
        sourceType: 'SOURCE_FILES',
        sourceFiles,
      }),
    }
  );
  const updateJson = await updateRes.json();
  if (updateRes.status >= 400) {
    console.error(`${actorName} version update failed:`, JSON.stringify(updateJson).slice(0, 500));
    return null;
  }
  console.log(`${actorName} source updated.`);

  // Trigger a build
  const buildRes = await fetch(
    `https://api.apify.com/v2/acts/${actorId}/builds?token=${token}&version=0.0`,
    {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({}),
    }
  );
  const buildJson = await buildRes.json();
  if (buildRes.status >= 400) {
    console.error(`${actorName} build trigger failed:`, JSON.stringify(buildJson).slice(0, 500));
    return null;
  }
  console.log(`${actorName} build started: id=${buildJson.data?.id} status=${buildJson.data?.status}`);
  return buildJson.data?.id;
}

(async () => {
  const toBuild = filterNames.length > 0
    ? actors.filter(a => filterNames.includes(a.name))
    : actors;

  const results = {};
  for (const a of toBuild) {
    results[a.name] = await pushActor(a.id, a.dir, a.name);
  }
  console.log('All build IDs:', JSON.stringify(results, null, 2));
})();
