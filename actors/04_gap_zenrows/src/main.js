// 04_gap_zenrows/main.js — v2.2
import { Actor } from 'apify';
import fetch from 'node-fetch';

Actor.main(async () => {
  const input = await Actor.getInput() || {};
  const ZENROWS_API_KEY = Actor.getEnv().ZENROWS_API_KEY || input.ZENROWS_API_KEY;
  if (!ZENROWS_API_KEY) { console.log('No ZENROWS_API_KEY; skipping.'); return; }

  const urls = input.urls || [];
  const render = !!input.renderJs;

  const kv = await Actor.openKeyValueStore('job-pipeline');
  const man = [];
  let idx = 0;

  for (const u of urls) {
    const api = new URL('https://api.zenrows.com/v1/');
    api.searchParams.set('url', u);
    api.searchParams.set('apikey', ZENROWS_API_KEY);
    if (render) api.searchParams.set('js_render', 'true');

    const res = await fetch(api.toString());
    const html = await res.text();

    man.push(`GET ${api.toString()}`);
    man.push(`STATUS ${res.status} FOR ${u}`);

    await kv.setValue(`zenrows_raw_${idx}.html`, html, { contentType: 'text/html' });
    idx++;
    await Actor.sleep(1000);
  }

  await kv.setValue('zenrows_manifest.log', man.join('\n'), { contentType: 'text/plain' });
  console.log(`ZenRows gap fetch complete for ${urls.length} urls`);
});
