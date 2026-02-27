// 02_update_registry/main.js — v2.2
import { Actor } from 'apify';

function parseCsv(csv) {
  const lines = csv.trim().split(/\r?\n/);
  const rows = [];
  for (let i = 1; i < lines.length; i++) {
    const parts = lines[i].split(',').map(s => (s || '').trim());
    if (parts.length >= 3) rows.push({ company: parts[0], ats: parts[1].toLowerCase(), slug: parts[2].toLowerCase() });
  }
  return rows;
}

function toCsv(rows) {
  const header = 'company,ats,slug';
  const body = rows.map(r => `${(r.company || '').replace(/,/g, ' ')},${r.ats},${r.slug}`).join('\n');
  return header + '\n' + body + '\n';
}

Actor.main(async () => {
  const kv = await Actor.openKeyValueStore('job-pipeline');

  const discovered = (await kv.getValue('discovered.json')) || [];
  const existingCsv = (await kv.getValue('companies_registry.csv')) || 'company,ats,slug\n';

  const existing = new Map();
  for (const r of parseCsv(existingCsv)) existing.set(`${r.ats}:${r.slug}`, r);

  let added = 0;
  for (const it of discovered) {
    const ats = (it.ats || '').toLowerCase();
    const slug = (it.slug || '').toLowerCase();
    if (!ats || !slug) continue;
    const key = `${ats}:${slug}`;
    if (!existing.has(key)) {
      existing.set(key, { company: (it.companyHint || '').slice(0, 80), ats, slug });
      added++;
    }
  }

  const rows = Array.from(existing.values()).sort((a, b) => (a.company || '').localeCompare(b.company || ''));
  await kv.setValue('companies_registry.csv', toCsv(rows), { contentType: 'text/csv' });
  await kv.setValue('registry_summary.json', { total: rows.length, added });

  // Clear handoff so "added" means something nightly
  await kv.setValue('discovered.json', []);

  console.log(`Registry updated. Total companies: ${rows.length}. Added this run: ${added}.`);
});
