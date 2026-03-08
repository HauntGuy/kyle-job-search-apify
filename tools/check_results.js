// Temporary script to check pipeline results
const token = process.argv[2];
const kvStoreId = 'KXRf1EAkVmKdWhc1T';

async function fetchKV(key) {
  const res = await fetch(`https://api.apify.com/v2/key-value-stores/${kvStoreId}/records/${key}?token=${token}`);
  if (res.status >= 400) return null;
  return await res.json();
}

(async () => {
  // Collect report
  const cr = await fetchKV('collect_report.json');
  if (cr) {
    console.log('=== COLLECT REPORT ===');
    console.log('Totals:', JSON.stringify(cr.totals));
    console.log('Duration:', cr.durationSecs, 'seconds');
    console.log('Sources:');
    for (const s of cr.sources) {
      const count = s.itemCount != null ? ` (${s.itemCount} items, used ${s.usedCount || 0})` : '';
      console.log(`  ${s.id} - ${s.status}${count}`);
    }
  }

  // Merge report
  const mr = await fetchKV('merge_report.json');
  if (mr) {
    console.log('\n=== MERGE REPORT ===');
    console.log(`Scanned: ${mr.scanned}, Merged: ${mr.merged}, Duplicates: ${mr.duplicates}`);
  }

  // Scoring report
  const sr = await fetchKV('scoring_report.json');
  if (sr) {
    console.log('\n=== SCORING REPORT ===');
    console.log(`Total scored: ${sr.totalScored}, Accepted: ${sr.accepted}, Threshold: ${sr.threshold}`);
    console.log(`Model: ${sr.model}`);
    console.log('OpenAI stats:', JSON.stringify(sr.openai));
    if (sr.warnings) console.log('Warnings:', sr.warnings.join('; '));
  }
})();
