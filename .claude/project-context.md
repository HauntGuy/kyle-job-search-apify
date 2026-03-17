# Project Context — Kyle Job Search Apify Pipeline

## Quick Reference IDs

### Apify Task IDs (stable — also in `tools/apify_tasks_map.json`)
| Actor | Task ID |
|---|---|
| 00_run_pipeline | `pf0pvDyiRgaKJjpvm` |
| 01_collect_jobs | `Dyi6N600rk2u2Mldh` |
| 02_merge_dedup | `DHKq2joAmDLv440Wg` |
| 03_score_jobs | `ZKIHCXx87xS9UXpKD` |
| 04_notify_email | `rpZd77CCueSLTS01Q` |
| 99_diagnostics_dump | `orAUomyzzlxeGXCcS` |

### Apify KV Store
- **Name:** `job-pipeline-v3`
- **Store ID:** `KXRf1EAkVmKdWhc1T`
- **Keys:** `accepted.xlsx`, `scored.xlsx`, `collected.xlsx`, `scoring_report.json`, `merge_report.json`

### Useful API Patterns
```bash
TOKEN=$(echo "$APIFY_TOKEN" | tr -d '\r\n')

# Run pipeline task
curl --ssl-no-revoke -s -X POST "https://api.apify.com/v2/actor-tasks/pf0pvDyiRgaKJjpvm/runs?token=$TOKEN"

# Check run status
curl --ssl-no-revoke -s "https://api.apify.com/v2/actor-runs/{RUN_ID}?token=$TOKEN"

# Get run log
curl --ssl-no-revoke -s "https://api.apify.com/v2/actor-runs/{RUN_ID}/log?token=$TOKEN"

# Get KV store record
curl --ssl-no-revoke -s "https://api.apify.com/v2/key-value-stores/KXRf1EAkVmKdWhc1T/records/{KEY}?token=$TOKEN"

# List actor builds
curl --ssl-no-revoke -s "https://api.apify.com/v2/acts/pigletsquid~{ACTOR}/builds?token=$TOKEN&limit=1&desc=true"
```

## External Config (manually updated by Randy)
- **config.json:** `http://forgaard.com/jobsearch/config.json`
- **rubric.txt:** `http://forgaard.com/jobsearch/rubric.txt`
- After changing `web/rubric.txt` in the repo, Randy must manually upload it to forgaard.com

## Diagnostics Gist
- **Gist ID:** `9db067b3e3d472ddafd58fd3705ed10c`
- **GitHub Pages URL:** `https://hauntguy.github.io/kyle-job-search-apify/job_search_log.html`

## Architecture
6 Apify actors orchestrated by `00_run_pipeline`:
1. `01_collect_jobs` — fetches from Fantastic Feed + LinkedIn, writes collected.xlsx + raw dataset
2. `02_merge_dedup` — multi-key dedup (URL + company+title), writes merged dataset
3. `03_score_jobs` — LLM scoring via OpenAI gpt-4o-mini, writes scored.xlsx + accepted.xlsx + datasets
4. `04_notify_email` — emails all 3 XLSX files to randy@forgaard.com
5. `99_diagnostics_dump` — updates GitHub gist with run diagnostics

## Key Preferences
- Don't kick off pipeline runs without Randy's explicit go-ahead
- GitHub is source of truth for code
- Rubric lives at forgaard.com and must be manually uploaded after changes
- External actors show LIMITED_PERMISSIONS — this is expected/OK
- Remind Randy to test GPT-5 mini as scoring model (future)
- Minimize asking Randy to inspect files; use API access instead

## Windows Environment Notes
- Shell: Git Bash on Windows
- curl needs `--ssl-no-revoke` flag
- Environment variables may have trailing `\r` — use `tr -d '\r\n'` when reading
- Tools available: Node.js, jq, Python (installed via winget)
