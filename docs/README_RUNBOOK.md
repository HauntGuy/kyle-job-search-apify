# Kyle Job Search Pipeline — Runbook (v3)

This repo powers a nightly Apify pipeline that finds programming jobs for Kyle Forgaard and emails an `accepted.csv` to Randy.

## Quick links

- **Pretty log (HTML viewer):**
  https://hauntguy.github.io/kyle-job-search-apify/job_search_log.html

- **Raw log (authoritative HTML source, HTTPS):**
  https://gist.githubusercontent.com/HauntGuy/9db067b3e3d472ddafd58fd3705ed10c/raw/job_search_log.html

- **Live config (knobs):**
  http://forgaard.com/jobsearch/config.json

- **Live rubric (LLM criteria):**
  http://forgaard.com/jobsearch/rubric.txt

## Architecture

Actors (Apify):
- `00_run_pipeline` — orchestrator (runs everything in order)
- `01_collect_jobs` — collects jobs from enabled sources in config.json
- `02_merge_dedup` — merges sources + dedupes into a single pool
- `03_score_jobs` — LLM scoring + filtering using rubric.txt
- `04_notify_email` — emails `accepted.csv`
- `99_diagnostics_dump` — writes the HTML job search log to a GitHub Gist

Data flow:
1) Collectors write raw results and reports to the KV store + datasets (Apify).
2) Merge produces a merged set of jobs.
3) Scoring produces `accepted.csv` + `scored.csv` + reports.
4) Notify emails `accepted.csv`.
5) Diagnostics writes/updates the log HTML in a GitHub Gist (and the `/docs/job_search_log.html` page renders it).

## Apify configuration

### Required Apify Actor environment variables

Set these on the appropriate actors:

**03_score_jobs**
- `OPENAI_API_KEY` (Secret)

**99_diagnostics_dump**
- `GIST_ID` (not secret)
- `GITHUB_TOKEN` (Secret; must have Gists read/write permission)
- `GIST_FILENAME` = `job_search_log.html` (not secret)
- `JOBSEARCH_CONFIG_URL` = `http://forgaard.com/jobsearch/config.json` (optional; can be provided in task input instead)

### Permissions
All actors should be set to **Full permissions** (Actors → Settings → Actor permissions).

## GitHub → Apify automation

### Auto-build
Each actor source is set to GitHub subfolder `actors/<actor_name>`.
GitHub webhooks trigger builds on push.

### Task input sync
Task inputs live in:
- `actors/<actor_name>/task_input.json`

GitHub Action workflow:
- `.github/workflows/sync-apify-task-inputs.yml`
- script: `tools/sync_apify_task_inputs.cjs`

Mapping file:
- `tools/apify_tasks_map.json` (taskName → apifyTaskId)

Secret:
- GitHub repo secret `APIFY_TOKEN`

## Operating procedure

### Run once (manual)
Run `00_run_pipeline` saved task in Apify.

### Nightly schedule
Schedule `00_run_pipeline` in Apify Scheduler (when ready).

### When something fails
1) Open the pretty log page (or raw log URL).
2) The log should show which step failed and relevant error text.
3) Fix config/rubric/actor code accordingly and rerun.

## Changing knobs safely

When changing any task input JSON:
1) Update `actors/<actor>/task_input.json`
2) Commit + push
3) Ensure Apify task input is synced (GitHub Action will apply it)

When changing search behavior:
- Edit `config.json` on forgaard.com (no code change required)

When changing scoring criteria:
- Edit `rubric.txt` on forgaard.com (no code change required)