# New numbered pipeline

Actors:
- 00_run_pipeline
- 01a_collector_fantastic_feed
- 01b_collector_linkedin_jobs
- 02_merge_dedup
- 03_score_jobs
- 04_notify_email
- 99_diagnostics_dump

## Diagnostics upload
You already created:
- http://forgaard.com/jobsearch/diag_upload.php
which writes:
- http://forgaard.com/jobsearch/diagnostics.html

Set env vars on Apify actor 99_diagnostics_dump:
- DIAG_UPLOAD_URL = http://forgaard.com/jobsearch/diag_upload.php
- DIAG_UPLOAD_TOKEN = (your token, do NOT commit to GitHub)

## Task input syncing
- Copy tools/apify_tasks_map.template.json -> tools/apify_tasks_map.json
- Fill in apifyTaskId for each Saved Task (from Apify)
- Create GitHub Secret: APIFY_TOKEN
- Push: GitHub Action syncs task_input.json into Apify task inputs
