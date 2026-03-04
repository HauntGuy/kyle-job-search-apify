# Kyle Job Search – Apify Pipeline (v3)

This repo contains a modular Apify pipeline designed to:
- Collect job listings from **multiple sources** (Apify Store actors + external APIs like RapidAPI, etc.)
- Merge + de-duplicate across sources
- Score jobs with an LLM using a **rubric loaded from an external file** (so a future web form can edit it)
- Notify you (email) and publish a diagnostics page to your GoDaddy site (optional)

## Design goals

- **No hard-coded search terms in code**: collectors read a **remote JSON config**.
- **Rubric lives outside the actor**: scoring reads a **remote rubric file**.
- Easy to add new sources later.
- Each actor is self-contained (no cross-folder imports) to work well with **Apify “Git repository” source + Folder**.

---

## Quick start checklist

### 1) Publish config + rubric on your GoDaddy site

Copy these two example files to your website (FTP/cPanel File Manager):

- `web/config.example.json`  →  `http://forgaard.com/jobsearch/config.json`
- `web/rubric.example.md`    →  `http://forgaard.com/jobsearch/rubric.md`

You can edit these later (and eventually your web form can generate them).

### 2) Create the Apify Actors (once)

In Apify Console → **Actors** → **+ New actor** → **Source: Git repository**:

Git URL:
- `https://github.com/HauntGuy/kyle-job-search-apify`

Branch:
- `main`

Folder (choose one per actor):
- `actors/00_run_pipeline`
- `actors/01_collect_jobs`
- `actors/02_merge_dedup`
- `actors/03_score_jobs`
- `actors/04_notify_email`
- `actors/99_diagnostics_dump`

> Important: after creating each actor, set **Settings → Actor permissions → Full permissions** (at least for `00_run_pipeline`, because it calls the others).

### 3) Set Actor environment variables (secrets)

In Apify Console → Actors → (actor) → **Settings → Environment variables**:

**Required**
- `OPENAI_API_KEY` (on `03_score_jobs`, and optionally on `00_run_pipeline` if you want the orchestrator to do extra checks)

**Diagnostics uploader (optional, but recommended)**
- `DIAG_UPLOAD_URL`   (example: `http://forgaard.com/jobsearch/diag_upload.php`)
- `DIAG_UPLOAD_TOKEN` (must match the token in your PHP uploader)

**RapidAPI / other API keys (optional)**
- `RAPIDAPI_KEY` (used by the optional `rapidapi_jsearch` source)

### 4) Create ONE scheduled Task (recommended)

You only need to schedule **00_run_pipeline**.

Apify Console → **Actors → 00_run_pipeline → Tasks → + Create new**  
Paste `actors/00_run_pipeline/task_input.json` as a starting point, then edit:

- `configUrl`
- `actorUser` (your Apify username, e.g. `pigletsquid`)
- `kvStoreName` (default: `job-pipeline-v3`)

Then schedule the task (e.g., daily).

---

## What gets produced

Everything is written into your Key-Value Store (default `job-pipeline-v3`) plus per-run datasets:

- Raw jobs dataset info: `raw_dataset.json`
- Merged jobs dataset info: `merged_dataset.json`
- Scored jobs dataset info: `scored_dataset.json`
- Accepted jobs CSV: `accepted.csv`
- Reports: `collect_report.json`, `merge_report.json`, `scoring_report.json`, `notify_report.json`

Diagnostics (optional):
- Uploads an HTML diagnostics page to your GoDaddy folder (via your PHP endpoint).

---

## Updating / deploying code

1. Unzip these files into your repo folder.
2. Commit + push.
3. In Apify, each actor will rebuild (if you enabled auto-build via Git integration) or you can click **Build**.

---

## Notes on encoding (BOM)

All files in this bundle are UTF-8 **without BOM**.
If you edit on Windows, prefer **VS Code** and keep the encoding as **UTF-8** (not “UTF-8 with BOM”).
