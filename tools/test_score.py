#!/usr/bin/env python
"""
Test scorer: Replicate the exact LLM scoring call from 03_score_jobs.

Usage:
    python tools/test_score.py <JOB_ID>
    python tools/test_score.py B:8518737

Fetches the job from the most recent merged dataset in KV store,
constructs the same prompt the scorer uses (rubric + job JSON),
calls the same OpenAI model with the same parameters, and prints
the LLM's response.

Requires: APIFY_TOKEN and OPENAI_API_KEY environment variables.
"""

import json
import os
import sys
import urllib.request
import re


def fetch_json(url):
    """Fetch JSON from a URL."""
    req = urllib.request.Request(url, headers={
        'User-Agent': 'Mozilla/5.0 (compatible; test-scorer/1.0)',
    })
    resp = urllib.request.urlopen(req, timeout=30)
    return json.loads(resp.read().decode('utf-8'))


def fetch_text(url):
    """Fetch text from a URL."""
    req = urllib.request.Request(url, headers={
        'User-Agent': 'Mozilla/5.0 (compatible; test-scorer/1.0)',
    })
    resp = urllib.request.urlopen(req, timeout=30)
    return resp.read().decode('utf-8')


def find_latest_scored_dataset(apify_token):
    """Find the most recent scored dataset ID."""
    base = 'https://api.apify.com/v2'
    url = f'{base}/datasets?token={apify_token}&unnamed=true&limit=20&desc=true'
    data = fetch_json(url)
    for item in data.get('data', {}).get('items', []):
        name = item.get('name') or ''
        if 'scored' in name:
            print(f'  Latest scored dataset: {item["id"]} ({name})')
            return item['id']
    return None


def find_job_in_dataset(apify_token, dataset_id, job_id):
    """Find a job by ID or company+title in a specific dataset."""
    base = 'https://api.apify.com/v2'
    # Parse job_id prefix to get source type and numeric ID
    prefix = job_id.split(':')[0] if ':' in job_id else ''
    numeric_id = job_id.split(':')[1] if ':' in job_id else job_id

    offset = 0
    while True:
        url = f'{base}/datasets/{dataset_id}/items?token={apify_token}&limit=250&offset={offset}'
        try:
            items = fetch_json(url)
        except Exception:
            break
        if not items:
            break
        for item in items:
            # Check jobIds (can be array or null)
            job_ids = item.get('jobIds') or []
            if isinstance(job_ids, str):
                job_ids = [j.strip() for j in job_ids.split(',')]
            if isinstance(job_ids, list) and job_id in job_ids:
                return item
            # Check url/applyUrl for the numeric ID (Built In uses /job/.../ID)
            for url_field in ['url', 'applyUrl']:
                u = str(item.get(url_field) or '')
                if numeric_id and numeric_id in u:
                    return item
            # Check id field
            if item.get('id') == job_id:
                return item
        offset += len(items)
        if len(items) < 250:
            break

    return None


def find_job_in_datasets(apify_token, job_id):
    """Find a job by ID in recent datasets (merged, scored, or accepted)."""
    base = f'https://api.apify.com/v2'

    # Search multiple pages of datasets to find merged/scored ones
    candidate_ids = []
    for page_offset in range(0, 300, 50):
        url = f'{base}/datasets?token={apify_token}&unnamed=true&limit=50&offset={page_offset}&desc=true'
        try:
            data = fetch_json(url)
        except Exception:
            break
        items = data.get('data', {}).get('items', [])
        if not items:
            break
        for item in items:
            name = item.get('name') or ''
            # Prefer scored datasets (they have full job data + evaluation)
            # Then merged, then accepted
            if 'scored' in name or 'merged' in name:
                candidate_ids.append((item['id'], name))

    print(f'Found {len(candidate_ids)} candidate datasets. Searching for job {job_id}...')

    for ds_id, ds_name in candidate_ids[:8]:  # check up to 8 most recent
        offset = 0
        while True:
            url = f'{base}/datasets/{ds_id}/items?token={apify_token}&limit=250&offset={offset}'
            try:
                items = fetch_json(url)
            except Exception:
                break
            if not items:
                break
            for item in items:
                # Check jobIds array
                job_ids = item.get('jobIds') or []
                if isinstance(job_ids, str):
                    job_ids = [j.strip() for j in job_ids.split(',')]
                if isinstance(job_ids, list) and job_id in job_ids:
                    print(f'  Found in dataset {ds_name or ds_id}')
                    return item
                # Also check single id field
                if item.get('id') == job_id:
                    print(f'  Found in dataset {ds_name or ds_id}')
                    return item
            offset += len(items)
            if len(items) < 250:
                break

    return None


def find_job_in_scored_xlsx(job_id):
    """Find a job by checking scored.xlsx from KV store."""
    apify_token = os.environ.get('APIFY_TOKEN', '').strip()
    kv_id = 'KXRf1EAkVmKdWhc1T'

    # Download scored dataset items via KV store's scoring_report to find dataset
    # Actually, let's just fetch scored.xlsx and look for the job
    import tempfile
    url = f'https://api.apify.com/v2/key-value-stores/{kv_id}/records/scored.xlsx?token={apify_token}'
    req = urllib.request.Request(url)
    resp = urllib.request.urlopen(req, timeout=30)

    tmp = os.path.join(tempfile.gettempdir(), 'test_score_scored.xlsx')
    with open(tmp, 'wb') as f:
        f.write(resp.read())

    try:
        import pandas as pd
        df = pd.read_excel(tmp, header=1)
        # Search Job IDs column
        for _, row in df.iterrows():
            ids_str = str(row.get('Job IDs', ''))
            if job_id in ids_str:
                return row.to_dict()
    except ImportError:
        print('Warning: pandas not available, cannot search scored.xlsx')

    return None


def fetch_builtin_description(url):
    """Fetch job description from Built In page (same as scorer does)."""
    req = urllib.request.Request(url, headers={
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    })
    try:
        resp = urllib.request.urlopen(req, timeout=15)
        html = resp.read().decode('utf-8', errors='replace')

        # Extract JSON-LD
        ld_matches = re.findall(
            r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
            html, re.DOTALL
        )
        for ld in ld_matches:
            try:
                d = json.loads(ld)
                if d.get('@type') == 'JobPosting':
                    return d.get('description', '')
            except json.JSONDecodeError:
                continue
    except Exception as e:
        print(f'Warning: Could not fetch Built In description: {e}')

    return None


def build_prompt(rubric_text, job, location_ok='known'):
    """Build the exact same prompt the scorer uses."""

    # Truncate description to 12000 chars (default maxDescChars)
    max_desc_chars = 12000
    desc = str(job.get('description') or '')
    if len(desc) > max_desc_chars:
        desc = desc[:max_desc_chars] + '\n\n[TRUNCATED]'

    job_for_prompt = {
        'title': job.get('title') or '',
        'company': job.get('company') or '',
        'location': job.get('location') or '',
        'workMode': job.get('workMode') or '',
        'url': job.get('url') or '',
        'applyUrl': job.get('applyUrl') or '',
        'sources': job.get('sources') or [job.get('source', '')],
        'postedAt': job.get('postedAt') or '',
        'salary': job.get('salary') or '',
        'employmentType': job.get('employmentType') or '',
        'description': desc,
    }

    location_prompt = ''
    if location_ok == 'unknown':
        location_prompt = (
            '\n\nIMPORTANT — LOCATION DETERMINATION: The geographic location for this job is ambiguous '
            '(just a city name with no country, or blank). Please carefully examine the job description '
            'for clues about the country:\n'
            '- Salary currency (PLN, GBP, CAD, EUR, etc. = non-US; USD/$ = likely US)\n'
            '- "Right to work in [country]" or "eligible to work in [country]" phrases\n'
            '- Benefits: NHS = UK, RRSP = Canada, 401(k) = US, "umowa o pracę" = Poland\n'
            '- "Based in [city, country]" or "office in [location]" phrases\n'
            '- Language requirements (e.g., Polish, German) suggesting a specific country\n'
            '- Company headquarters location mentioned in description\n'
            'Return is_us (true if US, false if non-US, null if truly unknown) and '
            'location_country (ISO3 code like "GBR", "POL", "CAN" if you can determine it, or "" if unknown).\n'
            'Also determine if the job is within commuting distance (~45 min drive) of Lexington, Massachusetts — '
            'return is_commutable_to_lexington_ma (true/false/null).'
        )

    messages = [
        {
            'role': 'system',
            'content': (
                rubric_text +
                '\n\n'
                'Return ONLY valid JSON, no markdown. Ensure fields: accept, score, confidence, '
                'reason_short, reasons, red_flags, tags, salary_extracted, company_url, role, '
                'location, work_mode, is_us, location_country.\n'
                "location: Your best determination of the job's geographic location (e.g., \"Boston MA\", \"JPN\", \"USA\"). "
                "Use the provided location as a starting point, but refine based on the description.\n"
                'work_mode: One of "Remote", "Hybrid", "On-Site", or "". Refine based on the description.\n'
                'salary_extracted: If the job description mentions any compensation info (salary range, hourly rate, '
                'annual pay, "up to $X", "$X-$Y/yr", OTE, etc.), extract it as a string (e.g., "$90,000-$120,000/yr", '
                '"$45/hr"). Look for explicit ranges, "base salary", "total compensation", "pay band". '
                'Return "" if no salary information is found.\n'
                'is_us: true if this job is located in the United States, false if not, null if truly unknown.\n'
                'location_country: ISO3 country code (e.g., "GBR", "POL", "USA") if determinable, or "" if unknown.'
                + location_prompt
            ),
        },
        {
            'role': 'user',
            'content': json.dumps(job_for_prompt, indent=2, ensure_ascii=False),
        },
    ]

    return messages


def call_openai(api_key, model, messages, temperature=0):
    """Call OpenAI Responses API — same as scorer's callOpenAIJson."""
    url = 'https://api.openai.com/v1/responses'

    is_gpt5 = model.startswith('gpt-5')
    max_output_tokens = 4096 if is_gpt5 else 700

    payload = {
        'model': model,
        'input': messages,
        'max_output_tokens': max_output_tokens,
        'text': {'format': {'type': 'json_object'}},
    }

    if is_gpt5:
        payload['reasoning'] = {'effort': 'medium'}
    else:
        payload['temperature'] = temperature

    data = json.dumps(payload).encode('utf-8')
    req = urllib.request.Request(url, data=data, headers={
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json',
    })

    resp = urllib.request.urlopen(req, timeout=120)
    result = json.loads(resp.read().decode('utf-8'))

    # Extract response text (same as extractResponseText in scorer)
    parts = []
    output = result.get('output', [])
    if isinstance(output, list):
        for o in output:
            if isinstance(o, dict):
                if isinstance(o.get('text'), str):
                    parts.append(o['text'])
                content = o.get('content', [])
                if isinstance(content, list):
                    for c in content:
                        if isinstance(c, dict):
                            if isinstance(c.get('text'), str):
                                parts.append(c['text'])

    text = ''.join(parts).strip()

    # Strip code fences (same as stripCodeFences)
    text = re.sub(r'^\s*```(?:json)?\s*', '', text, flags=re.IGNORECASE)
    text = re.sub(r'\s*```\s*$', '', text, flags=re.IGNORECASE)
    text = text.strip()

    usage = result.get('usage', {})

    return json.loads(text), usage


def replay_from_debug(job_id, apify_token, openai_key):
    """Replay the exact LLM call from debug_llm.json (for debug ID jobs)."""
    kv_id = 'KXRf1EAkVmKdWhc1T'
    url = f'https://api.apify.com/v2/key-value-stores/{kv_id}/records/debug_llm.json?token={apify_token}'
    debug = fetch_json(url)

    # Find the job in debug results
    target = None
    for job in debug.get('jobs', []):
        if job_id in (job.get('jobIds') or []):
            target = job
            break

    if not target:
        print(f'Job {job_id} not found in debug_llm.json')
        print(f'Available debug jobs: {[j.get("jobIds") for j in debug.get("jobs", [])]}')
        sys.exit(1)

    print(f'Found in debug_llm.json: "{target.get("title")}" at {target.get("company")}')
    print(f'Job IDs: {target.get("jobIds")}')

    messages = target.get('prompt', [])
    original = target.get('response', {})
    print(f'Original result: accept={original.get("accept")}, score={original.get("score")}')
    print(f'Original red_flags: {original.get("red_flags", [])}')

    # Fetch config to get model
    config = json.loads(fetch_text('http://forgaard.com/jobsearch/config.json'))
    model = config.get('scoring', {}).get('model', 'gpt-4.1-mini')
    print(f'\nReplaying with {model} using exact same prompt...')

    evaluation, usage = call_openai(openai_key, model, messages, temperature=0)

    print(f'\n{"="*60}')
    print(f'REPLAY RESULT')
    print(f'{"="*60}')
    print(f'  Accept: {evaluation.get("accept")}')
    print(f'  Score:  {evaluation.get("score")}')
    print(f'  Reason: {evaluation.get("reason_short")}')
    print(f'  Red flags: {evaluation.get("red_flags")}')

    if evaluation.get('accept') == original.get('accept') and evaluation.get('score') == original.get('score'):
        print(f'\n  MATCH: Same result as production')
    else:
        print(f'\n  DIFFERENT from production (accept={original.get("accept")}, score={original.get("score")})')

    print(f'\n  Tokens: input={usage.get("input_tokens", "?")}, output={usage.get("output_tokens", "?")}')
    print(f'\nFull JSON:')
    print(json.dumps(evaluation, indent=2, ensure_ascii=False))


def replay_from_file(prompt_file, openai_key):
    """Replay an LLM call from a saved prompt JSON file."""
    with open(prompt_file, 'r', encoding='utf-8') as f:
        messages = json.load(f)

    config = json.loads(fetch_text('http://forgaard.com/jobsearch/config.json'))
    model = config.get('scoring', {}).get('model', 'gpt-4.1-mini')
    print(f'Replaying from {prompt_file} with {model}...')

    evaluation, usage = call_openai(openai_key, model, messages, temperature=0)

    print(f'\n{"="*60}')
    print(f'REPLAY RESULT')
    print(f'{"="*60}')
    print(f'  Accept: {evaluation.get("accept")}')
    print(f'  Score:  {evaluation.get("score")}')
    print(f'  Reason: {evaluation.get("reason_short")}')
    print(f'  Red flags: {evaluation.get("red_flags")}')
    print(f'\n  Tokens: input={usage.get("input_tokens", "?")}, output={usage.get("output_tokens", "?")}')
    print(f'\nFull JSON:')
    print(json.dumps(evaluation, indent=2, ensure_ascii=False))


def replay_from_dataset(search_term, apify_token, openai_key):
    """Replay an LLM call using the llmInput saved in the scored dataset.

    search_term can be a job ID (F:12345) or a company name to search for.
    Finds the job in the most recent scored dataset, uses its llmInput
    to build the exact same messages, and calls the LLM.
    """
    base = 'https://api.apify.com/v2'

    # Find the most recent scored dataset
    data = fetch_json(f'{base}/datasets?token={apify_token}&unnamed=false&limit=10&desc=true')
    scored_ds = None
    for item in data.get('data', {}).get('items', []):
        name = item.get('name') or ''
        if 'scored' in name:
            scored_ds = item
            break

    if not scored_ds:
        print('No scored dataset found')
        sys.exit(1)

    print(f'Searching in: {scored_ds["name"]}')

    # Load items and find the job
    target = None
    search_lower = search_term.lower()
    offset = 0
    while True:
        url = f'{base}/datasets/{scored_ds["id"]}/items?token={apify_token}&limit=250&offset={offset}'
        items = fetch_json(url)
        if not items:
            break
        for item in items:
            # Match by job ID
            job_ids = item.get('sourceJobIds') or []
            if search_term in job_ids:
                target = item
                break
            # Match by company name (case-insensitive substring)
            company = (item.get('company') or '').lower()
            title = (item.get('title') or '').lower()
            if search_lower in company or search_lower in title:
                target = item
                break
        if target:
            break
        offset += len(items)
        if len(items) < 250:
            break

    if not target:
        print(f'Job matching "{search_term}" not found in scored dataset')
        sys.exit(1)

    print(f'Found: "{target.get("title")}" at {target.get("company")}')
    print(f'Job IDs: {target.get("sourceJobIds", [])}')
    print(f'filterReason: {target.get("filterReason", "")}')

    llm_input = target.get('llmInput')
    if not llm_input:
        print('ERROR: No llmInput saved for this job. It may have been a cache hit or pre-filtered.')
        print('Use --replay with a debug ID, or add this job to debugJobIds and re-run.')
        sys.exit(1)

    original = target.get('evaluation', {})
    print(f'Original result: accept={original.get("accept")}, score={original.get("score")}')
    print(f'Original red_flags: {original.get("red_flags", [])}')

    # Build messages from llmInput (same structure as scorer)
    rubric_text = fetch_text('http://forgaard.com/jobsearch/rubric.txt')
    config = json.loads(fetch_text('http://forgaard.com/jobsearch/config.json'))
    model = config.get('scoring', {}).get('model', 'gpt-4.1-mini')

    messages = build_prompt(rubric_text, llm_input)

    print(f'\nReplaying with {model} using llmInput from scored dataset...')
    print(f'  System prompt: {len(messages[0]["content"])} chars')
    print(f'  User content: {len(messages[1]["content"])} chars')

    evaluation, usage = call_openai(openai_key, model, messages, temperature=0)

    print(f'\n{"="*60}')
    print(f'REPLAY RESULT (from llmInput)')
    print(f'{"="*60}')
    print(f'  Accept: {evaluation.get("accept")}')
    print(f'  Score:  {evaluation.get("score")}')
    print(f'  Reason: {evaluation.get("reason_short")}')
    print(f'  Red flags: {evaluation.get("red_flags")}')

    if evaluation.get('accept') == original.get('accept') and evaluation.get('score') == original.get('score'):
        print(f'\n  MATCH: Same result as production')
    else:
        print(f'\n  DIFFERENT from production (accept={original.get("accept")}, score={original.get("score")})')

    print(f'\n  Tokens: input={usage.get("input_tokens", "?")}, output={usage.get("output_tokens", "?")}')
    print(f'\nFull JSON:')
    print(json.dumps(evaluation, indent=2, ensure_ascii=False))


def main():
    if len(sys.argv) < 2:
        print('Usage:')
        print('  python tools/test_score.py <JOB_ID>              # Rebuild prompt from dataset, call LLM')
        print('  python tools/test_score.py --replay <JOB_ID>     # Replay exact prompt from debug_llm.json')
        print('  python tools/test_score.py --replay-file <FILE>  # Replay from a saved messages JSON file')
        print('  python tools/test_score.py --from-dataset <TERM> # Replay using llmInput from scored dataset')
        print('')
        print('Examples:')
        print('  python tools/test_score.py B:8518737')
        print('  python tools/test_score.py --replay F:1972001947')
        print('  python tools/test_score.py --replay-file prompt.json')
        print('  python tools/test_score.py --from-dataset trimble')
        sys.exit(1)

    apify_token = os.environ.get('APIFY_TOKEN', '').strip()
    openai_key = os.environ.get('OPENAI_API_KEY', '').strip()

    if not openai_key:
        print('Error: OPENAI_API_KEY not set')
        sys.exit(1)

    # --replay mode: use exact prompt from debug_llm.json
    if sys.argv[1] == '--replay':
        if len(sys.argv) < 3:
            print('Error: --replay requires a JOB_ID')
            sys.exit(1)
        if not apify_token:
            print('Error: APIFY_TOKEN not set')
            sys.exit(1)
        replay_from_debug(sys.argv[2], apify_token, openai_key)
        return

    # --replay-file mode: use prompt from a saved JSON file
    if sys.argv[1] == '--replay-file':
        if len(sys.argv) < 3:
            print('Error: --replay-file requires a FILE path')
            sys.exit(1)
        replay_from_file(sys.argv[2], openai_key)
        return

    # --from-dataset mode: use llmInput from scored dataset
    if sys.argv[1] == '--from-dataset':
        if len(sys.argv) < 3:
            print('Error: --from-dataset requires a search term (job ID or company name)')
            sys.exit(1)
        if not apify_token:
            print('Error: APIFY_TOKEN not set')
            sys.exit(1)
        replay_from_dataset(sys.argv[2], apify_token, openai_key)
        return

    # Default mode: rebuild prompt from dataset
    job_id = sys.argv[1]

    if not apify_token:
        print('Error: APIFY_TOKEN not set')
        sys.exit(1)

    # 1. Fetch rubric from forgaard.com (same source as scorer)
    print('Fetching rubric from forgaard.com...')
    rubric_text = fetch_text('http://forgaard.com/jobsearch/rubric.txt')
    rubric_version = rubric_text.split('\n')[0] if rubric_text else '?'
    print(f'  Rubric: {rubric_version}')

    # 2. Fetch config to get model
    print('Fetching config from forgaard.com...')
    config = json.loads(fetch_text('http://forgaard.com/jobsearch/config.json'))
    model = config.get('scoring', {}).get('model', 'gpt-4.1-mini')
    print(f'  Model: {model}')

    # 3. Find the job in the most recent scored dataset (has full data + evaluation)
    print(f'\nSearching for job {job_id}...')

    # Find the most recent scored dataset
    scored_ds_id = find_latest_scored_dataset(apify_token)
    job = None
    if scored_ds_id:
        job = find_job_in_dataset(apify_token, scored_ds_id, job_id)

    if not job:
        # Fallback: broader search
        job = find_job_in_datasets(apify_token, job_id)

    if not job:
        print(f'Job {job_id} not found.')
        sys.exit(1)

    print(f'  Found: "{job.get("title")}" at {job.get("company")}')

    # 4. Check if description needs enrichment (Built In jobs)
    desc = job.get('description') or ''
    sources = job.get('sources') or [job.get('source', '')]

    if not desc and any(str(s).startswith('builtin_') for s in sources):
        print('\n  Description is empty — fetching from Built In...')
        bi_url = job.get('url') or job.get('applyUrl')
        if bi_url and 'builtin.com' in bi_url:
            fetched_desc = fetch_builtin_description(bi_url)
            if fetched_desc:
                job['description'] = fetched_desc
                print(f'  Fetched {len(fetched_desc)} chars from Built In')
            else:
                print('  Warning: Could not fetch description from Built In')

    desc = job.get('description') or ''
    print(f'  Description: {len(desc)} chars')

    # Show key details
    print(f'  Location: {job.get("location")}')
    print(f'  Work mode: {job.get("workMode")}')
    print(f'  Salary: {job.get("salary") or "(none)"}')
    print(f'  Sources: {sources}')

    # Check for seniority keywords in description (for debugging)
    seniority_words = ['senior', 'sr.', 'lead', 'principal', 'staff', 'director', 'vp', 'chief', 'head of']
    found_seniority = []
    desc_lower = desc.lower()
    for word in seniority_words:
        if word in desc_lower:
            idx = desc_lower.find(word)
            context = desc[max(0, idx-30):idx+50]
            found_seniority.append(f'  "{word}" at pos {idx}: ...{context}...')

    if found_seniority:
        print(f'\n  WARNING: Seniority keywords found in description:')
        for s in found_seniority:
            print(f'    {s}')

    # 5. Build prompt and call LLM
    print(f'\nCalling {model} with temperature=0...')
    messages = build_prompt(rubric_text, job)

    # Print prompt size for reference
    system_len = len(messages[0]['content'])
    user_len = len(messages[1]['content'])
    print(f'  System prompt: {system_len} chars')
    print(f'  User content: {user_len} chars')

    try:
        evaluation, usage = call_openai(openai_key, model, messages, temperature=0)
    except Exception as e:
        print(f'\nError calling OpenAI: {e}')
        sys.exit(1)

    # 6. Print results
    print(f'\n{"="*60}')
    print(f'LLM EVALUATION RESULT')
    print(f'{"="*60}')
    print(f'  Accept: {evaluation.get("accept")}')
    print(f'  Score:  {evaluation.get("score")}')
    print(f'  Confidence: {evaluation.get("confidence")}')
    print(f'  Reason: {evaluation.get("reason_short")}')
    print(f'  Role:   {evaluation.get("role")}')
    print(f'  Red flags: {evaluation.get("red_flags")}')
    print(f'  Reasons: {evaluation.get("reasons")}')
    print(f'  Tags: {evaluation.get("tags")}')
    print(f'  Salary: {evaluation.get("salary_extracted")}')
    print(f'  Location: {evaluation.get("location")}')
    print(f'  Work mode: {evaluation.get("work_mode")}')
    print(f'  is_us: {evaluation.get("is_us")}')
    print(f'  location_country: {evaluation.get("location_country")}')
    print(f'\n  Tokens: input={usage.get("input_tokens", "?")}, output={usage.get("output_tokens", "?")}')
    print(f'\nFull JSON:')
    print(json.dumps(evaluation, indent=2, ensure_ascii=False))


if __name__ == '__main__':
    main()
