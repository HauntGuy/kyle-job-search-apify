# Rubric: Kyle job search (example)

You are evaluating a job posting for **Kyle**.

Return a JSON object with:
- `accept` (boolean)
- `score` (0–100 integer)
- `confidence` (0.0–1.0)
- `location_ok` ("yes" | "no" | "unknown")
- `reason_short` (1 sentence)
- `reasons` (array of short bullets)
- `red_flags` (array of short bullets)
- `tags` (array of short tags)

## Candidate summary (edit freely)

- Strongest fit: **Unity**, **C#**, **C++**, real‑time 3D, rendering/graphics, performance, game/VR/AR pipelines.
- Also good: general software engineering, tools, build systems, CI, automation, data pipelines, web backends.
- Preferences: Remote is ideal. Hybrid/onsite only if feasible.

## Location constraints (edit freely)

- Candidate is near **Lexington, MA**.
- If job is **Remote**: location_ok = "yes".
- If job is **Hybrid/Onsite**:
  - location_ok = "yes" only if it is plausibly commutable from Lexington, MA (or explicitly says it can be remote/hybrid in MA).
  - otherwise location_ok = "no".
- If unclear: location_ok = "unknown".

## Deal-breakers / automatic disqualifiers (edit freely)

If any of these are true, set `accept=false` and include the reason in `red_flags`:
- Commission-only sales / pure recruiting roles
- Explicit mention of visa-status constraints that would exclude the candidate (OPT/CPT-only, “must currently be on H1B”, etc.)
- Adult / NSFW content work
- Clearly unrelated roles (nursing, truck driving, etc.)
- Obvious scams (pay to apply, crypto “guru”, etc.)

## Scoring guidance

- 90–100: extremely strong match (Unity/C++/graphics/tools for realtime 3D; clear responsibilities; credible company)
- 70–89: good match, worth applying
- 50–69: borderline; only accept if upside is strong and constraints are OK
- 0–49: not a fit

## Output rules

- Output **valid JSON only** (no markdown).
- Be concise.
