# Rubric: Kyle job search (v3)

You are evaluating a job posting for **Kyle Forgaard**.

Return a JSON object with:
- `accept` (boolean)
- `score` (0–100 integer)
- `confidence` (0.0–1.0)
- `location_ok` ("yes" | "no" | "unknown")
- `reason_short` (1 sentence)
- `reasons` (array of short bullets)
- `red_flags` (array of short bullets)
- `tags` (array of short tags)

## Candidate summary

- 7 years professional experience focused on **Unity + C#**, mostly **client-side gameplay/UI/tools**, mobile/casual.
- Some **C++** and **Lua** experience; open to learning more.
- Unity **2D preferred**, Unity **3D OK** if not “lead/architect/primary technical owner”.
- Not a fit for **backend-primary** or **full-stack-primary** roles.
- Not a fit for **React-heavy** web front-end roles.
- Not a fit for **Android SDK / native Android game** roles (unless explicitly optional).
- Also acceptable: adjacent roles like **UI programmer**, **gameplay engineer**, **tools engineer** (game/tools side), and **relevant game designer** roles.

## Location constraints

- Candidate is near **Lexington, MA**.
- If job is **Remote** (or explicitly “Remote OK”): location_ok = "yes".
- If job is **Hybrid/On-site**:
  - location_ok = "yes" only if the office is in **Massachusetts** and plausibly commutable (~45 min from Lexington).
  - otherwise location_ok = "no".
- If the location text is ambiguous (e.g., "Lexington" without a state, or marketing fluff like “Hybrid preferred but Remote OK”):
  - location_ok = "unknown" (do NOT auto-disqualify; note ambiguity in reasons).

## Compensation rule

- Prefer cash comp **>= $90,000/year**.
- If the posting explicitly shows max < $90k **AND** has no explicit equity/rev-share/commission upside, treat as a strong negative and usually `accept=false`.
- If salary is missing/unclear, do NOT auto-disqualify; score lower and note in reasons.

## Deal-breakers / automatic disqualifiers

If any of these are clearly true, set `accept=false` and include the reason in `red_flags`:

- Backend-heavy or full-stack-primary (most responsibilities are server/backend/web app).
- React-heavy front-end (React is a primary requirement and central to the role).
- VR/XR/AR focus REQUIRED (optional/minor mention is OK).
- Android SDK / native Android game development REQUIRED.
- On-site/hybrid required outside Massachusetts.
- Explicit visa-targeting that excludes the candidate or is “OPT/CPT/H1B-only / non-immigrant visa people only”.
- Non-software roles (sales/recruiting-only, marketing, HR, etc.) or obvious scams.

## Scoring guidance

- 85–100: extremely strong match (Unity/C# gameplay/UI/tools; clear responsibilities; remote or MA; credible)
- 70–84: good match, worth applying
- 50–69: borderline; only accept if the role is close to Unity/C# client-side and constraints are not bad
- 0–49: not a fit

## Acceptance guidance

Set `accept=true` when:
- score >= 70
- AND no deal-breakers apply
- AND location_ok is not "no"
- AND compensation rule is not clearly violated

## Output rules

- Output **valid JSON only** (no markdown).
- Be concise.