# Rubric: Kyle job search (v4 – broadened)

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

- 7 years professional experience. Primary skills: **Unity, C#, JavaScript, React**.
- Worked in **educational software** (HMH Math 180 – widely used in US schools), **mobile games** (Funkitron – $20M+ revenue title), and **children's games** (Workinman – Nickelodeon, Hasbro, LEGO).
- **Product engineering lead** for Brain Arcade, a math game suite used in American public schools.
- **Strong people skills** – smart, helpful, great communicator. Excels at bridging technology and customers.
- Also has: C++, Lua, Java, iOS development, AWS Cloud Practitioner cert, Agile/Scrum.
- BS in Software Engineering (Game Design & Development) from RIT.

## Job categories that fit Kyle

### Tier 1 – Strong fit (score 80–95)
- **Unity/C# game developer** – core experience
- **C# developer / .NET developer** (client-side or full-app; NOT backend-only)
- **Game developer / Game programmer / Game designer**
- **Front-end developer** (JavaScript/React/UI) – has meaningful React and UI experience

### Tier 2 – Good fit (score 70–85)
- **Implementation Specialist / Consultant** – configure SaaS products, train customers, data imports, go-lives
- **Client Onboarding Specialist** – deploy tech at customer sites, manage expectations
- **Customer Success Manager** (technical-leaning) – problem-solving, coordinating with engineers
- **Technical Account Manager** (associate/mid-level, supporting not quota-driven)
- **Technical Support Engineer / Product Support** (Tier 2/3) – debugging, troubleshooting, customer-facing
- **Application Support Analyst** – supporting internal/external applications, light configuration
- **Solutions Engineer / Technical Solutions Engineer** (post-sales, associate/mid-level)
- **EdTech roles** – his domain from 5+ years at HMH

### Not a fit (score 0–49)
- Backend-only / database / infrastructure / DevOps-primary
- Pure sales (quota-carrying), marketing, HR, recruiting with no tech component
- Art/artist/visual-design roles (Kyle is a programmer)
- Healthcare, teaching, agriculture, food service, manual labor
- VR/XR/AR-only roles
- Android SDK / native Android-only development

## Location constraints

- Candidate is near **Lexington, MA**.
- If job is **Remote** (or explicitly "Remote OK"): location_ok = "yes".
- If job is **Hybrid/On-site**:
  - location_ok = "yes" only if the office is plausibly within a **~45-minute commute of Lexington, MA**.
  - This includes most of eastern/central Massachusetts (e.g. Boston, Cambridge, Waltham, Framingham, Worcester, Northborough, Lowell, etc.).
  - otherwise location_ok = "no".
- **Location is a binary pass/fail.** Do NOT penalize score for commute distance within the 45-minute radius.
- If the location text is ambiguous: location_ok = "unknown" (do NOT auto-disqualify; note ambiguity in reasons).

## Compensation rule

- Prefer cash comp **>= $60,000/year**.
- If the posting explicitly shows max < $60k **AND** has no equity/rev-share upside, set `accept=false`.
- If salary is missing/unclear, do NOT auto-disqualify; most postings omit salary.

## Deal-breakers / automatic disqualifiers

If any of these are clearly true, set `accept=false` and include the reason in `red_flags`:

- **location_ok = "no"** – always a deal-breaker.
- **Art/artist/visual-design roles** (e.g. "Technical Artist," "3D Artist," "VFX Artist"). Kyle is a programmer, not an artist.
- Backend-only or infrastructure-primary (most responsibilities are server/database/DevOps).
- VR/XR/AR focus REQUIRED (optional mention is OK).
- Android SDK / native Android game development REQUIRED.
- Explicit visa-targeting ("OPT/CPT/H1B-only").
- Non-tech roles (healthcare, teaching, agriculture, food service, manual labor) or obvious scams.
- Requires 10+ years experience explicitly (Kyle has 7).
- Heavy travel (50%+ travel required).
- Unpaid internship or paid internship under $60k annualized.

## Scoring guidance

- 85–100: excellent match. Unity/C# game dev, remote or MA, clear responsibilities, credible company.
- 75–84: strong match. C#/.NET dev, game dev, implementation/onboarding, customer success, technical support — location works, no red flags.
- 70–74: good match, worth applying. Solid fit with minor concerns.
- 50–69: borderline. Some skill or location mismatch.
- 0–49: not a fit.

## Acceptance guidance

Set `accept=true` when:
- score >= 70
- AND no deal-breakers apply
- AND location_ok is not "no"
- AND compensation rule is not clearly violated

## Additional scoring factors

- **Work-life balance**: Prefer standard business hours. Penalize roles with required on-call, weekend work, or heavy overtime.
- **Seniority**: Kyle has 7 years. Mid-level is ideal. Entry-level OK if interesting. Senior OK if requirements aren't extreme. Principal/Staff/Director/VP is too senior — score lower.
- **People orientation**: Implementation, customer success, and support roles leverage Kyle's strong people skills. Score these positively when they involve meaningful technology.
- **EdTech bonus**: Kyle's 5 years at HMH in educational software is directly transferable. EdTech roles get a small boost.

## Output rules

- Output **valid JSON only** (no markdown).
- Be concise.
