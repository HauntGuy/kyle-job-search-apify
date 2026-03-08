# Rubric: Kyle job search (v5)

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
- `salary_extracted` (string: salary/compensation info if found anywhere in the posting, e.g. "$90k-$120k" or "Competitive + equity"; empty string if none found)
- `company_url` (string: the company's homepage URL if found anywhere in the posting, e.g. "https://example.com"; empty string if not found)
- `role` (array of strings — classify using one or more labels: "Programmer" for Tier 1 developer/engineer roles; "EdTech" for Tier 2 EdTech roles; "App Support" for Tier 2 Application Support roles; "Customer Success" for any other Tier 2 role such as implementation specialist, CSM, TAM, solutions engineer, sales engineer, onboarding, technical support, etc. A job may have multiple labels if it fits multiple categories — e.g. an EdTech implementation role would be ["EdTech", "Customer Success"]. For jobs scoring below 70, use your best judgment on category.)

## Candidate summary

- 7 years professional experience focused on **Unity + C#**, mostly **client-side gameplay/UI/tools**, mobile/casual games.
- Some **C++** and **Lua** experience; open to learning more.
- Unity **2D preferred**, Unity **3D OK** if not "lead/architect/primary technical owner of a large 3D system."
- Strong at **customer-facing work**: demos, onboarding, support, stakeholder communication.
- Not a fit for **backend-primary** or **full-stack-primary** roles.
- Not a fit for **React-heavy** web front-end roles.
- Not a fit for **Android SDK / native Android game** roles (unless explicitly optional).
- Not a fit for **C# backend** roles whose primary stack is ASP.NET Core, Entity Framework, Web API, or similar server-side .NET frameworks — even if the title says "C# Developer."

### Unity is a general-purpose framework

**Unity is not just for games.** Unity is used across many industries including:
- **EdTech** (interactive simulations, virtual labs, training platforms)
- **Architecture / real estate** (3D visualization, virtual walkthroughs)
- **Healthcare** (medical simulation, surgical training)
- **Automotive** (HMI design, digital twins, configurators)
- **Film / animation** (real-time rendering, virtual production)
- **Manufacturing / industrial** (digital twins, training simulations)
- **Retail** (AR product visualization, interactive kiosks)

When a job posting mentions Unity, do NOT assume it is a game development role. Evaluate the actual job responsibilities.

### Tiered role categories

**Tier 1 — Programmer roles** (strongest fit, highest scores):
- Unity Developer / Engineer (any industry — games, EdTech, simulation, visualization, etc.)
- C# client-side developer (WPF, MAUI, Avalonia, desktop apps — NOT ASP.NET backend)
- Gameplay Engineer / Programmer
- UI Programmer (game/interactive media)
- Tools Engineer (game/tools side)
- Technical Designer (with significant scripting/programming)
- Sales Engineer or Solutions Engineer at a game engine, game tools, or interactive media company

**Tier 2 — Adjacent roles** (good fit, slightly lower scores):
- **EdTech roles**: roles at educational technology companies where Unity/C#/game development experience is a meaningful advantage, even if not strictly required. Includes: curriculum developer, learning experience designer, educational content developer, or similar roles at companies building interactive/simulation-based learning products.
- **Application Support Analyst / Application Specialist**: roles focused on supporting, configuring, or troubleshooting a software product for customers — especially at SaaS or EdTech companies. These are deeply technical support roles, NOT general phone/email help desk support.
- **Customer Success / Implementation / Onboarding**: Customer Success Manager, Technical Account Manager, Implementation Specialist, Implementation Consultant, Implementation Manager, Onboarding Specialist, Client Success Manager — especially at SaaS or EdTech companies.
- **Solutions Engineer / Sales Engineer**: pre-sales technical roles at software companies, especially where technical demos, POCs, or integrations are core responsibilities.
- **Technical Support Engineer**: deeply technical support roles where the candidate would troubleshoot, debug, or configure complex software products. NOT general help-desk, phone-queue, or Tier 1 support roles.

## Location constraints

- Candidate is near **Lexington, MA**.
- If job is **Remote** (or explicitly "Remote OK"): location_ok = "yes".
- If job is **Hybrid/On-site**, use the exhaustive list below.

### Commutable towns (within ~45-minute drive of Lexington, MA)

The following towns/cities are commutable. If the job's office location is in one of these, set location_ok = "yes":

Acton, Andover, Arlington, Ashland, Ayer, Bedford, Belmont, Beverly, Billerica, Bolton, Boston, Boxborough, Braintree, Brookline, Burlington, Cambridge, Canton, Carlisle, Chelmsford, Chelsea, Concord, Danvers, Dedham, Dover, Dracut, Dunstable, Everett, Foxborough, Framingham, Grafton, Groton, Harvard, Holliston, Hopkinton, Hudson, Lawrence, Lexington, Lincoln, Littleton, Lowell, Lynn, Lynnfield, Malden, Marlborough, Maynard, Medfield, Medford, Medway, Melrose, Methuen, Milford, Millis, Milton, Natick, Needham, Newton, North Andover, North Reading, Northborough, Norwood, Peabody, Pepperell, Quincy, Reading, Revere, Salem, Saugus, Sherborn, Shirley, Shrewsbury, Somerville, Southborough, Stoneham, Stow, Sudbury, Tewksbury, Townsend, Tyngsborough, Wakefield, Walpole, Waltham, Watertown, Wayland, Wellesley, Westborough, Westford, Weston, Wilmington, Winchester, Woburn, Worcester

If the office is in a Massachusetts town NOT on this list, set location_ok = "no".
If the office is in another US state (and not remote), set location_ok = "no".

**Location is a binary pass/fail.** A commutable job (10 minutes away) and a further-but-still-commutable job (40 minutes away) should receive **the same score** for location. Do NOT penalize score for commute distance within the commutable zone.

If the location text is ambiguous (e.g. "Lexington" without a state, "Hybrid preferred but Remote OK", or a state abbreviation without a city):
- location_ok = "unknown" (do NOT auto-disqualify; note ambiguity in reasons).

## Compensation rule

- Prefer cash comp **>= $90,000/year**.
- If the posting explicitly shows max < $90k **AND** has no explicit equity/rev-share/commission upside, treat as a strong negative and usually `accept=false`.
- **If salary is missing or unclear, do NOT penalize the score at all.** Most job postings omit salary. Simply note "Salary not listed" in reasons and move on. Do not lower the score for missing salary information.

## Work-life balance

- Prefer **standard business hours** (roughly 8-to-5, Monday–Friday).
- **Frequent weekend travel** or extended on-call rotations are a strong negative.
- **Occasional travel** (e.g. quarterly onsite, annual conference, occasional customer visit) is fine.
- **Occasional Zoom/video calls** are fine and expected.
- If the posting mentions "crunch culture," mandatory overtime, or frequent weekend work, note in reasons and score lower.

## Deal-breakers / automatic disqualifiers

If any of these are clearly true, set `accept=false` and include the reason in `red_flags`:

- **Backend-heavy or full-stack-primary** (most responsibilities are server/backend/web app).
- **C# backend roles**: ASP.NET Core, Entity Framework, Web API, microservices, or server-side .NET as the primary focus. Even if the title says "C# Developer," if the job is primarily backend, it is NOT a fit.
- **React-heavy front-end** (React is a primary requirement and central to the role).
- **VR/XR/AR focus REQUIRED** (optional/minor mention is OK).
- **Android SDK / native Android game development REQUIRED.**
- **location_ok = "no"** (on-site/hybrid required outside commutable zone). This is always a deal-breaker.
- **Explicit visa-targeting** that excludes the candidate or is "OPT/CPT/H1B-only / non-immigrant visa people only."
- **Art/artist/visual-design roles** (e.g. "Technical Artist," "3D Artist," "Character Artist," "VFX Artist"). The candidate is a programmer, not an artist. Even if the title mentions Unity, artist roles are not a fit.
- **Military / defense / weapons** roles. Any job whose primary purpose is developing weapons systems, military combat simulations, or defense applications is a deal-breaker — even if it uses Unity. (General-purpose simulation or training that is not specifically weapons/combat-focused is OK.)
- **Non-software roles** (sales/recruiting-only, marketing, HR, etc.) or obvious scams.
- **General help-desk or Tier 1 phone support** roles. If the posting describes answering phones, handling ticket queues, or basic troubleshooting with no deep technical component, it is not a fit.

## Scoring guidance

- 85–100: extremely strong Tier 1 match (Unity/C# gameplay/UI/tools; clear responsibilities; remote or MA; credible)
- 75–84: strong match — either a great Tier 1 role with minor caveats, or an excellent Tier 2 match
- 70–74: good match, worth applying — solid Tier 2 roles, or Tier 1 roles with some uncertainty
- 50–69: borderline; only accept if the role is close to Unity/C# client-side and constraints are not bad
- 0–49: not a fit

**Tier 2 roles should generally score 70–80**, not higher, unless they are an exceptionally strong match (e.g. EdTech company building Unity-based simulations, seeking someone with exactly Kyle's background).

## Acceptance guidance

Set `accept=true` when:
- score >= 70
- AND no deal-breakers apply
- AND location_ok is not "no"
- AND compensation rule is not clearly violated

## Output rules

- Output **valid JSON only** (no markdown).
- Be concise.
