# Rubric: Kyle job search (v14)

Evaluate a job posting for **Kyle Forgaard**. Return JSON with: `accept` (bool), `score` (0–100), `confidence` (0.0–1.0), `reason_short` (1 sentence), `reasons` (short bullets), `red_flags` (short bullets), `tags` (short tags), `salary_extracted` (string or ""), `company_url` (string or ""), `role` (array from: "Game Designer", "Programmer", "EdTech", "App Support", "Customer Success" — may combine). **`role` rules:** Use "Programmer" ONLY for roles where the primary job duty is writing code (Unity dev, C# dev, gameplay engineer). Solutions Engineer, Sales Engineer, Implementation roles, and Support roles are NOT "Programmer" — classify them as "Customer Success" or "App Support".

Jobs with bad locations have already been filtered out. Do not evaluate location.
Jobs with seniority titles (Senior, Sr., Lead, Manager, Principal, Director, Head, VP, Chief, Staff) have already been filtered out. You will not see them.

## Candidate

- **Game design** degree; 7 years in games. Game Designer roles are the best possible fit.
- 7 years **Unity + C#**, client-side gameplay/UI/tools, mobile/casual games.
- Some C++ and Lua; open to learning more.
- Unity 2D preferred; 3D OK if not lead/architect of a large 3D system.
- **Excels at customer-facing work**: demos, onboarding, support, stakeholder communication.
- **Entry-level for customer-facing roles.** Kyle has never held a customer success, sales engineering, technical support, or account management title. Reject jobs that explicitly require **3+ years** of experience in those specific roles. **DO NOT reject based on "2 years experience required" or "1+ years" — that is within the acceptable range.** Many companies treat 1–2 year requirements as a soft filter and will interview strong candidates with adjacent experience.
- **Domain-specific requirements are deal-breakers for Tier 3.** If a Tier 3 (adjacent) job requires industry/domain experience outside of software/technology (e.g., "10+ years in manufacturing," "fluent in Spanish," "2+ years MedTech experience," "CPA/CFA required"), reject it. This rule does NOT apply to Tier 1 or Tier 2 jobs — evaluate those normally based on game design and Unity/C# fit. General tech skills (SQL, APIs, SaaS, cloud) are NOT disqualifying for any tier — Kyle can learn those.
- NOT a fit for backend-primary, full-stack-primary, or React-heavy roles.
- NOT a fit for C# backend (ASP.NET Core, Entity Framework, Web API) even if titled "C# Developer."

Unity is not just for games — it's used in EdTech, architecture, healthcare, automotive, film, manufacturing, and retail.

## Role tiers

**Tier 1 — Game Designer** (best possible fit, score 95–100):
Game Designer, Level Designer, Systems Designer, Combat Designer, Economy Designer, Technical Game Designer. These are rare and highly competitive — always accept them.

**Internships:** Score internships at **max 75**, even for Tier 1/2 roles. Kyle is 33 years old with 7 years of professional experience — internships target recent college graduates and are low-probability leads. Accept them (they're worth tracking) but don't score them higher than 75.

**Tier 2 — Programmer** (strongest technical fit, score 85–95):
Unity Developer, C# client-side (WPF/MAUI/Avalonia), Gameplay Engineer, UI Programmer, Tools Engineer, Technical Designer with scripting.

**Tier 3 — Adjacent** (score 70–80, BUT only at technology/software companies):
- Sales Engineer, Solutions Engineer/Consultant — pre-sales technical roles with demos/POCs
- Implementation Specialist/Consultant — onboarding and configuring software
- Application Support — configuring/troubleshooting software products (NOT phone help desk)
- Technical Support Engineer — deep technical troubleshooting (NOT Tier 1 phone queue)
- EdTech roles where game dev experience adds value

**CRITICAL — "tech company" test for Tier 3:** Kyle wants to work for companies whose core business IS software/technology. A software company that serves dentists or farmers = GOOD (it's a tech company). A tractor manufacturer, hospital chain, bank, or retailer with an IT department = BAD (NOT a tech company). **If the employer is not a technology/software company, score 40–60 max regardless of title.**

Tier 3 at a real tech company with acceptable salary → score 70–80. Do not penalize these for lacking Unity/C# — Kyle's customer-facing skills make them strong fits.

## Compensation

- Prefer >= $60K/year. If max < $60K with no equity/commission upside → strong negative.
- **Missing salary = no penalty.** Most postings omit it.

## Work-life balance

- Prefer standard 8-to-5. Frequent weekend travel or mandatory crunch → strong negative.
- Occasional travel (quarterly onsite, conferences) is fine.

## Deal-breakers (accept=false)

- Backend-heavy / full-stack-primary / C# backend (ASP.NET/EF/Web API)
- React-heavy front-end
- VR/XR/AR required (optional mention OK)
- Android SDK / native Android game dev required
- Military / defense / weapons (general simulation OK)
- Art/artist/visual-design roles (Technical Artist, 3D Artist, etc.) — but NOT Game Designer (that's Tier 1)
- Non-software roles (pure sales, marketing, HR) or scams
- General help-desk / Tier 1 phone support
- Visa-targeting (OPT/CPT/H1B-only)
- Travel > 30% of time
- Requires 3+ years of experience in customer success, sales engineering, technical support, or account management
- Requires a graduate degree (Master's, PhD, MBA) — Kyle has a bachelor's in game design

## Scoring

- 95–100: Game Designer (Tier 1)
- 85–95: strong Tier 2 (Unity/C# client-side, credible tech company)
- 70–84: solid Tier 2 with caveats, OR Tier 3 at a real tech company
- 50–69: borderline — Tier 3 at a non-tech company, or weak fit
- 0–49: not a fit

**Repeat: The employer must be a technology/software company for Tier 3 to score 70+.** Implementation Specialist at HubSpot → 75. Implementation Specialist at John Deere → 50. The company's core business matters.

Output valid JSON only, no markdown. Be concise.
