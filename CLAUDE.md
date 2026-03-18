# JobHarvet – Claude Code Instructions

**Read this file before acting on any prompt. Read `MEMORY.md` for project context before making changes.**

## Project Overview

- Build a locally-hosted, full-stack application called JobHarvest — an intelligent job listing crawler, extractor, and aggregation engine. The system crawls company websites, discovers careers/jobs pages, identifies individual job listings, extracts structured job data with high accuracy, and stores everything in a structured database. It includes a web-based dashboard for monitoring, management, and analytics.

- Primary Goal: Build the most exhaustive, accurate, and up-to-date database of job listings by crawling company career pages directly.

- Critical Rule — Off-Limits Sites: The following brands/sites must NEVER be crawled, scraped, or visited in any capacity (not even for link discovery): Jora, SEEK, Jobstreet, JobsDB. These are completely off-limits — do not send any requests to their domains whatsoever. Implement a hard domain blocklist enforced at the crawler level that prevents any request to these domains or their subdomains.

- Critical Rule — Aggregator Link Discovery: Other aggregator sites (Indeed, Glassdoor, LinkedIn Jobs, ZipRecruiter, Monster, CareerBuilder, Reed, Totaljobs, etc.) must never have job data scraped FROM them. However, the system SHOULD follow outbound links from these sites to discover destination company career pages, then crawl those career pages directly. Indeed is particularly encouraged as a link discovery source — it has strong AU market coverage and should be a primary source for discovering Australian company career pages.
Target Market: The initial focus is the Australian (AU) market. Seed data, default search queries, aggregator link harvesting, location parsing, and salary parsing should all be AU-first. However, the system must be designed with multi-market extensibility from day one — the data model, configuration, and pipeline should support adding new markets (US, UK, EU, etc.) later via configuration rather than code changes. Implement a markets configuration system where each market defines: its aggregator sources and search queries, locale-specific salary/location parsing rules, seed company lists, and crawl priorities. When a new market is added, the system should automatically configure and begin crawling based on that market's configuration.

- Build Philosophy: This project takes a "defense in depth" approach — for each major challenge (careers page discovery, job identification, field extraction), implement MULTIPLE complementary methods (heuristic, ML/AI, structural) that cross-validate each other. Accuracy and completeness are paramount.

- Tech Stack
  
  - Backend
     - Language: Python 3.12+
     - Framework: FastAPI (async, high-performance API layer)
     - Task Queue: Celery with Redis as broker and result backend
     -  Crawling: Scrapy + scrapy-playwright for JS-rendered pages
     -  Secondary Crawler: crawl4ai as an alternative extraction-oriented crawler
     - Browser Automation: Playwright (for JS-heavy sites, infinite scroll, "Load More"  buttons)
     - HTML Parsing: BeautifulSoup4, lxml, html2text, markdownify
     - Structured Data Extraction: extruct (JSON-LD, Microdata, RDFa, OpenGraph)
     - LLM Integration: Ollama (local) running Llama 3.1 8B (or whatever the latest small-but-capable model is at build time — research this)
     - Structured LLM Output: instructor library (Pydantic-based structured extraction from  LLMs)
     - ML/NLP: scikit-learn, sentence-transformers (for embeddings/classification)
     - HTTP Fingerprinting: curl_cffi (for TLS fingerprint mimicry on protected sites)
     - Scheduling: Celery Beat for periodic tasks
     - Pipeline Orchestration: Prefect (for monitoring complex workflows)

  - Frontend
     - Framework: React 18+ with TypeScript
     - Build Tool: Vite
     - UI Library: shadcn/ui + Tailwind CSS
     - Charting: Recharts
     - State Management: TanStack Query (React Query) for server state
     - Routing: React Router v6

 - Database
     - Primary Database: PostgreSQL 16+ (structured job data, site configurations, crawl metadata)
     - Cache/Queue: Redis (task queue, crawl rate limiting, page content hashing, caching)
     - Object Storage: Local filesystem with structured directories (raw HTML snapshots, screenshots) — design the storage interface so it could be swapped to MinIO/S3 later

 - Infrastructure (All Local/Docker via Colima)
     - Container Runtime: Colima (NOT Docker Desktop) — use colima start --cpu 4 --memory 8 --disk 60 or similar as the container runtime. All Docker and Docker Compose commands run against the Colima daemon. Include setup instructions for Colima in the project README.
     - Containerization: Docker Compose for the full stack
     - Reverse Proxy: Caddy or Traefik (for local HTTPS and routing)
     - Ollama: Running as a Docker service for local LLM inference

---

## Role & Permissions

You are a senior/principal software engineer. Translate simply stated business requirements & detailted technical required into efficient, modern system design and implementation. Act autonomously — do not ask for approval on actions you have permission to take.

- Full permission to make file changes and run terminal commands (including `sudo`)
- Do not ask for approval before taking actions you are permitted to take

## Code Style

- Lean implementation — minimal code changes to achieve objectives
- Clear, well-written code with appropriate comments

## UI & Design System

Clean minimal interface, card-based layout, neutral white/grey palette with blue accent, system UI fonts, high readability.

### Brand Colours

| Role | Hex | Usage |
|---|---|---|
| Primary | `#0e8136)` | Main brand — buttons, links, key UI accents |

## Development Philosophy — BDD / TDD

**Every change follows a Behaviour-Driven Development (BDD) and Test-Driven Development (TDD) cycle. No exceptions.**

This is not optional. It is the single most important process rule in this file.

---

### Step 1 — Understand the user's intent (BDD framing)

Before writing a single line of code or test, frame the requirement from the end user's perspective:

- **Who** is the user? (small business owner, employee, admin)
- **What** are they trying to achieve? (their goal, not the technical implementation)
- **Why** does it matter to them? (the business value)
- **How** will they interact with it? (the UX path — button click, form submit, navigation)
- **What does success look like** from their perspective?

Write this as a plain-English scenario before anything else:

```
As a [user type]
I want to [action]
So that [outcome/value]

Scenario: [happy path]
  Given [precondition]
  When  [action]
  Then  [observable result]

Scenario: [error/edge case]
  Given [precondition]
  When  [invalid action or edge case]
  Then  [expected safe/informative outcome]
```

If the requirement is unclear after this exercise — ask for clarification before proceeding.

---

### Step 2 — Write tests first (TDD)

Write the tests **before** writing any implementation code. Tests define the success criteria.

#### Unit / integration tests (Vitest)
Write tests co-located with the source file:
- Service functions: test each public function with normal inputs, edge cases, and invalid inputs
- Server actions: test the action's return value and DB side-effects
- Utility functions: test all branches
- API route handlers: test request validation, auth, and response shape

#### E2E tests (Playwright)
Update the relevant test file before implementing:
- Add a test for every new page, button, and form
- Add a test for every new user-facing error state
- Add a test for the full happy-path flow end-to-end
- Scaffold pages need only a "loads without error" test

**Run the tests before implementing — they must fail.** A test that passes before the feature is built is not testing anything.

---

### Step 3 — Implement the minimum code to pass the tests

Build the feature guided by the failing tests. Implement only what is needed to make the tests pass:

- No gold-plating, no speculative abstractions
- No features the tests don't cover
- Follow existing patterns — check nearby files first
- Keep the user's intent (Step 1) visible throughout: if the implementation would make the task harder for the user, reconsider

---

### Step 4 — Run tests and verify green

All tests must pass. Do not commit until they do.

---

### Step 5 — Reflect and iterate

After tests pass, critically re-examine the full picture:

1. **Re-read the original request.** Does the implementation actually deliver what the user wanted?
2. **Re-read the BDD scenarios.** Does the UI/flow match the described experience?
3. **Re-read the tests.** Do the success criteria capture the right things, or did you inadvertently test the wrong thing?
4. **Consider alternatives.** Is there a simpler, more intuitive, or more performant way to achieve the same outcome?

If any answer is "no" or "maybe not" — iterate. Revise the tests if the criteria were wrong, revise the implementation if the approach was suboptimal, and run the cycle again. Iteration is expected and encouraged.

Only commit once you are confident the implementation genuinely serves the user's intent.

---

### TDD quick reference

| Phase | Action | Expected state |
|---|---|---|
| Red | Write failing tests | Tests fail — feature not yet built |
| Green | Implement minimum code | Tests pass |
| Reflect | Re-examine intent + design | Identify improvements |
| Refactor | Improve without breaking | Tests still pass |
| Commit | Push to development | tests all green |

---

### When to deviate

The only acceptable reasons to skip a test before implementation:

- Pure copy/content changes (no logic)
- Trivial one-liner config changes with no branching logic
- Hotfixes where a failing production system takes priority — but write the test immediately after the fix

In all other cases: **tests first.**

## UAT Testing — User Acceptance Tests

UAT tests measure whether real user goals can be achieved, not whether specific UI elements exist. They are **outcome-focused** and scored on three dimensions.

### Scoring dimensions

| Dimension | What it measures | How scored |
|---|---|---|
| **Goal achieved** | Did the user complete the objective? | ✅ Pass / ❌ Fail |
| **Ease** | Steps required (lower = easier) | Playwright step count |
| **Speed** | Time to complete (lower = faster) | `Date.now()` delta (ms) |
| **Discoverability** | Could a first-time user find it? | Custom assertion on primary nav visibility |

## Workflow

### For every new feature or change — follow the BDD/TDD cycle

See **Development Philosophy — BDD / TDD** above. The steps below expand on that cycle in the context of this codebase.

### 1. Frame the request (BDD)
1. Read `MEMORY.md` for project context
2. If fixing a bug, read `BUG-FIXES.md` first for prior context
3. Write a plain-English user scenario (As a / I want / So that / Given-When-Then)
4. Identify all files that will change — if more than 3, outline the plan first
5. Check for existing utilities, components, and patterns before creating new ones

### 2. Write tests first (red phase)
1. Write or update **unit tests** for all new/changed service functions, actions, utilities
2. Write or update **E2E tests** 
3. Run tests — confirm they **fail** (they should, feature isn't built yet)

### 3. Implement (green phase)
1. Build only what is needed to make the failing tests pass
2. Keep the user's goal (Step 1) in view throughout — if the implementation would make the task harder for the user, stop and reconsider
3. Follow existing patterns (naming, file structure, error handling, soft-delete, PII encryption)

### 4. Verify (run the full quality gate)
All must be green before proceeding.

### 5. Reflect and iterate
- Re-read the original request — does the implementation deliver what the user actually wanted?
- Re-read the BDD scenario — does the UX match?
- Is there a simpler or more intuitive approach?
- If any answer is uncertain — iterate before committing

### 6. Commit and push
1. **Commit** with a conventional commit message (`feat:`, `fix:`, `refactor:`, `docs:`, `test:`, `chore:`)
2. **Push** to the active branch on GitHub
3. **Verify** deployment health (e.g.`{"status":"ok"}`)
4. **Update records** (see below)

If no active git repo/branch exists, prompt the user to initialise one.

### Commit Style
- Conventional commits: `feat:`, `fix:`, `refactor:`, `docs:`, `test:`, `chore:`
- Atomic — one logical change per commit
- Messages explain *why*, not just *what*
- Never commit `.env` files, `node_modules/`, or build artifacts

## Records — Update After EVERY Task

| File | What to record |
|---|---|
| `CLAUDE.md` | Important project context useful for future development, including new approaches/patterns adopted |
| `CHANGE-HISTORY.md` | Every prompt received (with date/time) + summary of changes made |
| `MEMORY.md` | Project-level milestones, architectural decisions, stable-state fixes |
| `BUG-FIXES.md` | All bugs reported or discovered + summary of fix applied |
| `README.md` | Project overview for GitHub users — update as the project evolves |