# JobHarvest — Agent Instructions

> **IMPORTANT:** This is the canonical instruction file for ALL AI agents working on this project (Claude Code, Codex, Claude Agent SDK, etc.). Do NOT modify `CLAUDE.md` or `AGENTS.md` directly — they simply reference this file. All project rules, guidelines, approaches, and context MUST be maintained here. Update this file continuously as the project evolves.

**Read this file before acting on any prompt. Read `MEMORY.md` for project context before making changes.**

---

## Project Overview

Build a locally-hosted, full-stack application called **JobHarvest** — an intelligent job listing crawler, extractor, and aggregation engine. The system crawls company websites, discovers careers/jobs pages, identifies individual job listings, extracts structured job data with high accuracy, and stores everything in a structured database. It includes a web-based dashboard for monitoring, management, and analytics.

**Primary Goal:** Build the most exhaustive, accurate, and up-to-date database of job listings by crawling company career pages directly.

### Critical Rules

- **Off-Limits Sites:** The following brands/sites must NEVER be crawled, scraped, or visited in any capacity (not even for link discovery): **Jora, SEEK, Jobstreet, JobsDB**. These are completely off-limits — do not send any requests to their domains whatsoever. Implement a hard domain blocklist enforced at the crawler level.

- **Aggregator Link Discovery:** Other aggregator sites (Indeed, Glassdoor, LinkedIn Jobs, ZipRecruiter, Monster, CareerBuilder, Reed, Totaljobs, etc.) must never have job data scraped FROM them. However, the system SHOULD follow outbound links from these sites to discover destination company career pages, then crawl those career pages directly. Indeed is particularly encouraged as a link discovery source for the Australian market.

- **Target Market:** Initial focus is the Australian (AU) market. Seed data, default search queries, location/salary parsing should all be AU-first. The system must support multi-market extensibility from day one via configuration.

### Build Philosophy

"Defense in depth" — for each major challenge (careers page discovery, job identification, field extraction), implement MULTIPLE complementary methods (heuristic, ML/AI, structural) that cross-validate each other. Accuracy and completeness are paramount.

---

## Tech Stack

### Backend
- **Language:** Python 3.12+
- **Framework:** FastAPI (async)
- **Task Queue:** Celery with Redis broker
- **Crawling:** Scrapy + scrapy-playwright, crawl4ai
- **Browser Automation:** Playwright
- **HTML Parsing:** BeautifulSoup4, lxml, html2text, markdownify
- **Structured Data:** extruct (JSON-LD, Microdata, RDFa, OpenGraph)
- **LLM:** Ollama (local) running qwen2.5:3b
- **Structured LLM Output:** instructor (Pydantic-based)
- **HTTP Fingerprinting:** curl_cffi
- **Scheduling:** Celery Beat

### Frontend
- React 18+ with TypeScript, Vite, shadcn/ui + Tailwind CSS, Recharts, TanStack Query, React Router v6

### Database
- **Primary:** PostgreSQL 16+
- **Cache/Queue:** Redis
- **Object Storage:** Local filesystem (S3-swappable interface)

### Infrastructure
- **Container Runtime:** Docker Compose (via Colima, NOT Docker Desktop)
- **Reverse Proxy:** Caddy
- **LLM Service:** Ollama (Docker)

---

## Database Management

### Schema and Backups

| File | Purpose | Location |
|------|---------|----------|
| `database/create_db.sql` | Schema-only dump — DDL for all tables, indexes, constraints | Always up to date |
| `database/jobharvest_latest.dump` | Full compressed backup (pg_dump custom format) | Replaced hourly |
| `database/backup.sh` | Backup script run by cron | Runs every hour |

### Rules

1. **When you change database structure** (new tables, columns, indexes, migrations): immediately update `database/create_db.sql` by running:
   ```bash
   docker exec jobharvest-postgres pg_dump -U jobharvest -d jobharvest --schema-only --no-owner --no-privileges > database/create_db.sql
   ```

2. **Automated hourly backups** are configured via cron (`crontab -l` to verify). The backup replaces the single `database/jobharvest_latest.dump` file — no accumulation.

3. **To restore on a new machine:**
   ```bash
   # Create the database
   docker exec -i jobharvest-postgres createdb -U jobharvest jobharvest 2>/dev/null || true
   # Restore from backup
   docker exec -i jobharvest-postgres pg_restore -U jobharvest -d jobharvest --clean --if-exists < database/jobharvest_latest.dump
   ```

4. **Git LFS** is used for `.dump` files (configured in `.gitattributes`). Ensure `git lfs install` is run on any new clone.

### Connection Details
- **Host (from host):** localhost:5434
- **Host (from Docker):** postgres:5432
- **Database:** jobharvest
- **User:** jobharvest
- **Password:** See `.env` file

---

## Role & Permissions

You are a senior/principal software engineer. Translate simply stated business requirements & detailed technical requirements into efficient, modern system design and implementation. Act autonomously — do not ask for approval on actions you have permission to take.

- Full permission to make file changes and run terminal commands (including `sudo`)
- Do not ask for approval before taking actions you are permitted to take

## Code Style

- Lean implementation — minimal code changes to achieve objectives
- Clear, well-written code with appropriate comments

## UI & Design System

Clean minimal interface, card-based layout, neutral white/grey palette with green accent, system UI fonts, high readability.

| Role | Hex | Usage |
|------|-----|-------|
| Primary | `#0e8136` | Main brand — buttons, links, key UI accents |

---

## Development Philosophy — BDD / TDD

**Every change follows a Behaviour-Driven Development (BDD) and Test-Driven Development (TDD) cycle. No exceptions.**

### Step 1 — Understand the user's intent (BDD framing)

Before writing a single line of code or test, frame the requirement from the end user's perspective:

```
As a [user type]
I want to [action]
So that [outcome/value]

Scenario: [happy path]
  Given [precondition]
  When  [action]
  Then  [observable result]
```

If the requirement is unclear — ask for clarification before proceeding.

### Step 2 — Write tests first (TDD)

Write the tests **before** writing any implementation code.

- **Unit/integration tests (Vitest):** Co-located with source. Test public functions, edge cases, API route handlers.
- **E2E tests (Playwright):** Test every new page, button, form, error state, and happy-path flow.
- **Run tests before implementing — they must fail.**

### Step 3 — Implement minimum code to pass tests

No gold-plating. No speculative abstractions. Follow existing patterns.

### Step 4 — Run tests and verify green

All tests must pass before committing.

### Step 5 — Reflect and iterate

Re-read original request, BDD scenarios, and tests. Is there a simpler approach? Iterate if uncertain.

### When to deviate

Only skip tests for: pure copy/content changes, trivial config changes, or production hotfixes (write test immediately after).

---

## Site Config — Champion/Challenger Model & Auto-Improve

The **Site Config** section of the app (`/site-config/*`) is the home of the champion/challenger loop that learns, per-domain, how to discover the career page and extract structured job listings. The other two sections (Extraction, Domain Discovery) consume the site configs Site Config produces and are feature-flagged off until the model is performant.

### Two-Model Pipeline

| Model | Purpose | Latest iteration files |
|-------|---------|------------------------|
| **Career Page Finder** (`CareerPageFinderVXX`) | Given a domain + company name, find the career/jobs listing URL | `career_page_finder_v6X.py` / `v7X.py` / … |
| **Tiered Extractor** (`TieredExtractorVXX`) | Given a career page URL + HTML, extract structured job listings (title, source_url, location, salary, employment_type, description) | `tiered_extractor_v6X.py` / `v7X.py` / … |

Both use a priority cascade: parent v1.6 heuristic → structured data (JSON-LD, `__NEXT_DATA__`) → dedicated ATS extractors → DOM fallbacks.

### Current Champion (2026-04-14)

- **Live champion:** `v6.9` (`tiered_extractor_v69.py` + `career_page_finder_v69.py`)
- **Objective composite score:** `85.4 / 100` (best across the full history when re-scored with the capped objective formula)
- **Breakdown:** discovery 100 · quality extraction 100 · volume accuracy 96.2 · field completeness 45.3
- **Benchmark run:** 179 sites (129 fixed regression + 50 exploration), test run id `c1f3caac-ff49-44ea-bd51-b477b40a9d8b`
- **Why v6.9, not a later version:** many later iterations (v6.1, v6.2, v7.2, etc.) posted higher *raw* composite scores only because of counting artefacts (e.g. `field_completeness` > 100 from multi-valued field extraction). When each axis is capped at the objective ceiling of 100, v6.9 is the clear winner — and with quality_extraction = 100 and volume_accuracy = 96.2 it achieves that with genuine breadth, not by over-emitting.

The Models page was cleared on 2026-04-14 and v6.9 re-instated as the sole live champion so the next improvement run starts from the objectively best baseline we have data for.

### Objective Quality Criteria (the Composite Score)

**This is the single yardstick for promotion.** All four axes are in [0, 100] and the composite is a weighted average (see [`backend/app/api/v1/endpoints/ml_models.py`](backend/app/api/v1/endpoints/ml_models.py#L792) `_composite_score_standalone`):

| Axis | Weight | What it measures | Objective ceiling |
|------|--------|------------------|------------------|
| **Discovery Rate** | 20% | % of sites where the career page URL was found | 100% |
| **Quality Extraction Rate** | 30% | % of sites with real jobs extracted, minus any with a `quality_warning` — **penalises Type 1 false positives** | 100% |
| **Field Completeness** | 25% | Average fields populated per job, out of 6 (title, source_url, location_raw, salary_raw, employment_type, description) | 100% — any value above 100 is an extraction bug, not an improvement |
| **Volume Accuracy** | 25% | How closely model job count matches the Jobstream baseline — symmetric penalty for both under- and over-extraction (peak at ratio 1.0; penalty starts once ratio > 1.5) | 100% |

**Composite = 0.20·Discovery + 0.30·QualityExtraction + 0.25·FieldCompleteness + 0.25·VolumeAccuracy**

**Promotion gate** (enforced in `backend/app/tasks/ml_tasks.py`):
- Challenger composite > 0
- Challenger composite > champion composite (capped objective score)
- Regression accuracy ≥ 60% on the fixed regression subset
- **Zero regressions** — challenger must not miss any site the champion passed
- If all four pass → challenger promoted to `status='live'`, old champion demoted to `tested`

When auditing historical runs or comparing candidates, **always cap each axis at 100 before computing the composite**. Raw values above 100 indicate over-counting and should not be rewarded.

### Champion/Challenger Infrastructure (landed 2026-04-14)

Beyond the per-iteration A/B test above, the full champion/challenger hardening lives under [`backend/app/ml/champion_challenger/`](backend/app/ml/champion_challenger/):

- **`registry.py`** — thin async helpers around `model_versions`; partial unique index `ix_model_versions_one_champion_per_name` enforces **one live champion per model_name** at DB level.
- **`domain_splitter.py`** — hard guard: domains never cross train/val/test. Compound-TLD aware (`.com.au`, `.co.uk`, `.co.nz`, `.com.sg`). `assert_holdout_isolation` is called at the start of every training run.
- **`promotion.py`** — bootstrap CIs, exact-binomial McNemar for small-sample significance, multi-metric promotion gate. Default: ≥2 of {f1, recall, job_coverage_rate, false_positive_rate↓} must improve, McNemar p<0.05, latency p95 within budget. **A single-metric win never promotes.**
- **`drift_monitor.py`** — PSI on numeric (quantile-binned) and categorical features. PSI ≥ 0.25 = significant; retraining is gated on drift, not calendar.
- **`failure_analysis.py`** — **local Ollama** (`OLLAMA_MODEL`), NOT the Claude API — keeps the loop offline-capable and zero-cost per iteration.
- **`uncertainty.py`** — margin-based + stratified active sampling so the review queue isn't dominated by one ATS/market.
- **`ats_quarantine.py`** — LLM-suggested ATS selectors go `proposed → shadow → active`, never straight to production. Strict defaults: ≥25 matches, ≤10% failure rate, ≥24h observation window.
- **`latency_budget.py`** — Redis ZSET of per-inference observations; p50/p95/p99 rollups → `inference_metrics_hourly`. Default per-page p95 budget = 200 ms.
- **`holdout_builder.py`** / **`holdout_evaluator.py`** — frozen GOLD holdout sourced from `lead_imports`, stratified by ATS and market, rapidfuzz title matching with substring fallback, bootstrap CI on F1.
- **`orchestrator.py`** — `ChampionChallengerOrchestrator.run_experiment` ties registry → evaluator → McNemar → multi-metric gates → latency budget → atomic promotion (old champion retired + new crowned in one tx).

DB tables: `model_versions`, `gold_holdout_{sets,domains,snapshots,jobs}`, `experiments`, `metric_snapshots`, `ats_pattern_proposals`, `drift_baselines`, `inference_metrics_hourly` (migration `0026_champion_challenger_infra`).

**Why the existing TF-IDF classifier's F1=0.9963 is misleading:** its training labels are derived from `quality_score`, which is itself a rule set. The model is mimicking the rules, not learning ground truth. Treat it as an unverified baseline until re-evaluated against the GOLD holdout.

### The Improvement-Run Loop (what happens per iteration)

1. **A/B test the current champion** on the fixed regression suite (129 sites) + 50 exploration sites. Three phases per site:
   - **Baseline** — Jobstream hand-tuned selectors (the ceiling)
   - **Champion** — current live model extracts blindly
   - **Challenger** — candidate model extracts blindly
2. **Score each phase** with the 4-axis composite (capped) above.
3. **Codex auto-improve** reads failure patterns from the test run + `storage/auto_improve_memory.json` and generates the next `tiered_extractor_vXX.py` / `career_page_finder_vXX.py` iteration. Runs as a host-side daemon (triggered via `/storage/auto_improve_triggers/<model_id>.trigger` files; host-only because `codex` must run outside the container).
4. **Register** the new version in `ml_models` and wire it into `_FINDER_MAP` in both [`ml_models.py`](backend/app/api/v1/endpoints/ml_models.py) and [`ml_tasks.py`](backend/app/tasks/ml_tasks.py).
5. **Execute A/B test** → if the promotion gate passes, the challenger is atomically promoted to `live` and the next iteration repeats from step 1 with the new champion as the source. If it fails, the challenger is kept as `tested` and a new improvement run is spawned from the same champion.
6. **Standalone mode:** the same champion can be run against a user-uploaded CSV of domains via the **Bulk Domain Processor** (`/site-config/bulk-process`) — upload CSV → get CSV back with `selector_*` columns filled when `extraction_confidence ≥ threshold` (default 0.8).

### Auto-Improve Agent Rules (Codex / Claude Agent SDK)

When acting as the auto-improve agent, follow [`new-prompt.md`](new-prompt.md) and:

- **Always read `storage/auto_improve_memory.json` BEFORE designing, UPDATE it after implementing.**
- **Query the live champion dynamically** — do NOT hardcode version numbers. `SELECT name FROM ml_models WHERE status = 'live' AND model_type = 'tiered_extractor'`, then `ls -t backend/app/crawlers/tiered_extractor_v*.py | head -1`.
- **Inherit from the stable base only** — `TieredExtractorV16` (extractors) and `CareerPageFinderV26` (finders). NEVER build inheritance chains deeper than 1 level (your v → stable base, full stop).
- **NEVER add single-site fixes** — every change must help 3+ sites. Prefer platform-level fixes (e.g. a new Workday handler) over pattern-level fixes (e.g. a CSS selector for one site).
- **Quality over quantity** — 10 real jobs > 50 fake ones. Type 1 false positives are critical.
- **Don't weaken title/jobset validation** to fix false negatives — it always creates more false positives than it fixes.
- **Keep total added lines under 200 per iteration.** If you need more, your approach is too complex.

### Quality Standards for a Valid Job

A valid extracted job MUST have:
1. **Real job title** — NOT a nav label, section heading, department name, or CMS artifact.
2. **Unique detail URL** — NOT the listing page itself.
3. **Core fields from actual page data** (never inferred): title, source_url, location_raw, description.
4. **Clean description text** — no `\t`/`\n` clutter, HTML entities, or boilerplate (see "Description Quality" in `new-prompt.md`). Type-4 errors degrade downstream use.

### Key Files

- [`tiered_extractor_v16.py`](backend/app/crawlers/tiered_extractor_v16.py) — stable base. **DO NOT MODIFY.**
- [`tiered_extractor_v60.py`](backend/app/crawlers/tiered_extractor_v60.py) — v6.0 consolidated reference. **DO NOT MODIFY.**
- [`tiered_extractor_v69.py`](backend/app/crawlers/tiered_extractor_v69.py) — **current live champion.**
- [`tiered_extractor_vXX.py`](backend/app/crawlers/) — challenger iterations (highest-numbered file ≠ champion; champion is whichever has `status='live'` in DB).
- [`career_page_finder_v26.py`](backend/app/crawlers/career_page_finder_v26.py) — proven discovery. **DO NOT MODIFY.**
- [`career_page_finder_v69.py`](backend/app/crawlers/career_page_finder_v69.py) — finder paired with the current champion.
- [`storage/auto_improve_memory.json`](storage/auto_improve_memory.json) — iteration history + anti-patterns (READ before, UPDATE after).
- [`new-prompt.md`](new-prompt.md) — auto-improve agent prompt.
- [`backend/app/ml/champion_challenger/`](backend/app/ml/champion_challenger/) — registry, promotion, drift, ATS quarantine, latency, orchestrator (hardened loop).
- [`backend/scripts/build_gold_holdout.py`](backend/scripts/build_gold_holdout.py) — materialise a frozen GOLD holdout from `lead_imports`.

### Pre-Flight Checklist (before producing the next challenger)

1. Migration `0026_champion_challenger_infra` applied (`alembic upgrade head`).
2. GOLD holdout materialised: `python -m scripts.build_gold_holdout --name au_baseline_v1 --market AU --max-domains 100`.
3. Manually verify `gold_holdout_jobs` (one-time human-in-the-loop labelling).
4. Re-evaluate the existing TF-IDF classifier against the GOLD holdout to establish the *true* baseline.
5. Register v6.9 in `model_versions` via `registry.register_model_version` then `registry.crown_initial_champion` (so the hardened orchestrator sees the same champion the Models page does).
6. Only then start producing challengers and running `ChampionChallengerOrchestrator.run_experiment`.

### Historical Note (pre-v6.9 iterations, for context only)

- v1.6: 66% (376 lines) — baseline
- v2.6: 82% (1,102 lines) — peak before consolidation
- v3.7–v5.2: 50–68% (3,000+ lines) — regression from complexity
- v6.0: 80% raw / 85.7% quality-adjusted (900 lines) — earlier "current best"
- **v6.9: objective composite 85.4 — crowned champion 2026-04-14**
- v7.0–v10.5: many iterations posted apparent gains but either regressed on the capped composite or inflated `field_completeness` beyond the 100 ceiling. History cleared from Models page on 2026-04-14; all post-v6.9 work starts fresh from v6.9.

---

## Workflow

### For every new feature or change

1. **Frame the request (BDD):** Read MEMORY.md, write user scenario
2. **Write tests first (red phase)**
3. **Implement (green phase)**
4. **Verify (run quality gate)**
5. **Reflect and iterate**
6. **Commit and push**

### Commit Style
- Conventional commits: `feat:`, `fix:`, `refactor:`, `docs:`, `test:`, `chore:`
- Atomic — one logical change per commit
- Messages explain *why*, not just *what*
- Never commit `.env` files, `node_modules/`, or build artifacts

### Records — Update After EVERY Task

| File | What to record |
|------|----------------|
| `agent-instructions.md` | Important project context, rules, guidelines, approaches |
| `CHANGE-HISTORY.md` | Every prompt received + summary of changes made |
| `MEMORY.md` | Project milestones, architectural decisions |
| `BUG-FIXES.md` | All bugs reported or discovered + fix summary |
| `README.md` | Project overview for GitHub users |

**Do NOT modify `CLAUDE.md` or `AGENTS.md`** — they reference this file.

---

## UAT Testing — User Acceptance Tests

UAT tests are **outcome-focused** and scored on:

| Dimension | What it measures |
|-----------|-----------------|
| **Goal achieved** | Did the user complete the objective? |
| **Ease** | Steps required (lower = easier) |
| **Speed** | Time to complete (lower = faster) |
| **Discoverability** | Could a first-time user find it? |

---

## Crawl Intelligence (from production analysis)

### How Production Crawling Works

| Pipeline Pattern | % of crawlers | Meaning |
|------------------|---------------|---------|
| `url_opener` only | 52% | Simple: fetch page, parse HTML |
| `url_opener → sleeper` | 35% | Page needs JS rendering time (avg 6.7s) |
| `url_opener → link_navigator → sleeper` | 3% | Click navigation → wait for content |
| `url_opener → form → submit` | 0.4% | Fill search forms to trigger listing |
| `http_downloader` | 3% | Direct API/feed download (XML/JSON) |

### Key Insights

1. **40% of sites need 5-8 seconds of JS rendering time** — short Playwright waits are insufficient
2. **Cookie consent banners block content** (210+ crawlers)
3. **Direct API/feed access works for 3,500+ sites** — JSON-LD, REST APIs, RSS feeds
4. **Click interactions required** for 2,265 crawlers — search submit, tab navigation, load more
5. **Iframe embedding** — ATS widgets in `<iframe>`, must detect and switch context
6. **Form submission** — career pages with search forms needing empty submit to trigger listings
