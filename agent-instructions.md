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

## ML Model Auto-Improve System

### Architecture

The job extraction system uses a champion/challenger model:

- **Career Page Finder** (`CareerPageFinderVXX`): Discovers career page URLs from a domain
- **Tiered Extractor** (`TieredExtractorVXX`): Extracts structured jobs from HTML

Current live champion is tracked in DB (`SELECT name FROM ml_models WHERE status = 'live'`). Latest iteration files are the highest-numbered `v6X` files.

### Key Files
- `tiered_extractor_v16.py` — Stable base. DO NOT MODIFY.
- `tiered_extractor_v60.py` — v6.0 consolidated reference. DO NOT MODIFY.
- `tiered_extractor_v6X.py` — Latest iteration (highest number is current)
- `career_page_finder_v26.py` — Proven discovery. DO NOT MODIFY.
- `career_page_finder_v60.py` — v6.0 consolidated finder
- `career_page_finder_v6X.py` — Latest finder iteration
- `storage/auto_improve_memory.json` — Iteration history and learnings (READ before, UPDATE after)
- `new-prompt.md` — Auto-improve agent prompt (for Codex/Claude Agent SDK)

### Quality Standards

A valid extracted job MUST have:
1. **Real job title** — NOT a nav label, section heading, department name, or CMS artifact
2. **Unique detail URL** — NOT the listing page itself
3. **Core fields from actual page data** (never inferred): title, source_url, location_raw, description
4. **Type 1 errors (false positives) are CRITICAL** — quality over quantity, always

### Historical Accuracy
- v1.6: 66% (376 lines) — baseline
- v2.6: 82% (1,102 lines) — peak before consolidation
- v3.7-v5.2: 50-68% (3,000+ lines) — regression from complexity
- v6.0: 80% raw / 85.7% quality-adjusted (900 lines) — current best
- v7.0: 32% — major regression from permissive linked-card title fallback; avoid broad short-title allowances tied only to URL shape
- v7.1: precision reset + platform extractors (SuccessFactors table, Homerun state) to recover coverage without widening Type-1 risk
- v7.4: added focused ATS/feed recovery (PageUp listing rows + pagination, Recruitee `/o/` paths, `jobs.json` shell feeds) while tightening login-label Type-1 rejection
- v9.0: added bounded progressive pagination sequencing (fills sparse `?pp`/`/page/` gaps), multilingual AWSM title recovery, and linked-card editorial-label rejection (`Career Guide`) with compact role recovery
- v10.2: improved local deterministic extraction quality in v100 via split-table CTA-row recovery, numeric query-id detail URL handling, and Unicode-safe multilingual title validation (benchmark rerun pending)
- v10.3: preserved JSON state scripts in v100 truncation (`application/json`, `__NEXT_DATA__`) and added metadata-rich local extraction for list-group/span-card/split-heading-CTA layouts to improve field completeness on recoverable static pages

---

## Codex / Auto-Improve Agent Instructions

When running as an auto-improve agent (Codex or Claude Agent SDK), refer to:
- `new-prompt.md` — The detailed prompt for the auto-improvement loop
- `storage/auto_improve_memory.json` — Must be read before and updated after each iteration

### Key Rules for Auto-Improve
- **NEVER add single-site fixes** — every change must help 3+ sites
- **Inherit from TieredExtractorV16** (extractors) and **CareerPageFinderV26** (finders)
- **NEVER build inheritance chains deeper than 1 level**
- **Quality over quantity** — 10 real jobs > 50 fake ones
- **Prefer dedicated ATS extractors** over heuristic fallback chains

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
