# JobHarvest

An intelligent job listing crawler, extractor, and aggregation engine. Crawls company career pages directly, extracts structured job data with high accuracy using a multi-method pipeline, and presents everything in a web-based operations dashboard.

**Primary market:** Australia (AU) — with multi-market extensibility built in from day one.

## Architecture

```
JobHarvest
├── backend/          FastAPI + Celery + SQLAlchemy + Scrapy/Playwright
├── frontend/         React 18 + TypeScript + Vite + Tailwind CSS
├── docker/           Caddyfile and Docker configs
└── storage/          Raw HTML snapshots and screenshots
```

### Pipeline Stages

1. ATS Fingerprinting (Greenhouse, Lever, Workday, BambooHR, iCIMS, Taleo, SmartRecruiters, Ashby, Jobvite, JazzHR)
2. Career Page Discovery (heuristic + LLM classification + ATS shortcut)
3. Job Listing Identification (schema.org, ATS APIs, repeating block detection, LLM)
4. Job Detail Extraction + Normalization (location, salary, skills)
5. Change Detection + Continuous Crawling

### App Structure (3 compartmentalised features)

The dashboard is split into three top-level sections, shown as cards on the post-login landing page:

- **Site Config** (`/site-config/*`) — champion/challenger model that takes a domain and produces the career-page URL plus CSS/XPath selectors for each baseline extraction field. Includes a **Bulk Domain Processor** standalone tool that accepts a CSV of domains and returns a CSV with selectors filled where model confidence is high, aligned to the production import schema. *Always enabled.*
- **Extraction** (`/extraction/*`) — scheduled scraping that uses the site configs to harvest jobs. Gated by `VITE_FEATURE_EXTRACTION`.
- **Domain Discovery** (`/discovery/*`) — crawls the web to find new in-scope company domains to feed into Site Config. Gated by `VITE_FEATURE_DISCOVERY`.

Both Extraction and Discovery are disabled by default while the Site Config model is being optimised. Set the corresponding env var to `true` and rebuild the frontend to re-enable.

### Hard-Blocked Sites (enforced at crawler level)

SEEK, Jora, Jobstreet, and JobsDB are completely off-limits and blocked at the HTTP request level.

## Prerequisites

- [Colima](https://github.com/abiosoft/colima) (container runtime)
- Docker + Docker Compose

## Quick Start

```bash
# Install and start Colima (macOS) — on Linux, Docker Engine is enough
brew install colima docker docker-compose
colima start --cpu 4 --memory 8 --disk 60

# Clone and start
git clone https://github.com/robertalford/JobHrvest
cd JobHrvest
git lfs install                      # LFS-tracked files: `database/jobharvest_latest.dump`
cp .env.example .env
make up
make db-migrate                      # apply Alembic schema
make models-restore                  # restore champion/challenger state + history (see below)
make db-seed                         # seed reference data (markets, seed companies, etc.)
```

Dashboard: http://localhost  
API docs: http://localhost:8000/api/v1/docs

### Server / production deploy

The default `docker-compose.yml` is **dev-only** (Caddy on :8080, Vite HMR). Server deploys MUST use `docker-compose.server.yml`:

```bash
docker compose -f docker-compose.server.yml up -d --build
docker compose -f docker-compose.server.yml exec api alembic upgrade head
make models-restore                  # same step on the server
```

### Champion/Challenger state on a fresh clone

The champion/challenger models, A/B history, experiments, metric snapshots, GOLD holdout fixtures, and Codex iteration memory are **committed** into the repo at `database/models_snapshot.sql` (plain SQL, diff-friendly, no Git LFS). Every auto-improve iteration regenerates and commits this file so a clone is always aligned to the current live champion.

- **Restore:** `make models-restore` wraps `psql < database/models_snapshot.sql` (TRUNCATE + INSERT under a transaction) and copies `database/auto_improve_memory.json` / `_history.json` into `storage/` where the running system reads them.
- **Play library restore:** `make models-restore` also rehydrates `storage/play_library/` from `database/play_library.json` when that snapshot exists, so prompt retrieval keeps working on a fresh clone.
- **Manual re-snapshot:** `make models-snapshot` regenerates, commits, and pushes on demand (the auto-improve daemon does this automatically at the end of every iteration).
- **Full-DB disaster-recovery backup** is a separate artefact at `database/jobharvest_latest.dump` (Git LFS, replaced hourly by cron). Use that if you also need jobs/companies/leads, not just model state.

## Common Commands

```bash
make up              # Start all services
make down            # Stop all services
make logs            # Tail logs
make db-migrate      # Run migrations
make db-seed         # Seed database
make models-restore  # Restore champion/challenger state from database/models_snapshot.sql
make models-snapshot # Manually dump + commit + push current model state
make crawl-trigger   # Trigger full crawl cycle
make health          # Check system health
make shell-api       # Shell into API container
```

## Tech Stack

Backend: Python 3.12, FastAPI, Celery, SQLAlchemy, Scrapy, Playwright, Ollama (llama3.1:8b), scikit-learn  
Frontend: React 18, TypeScript, Vite, Tailwind CSS, Recharts  
Database: PostgreSQL 16, Redis 7  
Infrastructure: Colima, Docker Compose, Caddy

## Implementation Phases

- [x] Phase 1: Foundation — project structure, Docker, database schema, FastAPI, React shell
- [ ] Phase 2: Core Crawling — ATS fingerprinting, Scrapy+Playwright, Celery tasks
- [ ] Phase 3: Extraction Pipeline — schema.org, ATS APIs, LLM extraction, cross-validation
- [ ] Phase 4: Intelligence Layer — LLM classification, template learning, location/salary parsing
- [ ] Phase 5: Full Frontend dashboard
- [ ] Phase 6: Scheduling, change detection, job lifecycle
- [ ] Phase 7: Advanced — sklearn classifier, self-discovery, anti-bot improvements

## Auto-Improve Status

- **Live champion: `v6.9`** — `backend/app/crawlers/tiered_extractor_v69.py` + `backend/app/crawlers/career_page_finder_v69.py`. Crowned 2026-04-14 after re-scoring the full iteration history with the objective capped composite formula.
- **Benchmark composite:** 85.4 / 100 (discovery 100, quality extraction 100, volume accuracy 96.2, field completeness 45.3) on 179 sites (129 fixed regression + 50 exploration).
- **Scoring:** the composite is weighted 20% discovery + 30% quality extraction + 25% field completeness + 25% volume accuracy, each axis capped at 100. Promotion requires beating the champion's composite, ≥60% regression accuracy, and zero regressions on champion-passing sites.
- **Next hotfix candidate:** `v6.10` (`backend/app/crawlers/tiered_extractor_v610.py`) is a deliberately minimal `v6.9 + DetailEnricher` shipment to test whether bounded detail-page enrichment closes most of the field-completeness gap without perturbing the other axes.
- **Signal v2:** the auto-improve prompt now carries structured site diff packages, recent rejection post-mortems, a 15-fixture smoke gate (`>=12/15`), and AST-based challenger linting before A/B.
- **Evo search scaffold:** when `EVO_ENABLED=1`, the daemon shells out to `python -m scripts.evo_cycle`, which uses diff-grounded mutation prompts, SEARCH/REPLACE application, and persisted `storage/evo/*.json` metrics. Current metrics are exposed at `/api/v1/ml-models/evo/metrics`.
- Later iteration files (`v7.x`–`v10.x`) remain on disk for historical reference but are not registered as champions — the Models page and `ml_models` table were reset on 2026-04-14 to a single-source-of-truth: v6.9.
