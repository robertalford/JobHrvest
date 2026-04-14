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
# Install and start Colima
brew install colima docker docker-compose
colima start --cpu 4 --memory 8 --disk 60

# Clone and start
git clone https://github.com/robertalford/JobHrvest
cd JobHrvest
cp .env.example .env
make up
make db-migrate
make db-seed
```

Dashboard: http://localhost  
API docs: http://localhost:8000/api/v1/docs

## Common Commands

```bash
make up              # Start all services
make down            # Stop all services
make logs            # Tail logs
make db-migrate      # Run migrations
make db-seed         # Seed database
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
- **Next improvement run** will start from v6.9 as the source and attempt to raise `field_completeness` (the one under-ceiling axis) without giving back quality or volume.
- Later iteration files (`v7.x`–`v10.x`) remain on disk for historical reference but are not registered as champions — the Models page and `ml_models` table were reset on 2026-04-14 to a single-source-of-truth: v6.9.
