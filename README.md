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
