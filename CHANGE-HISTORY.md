# Change History

---

## 2026-03-19

**Prompt:** Read the claude.md and initialise this project repo under a github repo for my account robertalford, call the new repo JobHrvest. Then read /Users/rob/Documents/JobHarvest/PROJECT-PROMPT.md and begin implementing this new app.

**Changes:**
- Created GitHub repo: https://github.com/robertalford/JobHrvest
- Initialised git repository
- Implemented Phase 1 — Foundation:
  - Project structure: monorepo with `/backend`, `/frontend`, `/docker`, `/storage`
  - `docker-compose.yml` — postgres:16, redis:7, ollama, api (FastAPI), celery-worker, celery-beat, frontend (React/nginx), caddy (reverse proxy)
  - Full PostgreSQL schema (SQLAlchemy models + Alembic migration `0001_initial_schema`):
    - markets, blocked_domains, aggregator_sources, companies, career_pages, jobs, job_tags, crawl_logs, site_templates, extraction_comparisons
    - Full-text search index on jobs(title, description)
  - FastAPI backend: health check, companies, career-pages, jobs, crawl, analytics, system endpoints
  - Celery tasks: crawl_company, crawl_career_page, full_crawl_cycle, scheduled_crawl_cycle
  - Domain blocklist module (SEEK, Jora, Jobstreet, JobsDB — hard-blocked)
  - ATS Fingerprinting Engine (Greenhouse, Lever, Workday, BambooHR, iCIMS, Taleo, SmartRecruiters, Ashby, Jobvite, JazzHR)
  - ATS-specific extractors: Greenhouse (JSON API + HTML), Lever (JSON API + HTML), Workday (HTML)
  - Career Page Discoverer: heuristic URL scoring + ATS shortcut
  - Job Extractor: schema.org/extruct, ATS extractors, repeating block detection
  - Seed script: AU market config, 57 Australian seed companies, aggregator sources, blocked domains
  - React frontend: Dashboard, Companies, Career Pages, Jobs, Crawl Monitor, Analytics, Settings
  - `.env.example`, `Makefile`, `README.md`, `.gitignore`
