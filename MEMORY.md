# JobHarvest — Project Memory

_Read this before making any changes._

## Current Status

- **Phase 1 complete** — Foundation is built and committed.
- GitHub: https://github.com/robertalford/JobHrvest
- Next: Phase 2 — Core Crawling (Scrapy + Playwright integration, Celery wiring)

## Architecture Decisions

- **Colima** (not Docker Desktop) is the container runtime. All `docker` commands target the Colima daemon.
- **Async SQLAlchemy** (`asyncpg`) for FastAPI endpoints; sync connection for Alembic migrations.
- **Domain blocklist** is enforced in `backend/app/crawlers/domain_blocklist.py` with a hardcoded emergency set (SEEK, Jora, Jobstreet, JobsDB) plus a DB-loaded set. Checked before EVERY HTTP request.
- **ATS extractors** prefer JSON API over HTML scraping (Greenhouse boards API, Lever postings API).
- **Market config** is stored in the `markets` DB table — AU is active, US/UK/NZ/SG are inactive stubs.
- **Aggregator sites** (Indeed, LinkedIn, Glassdoor, CareerOne, Adzuna) are **link-discovery-only** — we follow outbound links to company sites, never scrape job content from the aggregator.
- Frontend uses **Tailwind CSS** with brand green `#0e8136`. Clean, data-dense operations dashboard style.

## Key File Locations

- Domain blocklist: `backend/app/crawlers/domain_blocklist.py`
- ATS fingerprinter: `backend/app/crawlers/ats_fingerprinter.py`
- ATS extractors: `backend/app/extractors/ats_extractors.py`
- Career page discoverer: `backend/app/crawlers/career_page_discoverer.py`
- Job extractor: `backend/app/crawlers/job_extractor.py`
- Celery tasks: `backend/app/tasks/crawl_tasks.py`
- Seed data: `backend/scripts/seed.py`
- DB models: `backend/app/models/`
- Alembic migration: `backend/alembic/versions/0001_initial_schema.py`

## Seed Data Summary

- 57 Australian companies across: mining (BHP, Rio Tinto, Fortescue), banking (CBA, ANZ, Westpac, NAB, Macquarie), tech (Canva, Atlassian, Xero, SafetyCulture, CultureAmp), healthcare, education, government, professional services, retail
- ATS coverage: Greenhouse (many AU tech co's), Lever, Workday, BambooHR, iCIMS, Taleo, SmartRecruiters, Ashby, Jobvite, PageUp
- Blocked: seek.com.au, jora.com, au.jora.com, jobstreet.com (all variants), jobsdb.com (all variants)
- Aggregators (link-discovery): Indeed AU, LinkedIn Jobs, Glassdoor AU, CareerOne, Adzuna AU
