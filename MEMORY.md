# JobHarvest — Project Memory

_Read this before making any changes._

## Current Status

- **Phase 1 complete** — Foundation is built and committed.
- GitHub: https://github.com/robertalford/JobHrvest
- **Champion/challenger ML infrastructure landed (2026-04-14)** — registry, GOLD holdout, promotion gates, drift, ATS quarantine, latency budget, Ollama-backed failure analysis, orchestrator. 33 new tests, full suite 135 green. **Migration 0023 not yet applied**; needs `alembic upgrade head` against running stack.
- **3-section app redesign landed (2026-04-14)** — landing page at `/` now shows 3 cards (Site Config, Extraction, Discovery). Only Site Config is enabled; the other two are feature-flagged off (`VITE_FEATURE_EXTRACTION`, `VITE_FEATURE_DISCOVERY`). Sidebar is URL-aware and renders only the active section's nav. Legacy top-level URLs redirect into their new section-scoped paths.
- **Bulk Domain Processor (standalone Champion run) added** — new page at `/site-config/bulk-process` uploads a CSV of domains and downloads a CSV with selector columns aligned to `TARGET_FIELDS` (title, location_raw, employment_type, salary_raw, department, description, requirements, benefits, date_posted). Selectors are emitted only when model confidence ≥ threshold (default 0.8). Backend endpoint: `POST /api/v1/bulk-domain-process/run`. Orchestration calls into a stub (`_run_champion_for_domain`) that needs wiring to the real `SiteStructureExtractor` path — next increment.
- Next: build a manually-verified GOLD holdout (run `scripts/build_gold_holdout.py`), then re-evaluate the existing TF-IDF classifier on it to establish the *real* baseline before crowning a champion.

## Champion/Challenger ML Loop

- **Why the existing TF-IDF F1=0.9963 is misleading** — labels come from `quality_score` rules, so the model is just learning the rules. Treat it as an unverified baseline until re-evaluated against the GOLD holdout.
- **Ground truth lives in `gold_holdout_*` tables** materialised from `lead_imports` (script: `backend/scripts/build_gold_holdout.py`). Sets are FROZEN once built — to revise, mint a new `name` (e.g. `au_baseline_v2`).
- **Split-by-domain is mandatory.** `app.ml.champion_challenger.domain_splitter.split_by_domain` enforces no domain crosses train/val/test. `assert_holdout_isolation` is a hard guard called at the start of every training run.
- **Promotion gates require multi-metric wins + statistical significance** — see `app.ml.champion_challenger.promotion`. Default: at least 2 of {f1, recall, job_coverage_rate, false_positive_rate↓} must improve, AND McNemar p<0.05, AND latency p95 within budget. A single-metric "win" never promotes.
- **One champion per model_name enforced at the DB level** via partial unique index `ix_model_versions_one_champion_per_name`.
- **LLM-suggested ATS patterns are quarantined** — `proposed → shadow → active` with strict shadow-mode thresholds (≥25 matches, ≤10% failure rate, ≥24h observation window). Never goes straight to production.
- **Pseudo-labels go to training, uncertainty samples go to the human review queue.** `select_uncertain` / `stratified_uncertain` route boundary cases to `review_feedback` so each human label has the most marginal value.
- **Failure analysis uses local Ollama** (`OLLAMA_MODEL`), not the Claude API — keeps the loop offline-capable and free per iteration.
- **Drift monitor**: PSI ≥0.25 = significant, gate retraining on it (not on calendar). Baselines stored in `drift_baselines`; rolled up by feature_name.
- **Latency budget**: per-page p95 must stay under `latency_budget_ms` (default 200ms). Raw observations live in Redis; hourly rollup → `inference_metrics_hourly` table.
- All modules under `backend/app/ml/champion_challenger/`. Orchestrator: `orchestrator.ChampionChallengerOrchestrator.run_experiment`.

## Architecture Decisions

- **App structure is 3-compartmentalised** — `/site-config/*`, `/extraction/*`, `/discovery/*`. Section metadata + feature flags live in `frontend/src/lib/sections.ts` (single source of truth for nav, route paths, card metadata). `Sidebar` reads the active section from `useLocation()` and filters accordingly. Add a new page by adding a `NavEntry` to its section's `nav` array and a matching `<Route>` in `App.tsx`. Don't scatter nav config across components.
- **Bulk Domain Processor CSV schema is a contract** — column order in `CSV_OUTPUT_FIELDS` (`backend/app/services/bulk_domain_processor.py`) aligns to the production import schema; `selector_*` columns are derived from `TARGET_FIELDS` so they stay in sync with the extraction pipeline. Unit tests pin this — don't reorder columns without updating the external import side.
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
