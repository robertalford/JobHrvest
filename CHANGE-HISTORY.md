# Change History

---

## 2026-04-14 (session 14 — 3-section app redesign + Bulk Domain Processor)

**Prompt:**
Redesign the app so that after login the user lands on a page with 3 card links:
(1) **Site Config** — the champion/challenger model that takes a domain and outputs the site URLs plus CSS/XPath selectors for each baseline extraction field, (2) **Extraction** — scheduled scraping using the defined site configs, (3) **Domain Discovery** — crawling the web to find new in-scope domains. The second two should be toggled off for now while we focus on the Site Config model. Each card routes into a sub-app that shows only that section's menu items (Sites, Test Data, Models, etc. for Site Config). The champion model must also be usable standalone — upload a CSV of domains and download a CSV with the extraction-selector columns filled where confidence is high, aligned to the production import schema.

**Changes:**

Frontend — routing + layout:
- `frontend/src/App.tsx` — replaced flat routes with 3 section-prefixed route trees (`/site-config/*`, `/extraction/*`, `/discovery/*`); added `SectionGate` guard that renders a "paused" notice for disabled sections; added `LegacyRedirect` for pre-redesign bookmarks (e.g. `/companies` → `/extraction/companies`).
- `frontend/src/lib/sections.ts` — NEW: section definitions, feature flags (`VITE_FEATURE_EXTRACTION`, `VITE_FEATURE_DISCOVERY`), and section-scoped nav metadata consumed by the landing page + Sidebar. Single source of truth for section/nav config.
- `frontend/src/components/layout/Sidebar.tsx` — rewritten to be URL-aware: detects the active section from `pathname` and renders only that section's nav groups; the logo now links to `/` to return to the 3-card home; global nav (How To, System Health) stays visible everywhere.

Frontend — new pages:
- `frontend/src/components/pages/SectionLanding.tsx` — NEW: post-login landing, one card per section with tagline and "Disabled" badge for off sections.
- `frontend/src/components/pages/SectionDisabled.tsx` — NEW: deep-link-safe "paused" notice with instructions to flip the feature flag.
- `frontend/src/components/pages/BulkDomainProcessor.tsx` — NEW: CSV upload + confidence-threshold slider + result download; shows the output column schema fetched from the backend so users know what the production-import-compatible CSV contains.
- `frontend/src/components/pages/Models.tsx`, `TestData.tsx` — NEW: skeleton pages for the Site Config section (model registry UI + gold-holdout browser to follow).

Backend — bulk domain processor:
- `backend/app/services/bulk_domain_processor.py` — NEW: pure CSV parse/build functions + async `process_domains()` orchestrator stub. The `CSV_OUTPUT_FIELDS` list is derived from `TARGET_FIELDS` in `app/extractors/template_learner.py`, so the output schema can't drift from the extraction pipeline's baseline fields. Selectors are blanked for rows below the configured confidence threshold so the output is safe to import wholesale.
- `backend/app/api/v1/endpoints/bulk_domain_process.py` — NEW: `GET /schema` (column list + default threshold) and `POST /run` (multipart CSV upload → CSV download). Auth-gated via the existing v1 router dependency.
- `backend/app/api/v1/router.py` — registers the new router under `/bulk-domain-process`.
- `backend/tests/unit/test_bulk_domain_processor.py` — NEW: 13 tests covering CSV input normalisation (URL/scheme stripping, dedup, header handling), output schema contract (columns match `TARGET_FIELDS`), and confidence-gated selector emission. All passing.

**Verification:**
- Backend: `python3 -m pytest tests/unit/test_bulk_domain_processor.py` → 13 passed.
- Frontend: `tsc -b --noEmit` clean; `eslint` on all new/changed files clean; `vite build` succeeds; dev server boots without warnings.
- Browser walkthrough of the landing + Site Config sub-app is pending (no browser automation available in this session) — should be sanity-checked manually before merging.

**Scope notes / follow-ups:**
- `process_domains()` currently returns `pending`/"champion model serving not yet wired" placeholder rows. The CSV round-trip (upload → parse → run → download) works end-to-end; wiring the actual `ChampionChallengerOrchestrator` + `SiteStructureExtractor` path is the next increment.
- Most Site Config sub-pages (Sites, Runs, Excluded Sites) re-use existing components from the pre-redesign layout — no behavioural changes, only URL moves.
- Extraction + Discovery sections are fully wired but gated off. Flip `VITE_FEATURE_EXTRACTION=true` / `VITE_FEATURE_DISCOVERY=true` and rebuild to re-enable.

---

## 2026-04-14 (session 13 — Champion/challenger ML infrastructure)

**Prompts:**
- Review `job-crawler-technical-deep-dive.md` (third-party design doc) and recommend what to put in place BEFORE starting up the champion/challenger model and auto-improve loop.
- Implement all of the recommended fixes, hardening items, and opportunities now.

**Headline finding identified during review:**
The existing TF-IDF/LR description classifier reports F1=0.9963 — but its training labels are derived from `quality_score`, which is itself a rule set. The model is mimicking the rules, not learning ground truth. Promotion decisions against this label oracle are unfalsifiable.

**Changes:**

Backend — DB:
- `backend/alembic/versions/0023_champion_challenger_infra.py` — NEW: 10 tables to support a hard-gated champion/challenger loop
  - `model_versions` — registered model artifacts + lineage; partial unique index enforces one champion per model_name
  - `gold_holdout_sets` / `gold_holdout_domains` / `gold_holdout_snapshots` / `gold_holdout_jobs` — frozen evaluation sets sourced from `lead_imports` (split off so domains are the holdout unit, jobs are per-domain ground truth)
  - `experiments` + `metric_snapshots` (stratum-keyed, with bootstrap CI columns) — full audit trail of every champion-vs-challenger comparison
  - `ats_pattern_proposals` — quarantine for LLM-suggested ATS selectors (proposed → shadow → active)
  - `drift_baselines` — reference feature distributions for PSI-based drift detection
  - `inference_metrics_hourly` — rolled-up p50/p95/p99 latency + LLM escalation count per (model_version, hour)

Backend — SQLAlchemy ORM:
- `backend/app/models/champion_challenger.py` — NEW: ORM mappings for all 10 tables above
- `backend/app/models/__init__.py` — export the new model classes for Alembic discovery

Backend — ML modules (`backend/app/ml/champion_challenger/`):
- `__init__.py` — module overview
- `domain_splitter.py` — split-by-domain enforcement; AU/NZ/UK compound-TLD aware; `assert_holdout_isolation` hard guard against GOLD leakage
- `promotion.py` — bootstrap CIs, exact-binomial McNemar test (correct for small samples), multi-metric promotion gate with min_delta + p-value + min-metrics-won requirements
- `drift_monitor.py` — PSI on numeric (quantile-binned) and categorical features with negligible/moderate/significant classification
- `failure_analysis.py` — Ollama-backed (NOT Claude API per CLAUDE.md "use what's running") structured failure-pattern analyser with tolerant JSON parsing
- `uncertainty.py` — margin-based uncertainty + stratified active sampling for the review queue (so the human-review backlog isn't dominated by a single ATS or market)
- `ats_quarantine.py` — pure-functional state machine for proposed → shadow → active LLM-pattern promotion; strict defaults (≥25 matches, ≤10% failure rate, ≥24h window)
- `latency_budget.py` — Redis ZSET-based per-inference observation tracker + p50/p95/p99 percentile helpers + budget check that defers judgement on small samples
- `holdout_builder.py` — materialise a frozen GOLD holdout from `lead_imports` (snapshots HTML to disk; freezes the set on completion; idempotent on `name`)
- `holdout_evaluator.py` — stratified evaluator (overall + per-ATS + per-market); rapidfuzz title matching with substring fallback; bootstrap CI on F1
- `orchestrator.py` — `ChampionChallengerOrchestrator.run_experiment` ties registry → evaluator → McNemar → multi-metric gates → latency budget check → atomic promotion (retire old champion + crown new in one tx)
- `registry.py` — thin async helpers around `model_versions` (register, list, get_champion, crown_initial_champion bootstrap)

Backend — CLI:
- `backend/scripts/build_gold_holdout.py` — NEW: one-shot script to materialise a holdout set from `lead_imports`. Snapshots written under `/storage/gold_holdout/`. Manual follow-up required to populate `gold_holdout_jobs` (verification is intentionally human-in-the-loop).

Tests (BDD/TDD per CLAUDE.md):
- `tests/unit/test_domain_splitter.py` — 17 tests covering compound TLDs, no-leakage invariant, holdout-isolation guard
- `tests/unit/test_promotion.py` — 16 tests covering bootstrap CI, McNemar (incl. small-sample non-significance), multi-metric gate (promote/reject/inconclusive), lower-is-better metrics, min-delta noise filter
- `tests/unit/test_drift_monitor.py` — 9 tests covering PSI on numeric & categorical features with known distributions
- `tests/unit/test_uncertainty.py` — 9 tests covering margin uncertainty, top-K selection, per-stratum quotas
- `tests/unit/test_ats_quarantine.py` — 11 tests covering the full state machine (begin_shadow → record → evaluate → promote/reject/keep)
- `tests/unit/test_latency_budget.py` — 6 tests covering percentile correctness + budget-check edge cases
- `tests/unit/test_failure_analysis.py` — 9 tests covering case formatting + tolerant JSON parsing (fenced code, prose-wrapped JSON, garbage)
- `tests/unit/test_holdout_evaluator_helpers.py` — 9 tests covering stratified metric aggregation + fuzzy title matching
- `tests/unit/test_champion_challenger_imports.py` — 3 smoke tests guaranteeing every new module + ORM class imports cleanly

**Test results:** 135 tests passing (up from 33 prior). Migration 0023 parses cleanly; not yet applied to a running DB (stack was down).

**Pre-flight checklist before starting the loop:**
1. Apply migration 0023: `alembic upgrade head`
2. Run `python -m scripts.build_gold_holdout --name au_baseline_v1 --market AU --max-domains 100`
3. Manually verify the resulting `gold_holdout_jobs` (one-time human-in-the-loop labelling)
4. Re-evaluate the existing TF-IDF classifier against the GOLD holdout to establish the *true* baseline
5. Register the existing model in `model_versions` via `registry.register_model_version` then `registry.crown_initial_champion`
6. Only then start producing challengers and running `ChampionChallengerOrchestrator.run_experiment`

---

## 2026-03-20 (session 12 — Performance optimisations, menu restructure, unified Overview page)

**Prompts:**
- Continue from where you left off (performance optimisation work)
- Reorder Monitor Runs section: Overview, Discovery Runs, Company Config Runs, Site Config Runs, Site Crawling Runs
- Remove Home section; move Dashboard and Analytics into Prod Database section at top
- Combine Dashboard and Analytics into a single Overview page and menu item
- Audit page load times (>500ms), design and implement optimisations targeting <200ms
- Fix Overview page showing no data in Prod Database section

**Changes:**

Backend:
- `docker-compose.yml` — Workers 1-3: concurrency 8→32, prefetch-multiplier 2→1; added workers 4 and 5 (crawl queue only); Postgres: shared_buffers 128MB→512MB, work_mem 8MB→4MB, max_connections=300
- `.env` — Fixed CRAWL_RATE_LIMIT_SECONDS 2.0→0.5, CRAWL_TIMEOUT_SECONDS 30→20, CELERY_CONCURRENCY 4→8 (these were silently overriding config.py performance values)
- `backend/app/crawlers/http_client.py` — Added Redis-backed ETag/304 conditional request caching (7-day TTL); changed Playwright wait_until networkidle→domcontentloaded
- `backend/app/crawlers/job_extractor.py` — Handle 304 Not Modified response (refresh active jobs, return [])
- `backend/app/db/base.py` — Connection pool: pool_size 10→20, max_overflow 20→40, pool_recycle=3600
- `backend/alembic/versions/0022_api_perf_indexes.py` — NEW: Indexes on career_pages(created_at DESC), career_pages(company_id), partial index career_pages(created_at DESC) WHERE is_active=true, partial index jobs(company_id) WHERE is_active=true AND is_canonical=true; ANALYZE both tables
- `backend/app/api/v1/endpoints/analytics.py` — Added Redis caching (field_coverage 120s, trends 300s, quality_distribution 120s, quality_by_site 300s, market_breakdown 120s); fixed dashboard-snapshot asyncio.gather+shared-session bug (→sequential); added `/overview` unified endpoint running 7 sub-queries in parallel with separate AsyncSessionLocal sessions (safe parallel pattern) + 15s Redis cache
- `backend/app/api/v1/endpoints/jobs.py` — Added Redis caching to /stats endpoint (15s TTL)
- `backend/app/api/v1/endpoints/career_pages.py` — Added Redis caching to list endpoint (30s TTL)

Frontend:
- `frontend/src/components/layout/Sidebar.tsx` — Removed Home section; Dashboard+Analytics moved into Prod Database; Dashboard renamed Overview at route /; Monitor Runs reordered (Overview, Discovery Runs, Company Config Runs, Site Config Runs, Site Crawling Runs)
- `frontend/src/components/pages/Overview.tsx` — Rewrote to consume single `/analytics/overview` endpoint (was 8 separate calls); added skeleton loading state; refetchInterval 5000→30000
- `frontend/src/lib/api.ts` — Added getOverview() function
- `frontend/src/App.tsx` — Route / now uses Overview component; removed /analytics route

**Performance results (after):**
- Overview cold: ~600ms, warm: ~152ms (was 8 calls, 800ms+)
- Career Pages cold: 269ms, warm: 27ms
- Jobs cold: 217ms, warm: 24ms
- Companies cold: 98ms, warm: 21ms

**Key architectural decision — separate-session parallel pattern:**
SQLAlchemy AsyncSession is single-connection; asyncio.gather with a shared session causes IllegalStateChangeError. Solution: each sub-coroutine creates its own `async with AsyncSessionLocal() as s:` — separate sessions are safe to gather.

---

## 2026-03-19 (session 11 — Geocoder service, GeoNames seeding, frontend Geocoder page)

**Prompts:**
- Build a geocoder service with sub-geocoders per market (AU, NZ, SG, MY, HK, PH, ID, TH)
- L1=Country, L2=Region/State, L3=City, L4=Suburb/Town hierarchical structure with lat/lng centroids
- Seed from open-source GeoNames data; use classifier + LLM + web (Nominatim) as fallback resolvers
- Integrate into job crawl pipeline; retroactively geocode existing jobs
- Jobs without valid geo location are flagged (geo_resolved=false) and excluded from default view
- Create Geocoder admin page in Settings section

**Changes:**

Backend:
- `backend/alembic/versions/0021_geocoder.py` — NEW: Creates `geo_locations` table (hierarchical, L1-L4, with pg_trgm GIN indexes for fuzzy search), `geocode_cache` table (text→location cache with hit counting), adds `geo_location_id`, `geo_level`, `geo_confidence`, `geo_resolution_method`, `geo_resolved` to jobs
- `backend/app/models/geo_location.py` — NEW: SQLAlchemy models for GeoLocation (self-referential hierarchy via parent_id) and GeocodeCache
- `backend/app/models/__init__.py` — Added GeoLocation, GeocodeCache imports
- `backend/app/services/geocoder.py` — NEW: GeocoderService with: (1) cache lookup, (2) SQL exact match on name/ascii_name/alt_names, (3) pg_trgm fuzzy match (similarity threshold 0.35), (4) comma-separated term splitting (most specific first), (5) LLM extraction via Ollama, (6) cache result including failures. Remote-only text (WFH, remote, etc.) returns None. Module-level singleton `geocoder_service`.
- `backend/app/tasks/geocoder_tasks.py` — NEW: `geocoder.seed_geonames` task (downloads GeoNames country files for all 8 markets, creates L1 country records, L2 ADM1 regions, L3/L4 populated places in 500-record batches), `geocoder.geocode_new_jobs` beat task (every 2 min, geocodes up to 200 unresolved jobs), `geocoder.retro_geocode_jobs` bulk task (geocodes all geo_resolved=NULL jobs)
- `backend/app/tasks/celery_app.py` — Added `geocoder_tasks` to include list; added geocoder task routes; added `geocode-new-jobs` beat task (every 2 min, limit 200)
- `backend/app/tasks/crawl_tasks.py` — Added inline geocoding step after quality scoring in `crawl_company`; geocodes all newly crawled jobs (geo_resolved=NULL) immediately; uses `geocoder_service.geocode(db, loc_text, market_code)` then updates geo fields on Job objects
- `backend/app/api/v1/endpoints/geocoder.py` — NEW: REST API: GET / (paginated, search/level/market filters), GET /stats (counts by level/market, cache stats, jobs geocoding status), GET /cache (paginated cache browser), POST /test (test geocode), POST /seed (trigger seed task), POST /retro (trigger retro task)
- `backend/app/api/v1/router.py` — Registered geocoder router at /geocoder

Frontend:
- `frontend/src/components/pages/Geocoder.tsx` — NEW: Settings-section page with 8-card stats row (total/countries/regions/cities/suburbs/resolved/pending/cache rate), jobs geocoding progress bar, test geocoder input (shows full path + level + coords + method + confidence), Locations tab (paginated table with level badge, name, parent, market, coords, population, search/level/market filters), Cache tab (resolution method badges, confidence, use count)
- `frontend/src/App.tsx` — Added Geocoder import and /geocoder route
- `frontend/src/components/layout/Sidebar.tsx` — Added MapPin import and Geocoder nav item in Settings section

**Data:**
- Migration 0021 applied; pg_trgm extension enabled
- `geocoder.seed_geonames` triggered (task ID: 05f7854b) — downloads GeoNames for AU/NZ/SG/MY/HK/PH/ID/TH
- `geocoder.retro_geocode_jobs` triggered — will geocode all 15,935 existing jobs after seeding completes
- Beat task `geocode-new-jobs` registered — runs every 2 min to geocode newly crawled jobs

**Architecture:**
- `geo_resolved` on jobs: NULL=not yet attempted, TRUE=resolved, FALSE=tried and failed
- Jobs with geo_resolved=FALSE are flagged and excluded from default search results
- L3_MIN_POP per market: AU≥5K, NZ≥2K, SG=0 (city-state), MY≥3K, HK=0, PH≥5K, ID≥10K, TH≥5K
- GeoNames feature code mapping: ADM1→L2, PPLC/PPLA/PPLA2→L3, PPLX/PPLF/PPLS→L4, PPL→L3 or L4 by population

---

## 2026-03-19 (session 11 — continued: Pipeline cascade fix, queue routing)

**Prompts:**
- Investigate why 121 company_config completions produced zero site_config items
- Fix the cascade so company → sites → jobs flows automatically

**Root Causes Found & Fixed:**
1. `fix_company_sites` discovered career pages but never enqueued them into `site_config` queue — CASCADE WAS MISSING. Added `queue_manager.enqueue(db, "site_config", page.id)` after `extractor.extract(company)`.
2. `fix_site_structure` mapped site structure but never enqueued pages into `job_crawling` — SECOND CASCADE MISSING. Added `queue_manager.enqueue(db, "job_crawling", page.id)` after `extractor.extract(page)`.
3. Old `crawl.scheduled` beat task was flooding the `crawl` Redis queue with 278,870 stale `crawl.company` tasks, starving workers of capacity. Removed `scheduled-crawl-cycle` from beat_schedule.
4. `crawl.fix_site_structure` routed to `default` queue competing with `fix_company_sites`. Moved to `crawl` queue.

**Changes:**
- `backend/app/tasks/crawl_tasks.py` — cascade enqueues added to both fix_company_sites and fix_site_structure
- `backend/app/tasks/celery_app.py` — removed legacy beat entry; rerouted fix_site_structure to crawl queue

**Pipeline state after fix:** company_config→site_config→job_crawling all cascading correctly ✓

---

## 2026-03-19 (session 10 — Aggregator rewrite, market scope, discovery sources UI, domain import, queue/job reset)

**Prompts:**
- Confirm discovery run behaviour (headless browser, follows links)
- Switch harvester to Playwright, blank search, exhaust all pages via pagination
- Enforce unique-by-domain for companies, unique-by-URL for career pages
- Research and add exhaustive job aggregator list across target markets
- Remove UK/US, support only: AU, NZ, SG, MY, HK, PH, ID, TH
- Rewrite Discovery Sources UI to Companies/Sites/Jobs style
- Fix UI deployment (sidebar not updating) — restart frontend container
- Reset duplicate and poor-quality jobs for reprocessing
- Research exhaustive domain lists and import as companies (Tranco, Majestic, ASIC)
- Non-negotiable: always follow CLAUDE.md rules

**Changes:**

Backend:
- `backend/alembic/versions/0019_career_pages_url_unique.py` — Dedup 89 duplicate career_pages rows, add UNIQUE index on `career_pages.url`
- `backend/alembic/versions/0020_seed_aggregator_sources.py` — Add UNIQUE constraint on `aggregator_sources.name`; seed 36 aggregator sources across AU/NZ/SG/MY/HK/PH/ID/TH markets; reseed discovery queue
- `backend/app/crawlers/aggregator_harvester.py` — Complete rewrite: curl_cffi static fetcher, Playwright headless fetcher, BaseHarvester with full pagination (MAX_PAGES), early-stop, 1.5s polite delay; 14 harvester classes including SEA-specific (Glints, Hiredly, Kalibrr, Karir, JobThai); factory `get_harvester_for_source()` mapping 40+ source names; `ON CONFLICT (domain) DO NOTHING` dedup; blank search queries
- `backend/app/tasks/crawl_tasks.py` — Updated `harvest_aggregator_source` to use factory pattern; updated `company_config` crawl task to auto-enqueue `site_config` on career page discovery
- `backend/app/tasks/domain_import_tasks.py` — NEW: `import_tranco_domains` (Tranco Top 1M), `import_majestic_domains` (Majestic Million), `import_asic_companies` (ASIC AU company registry); all filter by market TLDs, batch-insert with `ON CONFLICT DO NOTHING`, auto-enqueue new companies
- `backend/app/tasks/celery_app.py` — Added `domain_import_tasks` to include list; added 3 domain_import task routes
- `backend/app/api/v1/endpoints/discovery_sources.py` — NEW: REST CRUD for aggregator_sources (GET paginated/filtered, POST, PUT, DELETE)
- `backend/app/api/v1/router.py` — Registered discovery_sources router

Frontend:
- `frontend/src/components/layout/Sidebar.tsx` — New structure: Home (Dashboard, Analytics), Prod Database (Link Discovery first), Monitor Runs (Overview first), Settings (Company Import, Live Markets), More (How To, About); renamed Settings→About, Run Settings→Settings, Lead Imports→Company Import, Markets→Live Markets
- `frontend/src/components/pages/MonitorRunsOverview.tsx` — NEW: overview page with 4 queue-type cards, progress bars, global summary, Run Now buttons, auto-refresh every 5s
- `frontend/src/components/pages/DiscoverySources.tsx` — Complete rewrite: paginated table, search/filter bar (market, active status), stats cards, inline edit/add, delete with confirm; uses new discovery-sources API
- `frontend/src/lib/api.ts` — Added discovery sources CRUD functions
- `frontend/src/App.tsx` — Added `/monitor-runs` route

Database (direct SQL):
- Removed UK and US aggregator sources; inserted 28 SEA+NZ sources; reseeded discovery queue with 36 items
- Reset 4,800 duplicate jobs (cleared canonical_job_id, set is_canonical=true)
- Reset 503 poor-quality jobs (cleared quality_score, quality_scored_at, quality fields)

**Memory saved:** `feedback_claude_md_compliance.md` — non-negotiable CLAUDE.md compliance rule

---

## 2026-03-19 (session 9 — Status fields, heuristic extractors, Celery tasks, Monitor Runs UI)

**Prompt:** "Read /Users/rob/Documents/JobHarvest/todo.md and build a comprehensive implementation plan to evolve our current application, to complete each item listed - with strict adherence. When ready, implement the plan."

**Changes:**

Backend:
- `backend/alembic/versions/0017_status_fields_and_seeding.py` — Migration adding `company_status` (4 values: ok/at_risk/no_sites_new/no_sites_broken) to companies, `site_status` (4 values: ok/at_risk/no_structure_new/no_structure_broken) to career_pages; seeds excluded_sites (ricebowl.my, seek.com.au, jobsdb.com, jobstreet.com, jora.com) and aggregator_sources (Indeed AU, LinkedIn); partial status indexes; derives initial values from existing data
- `backend/app/services/company_site_extractor.py` — New layered extractor: ATS fingerprint → heuristic BFS → TF-IDF classifier → 3B LLM → 8B LLM; updates company_status
- `backend/app/services/site_structure_extractor.py` — New layered extractor: extruct JSON-LD/Microdata → repeating block detector → learned selectors → 3B LLM → 8B LLM; saves site_templates; updates site_status
- `backend/app/tasks/crawl_tasks.py` — Added `fix_company_sites`, `fix_site_structure`, `fix_companies_batch`, `fix_sites_batch` Celery tasks; updated `harvest_aggregators` to create discovery CrawlLog entries
- `backend/app/tasks/celery_app.py` — Beat schedule: all 4 run types set to 2-hour intervals; added routes for new tasks; changed main crawl and discovery from 1h/6h to 2h/2h
- `backend/app/api/v1/endpoints/crawl.py` — Added `crawl_type` filter to history endpoint; added `POST /trigger/{run_type}` (with dual trailing-slash routes); added `GET/PUT /schedule-settings` backed by Redis
- `backend/app/api/v1/endpoints/review.py` — Fixed `::jsonb` → `CAST(:features AS JSONB)` SQL syntax; added trailing-slash route variants; fixed confirm to deactivate job

Frontend:
- `frontend/src/components/layout/Sidebar.tsx` — Renamed "Crawling" → "Run Settings"; added "Monitor Runs" section (Crawling Runs, Site Config Runs, Company Config Runs, Discovery Runs); "Crawl Monitor" → "Crawling Runs"; "Crawl Schedule" → "Scheduled Runs"
- `frontend/src/components/pages/RunLogsPage.tsx` — New generic run log component with paginated table + detail panel sidebar + trigger button
- `frontend/src/components/pages/SiteConfigRuns.tsx` — Site Config Runs monitor page
- `frontend/src/components/pages/CompanyConfigRuns.tsx` — Company Config Runs monitor page
- `frontend/src/components/pages/DiscoveryRuns.tsx` — Discovery Runs monitor page
- `frontend/src/components/pages/CrawlSchedule.tsx` — Fully redesigned as "Scheduled Runs" — 4 cards (Discovery, Company Config, Site Config, Job Crawling); each with enabled toggle, hourly interval field, edit/save/cancel pattern, next-run estimate
- `frontend/src/components/pages/settings-shared.tsx` — BadWords/ScamWords table redesigned with aligned market columns (✓ checkmarks per market per column) instead of badge bubbles
- `frontend/src/lib/api.ts` — Added `triggerRun`, `getScheduleSettings`, `updateScheduleSettings`; added `crawl_type` param to `getCrawlHistory`
- `frontend/src/App.tsx` — Added routes for 3 new monitor pages

Infrastructure:
- All 4 run types triggered immediately on session completion
- Verified DB migration 0017 complete (company_status/site_status seeded)
- Workers restarted to pick up new task code

---

## 2026-03-19 (session 8 — ML classifier Layer 4.5 + DB performance + loading UX)

**Prompt (session 7):** Build TF-IDF + Logistic Regression job description block classifier (Layer 4.5) to replace LLM inference for the bulk of extraction cases.
**Prompt (session 8):** "Investigate why loading the companies, sites or jobs page takes so long. Fix to < 500ms. Also remove the double-loading indicator (modal + inline 'Loading…' text)."

**Changes:**

ML Classifier (Layer 4.5):
- `backend/app/ml/__init__.py` — Created module init
- `backend/app/ml/description_classifier.py` — TF-IDF + LogisticRegression pipeline; FeatureUnion of word n-grams (1-2), char n-grams (3-5), 16 engineered features; singleton lazy-loader from `/storage/models/description_classifier.joblib`
- `backend/scripts/train_description_classifier.py` — Training script; F1=0.9963 on 5,507 examples
- `backend/app/extractors/description_extractor.py` — Added Layer 4.5 classifier between content-density (4) and LLM fast (6); LLM layers renumbered
- `backend/requirements.txt` — Added numpy, scikit-learn, scipy, joblib

Performance (DB + query):
- `backend/alembic/versions/0013_company_stats_goldtable.py` — `company_stats` gold table with triggers on `crawl_logs`, `career_pages`, `lead_imports`; trigram indexes on companies.name/domain, jobs.title
- `backend/alembic/versions/0014_companies_sort_index.py` — `sites_json` JSONB column in company_stats; `ix_companies_active_name` partial index; trigger updated to rebuild sites_json
- `backend/alembic/versions/0015_companies_name_full_index.py` — Full non-partial `ix_companies_name` index; ANALYZE
- `backend/alembic/versions/0016_jobs_sort_index.py` — `ix_jobs_active_canonical_seen` partial index on `jobs(first_seen_at DESC) WHERE is_active AND is_canonical`; jobs query drops from 1794ms to 5.7ms
- `backend/app/api/v1/endpoints/companies.py` — Rewrote list query to JOIN company_stats gold table; `pg_class.reltuples` for unfiltered count; Redis response caching (30s TTL) with cache invalidation on writes; `CAST(:param AS TYPE) IS NULL` pattern for asyncpg NULL parameter handling
- `backend/app/api/v1/endpoints/career_pages.py` — Rewrote `_SITES_SELECT` to JOIN company_stats; removed 4 correlated subqueries; unfiltered count uses reltuples; same CAST pattern
- `backend/app/api/v1/endpoints/jobs.py` — Added `pg_class.reltuples` for unfiltered count; Redis response caching (30s TTL)
- `docker-compose.yml` — Added postgres `command:` with planner cost tuning: `random_page_cost=1.1`, `effective_cache_size=512MB`, `work_mem=8MB`, `effective_io_concurrency=200`, `idle_in_transaction_session_timeout=30000`

Loading UX fix:
- `frontend/src/components/pages/Companies.tsx` — Removed inline "Loading…" row; table shows empty state only when not loading
- `frontend/src/components/pages/Jobs.tsx` — Same fix
- `frontend/src/components/pages/CareerPages.tsx` — Removed inline "Loading…" in header count + table body

**Results:**
- Companies: ~1800ms → 10-25ms (Redis cached), 400ms cold start
- Sites: ~400ms → 5-20ms
- Jobs: ~1800ms → 10-75ms (Redis cached), 5.7ms DB query with new index

---

## 2026-03-19 (session 6 — Export + search/filter consistency across Companies, Sites, Jobs)

**Prompt:** "Add the same 'export' button and functionality to the Companies, Sites and Jobs pages. Also add the same search and filter controls at the top (text search with submit button, filters row with structured dropdowns). Make the rest of the page consistent also — same style page heading, same style paginated table/list etc."

**Changes:**
- `backend/app/api/v1/endpoints/career_pages.py` — Refactored list endpoint to use `_sites_where`/`_sites_params` helpers and added new filter params (`page_type`, `discovery_method`, `is_primary`, `has_template`, `requires_js`). Extracted `_SITES_SELECT` constant and `_compute_expected_jobs` helper. Added `GET /export` endpoint (CSV) respecting all active filters.
- `backend/app/api/v1/endpoints/companies.py` — Added `GET /export` endpoint (CSV) with `search`, `ats_platform`, `is_active` filter params; added `Response` import.
- `frontend/src/lib/api.ts` — Updated `getCareerPages` to accept a generic params object (supports new filter fields). Added `exportCareerPages`, `exportCompanies`, `exportJobs` helpers (build query string and `window.open`).
- `frontend/src/components/pages/Jobs.tsx` — Full rewrite: consistent header (icon + title + subtitle + Export CSV button), submit-based search bar, filter row (Quality Band, Employment Type, Remote Type, Seniority Level dropdowns), company name + domain column (replacing truncated UUID), consistent paginated table with Prev/Next controls.
- `frontend/src/components/pages/Companies.tsx` — Added icon header, Export CSV button, filter row (ATS Platform, Active/Inactive dropdowns), updated query to use combined filterParams object.
- `frontend/src/components/pages/CareerPages.tsx` — Added Export CSV button, filter row (Page Type, Discovery Method, Primary, Has Template, Requires JS dropdowns + Active Only checkbox), updated query key and API call to use new filter params.

---

## 2026-03-19 (session 5 — Companies page: Sites + job count columns)

**Prompt:** "Update Companies page to include Sites column (list of URLs), Expected Jobs column (sum from sites), and Last Crawl Jobs column."

**Changes:**
- `backend/app/api/v1/endpoints/companies.py` — Rewrote `list_companies`: replaced Pydantic ORM query with a raw SQL query that aggregates career pages per company (JSON array of `{id, url, page_type, is_primary}`), computes `site_count`, `last_crawl_jobs` (most recent successful crawl log), and `expected_jobs` (priority chain: avg of last 3 crawls → last crawl → sum of lead import expected counts). Results sorted A–Z by name. Added pagination.
- `frontend/src/components/pages/Companies.tsx` — Added Sites column: shows site count badge + truncated URL list (2 visible + "N more" expand/collapse, primary site marked with a dot). Added Expected Jobs and Last Crawl Jobs columns. Added proper pagination. Added search submit on Enter. Used `align-top` on rows so multi-URL cells don't stretch single-URL peers.

---

## 2026-03-19 (session 4 — Sites page rebuild)

**Prompt:** "Update 'Career Pages' page to 'Sites', add view-details action to each row (modal with selectors), add Company column, Expected Job Count column, and Last Crawl Jobs column."

**Changes:**
- `backend/app/api/v1/endpoints/career_pages.py` — Rewrote list endpoint: paginated, searchable (company name or URL), JOINs `companies` for company name/domain, correlated subqueries for `last_crawl_jobs` and `avg_last_3_jobs`, fallback priority chain for `expected_jobs`. Added `GET /{page_id}/detail` endpoint returning full page metadata + active template (with all selectors) + 5 most recent crawl logs.
- `frontend/src/components/pages/CareerPages.tsx` — Rebuilt as "Sites" page: search/filter bar, pagination, Company column, Expected Jobs column (priority: avg3 → last → imported), Last Crawl Jobs column, Template column (accuracy badge), Details button per row. `SiteDetailModal` component shows page metadata cards, full selector table (12 known fields + unknown extras), raw JSON toggle, and recent crawl history timeline.
- `frontend/src/components/layout/Sidebar.tsx` — Renamed "Career Pages" → "Sites".
- `frontend/src/lib/api.ts` — Added `getCareerPages`, `getCareerPageDetail`, `recrawlCareerPage`.

---

## 2026-03-19 (session 3 — unify excluded sites / blocked domains)

**Prompt:** "Combine the 'excluded sites' and 'blocked domains' page and menu items as they do the same thing. Keep a single list, add SEEK.com.au, Jora.com, JobsDB.com and Jobstreet.com to it + all imported sites with disabled_state=true. Ensure crawl monitor correctly excludes these sites."

**Changes:**
- `alembic/versions/0012_unify_blocklist.py` — New migration: seeds SEEK, Jora, JobsDB, JobStreet into `excluded_sites`; migrates any rows from `blocked_domains_config`; drops `blocked_domains_config` and `blocked_domains` tables.
- `app/crawlers/domain_blocklist.py` — Added `refresh_from_db_async()`: loads all domains from `excluded_sites` into the runtime blocklist cache. Called at API startup and before each crawl cycle.
- `app/main.py` — Lifespan now calls `refresh_from_db_async()` at startup so the DB-backed blocklist is live immediately.
- `app/tasks/crawl_tasks.py` — `full_crawl_cycle` now (a) refreshes the blocklist from DB before scheduling and (b) filters out companies whose domain is in `excluded_sites` via SQL subquery.
- `app/api/v1/endpoints/excluded_sites.py` — Rewrote to accept JSON body on POST; added PUT for editing reason/company_name; POST and DELETE both call `refresh_from_db_async()` so changes are immediately enforced.
- `app/models/settings.py` — Removed `BlockedDomain` class (table dropped by migration).
- `app/models/__init__.py` — Removed `BlockedDomain` imports.
- `app/models/blocked_domain.py` — Deleted (was unused; table now gone).
- `app/api/v1/endpoints/settings.py` — Removed all `/settings/blocked-domains` endpoints.
- `scripts/seed.py` — Updated blocked-domain seeding to insert into `excluded_sites` instead of `blocked_domains`.
- `frontend/src/components/pages/ExcludedSites.tsx` — Added CRUD controls: "+ Add Domain" form (domain, company name, reason) and per-row delete button. Colour-coded reason badges.
- `frontend/src/components/pages/BlockedDomains.tsx` — Deleted.
- `frontend/src/components/layout/Sidebar.tsx` — Removed "Blocked Domains" nav item.
- `frontend/src/App.tsx` — Removed BlockedDomains import and route.
- `frontend/src/lib/api.ts` — Removed `getBlockedDomains`, `createBlockedDomain`, `updateBlockedDomain`, `deleteBlockedDomain`; updated `addExcludedSite` to use JSON body; added `updateExcludedSite`.

---

## 2026-03-19 (session 2 — system architecture gaps)

**Prompt:** "Have you implemented the full scope of the project-prompt document... Make a detailed implementation plan, and implement it."

**Changes:**
- `app/crawlers/job_extractor.py` — Wired `CrossValidator.merge()` into `_extract_from_page()`. Results from multiple extraction methods are now merged per job URL using trust-ranked field resolution instead of just keeping highest-confidence. Added `_merge_by_url()` method.
- `app/crawlers/aggregator_harvester.py` — Fixed LinkedIn harvester: now follows job detail links to extract real company domains and career URLs (previously left domain/career_url blank). Added `_extract_company_url_from_job_page()` and per-class `_upsert_company()`.
- `app/crawlers/career_page_discoverer.py` — Added `_detect_js_rendering_required()`: detects SPAs by checking visible text ratio, known ATS domains (Workday, iCIMS, Taleo), and framework markers. Sets `requires_js_rendering=True` on career pages automatically during BFS.
- `app/tasks/ml_tasks.py` — Added `llm_extract_page` Celery task (queue: `ml`): runs LLM extraction on pages that returned zero results from all other methods.
- `app/crawlers/job_extractor.py` — After zero extraction results, automatically queues `llm_extract_page` to the `ml` Celery queue.
- `app/core/markets.py` — Created market configuration system: `MarketConfig` dataclass with per-market aggregator configs, salary/location parsing rules, major cities, and seed domains. Covers AU (active), NZ, SG, MY, HK, PH, ID, TH (inactive, ready to enable).
- `alembic/versions/0010_seed_markets.py` — Migration seeds all 8 market rows into the `markets` table.
- `app/tasks/crawl_tasks.py` — Updated `harvest_aggregators` to use market config (queries, location, aggregator sources). Added `seed_market_companies` task for seeding initial companies from market config.
- `app/tasks/celery_app.py` — Updated beat schedule: market-driven harvesting, quality scoring backfill every 30 min, added `ml.llm_extract_page` route.

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
