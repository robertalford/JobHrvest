# JobHarvest — Project Memory

_Read this before making any changes._

## Current Status

- **Phase 1 complete** — Foundation is built and committed.
- GitHub: https://github.com/robertalford/JobHrvest
- **Champion/challenger ML infrastructure landed (2026-04-14)** — registry, GOLD holdout, promotion gates, drift, ATS quarantine, latency budget, Ollama-backed failure analysis, orchestrator. 33 new tests, full suite 135 green. Migration applied as `0026_champion_challenger_infra` (renumbered from `0023` to resolve a collision with the pre-existing `0023_app_users`).
- **3-section app redesign landed (2026-04-14)** — landing page at `/` now shows 3 cards (Site Config, Extraction, Discovery). Only Site Config is enabled; the other two are feature-flagged off (`VITE_FEATURE_EXTRACTION`, `VITE_FEATURE_DISCOVERY`). Sidebar is URL-aware and renders only the active section's nav. Legacy top-level URLs redirect into their new section-scoped paths (including `/test-data` → `/site-config/test-data` and `/models` → `/site-config/models`).
- **Bulk Domain Processor (standalone Champion run) added** — new page at `/site-config/bulk-process` uploads a CSV of domains and downloads a CSV with selector columns aligned to `TARGET_FIELDS` (title, location_raw, employment_type, salary_raw, department, description, requirements, benefits, date_posted). Selectors are emitted only when model confidence ≥ threshold (default 0.8). Backend endpoint: `POST /api/v1/bulk-domain-process/run`. Orchestration calls into a stub (`_run_champion_for_domain`) that needs wiring to the real `SiteStructureExtractor` path — next increment.
- **Models page history cleared + v6.9 crowned champion (2026-04-14)** — re-scored the full iteration history using the objective capped composite formula (each axis capped at 100 before weighting) and v6.9 was the clear winner at **85.4** (discovery 100, quality extraction 100, volume accuracy 96.2, field completeness 45.3). All 76 other model records, 94 non-winning test runs, and 285 improvement-run entries were deleted from the DB. v6.9 is now the sole `status='live'` model in `ml_models`, with a single benchmark test run retained (`c1f3caac-ff49-44ea-bd51-b477b40a9d8b`, 179 sites). The next improvement run starts from v6.9 as the source. Backup: `/tmp/jobharvest_models_backup_20260414_213403.sql`.
- **GOLD holdout seeded + v6.9 registered in `model_versions` (2026-04-14)** — `au_baseline_v1` frozen set built with 60 legitimate AU company career domains (Coles, Woolworths, NAB, ANZ, Westpac, Macquarie, Deloitte, PwC, Amazon, McDonald's, BAE, Stryker, + NSW/VIC/QLD government, etc.). 38 HTML snapshots captured under `/storage/gold_holdout/`; 22 domains blocked anti-bot or DNS-failed during snapshot fetch (acceptable loss — they're still in the domain list for future re-snap). `gold_holdout_jobs` intentionally empty pending human verification. v6.9 also registered in `model_versions` (id `41c7897c-9e61-41b7-b8cf-5cda7fc6b857`, `status='champion'`) so the hardened `ChampionChallengerOrchestrator` sees the same champion as the legacy Models page.
- **Auto-improve track** — file history still contains v10.x iterations in `backend/app/crawlers/` for reference, but they are no longer registered as champions. The next Codex run will produce a challenger *from* v6.9, not from v10.5.
- Next: build a manually-verified GOLD holdout (run `scripts/build_gold_holdout.py`), then re-evaluate v6.9 against it to establish the *real* baseline (the DB-tracked composite is from a live-site A/B test; the GOLD holdout gives a frozen, domain-isolated measurement).
- **Universality-first auto-improve redesign landed (2026-04-14)** — closes the observed regression-cycling where fix-A-break-B alternated each iteration. Promotion gate now enforces 7 conditions (was 4): legacy composite/regression checks *plus* per-ATS cluster gate (no gate-eligible cluster may drop more than 2 pts), ever-passed ratchet (monotonic set; challenger cannot silently lose a site any prior version passed, ±15% slack), and oscillation detector (sites flipping ≥2× in the last 5 runs block promotion if the challenger is currently failing them). Migration `0028_universality_gate` adds `ever_passed_sites` + `site_result_history`. Codex prompt rewritten to lead with anonymised pattern cards for ≥3-site clusters instead of per-site named failures. New endpoint `GET /api/v1/ml-models/{id}/stratum-report` + `backend/scripts/backfill_ever_passed.py` for UI observability + historical replay. Stratified scorer always caps each axis at 100 to defeat the known `field_completeness > 100` extraction-bug inflation. 14 new tests, full suite green.

## Champion/Challenger ML Loop

- **Live champion:** v6.9 (crowned 2026-04-14). Files: [`backend/app/crawlers/tiered_extractor_v69.py`](backend/app/crawlers/tiered_extractor_v69.py) + [`backend/app/crawlers/career_page_finder_v69.py`](backend/app/crawlers/career_page_finder_v69.py). Benchmark composite **85.4** (capped objective formula) on 179 sites.
- **Promotion gate has 7 conditions** (redesigned 2026-04-14). See `agent-instructions.md` §Site Config → "Promotion gate". Gates that matter most for preventing regression cycling: (5) per-ATS cluster gate — fixing one ATS cannot trade off another by more than 2 pts, (6) ever-passed ratchet — challenger cannot silently lose a site any prior version passed, (7) oscillation gate — sites that flipped pass/fail ≥2× in the last 5 runs block promotion. Enforced in [`backend/app/tasks/ml_tasks.py`](backend/app/tasks/ml_tasks.py) `_aggregate`.
- **Stratified scoring always caps each axis at 100** — `field_completeness > 100` has historically inflated composites from multi-valued extraction bugs; the cluster gate would be trivially gameable without the cap. Legacy `_composite_score_standalone` preserves uncapped behaviour (backwards compat for the "all" scorecard on the Models page) unless `cap_axes=True`.
- **Objective composite score** — the *only* yardstick for promotion — lives in [`backend/app/api/v1/endpoints/ml_models.py:792`](backend/app/api/v1/endpoints/ml_models.py#L792) (`_composite_score_standalone`). Weighted: 20% discovery + 30% quality extraction + 25% field completeness + 25% volume accuracy. **Always cap each axis at 100 before computing the composite** — historical runs with `field_completeness > 100` are extraction bugs, not improvements, and must not be treated as wins.
- **Per-iteration promotion gate** (in `backend/app/tasks/ml_tasks.py`): challenger composite > 0, > champion composite, regression accuracy ≥ 60%, **zero regressions on champion-passing sites**. Atomic in one tx: old champion → `tested`, new challenger → `live`.
- **Why the existing TF-IDF F1=0.9963 is misleading** — labels come from `quality_score` rules, so the model is just learning the rules. Treat it as an unverified baseline until re-evaluated against the GOLD holdout.
- **Ground truth lives in `gold_holdout_*` tables** materialised from `lead_imports` (script: `backend/scripts/build_gold_holdout.py`). Sets are FROZEN once built — to revise, mint a new `name` (e.g. `au_baseline_v2`).
- **Split-by-domain is mandatory.** `app.ml.champion_challenger.domain_splitter.split_by_domain` enforces no domain crosses train/val/test. `assert_holdout_isolation` is a hard guard called at the start of every training run.
- **Hardened promotion gates (orchestrator path) require multi-metric wins + statistical significance** — see `app.ml.champion_challenger.promotion`. Default: at least 2 of {f1, recall, job_coverage_rate, false_positive_rate↓} must improve, AND McNemar p<0.05, AND latency p95 within budget. A single-metric "win" never promotes.
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
- Tiered extractor: `backend/app/crawlers/tiered_extractor.py` (3-tier hybrid: ATS templates, heuristic scoring, LLM fallback)
- Celery tasks: `backend/app/tasks/crawl_tasks.py`
- Seed data: `backend/scripts/seed.py`
- DB models: `backend/app/models/`
- Alembic migration: `backend/alembic/versions/0001_initial_schema.py`

## Seed Data Summary

- 57 Australian companies across: mining (BHP, Rio Tinto, Fortescue), banking (CBA, ANZ, Westpac, NAB, Macquarie), tech (Canva, Atlassian, Xero, SafetyCulture, CultureAmp), healthcare, education, government, professional services, retail
- ATS coverage: Greenhouse (many AU tech co's), Lever, Workday, BambooHR, iCIMS, Taleo, SmartRecruiters, Ashby, Jobvite, PageUp
- Blocked: seek.com.au, jora.com, au.jora.com, jobstreet.com (all variants), jobsdb.com (all variants)
- Aggregators (link-discovery): Indeed AU, LinkedIn Jobs, Glassdoor AU, CareerOne, Adzuna AU

## Recent Milestones

- v7.1 introduced a dedicated SuccessFactors table extractor (`tr.data-row` + `a.jobTitle-link`) with bounded `startrow` pagination candidate support.
- v7.1 introduced a dedicated Homerun parser for `<job-list v-bind>` payloads to recover jobs from config-driven pages where DOM listings are sparse.
- v7.1 tightened title filtering for location/company/generic career headings and improved description cleanup (HTML entity/tag normalization).
- v7.3 added a precision-focused extraction layer: linked-card/nav URL suppression, stricter link-fallback evidence gating, and Nuxt/Drupal row extraction with bounded page traversal to reduce Type-1 noise while preserving volume on paginated listing boards.
- v7.4 added dedicated ATS/feed recovery for current regression gaps: PageUp listing-row extraction with bounded pagination, Recruitee `/o/` extraction (including `.NET` titles), and `jobs.json` shell-feed fallback for pages that render jobs only client-side.
- v7.6 added focused strong-URL recovery for multilingual ATS rows: Teamtailor numeric detail-url row extraction, Bootstrap query-id card extraction, broadened Connx row/app-shell handling, and split-row PageUp link association improvements.
- v7.7 added row-context quality recovery: metadata-aware row container selection for location backfill, summary-first row description extraction, and description deglue/CTA-tail cleanup for cleaner downstream text.
- v7.8 added careers-page.com API recovery (`/api/v1.0/c/<slug>/jobs`) and Connx same-page URL repair for `/job/details/...` backfill.
- v7.9 tightened linked-card precision (date/filter title rejection + `/jobs?...` filter URL rejection) and added numeric-detail fallback for legacy `/jobs/<id>/...` vacancy tables.
- v9.0 added bounded progressive pagination URL synthesis (`?pp`/`/page/` gaps), multilingual AWSM row-title recovery for `wp-job-openings`, and linked-card title precision updates to reject editorial labels like `Career Guide` while recovering compact structured roles.
- v10.1 refactored `TieredExtractorV100` into a local-first hybrid extractor with dedicated Breezy/Teamtailor/generic job-grid/WordPress-card/table/JSON-LD extraction, then bounded queue-based LLM fallback.
- v10.1 reduced timeout risk in the host worker by tuning fallback defaults and parsing Codex JSONL output directly (no temporary output-file roundtrip).
- v10.2 added split-table card recovery (title row + generic CTA row), numeric query-id detail URL support (`?id=<digits>` and related keys), and Greenhouse table-title cleanup by preferring role nodes over full-anchor text.
- v10.2 switched v100 title validation to Unicode-aware alphabetic checks, recovering multilingual role titles (for example Thai) while tightening punctuation-variant CTA rejection (`Apply Now!`).
- v10.3 preserved app-shell JSON scripts in LLM truncation (`application/json`, `__NEXT_DATA__`) and added embedded-state JSON extraction in v100 before fallback.
- v10.3 added metadata-first local extractors for Bootstrap list-group rows, span-card job layouts, and split heading+CTA cards, lifting field completeness on recoverable pages.
- v10.4 added canonical URL dedupe (tracking-query stripping) so state+DOM Greenhouse duplicates collapse correctly without changing emitted source URLs.
- v10.4 improved anchor-only extraction field completeness by pulling location metadata from ancestor row context (for example Hays `p.location` siblings).
- v10.4 expanded embedded-state parsing to support `absolute_url`/`apply_url` and structured location objects/lists, improving recoverable app-shell job extraction.
- v10.5 added bounded JS-shell endpoint recovery before LLM fallback in v100 (`fetch('*.json')`, Workday tenant/site shell API probing, Martian client/recruiter endpoint probing) for app-shell pages with empty DOM listings.
- v10.5 added a same-page heading+metadata extractor for role sections without detail links, emitting deterministic fragment URLs and inline location/job-type metadata.
- v10.5 added WordPress/Divi `article.post` entry-title extraction for role-slug job feeds and tightened state-JSON ID-fallback acceptance to reject department/team labels.
