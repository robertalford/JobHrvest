## 2026-04-15

- Fixed pagination controls being emitted as jobs (for example `Show 8 more`) by adding a generic post-extraction non-job filter in `TieredExtractorV70`.
- Fixed section-heading leakage (`Working with us`) in linked-card outputs by extending the same v7.0 post-filter to reject obvious non-role labels.
- Preserved extraction volume while improving quality/coverage on the v7.0 fixture harness: job count unchanged, non-job titles reduced to zero, and description/location fill significantly improved via the existing v7.0 listing-context enrichment.

## 2026-04-07

- Fixed discovery timeout pattern on TLS-mismatch hint URLs by adding insecure hint retry + scheme alternate fallback in `CareerPageFinderV67`.
- Fixed linked-card under-extraction where full-anchor text caused title rejection and nav-label leakage by extracting role titles from inner heading/large-text nodes in `TieredExtractorV67`.
- Fixed generic non-job labels (`Job Board`, `How It Works`) being accepted as extracted jobs by explicitly rejecting them in v6.7 title validation.
- Fixed Jobs2Web probe-order misses where the bounded endpoint budget could skip same-host `/search` variants, by ranking same-host search endpoints earlier in `TieredExtractorV69`.
- Fixed `Job Vacancies` navigation-menu headings being emitted as jobs by adding explicit vacancy-heading rejection in `TieredExtractorV69` title validation.
- Fixed v7.0 linked-card false positives (location/company/generic career headings) by tightening v7.1 title validation and card fallback gating.
- Fixed SuccessFactors table under-capture by adding dedicated `tr.data-row` + `a.jobTitle-link` extraction with bounded `startrow` pagination planning in v7.1.
- Fixed Homerun false negatives on marketing-first pages by parsing `<job-list v-bind>` vacancies state in v7.1.
- Fixed noisy HTML-encoded descriptions by adding v7.1 description cleanup (entity decode, tag stripping, whitespace normalization).
- Fixed nav/corporate link bleed-through in linked-card extraction (`Saved jobs`, `Find Your Dream Job`, company-root links) by adding v7.3 nav-aware URL/title gating.
- Fixed title-only promotional/event links being emitted as jobs in generic link fallback by requiring URL/job-context evidence in v7.3 post-filtering.
- Fixed under-capture on Nuxt/Drupal job-row pages by adding v7.3 structured row extraction and bounded `?page=` pagination follow-up.
- Fixed Recruitee false negatives on `/o/<slug>` detail links (including `.NET Developer`) by adding dedicated `/o/` extraction and strong-detail path support in `TieredExtractorV74`.
- Fixed PageUp listing under-capture by adding dedicated `h3.list-title` row extraction with bounded same-host pagination follow-up from `a.more-link`/`page=` URLs in `TieredExtractorV74`.
- Fixed JS-shell zero-job cases where listings are loaded through `fetch('/jobs.json')` by adding bounded same-host JSON-feed probing and item-to-job conversion in `TieredExtractorV74`.
- Fixed `Associate Login`/`Candidate Login` type-1 leakage by adding explicit login-label rejection in v7.4 title filtering.

## 2026-04-08

- Fixed Teamtailor multilingual short-title under-capture (`Võlaagent`, `CRM Expert`, `Klienditeeninduse konsultant`) by adding strong-detail-url row extraction in `TieredExtractorV76`.
- Fixed Indonesian Bootstrap career card misses (`/career?id=...`) by adding dedicated repeated query-id card extraction with unique detail URLs in `TieredExtractorV76`.
- Fixed PageUp split-column row/link association gaps by pairing `h3.list-title` with ancestor/sibling detail links in `TieredExtractorV76`.
- Fixed Connx alternative GridTable markup under-capture (anchor/table rows) by broadening row selectors and URL extraction in `TieredExtractorV76`.
- Fixed description boilerplate noise from skip links by removing `Skip to primary navigation`, `Skip to main content`, and `Back to all positions` during v7.6 description cleaning.
- Fixed linked-card quality collapse on split metadata rows (title block + sibling location block) by replacing first-hit row ancestor selection with score-based metadata-aware container selection in `TieredExtractorV77`.
- Fixed row-description noise where container-level extraction mixed summary copy with CTA/meta text by preferring semantic summary nodes (`<p>/<li>`) and trimming CTA tails in `TieredExtractorV77`.
- Fixed glued description prefixes/tails (`TitleLocationApply... Read More`) by adding deglue and prefix/tail normalization in `TieredExtractorV77` description cleaning.
- Fixed linked-card Type-1 date/filter noise (`Apr 7, 2026`, `Job Index`, `Jobs near ...`) by adding v7.9 date/listing title rejection guardrails.
- Fixed job-board filter links (`/jobs?jobtype=...`, `/jobs?district=...`) being treated as job-detail URLs by adding v7.9 query-filter URL rejection unless explicit detail-ID keys exist.
- Fixed under-capture on legacy numeric-detail vacancy pages (`/jobs/<id>/...`) by adding v7.9 numeric-detail fallback extraction with bounded relaxed title acceptance under existing non-job safeguards.
- Fixed editorial/sidebar link bleed-through on dense numeric vacancy tables by adding v7.9 numeric-table filtering that keeps numeric-detail URLs when the page is clearly a large numeric jobs listing.
- Fixed sparse pagination under-capture on boards where page 1 omits intermediate links (`?pp=6`, `/page/3`) by adding bounded progressive pagination URL synthesis in `TieredExtractorV90`.
- Fixed AWSM multilingual title false negatives (for example `Konsultan Accurate`) by adding strict multilingual fallback validation in `TieredExtractorV90` for `wp-job-openings` rows.
- Fixed linked-card editorial title leakage (`Career Guide`) while preserving compact structured roles (`Powerline Workers`, `Storeperson`) via v9.0 title fallback/guard updates.

## 2026-04-09

- Fixed v10 zero-job outcomes on JS-shell pages that only expose endpoint hints in scripts by adding bounded shell endpoint recovery (`fetch('*.json')`, Workday `wday/cxs`, Martian client/recruiter probes) in `TieredExtractorV100`.
- Fixed same-page static career sections (heading + metadata, no detail links) being dropped entirely by adding a role-section extractor with synthetic per-role fragment URLs and inline location/job-type parsing in `TieredExtractorV100`.
- Fixed WordPress/Divi entry-title role posts (for example Tom Orange `article.post` feeds) being rejected by strict detail-path heuristics by adding a dedicated entry-title job-post extractor with role-aware URL/title gating in `TieredExtractorV100`.
- Fixed state-JSON label pollution (`... Department`, `... Team`) when ID fallback URLs were synthesized from weak objects by requiring stronger job-key evidence before accepting ID-only fallback nodes in `TieredExtractorV100`.
- Fixed missing support for common query-id detail keys (`jobAdId`, `adId`, `career_job_req_id`) by extending v10 detail-query URL recognition and probable-job validation.

- Fixed v10 timeout-heavy zero-output behavior by adding deterministic local-first extraction in `TieredExtractorV100` before queue/LLM fallback.
- Fixed Breezy rows being missed in v10 despite server-rendered listings by adding dedicated `li.position` + `/p/<id>` extraction.
- Fixed Teamtailor server-rendered rows being missed in v10 by adding dedicated `li.w-full` + `/jobs/<id>-slug` extraction.
- Fixed generic `.job` card-grid under-capture (including `/career/openings/...` patterns) by adding focused row parsing with `job__name` title extraction.
- Fixed WordPress vacancy-card misses (`div.col-md-6` + `/career/<slug>`) by adding a dedicated card extractor with location capture.
- Fixed v10 extractor crashes in restricted environments when `/storage` queue path is unavailable by adding queue-unavailable handling that returns safe fallback results.
- Fixed inheritance smoke-test failure for the latest extractor by making `TieredExtractorV100` inherit from `TieredExtractorV16`.
- Fixed slow worker fallback path by reducing v10 worker timeout/concurrency defaults and removing output-file roundtrips in favor of direct Codex JSONL parsing.
- Fixed Greenhouse title corruption (`RoleLocation` glue) by making v10 table extraction prefer role nodes (`.body--medium`, heading tags) before full-anchor fallback.
- Fixed split-table card misses where role titles and detail links live in different rows (generic CTA anchors like `Selengkapnya`) by adding `_extract_split_table_cards` in `TieredExtractorV100`.
- Fixed query-id detail URL false negatives (`/career?id=282`, similar numeric-id query routes) by extending v10 URL validation to treat numeric query IDs as detail evidence.
- Fixed multilingual title false negatives (Thai/CJK/non-Latin scripts) by replacing Latin-only title checks with Unicode-aware alphabetic validation.
- Fixed punctuation-variant CTA leakage (`Apply Now!`) by normalizing trailing punctuation before non-job title rejection.
- Fixed v10 LLM app-shell blindness where JSON state payloads were removed before fallback by preserving `application/json`/`__NEXT_DATA__` scripts in `_truncate_html`.
- Fixed metadata-quality regression on high-volume list/card pages that fell back to anchor-only extraction by adding dedicated metadata extractors for Bootstrap list rows and span-card layouts.
- Fixed split heading+CTA card under-extraction (Elementor/CMS role headings with separate `Apply` links) by adding `_extract_heading_cta_cards` with apply-context URL gating.
- Fixed shared-generic apply URL over-capture on card pages by bounding dedupe retention for generic apply paths and rejecting `We are hiring` titles.
- Fixed duplicate inflation on Greenhouse boards where the same job appeared from embedded state (`absolute_url`) and DOM rows with tracking query variants by canonicalizing dedupe keys to ignore tracking params.
- Fixed missing `location_raw` on anchor-only Hays-style rows by adding ancestor-aware row metadata lookup (`p.location`, `body__secondary`, `body--metadata`) in anchor extraction.
- Fixed embedded-state false negatives when URL was exposed as `absolute_url` / `apply_url` and location was an object/list, by extending v10 state parser key coverage and structured location parsing.
- Fixed noisy anchor fallback overreach on pages already covered by deterministic extractors by lowering v10 anchor fallback trigger from `<5` to `<3` jobs.
