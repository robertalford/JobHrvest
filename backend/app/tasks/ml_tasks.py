"""ML/classifier Celery tasks."""

import logging
from datetime import datetime, timezone
from app.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)


@celery_app.task
def retrain_page_classifier():
    """Retrain the scikit-learn page classifier from accumulated LLM labels."""
    logger.info("Starting page classifier retraining (Phase 4)")
    # Phase 4 implementation: query LLM-labeled pages, train TF-IDF + LogisticRegression, persist model


@celery_app.task
def rebuild_all_templates():
    """Re-validate all active site templates against fresh LLM extraction."""
    logger.info("Starting template rebuild cycle (Phase 4)")
    # Phase 4 implementation


@celery_app.task
def score_jobs_batch(limit: int = 500):
    """Score a batch of unscored (or stale) jobs for quality."""
    import asyncio
    from sqlalchemy import select, or_
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
    from app.core.config import settings
    from app.models.job import Job
    from app.models.company import Company
    from app.services.quality_scorer import score_job, compute_site_quality

    async def _run():
        engine = create_async_engine(settings.DATABASE_URL, echo=False)
        Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        async with Session() as db:
            # Fetch unscored jobs
            result = await db.execute(
                select(Job)
                .where(
                    or_(Job.quality_score.is_(None), Job.quality_scored_at.is_(None))
                )
                .order_by(Job.first_seen_at.desc())
                .limit(limit)
            )
            jobs = result.scalars().all()

            if not jobs:
                logger.info("No unscored jobs found.")
                return

            company_job_scores: dict[str, list[float]] = {}
            company_flags: dict[str, dict] = {}

            for job in jobs:
                qr = score_job(
                    title=job.title,
                    description=job.description,
                    location_raw=job.location_raw,
                    employment_type=job.employment_type,
                    date_posted=job.date_posted,
                    salary_raw=job.salary_raw,
                    requirements=job.requirements,
                    source_url=job.source_url,
                )
                job.quality_score = qr.score
                job.quality_completeness = qr.completeness_score
                job.quality_description = qr.description_score
                job.quality_issues = qr.issues
                job.quality_flags = {
                    "scam_detected": qr.scam_detected,
                    "bad_words_detected": qr.bad_words_detected,
                    "discrimination_detected": qr.discrimination_detected,
                }
                job.quality_scored_at = datetime.now(timezone.utc)

                # Track per-company
                cid = str(job.company_id)
                if cid not in company_job_scores:
                    company_job_scores[cid] = []
                    company_flags[cid] = {"scam": False, "discrimination": False}
                company_job_scores[cid].append(qr.score)
                if qr.scam_detected:
                    company_flags[cid]["scam"] = True
                if qr.discrimination_detected:
                    company_flags[cid]["discrimination"] = True

            await db.commit()
            logger.info(f"Scored {len(jobs)} jobs.")

            # Update company quality scores
            for company_id, scores in company_job_scores.items():
                flags = company_flags[company_id]
                site_score = compute_site_quality(
                    scores, flags["scam"], flags["discrimination"]
                )
                company = await db.get(Company, company_id)
                if company:
                    company.quality_score = site_score
                    company.quality_scored_at = datetime.now(timezone.utc)

            await db.commit()
            logger.info(f"Updated quality scores for {len(company_job_scores)} companies.")

    asyncio.run(_run())


@celery_app.task(queue="ml", name="ml.llm_extract_page")
def llm_extract_page(career_page_id: str):
    """
    Run LLM extraction on a career page that returned zero results from other methods.
    Queued to the 'ml' worker to avoid blocking crawl workers.
    """
    import asyncio
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
    from app.core.config import settings
    from app.models.career_page import CareerPage
    from app.models.company import Company
    from app.crawlers.http_client import ResilientHTTPClient
    from app.extractors.llm_extractor import LLMJobExtractor
    from markdownify import markdownify

    async def _run():
        engine = create_async_engine(settings.DATABASE_URL, echo=False)
        Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        async with Session() as db:
            page = await db.get(CareerPage, career_page_id)
            if not page or not page.is_active:
                return

            company = await db.get(Company, page.company_id)
            if not company:
                return

            client = ResilientHTTPClient()
            llm = LLMJobExtractor()

            try:
                if page.requires_js_rendering:
                    html = await client.get_rendered(page.url)
                else:
                    resp = await client.get(page.url)
                    html = resp.text
            except Exception as e:
                logger.error(f"LLM extract page: failed to fetch {page.url}: {e}")
                return

            md = markdownify(html, strip=["script", "style"])
            result = await llm.extract(page.url, md)

            if not result or not result.get("title"):
                logger.info(f"LLM extraction found no job for {page.url}")
                return

            # Persist via job_extractor upsert
            from app.crawlers.job_extractor import JobExtractor
            extractor = JobExtractor(db)
            result["source_url"] = page.url
            result["extraction_method"] = result.get("extraction_method", "llm_raw")
            result = extractor._enrich(result, company)
            job = await extractor._upsert_job(company, page, result)
            if job:
                await extractor._save_tags(job, result)
                logger.info(f"LLM extracted job '{job.title}' from {page.url}")

    asyncio.run(_run())


@celery_app.task
def score_all_jobs():
    """Backfill quality scores for ALL unscored jobs (may take a while)."""
    logger.info("Starting full quality scoring backfill.")
    batch = 1000
    for _ in range(100):  # max 100 batches = 100k jobs
        score_jobs_batch(limit=batch)


@celery_app.task(name="ml.enrich_job_descriptions")
def enrich_job_descriptions(limit: int = 150):
    """
    Re-fetch and re-extract descriptions for jobs with missing or very short descriptions.

    Strategy:
    - Fetch source URL (with Playwright if the company requires JS rendering)
    - Run improved heuristic extractor + LLM extractor as fallback
    - Update the job's description field and reset quality_score so it gets re-scored
    - Record description_enriched_at to prevent repeated attempts

    Targets jobs where:
      - description IS NULL or len(description) < 200
      - description_enriched_at IS NULL (not yet attempted)
      - source_url is set and doesn't point to a known aggregator/broken URL
      - job is active
    """
    import asyncio
    from sqlalchemy import select, or_, and_
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
    from app.core.config import settings
    from app.models.job import Job
    from app.models.company import Company

    async def _run():
        engine = create_async_engine(settings.DATABASE_URL, echo=False)
        Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        async with Session() as db:
            result = await db.execute(
                select(Job)
                .join(Company, Job.company_id == Company.id)
                .where(
                    Job.is_active == True,
                    Job.description_enriched_at.is_(None),
                    or_(
                        Job.description.is_(None),
                        Job.description == "",
                        # SQLAlchemy func.length for short descriptions
                        Job.description.op("~")(r"^.{0,199}$"),
                    ),
                    Job.source_url.isnot(None),
                )
                .order_by(Job.first_seen_at.desc())
                .limit(limit)
            )
            jobs = result.scalars().all()

            if not jobs:
                logger.info("enrich_job_descriptions: no jobs need enrichment")
                return

            logger.info(f"enrich_job_descriptions: enriching {len(jobs)} jobs")
            enriched = 0
            failed = 0

            for job in jobs:
                try:
                    company = await db.get(Company, job.company_id)
                    new_description = await _fetch_description(job, company)

                    job.description_enriched_at = datetime.now(timezone.utc)

                    if new_description and len(new_description) > len(job.description or ""):
                        job.description = new_description
                        # Reset quality score so it gets re-scored with the new description
                        job.quality_score = None
                        job.quality_scored_at = None
                        enriched += 1
                        logger.debug(f"  enriched job {job.id}: {len(new_description)} chars")
                    else:
                        logger.debug(f"  no improvement for job {job.id} ({job.source_url[:80]})")

                except Exception as e:
                    logger.warning(f"  failed to enrich job {job.id}: {e}")
                    failed += 1
                    job.description_enriched_at = datetime.now(timezone.utc)

            await db.commit()
            logger.info(
                f"enrich_job_descriptions: {enriched} enriched, "
                f"{len(jobs) - enriched - failed} unchanged, {failed} failed"
            )

    asyncio.run(_run())


async def _fetch_description(job: "Job", company: "Company") -> str:
    """
    Fetch job detail page and extract description using the full
    DescriptionExtractor pyramid (layers 0-5, skipping vision for speed).

    Layer 0  Structured data (JSON-LD / Schema.org)
    Layer 1  ATS platform selectors
    Layer 2  Learned site-specific CSS selectors
    Layer 3  Universal semantic selectors
    Layer 4  Content-density DOM analysis
    Layer 5  LLM with focused description prompt (Ollama)
    """
    from app.crawlers.http_client import ResilientHTTPClient
    from app.extractors.description_extractor import DescriptionExtractor

    requires_js = getattr(company, "requires_js_rendering", False)
    client = ResilientHTTPClient()
    try:
        if requires_js:
            html = await client.get_rendered(job.source_url)
        else:
            resp = await client.get(job.source_url)
            html = resp.text
    except Exception as e:
        logger.debug(f"_fetch_description: HTTP error for {job.source_url}: {e}")
        return ""

    if not html or len(html) < 200:
        return ""

    extractor = DescriptionExtractor()  # stateless for enrichment batches
    result = await extractor.extract(
        html=html,
        url=job.source_url,
        ats_platform=getattr(company, "ats_platform", None),
        max_layer=5,  # Include LLM; skip vision (too slow for batch enrichment)
    )
    return result.text if result else ""


@celery_app.task(name="ml.reprocess_company")
def reprocess_company(company_id: str):
    """
    Full reprocessing pipeline for a single company:

    1. Collect ALL active jobs (canonical + duplicates)
    2. Enrich descriptions for those with missing/short text using DescriptionExtractor
    3. Re-run deduplication with the enriched data (fixed null-null scoring)
    4. Reset quality_score on jobs whose description improved → picked up by scorer

    This corrects two classes of error:
    - Jobs marked as duplicates purely because both had null descriptions
    - Low-quality jobs that failed quality checks due to missing fields
    """
    import asyncio
    import uuid as uuid_lib
    from sqlalchemy import select, update, or_
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
    from app.core.config import settings
    from app.models.job import Job
    from app.models.company import Company
    from app.services.job_deduplicator import run_company_dedup
    from app.services.quality_scorer import score_job

    async def _run():
        engine = create_async_engine(settings.DATABASE_URL, echo=False)
        Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

        async with Session() as db:
            cid = uuid_lib.UUID(company_id)
            company = await db.get(Company, cid)
            if not company:
                logger.warning(f"reprocess_company: company {company_id} not found")
                return

            # Fetch ALL active jobs — canonical AND duplicates
            result = await db.execute(
                select(Job).where(Job.company_id == cid, Job.is_active == True)
            )
            jobs = result.scalars().all()

            if not jobs:
                return

            logger.info(
                f"reprocess_company [{company.name}]: "
                f"{len(jobs)} jobs ({sum(1 for j in jobs if not j.is_canonical)} duplicates)"
            )

            enriched = 0
            for job in jobs:
                desc_len = len(job.description or "")
                if desc_len >= 300:
                    continue  # Already has a good description

                try:
                    new_desc = await _fetch_description(job, company)
                    if new_desc and len(new_desc) > desc_len:
                        job.description = new_desc
                        job.description_enriched_at = datetime.now(timezone.utc)
                        job.quality_score = None       # Reset for re-scoring
                        job.quality_scored_at = None
                        enriched += 1
                except Exception as e:
                    logger.debug(f"  enrich failed for {job.source_url}: {e}")
                    job.description_enriched_at = datetime.now(timezone.utc)

            if enriched:
                await db.commit()
                logger.info(f"  enriched {enriched} descriptions")

            # Re-run deduplication with the improved data
            before_dups = sum(1 for j in jobs if not j.is_canonical)
            dedup_result = await run_company_dedup(db, cid)
            after_dups = dedup_result["duplicates"]

            rescued = before_dups - after_dups
            if rescued > 0:
                logger.info(
                    f"  dedup: {before_dups} → {after_dups} duplicates "
                    f"({rescued} jobs rescued from duplicate status)"
                )

            # Re-score jobs whose description changed (quality_score is NULL)
            scored = 0
            result2 = await db.execute(
                select(Job).where(
                    Job.company_id == cid,
                    Job.is_active == True,
                    Job.quality_score.is_(None),
                )
            )
            unscored = result2.scalars().all()
            for job in unscored:
                qr = score_job(
                    title=job.title,
                    description=job.description,
                    location_raw=job.location_raw,
                    employment_type=job.employment_type,
                    date_posted=job.date_posted,
                    salary_raw=job.salary_raw,
                    requirements=job.requirements,
                    source_url=job.source_url,
                )
                job.quality_score = qr.score
                job.quality_completeness = qr.completeness_score
                job.quality_description = qr.description_score
                job.quality_issues = qr.issues
                job.quality_flags = {
                    "scam_detected": qr.scam_detected,
                    "bad_words_detected": qr.bad_words_detected,
                    "discrimination_detected": qr.discrimination_detected,
                }
                job.quality_scored_at = datetime.now(timezone.utc)
                scored += 1

            if scored:
                await db.commit()

            logger.info(
                f"reprocess_company [{company.name}]: done — "
                f"enriched={enriched}, rescued_from_dedup={rescued}, re_scored={scored}"
            )
            return {
                "company_id": company_id,
                "company_name": company.name,
                "jobs_total": len(jobs),
                "enriched": enriched,
                "rescued_from_dedup": rescued,
                "re_scored": scored,
            }

    return asyncio.run(_run())


@celery_app.task(name="ml.batch_reprocess")
def batch_reprocess(limit: int = 50, quality_threshold: float = 40.0):
    """
    Find companies with low-quality or heavily-duplicated jobs and reprocess them.

    Targets companies where:
    - Any canonical job has quality_score < threshold (low-quality jobs)
    - OR has duplicate_count > 0 (has known duplicates that may be false positives)
    - AND has at least one job with missing/short description (enrichable)

    Runs reprocess_company() per company, staggered via Celery.
    Returns a summary of what was queued.
    """
    import asyncio
    from sqlalchemy import select, text as sa_text
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
    from app.core.config import settings

    async def _find_companies():
        engine = create_async_engine(settings.DATABASE_URL, echo=False)
        Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        async with Session() as db:
            result = await db.execute(sa_text("""
                SELECT DISTINCT j.company_id::text
                FROM jobs j
                WHERE j.is_active = true
                  AND (
                      -- Has low-quality canonical jobs
                      (j.is_canonical = true AND j.quality_score < :threshold)
                      OR
                      -- Has jobs marked as duplicates (potential false positives)
                      (j.is_canonical = false)
                  )
                  AND (
                      -- And at least one job in the company needs enrichment
                      EXISTS (
                          SELECT 1 FROM jobs j2
                          WHERE j2.company_id = j.company_id
                            AND j2.is_active = true
                            AND (j2.description IS NULL OR LENGTH(j2.description) < 300)
                      )
                  )
                LIMIT :lim
            """), {"threshold": quality_threshold, "lim": limit})
            return [row[0] for row in result.fetchall()]

    company_ids = asyncio.run(_find_companies())

    if not company_ids:
        logger.info("batch_reprocess: no companies need reprocessing")
        return {"queued": 0}

    logger.info(f"batch_reprocess: queuing reprocess for {len(company_ids)} companies")

    for i, cid in enumerate(company_ids):
        reprocess_company.apply_async(
            args=[cid],
            queue="ml",
            countdown=i * 2,  # Stagger by 2s to avoid HTTP rate-limit thundering herd
        )

    return {"queued": len(company_ids), "company_ids": company_ids[:10]}


# ---------------------------------------------------------------------------
# Parallel A/B Test Execution
# ---------------------------------------------------------------------------
# Architecture: fan-out per-site tasks across workers, then aggregate.
#   chord(group([test_single_site(site1), test_single_site(site2), ...]),
#         aggregate_test_results(run_id, ...))
# ---------------------------------------------------------------------------

def _pick_extractor(name: str):
    """Select extractor class by model name. Dynamic import."""
    import importlib, re
    from app.crawlers.tiered_extractor import TieredExtractor
    match = re.search(r"v(\d+)\.(\d+)", name)
    if match:
        file_ver = int(match.group(1)) * 10 + int(match.group(2))
        try:
            mod = importlib.import_module(f"app.crawlers.tiered_extractor_v{file_ver}")
            return getattr(mod, f"TieredExtractorV{file_ver}")
        except (ImportError, AttributeError):
            pass
    return TieredExtractor


def _pick_finder(name: str):
    """Select career page finder class by model name. Dynamic import.

    Only the actively-maintained finders remain after the 2026-04-14 reset:
    v4 (legacy), v26 (stable), v60 (consolidated), v69 (paired with champion).
    Any model version not in the map falls through to the base CareerPageFinder.
    """
    import importlib, re
    _FINDER_MAP = {
        70: 70,  # v7.0 challenger
        69: 69,  # v6.9 champion
        60: 60,  # v6.0 consolidated
        26: 26,  # v2.6 stable
        20: 20,  # v2.0 → keep for legacy tests
        17: 5, 16: 4, 15: 3, 14: 2, 13: 2, 12: 2,
    }
    match = re.search(r"v(\d+)\.(\d+)", name)
    if match:
        file_ver = int(match.group(1)) * 10 + int(match.group(2))
        finder_ver = _FINDER_MAP.get(file_ver, file_ver)
        try:
            mod = importlib.import_module(f"app.crawlers.career_page_finder_v{finder_ver}")
            return getattr(mod, f"CareerPageFinderV{finder_ver}")
        except (ImportError, AttributeError):
            pass
    from app.crawlers.career_page_finder import CareerPageFinder
    return CareerPageFinder


# ── ATS detection for stratified scoring ─────────────────────────────────────
# Kept in sync with `_stratum_key` in app/api/v1/endpoints/ml_models.py and
# `_derive_ats` in app/ml/champion_challenger/failure_analysis.py. Three call
# sites, one canonical mapping — adding a platform means updating all three.
_ATS_NEEDLES = (
    ("greenhouse", "greenhouse"),
    ("lever.co", "lever"),
    ("ashby", "ashby"),
    ("workday", "workday"),
    ("myworkdayjobs", "workday"),
    ("bamboohr", "bamboohr"),
    ("smartrecruiters", "smartrecruiters"),
    ("icims", "icims"),
    ("taleo", "taleo"),
    ("successfactors", "successfactors"),
    ("jobvite", "jobvite"),
    ("breezy", "breezyhr"),
    ("rippling", "rippling"),
    ("oracle", "oracle_cx"),
    ("salesforce", "salesforce"),
    ("martianlogic", "martianlogic"),
    ("pageup", "pageup"),
    ("recruitee", "recruitee"),
    ("teamtailor", "teamtailor"),
    ("applyflow", "applyflow"),
    ("jobs2web", "jobs2web"),
)


def _detect_entry_ats(url: str, known: dict | None) -> str | None:
    """Best-effort ATS tag from the pre-run signals we have.

    Cheap path only — no HTTP. Called at entry construction in test_single_site
    before any discovery runs. Prefers an explicit `atsName` on the baseline
    wrapper, then falls back to substring matching on the URL + wrapper JSON.
    Returns None when we genuinely can't tell; the aggregator will refine using
    the discovered URL/tier after the model phase runs.
    """
    if isinstance(known, dict):
        explicit = known.get("atsName") or known.get("ats_name") or known.get("ats")
        if isinstance(explicit, str) and explicit.strip():
            return explicit.strip().lower().replace(" ", "_")

    import json as _j
    wrapper_blob = _j.dumps(known or {})[:400].lower() if known else ""
    hay = f"{(url or '').lower()} {wrapper_blob}"
    for needle, label in _ATS_NEEDLES:
        if needle in hay:
            return label
    return None


def _refine_entry_ats(entry: dict) -> str | None:
    """Second-pass ATS detection using post-discovery signals.

    Called by the aggregator after every site test has completed. Uses the
    discovered URL, discovery method, and tier label that the extractor
    reported — signals that weren't available at entry-construction time.
    """
    existing = (entry.get("ats_platform") or "").strip().lower()
    if existing and existing not in ("none", "null", "unknown"):
        return existing
    model = entry.get("model") or {}
    url_found = (model.get("url_found") or entry.get("url") or "").lower()
    disc = (model.get("discovery_method") or "").lower()
    tier = (model.get("tier_used") or "").lower()
    hay = f"{url_found} {disc} {tier}"
    for needle, label in _ATS_NEEDLES:
        if needle in hay:
            return label
    # Structural fallback — shape of the page determines the bucket when no
    # platform detection matches. Kept coarse on purpose.
    if any(k in hay for k in ("__next_data__", "_next/data", "nuxt", "react-root")):
        return "spa_shell"
    if any(k in hay for k in ("wordpress", "elementor", "drupal", "joomla")):
        return "generic_cms"
    return "bespoke"


@celery_app.task(
    name="ml.test_single_site",
    time_limit=300,       # 5 min hard limit per site
    soft_time_limit=270,  # 4.5 min soft limit
    max_retries=0,
)
def test_single_site(
    site_data: list,
    model_name: str,
    champion_name: str | None,
    site_index: int,
    run_id: str = "",
    model_id: str = "",
    total_sites: int = 0,
    fixed_count: int = 0,
    auto_improve: bool = False,
):
    """Test a single site: baseline + champion + challenger.

    Stores result in Redis. When this is the last site to complete,
    triggers aggregation automatically.
    """
    import asyncio
    import json as _json
    import redis

    url = site_data[0] if site_data else "unknown"
    company = site_data[1] if len(site_data) > 1 else "unknown"

    try:
        result = asyncio.run(_run_site(site_data, model_name, champion_name, site_index, run_id))
    except Exception as e:
        logger.error("Site %d failed: %s", site_index, e, exc_info=True)
        result = {
            "url": url, "domain": "", "company": company,
            "http_ok": False,
            "baseline": {"jobs": 0, "fields": {}, "sample_titles": []},
            "champion": None,
            "model": {"jobs": 0, "fields": {}, "tier_used": None,
                      "sample_titles": [], "error": f"Task error: {str(e)[:100]}",
                      "url_found": None, "discovery_method": None},
            "match": "model_failed",
        }

    # Store result in Redis and check if we're the last site
    from app.core.config import settings
    r = redis.Redis.from_url(settings.CELERY_BROKER_URL)
    key = f"test_run:{run_id}:results"
    count_key = f"test_run:{run_id}:count"

    r.hset(key, str(site_index), _json.dumps(result, default=str))
    completed = r.incr(count_key)

    logger.info("Site %d/%d done (%s): %s", completed, total_sites, company, result.get("match", "?"))

    # If we're the last site, trigger aggregation
    if completed >= total_sites and total_sites > 0:
        logger.info("All %d sites done for run %s — triggering aggregation", total_sites, run_id)
        # Collect all results from Redis
        all_raw = r.hgetall(key)
        site_results = []
        for idx in range(total_sites):
            raw = all_raw.get(str(idx).encode(), all_raw.get(str(idx), None))
            if raw:
                site_results.append(_json.loads(raw))
            else:
                site_results.append({
                    "url": "missing", "domain": "", "company": f"site_{idx}",
                    "http_ok": False,
                    "baseline": {"jobs": 0, "fields": {}, "sample_titles": []},
                    "champion": None,
                    "model": {"jobs": 0, "fields": {}, "tier_used": None,
                              "sample_titles": [], "error": "Result missing from Redis"},
                    "match": "model_failed",
                })

        # Clean up Redis
        r.delete(key, count_key)

        # Aggregate inline
        asyncio.run(_aggregate(site_results, run_id, model_id, model_name,
                               champion_name, fixed_count, auto_improve))


async def _run_site(site_data: list, model_name: str, champion_name: str | None,
                     site_index: int, run_id: str = "") -> dict:
    """Async implementation of single-site test.

    `run_id` keys the run-scoped discovery cache used inside _run_model_phase.
    Was a pre-existing NameError when omitted (introduced when the cache
    landed in session 17); now defaulted to "" so callers without the cache
    optimisation still work.
    """
    import asyncio
    import json as _json
    import httpx
    from app.crawlers.job_extractor import JobExtractor
    from app.crawlers.career_page_finder import extract_domain
    from app.api.v1.endpoints.ml_models import (
        _field_coverage, _truncate_jobs, _count_real_jobs,
        _execute_baseline_with_steps, _parse_html_safe, _find_next_url,
    )

    url, company_name, sel_str = site_data[0], site_data[1], site_data[2]
    try:
        known = _json.loads(sel_str) if isinstance(sel_str, str) else sel_str
    except Exception:
        known = {}

    domain = extract_domain(url)

    extractor_cls = _pick_extractor(model_name)
    champion_cls = _pick_extractor(champion_name) if champion_name else None
    challenger_finder_cls = _pick_finder(model_name)
    champion_finder_cls = _pick_finder(champion_name) if champion_name else challenger_finder_cls

    extractor = extractor_cls()
    champion_extractor = champion_cls() if champion_cls else None

    entry = {
        "url": url,
        "domain": domain,
        "company": company_name,
        "ats_platform": _detect_entry_ats(url, known),
        "http_ok": False,
        "baseline": {
            "jobs": 0, "fields": {}, "sample_titles": [],
            "url_used": url,
            "selectors_used": {
                "boundary": (known.get("record_boundary_path") or "")[:60],
                "title": (known.get("job_title_path") or "")[:60],
            },
            "full_wrapper": known,
        },
        "champion": {
            "jobs": 0, "fields": {}, "tier_used": None,
            "sample_titles": [], "error": None,
            "url_found": None, "discovery_method": None,
        } if champion_extractor else None,
        "model": {
            "jobs": 0, "fields": {}, "tier_used": None,
            "sample_titles": [], "error": None,
            "url_found": None, "discovery_method": None,
        },
        "match": None,
    }

    async with httpx.AsyncClient(
        timeout=20, follow_redirects=True, verify=False,
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
    ) as client:
        # ── Phase A: Baseline (with Redis cache for fixed test sites) ──
        import hashlib, redis as _redis
        from app.core.config import settings as _settings
        _r = _redis.Redis.from_url(_settings.CELERY_BROKER_URL)
        _cache_key = f"baseline_cache:{hashlib.md5(url.encode()).hexdigest()}"
        _cached = None
        try:
            _cached_raw = _r.get(_cache_key)
            if _cached_raw:
                _cached = _json.loads(_cached_raw)
        except Exception:
            pass

        if _cached:
            # Use cached baseline
            entry["http_ok"] = _cached.get("http_ok", True)
            entry["baseline"] = _cached["baseline"]
            logger.info("Site %d: baseline cache HIT for %s", site_index, company_name)
        else:
          try:
            baseline_html = await _execute_baseline_with_steps(url, known, client)
            if len(baseline_html) > 200:
                entry["http_ok"] = True
                baseline_jobs = JobExtractor._static_extract_wrapper(baseline_html, url, known)

                # Pagination
                next_page_sel = known.get("next_page_path", "")
                if next_page_sel and next_page_sel not in ("null", "", "//no-next-page", "#none", "/nonextpagelink"):
                    try:
                        pages_followed, seen = 0, {url}
                        current_html, current_url = baseline_html, url
                        while pages_followed < 5:
                            nroot = _parse_html_safe(current_html)
                            if nroot is None:
                                break
                            next_url = _find_next_url(nroot, current_url, next_page_sel)
                            if not next_url or next_url in seen:
                                break
                            seen.add(next_url)
                            try:
                                nr = await client.get(next_url)
                                if nr.status_code != 200 or len(nr.text) < 200:
                                    break
                                current_html, current_url = nr.text, next_url
                                more_jobs = JobExtractor._static_extract_wrapper(current_html, current_url, known)
                                existing = {j["source_url"] for j in baseline_jobs}
                                new = [j for j in more_jobs if j["source_url"] not in existing]
                                if not new:
                                    break
                                baseline_jobs.extend(new)
                                pages_followed += 1
                            except Exception:
                                break
                    except Exception:
                        pass

                # Detail page enrichment
                _parse = JobExtractor._parse_selector_paths
                detail_desc_sels = _parse(known.get("details_page_description_paths", []))
                detail_loc_sels = _parse(known.get("details_page_location_paths", []))
                detail_salary_sel = known.get("details_page_salary_path", "")
                detail_type_sels = _parse(known.get("details_page_job_type_paths", []))
                has_detail_sels = bool(detail_desc_sels or detail_loc_sels)

                if has_detail_sels and baseline_jobs:
                    from lxml import etree as _etree

                    def _try_detail(detail_html, sel_list):
                        try:
                            parser = _etree.HTMLParser(encoding="utf-8")
                            tree = _etree.fromstring(detail_html.encode("utf-8", errors="replace"), parser)
                        except Exception:
                            return None
                        for sel in sel_list:
                            if not sel or sel in ("null", ""):
                                continue
                            try:
                                is_xp = sel.startswith("//") or sel.startswith(".//") or sel.startswith("(")
                                els = tree.xpath(sel) if is_xp else tree.cssselect(sel)
                                if els:
                                    txt = els[0].text_content().strip() if hasattr(els[0], 'text_content') else _etree.tostring(els[0], method="text", encoding="unicode").strip()
                                    if txt and len(txt) > 1:
                                        return txt
                            except Exception:
                                continue
                        return None

                    # Fetch + enrich up to 10 detail pages concurrently. The
                    # sequential loop used to dominate per-site wall-clock on
                    # detail-heavy sites (10 × ~3 s = ~30 s added). Semaphore
                    # caps concurrent fetches at 6 per site to stay polite.
                    candidates = []
                    for job in baseline_jobs[:10]:
                        detail_url = job.get("source_url", "")
                        if not detail_url or detail_url == url:
                            continue
                        needs_loc = not job.get("location_raw")
                        needs_desc = (not job.get("description")
                                      or len(job.get("description", "")) < 50)
                        if needs_loc or needs_desc:
                            candidates.append((job, detail_url, needs_loc, needs_desc))

                    if candidates:
                        _sem = asyncio.Semaphore(6)
                        async def _enrich_one(job, detail_url, needs_loc, needs_desc):
                            async with _sem:
                                try:
                                    dr = await client.get(detail_url, timeout=8)
                                except Exception:
                                    return
                                if dr.status_code != 200 or len(dr.text) < 200:
                                    return
                                if needs_desc and detail_desc_sels:
                                    desc = _try_detail(dr.text, detail_desc_sels)
                                    if desc and len(desc) > 50:
                                        job["description"] = desc[:5000]
                                if needs_loc and detail_loc_sels:
                                    loc = _try_detail(dr.text, detail_loc_sels)
                                    if loc and 1 < len(loc) < 200:
                                        job["location_raw"] = loc
                                if detail_salary_sel and not job.get("salary_raw"):
                                    sal = _try_detail(dr.text, _parse(detail_salary_sel))
                                    if sal:
                                        job["salary_raw"] = sal
                                if detail_type_sels and not job.get("employment_type"):
                                    jtype = _try_detail(dr.text, detail_type_sels)
                                    if jtype:
                                        job["employment_type"] = jtype

                        await asyncio.gather(
                            *(_enrich_one(*c) for c in candidates),
                            return_exceptions=True,
                        )

                entry["baseline"]["jobs"] = len(baseline_jobs)
                entry["baseline"]["fields"] = _field_coverage(baseline_jobs)
                entry["baseline"]["sample_titles"] = [j["title"][:80] for j in baseline_jobs[:5]]
                entry["baseline"]["extracted_jobs"] = _truncate_jobs(baseline_jobs)
          except Exception:
            pass

          # Cache baseline result (24h TTL)
          try:
            _r.setex(_cache_key, 86400, _json.dumps({
                "http_ok": entry["http_ok"],
                "baseline": entry["baseline"],
            }, default=str))
          except Exception:
            pass

        # ── Helper: run discovery + extraction for a model ──
        async def _run_model_phase(ext, finder_cls, phase_dict):
            # Run-scoped discovery cache: when champion and challenger use the
            # SAME finder class, the second phase reuses the first phase's URL
            # + HTML instead of re-probing. Saves 3–5 HTTP fetches per site ×
            # 179 sites × 2 phases = ~600 fetches per A/B run. Keyed on
            # (run_id, domain, finder_class) so finder upgrades still re-probe.
            finder_key = getattr(finder_cls, "__name__", "unknown")
            _disc_cache_key = f"career_disc:{run_id}:{domain}:{finder_key}" if run_id else None
            _cached_disc = None
            if _disc_cache_key:
                try:
                    raw = _r.get(_disc_cache_key)
                    if raw:
                        _cached_disc = _json.loads(raw)
                except Exception:
                    pass

            if _cached_disc:
                disc = {
                    "url": _cached_disc.get("url"),
                    "method": (_cached_disc.get("method") or "") + "+cached",
                    "html": _cached_disc.get("html"),
                }
            else:
                finder = finder_cls(timeout=6)
                if hasattr(finder, 'set_hint'):
                    finder.set_hint(url)
                disc = {"url": None, "method": "not_run", "html": None}
                try:
                    disc = await finder.find(domain, company_name)
                except Exception as e:
                    disc = {"url": None, "method": f"error:{str(e)[:40]}", "html": None}
                # Cache only successful discoveries; 1h TTL is plenty for a single run
                if _disc_cache_key and disc.get("url") and disc.get("html"):
                    try:
                        _r.setex(_disc_cache_key, 3600, _json.dumps({
                            "url": disc["url"],
                            "method": disc.get("method"),
                            "html": disc["html"],
                        }, default=str))
                    except Exception:
                        pass

            f_url, f_html = disc["url"], disc["html"]
            phase_dict["url_found"] = f_url
            phase_dict["discovery_method"] = disc["method"]

            if not f_url:
                phase_dict["error"] = "Could not discover careers page"
                return
            if not f_html or len(f_html) < 200:
                try:
                    from app.crawlers.career_page_finder_v2 import CareerPageFinderV2
                    rendered = await CareerPageFinderV2._try_playwright(f_url)
                    if rendered and len(rendered) > 200:
                        f_html = rendered
                        phase_dict["discovery_method"] = (phase_dict.get("discovery_method") or "") + "+playwright"
                except Exception:
                    pass
            if not f_html or len(f_html) < 200:
                phase_dict["error"] = f"Page too short ({len(f_html or '')} bytes)"
                return

            try:
                class _P:
                    def __init__(s): s.url = f_url; s.id = None
                class _C:
                    def __init__(s): s.ats_platform = None; s.name = company_name
                jobs = await ext.extract(_P(), _C(), f_html)
                phase_dict["jobs"] = len(jobs)
                phase_dict["fields"] = _field_coverage(jobs)
                phase_dict["sample_titles"] = [j["title"][:80] for j in jobs[:5]]
                phase_dict["extracted_jobs"] = _truncate_jobs(jobs)
                if jobs:
                    phase_dict["tier_used"] = jobs[0].get("extraction_method", "unknown")
            except Exception as e:
                phase_dict["error"] = str(e)[:150]

        # ── Phase B: Champion ──
        if champion_extractor and entry["champion"] is not None:
            try:
                await asyncio.wait_for(
                    _run_model_phase(champion_extractor, champion_finder_cls, entry["champion"]),
                    timeout=60,
                )
            except asyncio.TimeoutError:
                entry["champion"]["error"] = "Phase timeout (60s)"

            cf = entry["champion"].get("fields", {})
            cc = cf.get("_core_complete", 0)
            cr = entry["champion"].get("jobs", 0)
            ce = entry["champion"].get("extracted_jobs", [])
            creal = cr
            if ce:
                creal = _count_real_jobs(ce)
                if creal < len(ce):
                    entry["champion"]["quality_warning"] = f"Only {creal}/{len(ce)} titles look like real jobs"
                    entry["champion"]["real_jobs"] = creal
            entry["champion"]["jobs_quality"] = min(creal, cc) if cc > 0 else creal
            entry["champion"]["jobs_complete"] = cc

        # ── Phase C: Challenger ──
        try:
            await asyncio.wait_for(
                _run_model_phase(extractor, challenger_finder_cls, entry["model"]),
                timeout=60,
            )
        except asyncio.TimeoutError:
            entry["model"]["error"] = "Phase timeout (60s)"

        mf = entry["model"].get("fields", {})
        mc = mf.get("_core_complete", 0)
        mr = entry["model"].get("jobs", 0)
        me = entry["model"].get("extracted_jobs", [])
        mreal = mr
        if me:
            mreal = _count_real_jobs(me)
            if mreal < len(me):
                entry["model"]["quality_warning"] = f"Only {mreal}/{len(me)} titles look like real jobs"
                entry["model"]["real_jobs"] = mreal

        has_mf = bool(mf)
        mj = mc if has_mf else mreal
        entry["model"]["jobs_quality"] = mj
        entry["model"]["jobs_complete"] = mc

        bf = entry["baseline"].get("fields", {})
        has_bf = bool(bf)
        bc = bf.get("_core_complete", 0)
        bj = bc if has_bf else entry["baseline"]["jobs"]
        if bj == 0 and mj == 0:
            entry["match"] = "both_failed"
        elif mj == 0:
            entry["match"] = "model_failed"
        elif bj == 0:
            entry["match"] = "model_only"
        elif mj >= bj * 0.9:
            entry["match"] = "model_equal_or_better"
        elif mj >= bj * 0.5:
            entry["match"] = "partial"
        else:
            entry["match"] = "model_worse"

    logger.info("Site %d/%s (%s): %s, B=%d M=%d",
                site_index, company_name, domain, entry["match"],
                entry["baseline"]["jobs"], entry["model"]["jobs"])
    return entry


@celery_app.task(
    name="ml.aggregate_test_results",
    time_limit=120,
    max_retries=0,
)
def aggregate_test_results(
    site_results: list[dict],
    run_id: str,
    model_id: str,
    model_name: str,
    champion_name: str | None,
    fixed_count: int,
    auto_improve: bool,
):
    """Collect all per-site results, compute scores, promote winner, save to DB.

    Called automatically by Celery chord after all test_single_site tasks complete.
    """
    import asyncio
    asyncio.run(_aggregate(site_results, run_id, model_id, model_name,
                           champion_name, fixed_count, auto_improve))


async def _aggregate(
    site_results: list[dict],
    run_id: str, model_id: str, model_name: str,
    champion_name: str | None, fixed_count: int, auto_improve: bool,
):
    from collections import Counter
    from uuid import UUID as _UUID
    from sqlalchemy import select
    from app.db.base import AsyncSessionLocal
    from app.models.ml_model import MLModel, MLModelTestRun
    from app.api.v1.endpoints.ml_models import (
        _composite_score_standalone,
        _composite_score_stratified,
        _cluster_gate_verdict,
    )
    from app.ml.champion_challenger import stability

    _run_id = _UUID(run_id)
    _model_id = _UUID(model_id)

    # Refine ats_platform using post-discovery signals — many entries were
    # tagged as None at test_single_site time because we hadn't fetched the
    # page yet. This is the one place we have all the evidence to bucket them.
    for entry in site_results:
        refined = _refine_entry_ats(entry)
        if refined:
            entry["ats_platform"] = refined

    def _phase_stats(results):
        match_counts = Counter(e["match"] for e in results)
        tier_counts = Counter(
            e["model"]["tier_used"] or "none"
            for e in results if e["model"]["jobs"] > 0
        )
        passed = sum(1 for e in results if e["match"] in ("model_equal_or_better", "model_only"))
        sites_any = sum(1 for e in results if e["model"].get("jobs_quality", e["model"]["jobs"]) > 0)
        return {
            "total_sites": len(results),
            "model_extracted": passed,
            "model_partial": sum(1 for e in results if e["match"] == "partial"),
            "model_failed": len(results) - sites_any,
            "accuracy": passed / max(1, len(results)),
            "match_breakdown": dict(match_counts),
            "tier_breakdown": dict(tier_counts),
            "jobs": {
                "baseline_total": sum(e["baseline"]["jobs"] for e in results),
                "model_total": sum(e["model"]["jobs"] for e in results),
                "ratio": round(sum(e["model"]["jobs"] for e in results) / max(1, sum(e["baseline"]["jobs"] for e in results)), 2),
            },
            "quality": {
                "baseline_core_complete": sum(e["baseline"]["fields"].get("_core_complete", 0) for e in results),
                "model_core_complete": sum(e["model"]["fields"].get("_core_complete", 0) for e in results),
            },
        }

    regression = site_results[:fixed_count]
    exploration = site_results[fixed_count:]
    summary = _phase_stats(site_results)
    summary["regression"] = _phase_stats(regression) if regression else None
    summary["exploration"] = _phase_stats(exploration) if exploration else None

    async with AsyncSessionLocal() as db:
        run = await db.get(MLModelTestRun, _run_id)
        if not run:
            logger.error("aggregate: test run %s not found", run_id)
            return

        passed = summary["model_extracted"]
        run.tests_passed = passed
        run.tests_failed = summary["model_failed"]
        run.accuracy = summary["accuracy"]
        run.status = "completed"
        run.completed_at = datetime.now(timezone.utc)

        model = await db.get(MLModel, _model_id)
        if model:
            model.status = "tested"

            ch_scores = _composite_score_standalone(site_results, "model")
            champ_scores = _composite_score_standalone(site_results, "champion") if champion_name else {
                "composite": 0, "discovery": 0, "quality_extraction": 0,
                "field_completeness": 0, "volume_accuracy": 0,
            }

            # Stratified scorecards — per-ATS composites feed the cluster gate.
            ch_strat = _composite_score_stratified(site_results, "model")
            champ_strat = (
                _composite_score_stratified(site_results, "champion")
                if champion_name else {"all": champ_scores, "by_stratum": {},
                                         "worst_gate_eligible": None,
                                         "n_strata_total": 0,
                                         "n_strata_gate_eligible": 0}
            )

            summary["challenger_composite"] = ch_scores
            summary["champion_composite"] = champ_scores
            summary["challenger_stratified"] = ch_strat
            summary["champion_stratified"] = champ_strat

            reg_stats = summary.get("regression")
            reg_acc = reg_stats["accuracy"] if reg_stats else summary["accuracy"]

            # ─── Gate 1: legacy regression gate (current-champion passes) ─────
            champ_passed_urls = set()
            challenger_missed = set()
            if champion_name:
                for sr in site_results:
                    champ_data = sr.get("champion") or {}
                    model_data = sr.get("model") or {}
                    champ_quality = champ_data.get("jobs_quality", champ_data.get("jobs", 0))
                    model_quality = model_data.get("jobs_quality", model_data.get("jobs", 0))
                    baseline_jobs = sr.get("baseline", {}).get("jobs", 0)
                    if baseline_jobs > 0 and champ_quality >= baseline_jobs * 0.9:
                        champ_passed_urls.add(sr["url"])
                        if model_quality < baseline_jobs * 0.9:
                            challenger_missed.add(sr.get("company", sr["url"]))

            regression_ok = len(challenger_missed) == 0
            if challenger_missed:
                logger.warning("Regression: challenger missed %d sites champion passed: %s",
                               len(challenger_missed), ", ".join(list(challenger_missed)[:5]))

            # ─── Gate 2: per-stratum cluster gate ─────────────────────────────
            cluster_verdict = _cluster_gate_verdict(ch_strat, champ_strat)
            summary["cluster_gate"] = cluster_verdict
            if not cluster_verdict["passed"]:
                regs = cluster_verdict["regressions"]
                logger.warning(
                    "Cluster gate FAIL — %d strata regressed: %s",
                    len(regs),
                    ", ".join(
                        f"{r['stratum']}({r['champ']:.1f}→{r['challenger']:.1f}, "
                        f"n={r['n']})" for r in regs[:5]
                    ),
                )

            # ─── Gate 3: ever-passed regression gate ──────────────────────────
            ever_regressions = await stability.fetch_ever_passed_regressions(
                db, site_results=site_results,
            )
            summary["ever_passed_regressions"] = [
                {"url": r["url"], "company": r["company"],
                 "ats_platform": r["ats_platform"],
                 "prev_version": r["prev_best_version"],
                 "prev_jobs_quality": r["prev_jobs_quality"],
                 "cur_jobs_quality": r["cur_jobs_quality"]}
                for r in ever_regressions
            ]
            ever_passed_ok = len(ever_regressions) == 0
            if ever_regressions:
                logger.warning(
                    "Ever-passed gate FAIL — challenger regressed %d sites some version had previously passed: %s",
                    len(ever_regressions),
                    ", ".join(r["company"] or r["url"] for r in ever_regressions[:5]),
                )

            # ─── Gate 4: oscillation detector ─────────────────────────────────
            # Sites that have flipped pass/fail ≥2 times in the last 5 runs are
            # 'unstable'. Block promotion if the challenger is *currently
            # failing* any of these — it's very likely about to cause the next
            # cycle of the oscillation loop.
            all_urls = [s.get("url") for s in site_results if s.get("url")]
            unstable = await stability.unstable_site_urls(db, urls=all_urls)
            oscillating_failures: list[str] = []
            for sr in site_results:
                url = sr.get("url") or ""
                if url in unstable and (sr.get("match") or "") not in (
                    "model_equal_or_better", "model_only"
                ):
                    oscillating_failures.append(sr.get("company") or url)
            summary["unstable_site_failures"] = oscillating_failures
            oscillation_ok = len(oscillating_failures) == 0
            if oscillating_failures:
                logger.warning(
                    "Oscillation gate FAIL — challenger failing %d unstable sites: %s",
                    len(oscillating_failures),
                    ", ".join(oscillating_failures[:5]),
                )

            should_promote = (
                ch_scores["composite"] > 0
                and reg_acc >= 0.60
                and ch_scores["composite"] > champ_scores["composite"]
                and regression_ok
                and cluster_verdict["passed"]
                and ever_passed_ok
                and oscillation_ok
            )

            summary["promotion_decision"] = {
                "promote": should_promote,
                "reasons": {
                    "composite_positive": ch_scores["composite"] > 0,
                    "regression_accuracy_ok": reg_acc >= 0.60,
                    "beats_champion": ch_scores["composite"] > champ_scores["composite"],
                    "no_champion_regressions": regression_ok,
                    "cluster_gate_ok": cluster_verdict["passed"],
                    "ever_passed_gate_ok": ever_passed_ok,
                    "oscillation_gate_ok": oscillation_ok,
                },
            }

            if should_promote:
                old_live = list(await db.scalars(
                    select(MLModel).where(MLModel.model_type == model.model_type, MLModel.status == "live")
                ))
                for old in old_live:
                    old.status = "tested"
                model.status = "live"
                logger.info(
                    "Auto-promoted %s to live (%.1f > %.1f, %d strata ok, ever-passed ok)",
                    model.name, ch_scores["composite"], champ_scores["composite"],
                    ch_strat["n_strata_gate_eligible"],
                )
            else:
                logger.info(
                    "Did NOT promote %s (%.1f vs %.1f, reg=%.2f, cluster=%s, ever=%s, osc=%s)",
                    model.name, ch_scores["composite"], champ_scores["composite"],
                    reg_acc, cluster_verdict["passed"], ever_passed_ok, oscillation_ok,
                )

            run.results_detail = {"sites": site_results, "summary": summary}

            # Universality infrastructure: record history, ratchet ever-passed
            # set. Done regardless of promotion decision — we learn from every
            # run.
            try:
                await stability.record_run_history(
                    db,
                    run_id=_run_id,
                    model_id=_model_id,
                    model_name=model_name,
                    site_results=site_results,
                )
                if should_promote or not champion_name:
                    # Only ratchet the ever-passed set when the challenger is
                    # validated (promoted) OR this is a first-run champion.
                    # Otherwise we'd pollute the set with unvalidated wins.
                    await stability.upsert_ever_passed(
                        db,
                        run_id=_run_id,
                        model_name=model_name,
                        site_results=site_results,
                    )
            except Exception as stab_err:  # noqa: BLE001 — advisory persistence
                logger.warning("stability persistence failed: %s", stab_err)

            await db.commit()

            if (run.test_config or {}).get("auto_improve", auto_improve):
                import os as _os, json as _j2
                trigger_dir = "/storage/auto_improve_triggers"
                _os.makedirs(trigger_dir, exist_ok=True)
                with open(_os.path.join(trigger_dir, f"{model_id}.trigger"), "w") as tf:
                    _j2.dump({
                        "model_id": model_id,
                        "model_name": model.name,
                        "triggered_at": datetime.now(timezone.utc).isoformat(),
                        "auto_improve": True,
                    }, tf)
                logger.info("Auto-improve trigger written for %s", model.name)

    logger.info("Test run %s completed: %d sites, %d passed", run_id, len(site_results), passed)


def execute_model_test(
    run_id: str,
    model_id: str,
    model_name: str,
    champion_name: str | None,
    pages_data: list,
    fixed_count: int,
    auto_improve: bool,
):
    """Dispatch parallel per-site test tasks. Called from API endpoint.

    Each site task stores its result in Redis and increments a counter.
    The last site to finish triggers aggregation automatically.
    """
    import redis
    from app.core.config import settings

    total = len(pages_data)

    # Initialize Redis counter
    r = redis.Redis.from_url(settings.CELERY_BROKER_URL)
    r.delete(f"test_run:{run_id}:results", f"test_run:{run_id}:count")

    # Dispatch all site tasks
    for idx, page in enumerate(pages_data):
        test_single_site.apply_async(
            kwargs={
                "site_data": page,
                "model_name": model_name,
                "champion_name": champion_name,
                "site_index": idx,
                "run_id": run_id,
                "model_id": model_id,
                "total_sites": total,
                "fixed_count": fixed_count,
                "auto_improve": auto_improve,
            },
            queue="ml_test",
        )

    logger.info("Dispatched %d parallel site tasks for run %s (model=%s, champion=%s)",
                total, run_id, model_name, champion_name)
