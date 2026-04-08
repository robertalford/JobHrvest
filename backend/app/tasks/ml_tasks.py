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


@celery_app.task(
    bind=True,
    name="ml.execute_test_run",
    time_limit=7200,      # 2 hour hard limit
    soft_time_limit=6900,  # 1h55m soft limit
    max_retries=0,
)
def execute_model_test(
    self,
    run_id: str,
    model_id: str,
    model_name: str,
    champion_name: str | None,
    pages_data: list,
    fixed_count: int,
    auto_improve: bool,
):
    """Execute an A/B model test as a Celery task (survives API restarts).

    Moved from asyncio.create_task inside the API endpoint to ensure tests
    complete even if the API container is rebuilt.
    """
    import asyncio

    async def _run():
        import json as _json
        import re
        from collections import Counter
        from uuid import UUID as _UUID
        from datetime import datetime as _dt, timezone as _tz

        from app.db.base import AsyncSessionLocal
        from app.models.ml_model import MLModel, MLModelTestRun
        from app.crawlers.tiered_extractor import TieredExtractor
        from app.crawlers.job_extractor import JobExtractor
        from sqlalchemy import select

        # --- Import helpers from the endpoint module ---
        from app.api.v1.endpoints.ml_models import (
            _field_coverage, _truncate_jobs, _count_real_jobs,
            _execute_baseline_with_steps, _parse_html_safe, _find_next_url,
            _composite_score_standalone,
        )

        _run_id = _UUID(run_id)
        _model_id = _UUID(model_id)

        # --- Reconstruct extractors ---
        def _pick_extractor(name: str):
            import importlib
            match = re.search(r"v(\d+)\.(\d+)", name)
            if match:
                major, minor = int(match.group(1)), int(match.group(2))
                file_ver = major * 10 + minor
                module_name = f"app.crawlers.tiered_extractor_v{file_ver}"
                class_name = f"TieredExtractorV{file_ver}"
                try:
                    mod = importlib.import_module(module_name)
                    return getattr(mod, class_name)
                except (ImportError, AttributeError):
                    pass
            return TieredExtractor

        _FINDER_MAP = {
            82: 82, 81: 81, 80: 80, 79: 79, 78: 78, 77: 77, 76: 76,
            75: 75, 74: 74, 73: 73, 72: 72, 71: 71, 70: 70, 69: 69,
            68: 68, 67: 67, 66: 66, 65: 65, 64: 64, 63: 63, 62: 62,
            61: 61, 60: 60, 20: 20, 17: 5, 16: 4, 15: 3, 14: 2, 13: 2, 12: 2,
        }

        def _pick_finder(name: str):
            import importlib
            match = re.search(r"v(\d+)\.(\d+)", name)
            if match:
                major, minor = int(match.group(1)), int(match.group(2))
                file_ver = major * 10 + minor
                finder_ver = _FINDER_MAP.get(file_ver, file_ver)
                module_name = f"app.crawlers.career_page_finder_v{finder_ver}"
                class_name = f"CareerPageFinderV{finder_ver}"
                try:
                    mod = importlib.import_module(module_name)
                    return getattr(mod, class_name)
                except (ImportError, AttributeError):
                    pass
            from app.crawlers.career_page_finder import CareerPageFinder
            return CareerPageFinder

        extractor_cls = _pick_extractor(model_name)
        champion_cls = _pick_extractor(champion_name) if champion_name else None
        challenger_finder_cls = _pick_finder(model_name)
        champion_finder_cls = _pick_finder(champion_name) if champion_name else challenger_finder_cls

        # --- Reconstruct pages from serialized data ---
        pages = []
        for row in pages_data:
            url, company, sel_str = row[0], row[1], row[2]
            try:
                known = _json.loads(sel_str) if isinstance(sel_str, str) else sel_str
            except Exception:
                known = {}
            pages.append((url, company, known))

        # --- Build summary helper ---
        def _build_summary(site_results: list[dict]) -> dict:
            def _phase_stats(results):
                match_counts = Counter(e["match"] for e in results)
                tier_counts = Counter(
                    e["model"]["tier_used"] or "none"
                    for e in results if e["model"]["jobs"] > 0
                )
                passed = sum(1 for e in results if e["match"] in ("model_equal_or_better", "model_only"))
                sites_with_any_jobs = sum(1 for e in results if e["model"].get("jobs_quality", e["model"]["jobs"]) > 0)
                return {
                    "total_sites": len(results),
                    "model_extracted": passed,
                    "model_partial": sum(1 for e in results if e["match"] == "partial"),
                    "model_failed": len(results) - sites_with_any_jobs,
                    "accuracy": passed / max(1, len(results)),
                    "match_breakdown": dict(match_counts),
                    "tier_breakdown": dict(tier_counts),
                    "jobs": {
                        "baseline_total": sum(e["baseline"]["jobs"] for e in results),
                        "model_total": sum(e["model"]["jobs"] for e in results),
                        "ratio": round(
                            sum(e["model"]["jobs"] for e in results)
                            / max(1, sum(e["baseline"]["jobs"] for e in results)),
                            2,
                        ),
                    },
                    "quality": {
                        "baseline_core_complete": sum(e["baseline"]["fields"].get("_core_complete", 0) for e in results),
                        "model_core_complete": sum(e["model"]["fields"].get("_core_complete", 0) for e in results),
                    },
                }
            regression_results = site_results[:fixed_count]
            exploration_results = site_results[fixed_count:]
            overall = _phase_stats(site_results)
            overall["regression"] = _phase_stats(regression_results) if regression_results else None
            overall["exploration"] = _phase_stats(exploration_results) if exploration_results else None
            return overall

        # --- Flush progress helper ---
        async def _flush_progress(site_results: list[dict], done: int, total: int):
            async with AsyncSessionLocal() as flush_db:
                flush_run = await flush_db.get(MLModelTestRun, _run_id)
                if not flush_run:
                    return
                summary = _build_summary(site_results)
                flush_run.tests_passed = summary["model_extracted"]
                flush_run.tests_failed = summary["model_failed"]
                flush_run.accuracy = summary["accuracy"]
                flush_run.results_detail = {
                    "sites": site_results,
                    "summary": summary,
                    "progress": {"done": done, "total": total},
                }
                await flush_db.commit()

        # --- Main extraction loop (moved from ml_models.py _execute()) ---
        import httpx

        extractor = extractor_cls()
        champion_extractor = champion_cls() if champion_cls else None
        site_results: list[dict] = []
        total_pages = len(pages)

        logger.info("Starting test run %s: %d sites, model=%s, champion=%s",
                     run_id, total_pages, model_name, champion_name)

        async with httpx.AsyncClient(
            timeout=20, follow_redirects=True, verify=False,
            headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"},
        ) as client:
            for idx, row in enumerate(pages):
                url, company_name, known = row[0], row[1], row[2]
                if isinstance(known, str):
                    try:
                        known = _json.loads(known)
                    except Exception:
                        known = {}

                from app.crawlers.career_page_finder import extract_domain
                domain = extract_domain(url)

                entry = {
                    "url": url,
                    "domain": domain,
                    "company": company_name,
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

                # ── Phase A: Baseline ──
                try:
                    baseline_html = await _execute_baseline_with_steps(url, known, client)
                    if len(baseline_html) > 200:
                        entry["http_ok"] = True
                        baseline_jobs = JobExtractor._static_extract_wrapper(baseline_html, url, known)

                        # Pagination
                        next_page_sel = known.get("next_page_path", "")
                        if next_page_sel and next_page_sel not in ("null", "", "//no-next-page", "#none", "/nonextpagelink"):
                            try:
                                pages_followed = 0
                                seen = {url}
                                current_html = baseline_html
                                current_url = url
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
                                        current_html = nr.text
                                        current_url = next_url
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

                            def _try_detail_selectors(detail_html, detail_url, sel_list):
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

                            for job in baseline_jobs[:50]:
                                detail_url = job.get("source_url", "")
                                if not detail_url or detail_url == url:
                                    continue
                                needs_loc = not job.get("location_raw")
                                needs_desc = not job.get("description") or len(job.get("description", "")) < 50
                                if not needs_loc and not needs_desc:
                                    continue
                                try:
                                    dr = await client.get(detail_url, timeout=8)
                                    if dr.status_code != 200 or len(dr.text) < 200:
                                        continue
                                    if needs_desc and detail_desc_sels:
                                        desc = _try_detail_selectors(dr.text, detail_url, detail_desc_sels)
                                        if desc and len(desc) > 50:
                                            job["description"] = desc[:5000]
                                    if needs_loc and detail_loc_sels:
                                        loc = _try_detail_selectors(dr.text, detail_url, detail_loc_sels)
                                        if loc and len(loc) > 1 and len(loc) < 200:
                                            job["location_raw"] = loc
                                    if detail_salary_sel and not job.get("salary_raw"):
                                        sal = _try_detail_selectors(dr.text, detail_url, _parse(detail_salary_sel))
                                        if sal:
                                            job["salary_raw"] = sal
                                    if detail_type_sels and not job.get("employment_type"):
                                        jtype = _try_detail_selectors(dr.text, detail_url, detail_type_sels)
                                        if jtype:
                                            job["employment_type"] = jtype
                                except Exception:
                                    continue

                        entry["baseline"]["jobs"] = len(baseline_jobs)
                        entry["baseline"]["fields"] = _field_coverage(baseline_jobs)
                        entry["baseline"]["sample_titles"] = [j["title"][:80] for j in baseline_jobs[:5]]
                        entry["baseline"]["extracted_jobs"] = _truncate_jobs(baseline_jobs)
                except Exception:
                    pass

                # ── Helper: run discovery + extraction for a model ──
                async def _run_model_phase(ext, finder_cls, phase_dict):
                    finder = finder_cls(timeout=6)
                    if hasattr(finder, 'set_hint'):
                        finder.set_hint(url)
                    disc = {"url": None, "method": "not_run", "html": None}
                    try:
                        disc = await finder.find(domain, company_name)
                    except Exception as e:
                        disc = {"url": None, "method": f"error:{str(e)[:40]}", "html": None}

                    f_url = disc["url"]
                    f_html = disc["html"]
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
                        phase_dict["error"] = f"Page too short ({len(f_html or '')} bytes), even after Playwright"
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

                    champ_fields = entry["champion"].get("fields", {})
                    champ_complete = champ_fields.get("_core_complete", 0)
                    champ_raw = entry["champion"].get("jobs", 0)
                    champ_extracted = entry["champion"].get("extracted_jobs", [])
                    champ_real = champ_raw
                    if champ_extracted:
                        champ_real = _count_real_jobs(champ_extracted)
                        if champ_real < len(champ_extracted):
                            entry["champion"]["quality_warning"] = f"Only {champ_real}/{len(champ_extracted)} titles look like real jobs"
                            entry["champion"]["real_jobs"] = champ_real
                    entry["champion"]["jobs_quality"] = min(champ_real, champ_complete) if champ_complete > 0 else champ_real
                    entry["champion"]["jobs_complete"] = champ_complete

                # ── Phase C: Challenger ──
                try:
                    await asyncio.wait_for(
                        _run_model_phase(extractor, challenger_finder_cls, entry["model"]),
                        timeout=60,
                    )
                except asyncio.TimeoutError:
                    entry["model"]["error"] = "Phase timeout (60s)"

                model_fields = entry["model"].get("fields", {})
                model_complete = model_fields.get("_core_complete", 0)
                model_raw = entry["model"].get("jobs", 0)
                model_extracted = entry["model"].get("extracted_jobs", [])
                real_title_count = model_raw
                if model_extracted:
                    real_title_count = _count_real_jobs(model_extracted)
                    if real_title_count < len(model_extracted):
                        entry["model"]["quality_warning"] = f"Only {real_title_count}/{len(model_extracted)} titles look like real jobs"
                        entry["model"]["real_jobs"] = real_title_count

                has_model_fields = bool(model_fields)
                mj_for_match = model_complete if has_model_fields else real_title_count
                entry["model"]["jobs_quality"] = mj_for_match
                entry["model"]["jobs_complete"] = model_complete

                baseline_fields = entry["baseline"].get("fields", {})
                has_baseline_fields = bool(baseline_fields)
                baseline_complete = baseline_fields.get("_core_complete", 0)
                bj = baseline_complete if has_baseline_fields else entry["baseline"]["jobs"]
                mj = mj_for_match
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

                site_results.append(entry)
                await _flush_progress(site_results, idx + 1, total_pages)

        # --- Final flush as completed ---
        async with AsyncSessionLocal() as bg_db:
            bg_run = await bg_db.get(MLModelTestRun, _run_id)
            summary = _build_summary(site_results)
            passed = summary["model_extracted"]
            bg_run.tests_passed = passed
            bg_run.tests_failed = summary["model_failed"]
            bg_run.accuracy = summary["accuracy"]
            bg_run.status = "completed"
            bg_run.completed_at = _dt.now(_tz.utc)

            bg_model = await bg_db.get(MLModel, _model_id)
            if bg_model:
                bg_model.status = "tested"

                challenger_scores = _composite_score_standalone(site_results, "model")
                champion_scores = _composite_score_standalone(site_results, "champion") if champion_name else {
                    "composite": 0, "discovery": 0, "quality_extraction": 0,
                    "field_completeness": 0, "volume_accuracy": 0,
                }

                summary["challenger_composite"] = challenger_scores
                summary["champion_composite"] = champion_scores

                reg_stats = summary.get("regression")
                challenger_reg_acc = reg_stats["accuracy"] if reg_stats else summary["accuracy"]

                should_promote = (
                    challenger_scores["composite"] > 0
                    and challenger_reg_acc >= 0.60
                    and challenger_scores["composite"] > champion_scores["composite"]
                )

                if should_promote:
                    old_live = list(await bg_db.scalars(
                        select(MLModel).where(
                            MLModel.model_type == bg_model.model_type,
                            MLModel.status == "live",
                        )
                    ))
                    for old in old_live:
                        old.status = "tested"
                    bg_model.status = "live"
                    logger.info(
                        "Auto-promoted %s to live (composite %.1f > %.1f) "
                        "[disc=%.0f%% qual=%.0f%% fields=%.0f%% vol=%.0f%%]",
                        bg_model.name, challenger_scores["composite"],
                        champion_scores["composite"],
                        challenger_scores["discovery"], challenger_scores["quality_extraction"],
                        challenger_scores["field_completeness"], challenger_scores["volume_accuracy"],
                    )
                else:
                    logger.info(
                        "Did NOT promote %s (composite %.1f vs champion %.1f, reg_acc=%.2f)",
                        bg_model.name, challenger_scores["composite"],
                        champion_scores["composite"], challenger_reg_acc,
                    )

                bg_run.results_detail = {"sites": site_results, "summary": summary}
                await bg_db.commit()

                _should_auto_improve = (bg_run.test_config or {}).get("auto_improve", False) if bg_run else auto_improve
                if _should_auto_improve:
                    import os as _os
                    trigger_dir = "/storage/auto_improve_triggers"
                    _os.makedirs(trigger_dir, exist_ok=True)
                    import json as _j2
                    with open(_os.path.join(trigger_dir, f"{model_id}.trigger"), "w") as _tf:
                        _j2.dump({
                            "model_id": model_id,
                            "model_name": bg_model.name if bg_model else model_id,
                            "triggered_at": _dt.now(_tz.utc).isoformat(),
                            "auto_improve": True,
                        }, _tf)
                    logger.info("Auto-improve trigger written for %s", bg_model.name if bg_model else model_id)

        logger.info("Test run %s completed: %d sites, %d passed", run_id, total_pages, passed)

    try:
        asyncio.run(_run())
    except Exception as e:
        logger.error("Test run %s failed: %s", run_id, e, exc_info=True)
        # Mark the run as failed in DB
        import asyncio as _aio

        async def _mark_failed():
            from app.db.base import AsyncSessionLocal
            from app.models.ml_model import MLModelTestRun
            from uuid import UUID as _UUID
            async with AsyncSessionLocal() as db:
                run = await db.get(MLModelTestRun, _UUID(run_id))
                if run and run.status == "running":
                    run.status = "completed"
                    run.completed_at = datetime.now(timezone)
                    run.error_message = f"Celery task failed: {str(e)[:200]}"
                    await db.commit()

        try:
            _aio.run(_mark_failed())
        except Exception:
            pass
        raise
