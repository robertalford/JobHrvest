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
