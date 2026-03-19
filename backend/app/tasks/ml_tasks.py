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

    asyncio.get_event_loop().run_until_complete(_run())


@celery_app.task
def score_all_jobs():
    """Backfill quality scores for ALL unscored jobs (may take a while)."""
    logger.info("Starting full quality scoring backfill.")
    batch = 1000
    for _ in range(100):  # max 100 batches = 100k jobs
        score_jobs_batch(limit=batch)
