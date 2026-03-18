"""Crawl Celery tasks."""

import logging
from datetime import datetime, timezone

from app.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)


@celery_app.task(bind=True, max_retries=3, default_retry_delay=60)
def crawl_company(self, company_id: str):
    """Full crawl pipeline for a single company: ATS detection → career page discovery → job extraction."""
    from app.db.base import AsyncSessionLocal
    import asyncio

    async def _run():
        async with AsyncSessionLocal() as db:
            from app.models.company import Company
            from app.models.crawl_log import CrawlLog
            from sqlalchemy import select
            import uuid

            company = await db.get(Company, uuid.UUID(company_id))
            if not company:
                logger.warning(f"Company {company_id} not found")
                return

            log = CrawlLog(
                company_id=company.id,
                crawl_type="full_crawl",
                status="running",
                started_at=datetime.now(timezone.utc),
            )
            db.add(log)
            await db.commit()

            try:
                # Stage 1: ATS fingerprinting
                from app.crawlers.ats_fingerprinter import ATSFingerprinter
                fingerprinter = ATSFingerprinter()
                ats_result = await fingerprinter.fingerprint(company.root_url)
                if ats_result:
                    company.ats_platform = ats_result["platform"]
                    company.ats_confidence = ats_result["confidence"]

                # Stage 2: Career page discovery
                from app.crawlers.career_page_discoverer import CareerPageDiscoverer
                discoverer = CareerPageDiscoverer(db)
                career_pages = await discoverer.discover(company)

                # Stage 3: Job extraction for each career page
                from app.crawlers.job_extractor import JobExtractor
                extractor = JobExtractor(db)
                total_jobs = 0
                for page in career_pages:
                    jobs = await extractor.extract(company, page)
                    total_jobs += len(jobs)

                company.last_crawl_at = datetime.now(timezone.utc)
                log.status = "success"
                log.completed_at = datetime.now(timezone.utc)
                log.jobs_found = total_jobs
                await db.commit()

            except Exception as e:
                logger.error(f"Crawl failed for {company_id}: {e}")
                log.status = "failed"
                log.error_message = str(e)
                log.completed_at = datetime.now(timezone.utc)
                await db.commit()
                raise self.retry(exc=e)

    import asyncio
    asyncio.run(_run())


@celery_app.task(bind=True, max_retries=3)
def crawl_career_page(self, career_page_id: str):
    """Crawl a specific career page for new/updated job listings."""
    import asyncio

    async def _run():
        async with __import__("app.db.base", fromlist=["AsyncSessionLocal"]).AsyncSessionLocal() as db:
            from app.models.career_page import CareerPage
            from app.models.company import Company
            from app.crawlers.job_extractor import JobExtractor
            import uuid

            page = await db.get(CareerPage, uuid.UUID(career_page_id))
            if not page:
                return
            company = await db.get(Company, page.company_id)
            extractor = JobExtractor(db)
            await extractor.extract(company, page)

    asyncio.run(_run())


@celery_app.task
def full_crawl_cycle():
    """Trigger crawl for all companies whose next_crawl_at has passed."""
    import asyncio

    async def _run():
        from app.db.base import AsyncSessionLocal
        from app.models.company import Company
        from sqlalchemy import select
        from datetime import datetime, timezone

        async with AsyncSessionLocal() as db:
            due = await db.scalars(
                select(Company).where(
                    Company.is_active == True,
                    (Company.next_crawl_at <= datetime.now(timezone.utc)) | (Company.next_crawl_at.is_(None)),
                )
            )
            for company in due:
                crawl_company.delay(str(company.id))

    asyncio.run(_run())


@celery_app.task
def scheduled_crawl_cycle():
    """Celery Beat task — runs the full crawl cycle on schedule."""
    full_crawl_cycle.delay()


@celery_app.task
def validate_page_template(career_page_id: str):
    """Validate an existing site template against a fresh LLM extraction."""
    logger.info(f"Validating template for career page {career_page_id}")
    # Phase 4 implementation
