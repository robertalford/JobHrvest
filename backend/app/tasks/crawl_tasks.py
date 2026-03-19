"""Crawl Celery tasks — full pipeline wiring."""

import asyncio
import logging
import uuid
from datetime import datetime, timedelta, timezone

from app.tasks.celery_app import celery_app

logger = logging.getLogger(__name__)


def _run_async(coro):
    """Run a coroutine in a new event loop (Celery workers are sync)."""
    return asyncio.run(coro)


@celery_app.task(bind=True, max_retries=3, default_retry_delay=120, name="crawl.company")
def crawl_company(self, company_id: str):
    """Full crawl pipeline: ATS detection → career page discovery → job extraction."""

    async def _run():
        from app.db.base import AsyncSessionLocal
        from app.models.company import Company
        from app.models.crawl_log import CrawlLog
        from app.crawlers.ats_fingerprinter import ATSFingerprinter
        from app.crawlers.career_page_discoverer import CareerPageDiscoverer
        from app.crawlers.job_extractor import JobExtractor

        async with AsyncSessionLocal() as db:
            company = await db.get(Company, uuid.UUID(company_id))
            if not company or not company.is_active:
                return

            log = CrawlLog(
                company_id=company.id,
                crawl_type="full_crawl",
                status="running",
                started_at=datetime.now(timezone.utc),
            )
            db.add(log)
            await db.commit()

            started_at = datetime.now(timezone.utc)
            total_jobs = 0

            try:
                # Stage 1: ATS fingerprinting
                if not company.ats_platform or company.ats_platform == "unknown":
                    fingerprinter = ATSFingerprinter()
                    ats_result = await fingerprinter.fingerprint(company.root_url)
                    if ats_result and ats_result.get("platform") != "unknown":
                        company.ats_platform = ats_result["platform"]
                        company.ats_confidence = ats_result["confidence"]
                        await db.commit()

                # Stage 2: Career page discovery
                discoverer = CareerPageDiscoverer(db)
                career_pages = await discoverer.discover(company)
                log.pages_crawled = len(career_pages)

                # Stage 3+4: Job extraction for each page
                extractor = JobExtractor(db)
                jobs_new = 0
                for page in career_pages:
                    if not page.is_active:
                        continue
                    jobs = await extractor.extract(company, page)
                    jobs_new += len([j for j in jobs if j.created_at >= started_at])
                    total_jobs += len(jobs)

                # Update schedule: next crawl based on frequency
                company.last_crawl_at = datetime.now(timezone.utc)
                company.next_crawl_at = datetime.now(timezone.utc) + timedelta(hours=company.crawl_frequency_hours)

                log.status = "success"
                log.completed_at = datetime.now(timezone.utc)
                log.jobs_found = total_jobs
                log.jobs_new = jobs_new
                log.duration_seconds = (datetime.now(timezone.utc) - started_at).total_seconds()
                await db.commit()

                logger.info(f"Crawl complete for {company.domain}: {total_jobs} jobs ({jobs_new} new)")

            except Exception as e:
                logger.error(f"Crawl failed for {company_id}: {e}", exc_info=True)
                log.status = "failed"
                log.error_message = str(e)[:500]
                log.completed_at = datetime.now(timezone.utc)
                log.duration_seconds = (datetime.now(timezone.utc) - started_at).total_seconds()
                await db.commit()
                raise self.retry(exc=e)

    _run_async(_run())


@celery_app.task(bind=True, max_retries=3, name="crawl.career_page")
def crawl_career_page(self, career_page_id: str):
    """Crawl a specific career page for new/updated job listings."""

    async def _run():
        from app.db.base import AsyncSessionLocal
        from app.models.career_page import CareerPage
        from app.models.company import Company
        from app.crawlers.job_extractor import JobExtractor

        async with AsyncSessionLocal() as db:
            page = await db.get(CareerPage, uuid.UUID(career_page_id))
            if not page or not page.is_active:
                return
            company = await db.get(Company, page.company_id)
            if not company:
                return
            extractor = JobExtractor(db)
            await extractor.extract(company, page)

    _run_async(_run())


@celery_app.task(name="crawl.full_cycle")
def full_crawl_cycle():
    """Trigger crawl for all companies whose next_crawl_at has passed."""

    async def _run():
        from app.db.base import AsyncSessionLocal
        from app.models.company import Company
        from sqlalchemy import select

        async with AsyncSessionLocal() as db:
            due_companies = await db.scalars(
                select(Company).where(
                    Company.is_active == True,
                    (Company.next_crawl_at <= datetime.now(timezone.utc)) | (Company.next_crawl_at.is_(None)),
                ).order_by(Company.crawl_priority.asc())
            )
            count = 0
            for company in due_companies:
                crawl_company.apply_async(args=[str(company.id)], countdown=count * 2)
                count += 1
            logger.info(f"Queued crawls for {count} companies")

    _run_async(_run())


@celery_app.task(name="crawl.scheduled")
def scheduled_crawl_cycle():
    """Celery Beat periodic task."""
    full_crawl_cycle.delay()


@celery_app.task(bind=True, name="crawl.harvest_aggregators")
def harvest_aggregators(self, queries: list[str] = None):
    """
    Harvest company links from permitted aggregator sites (Indeed AU first).
    This task discovers new companies by following aggregator outbound links.
    """

    async def _run():
        from app.db.base import AsyncSessionLocal
        from app.crawlers.aggregator_harvester import IndeedAUHarvester

        default_queries = [
            "software engineer", "data analyst", "project manager",
            "marketing manager", "finance", "operations", "sales",
            "engineer", "developer", "designer",
        ]
        search_queries = queries or default_queries

        async with AsyncSessionLocal() as db:
            harvester = IndeedAUHarvester()
            total_discovered = 0
            for query in search_queries:
                discovered = await harvester.harvest(db, query=query, max_pages=3)
                total_discovered += len(discovered)
                logger.info(f"Indeed AU harvest '{query}': {len(discovered)} companies discovered")

            logger.info(f"Aggregator harvest complete: {total_discovered} total discoveries")

    _run_async(_run())


@celery_app.task(name="crawl.validate_page_template")
def validate_page_template(career_page_id: str):
    """Validate an existing site template against fresh LLM extraction."""

    async def _run():
        from app.db.base import AsyncSessionLocal
        from app.models.career_page import CareerPage
        from app.models.company import Company
        from app.models.site_template import SiteTemplate
        from app.extractors.template_learner import TemplateLearner
        from app.extractors.llm_extractor import LLMJobExtractor
        from app.crawlers.http_client import ResilientHTTPClient
        from sqlalchemy import select
        from markdownify import markdownify

        async with AsyncSessionLocal() as db:
            page = await db.get(CareerPage, uuid.UUID(career_page_id))
            if not page:
                return
            company = await db.get(Company, page.company_id)
            template = await db.scalar(
                select(SiteTemplate).where(
                    SiteTemplate.career_page_id == page.id,
                    SiteTemplate.is_active == True,
                )
            )
            if not template:
                return

            # Fetch and compare
            client = ResilientHTTPClient()
            llm = LLMJobExtractor()
            learner = TemplateLearner()

            try:
                resp = await client.get(page.url)
                html = resp.text
                md = markdownify(html, strip=["script", "style"])

                template_result = learner.extract_with_template(html, template.selectors)
                llm_result = await llm.extract(page.url, md)

                if llm_result:
                    accuracy = learner.calculate_template_accuracy(template_result, llm_result)
                    template.accuracy_score = accuracy
                    template.last_validated_at = datetime.now(timezone.utc)

                    # Deactivate template if accuracy is too low
                    if accuracy < 0.5:
                        logger.warning(f"Template accuracy {accuracy:.2f} for {page.url} — deactivating")
                        template.is_active = False

                    await db.commit()
                    logger.info(f"Template validation for {page.url}: accuracy={accuracy:.2f}")
            except Exception as e:
                logger.error(f"Template validation failed for {career_page_id}: {e}")

    _run_async(_run())


@celery_app.task(name="crawl.mark_inactive_jobs")
def mark_inactive_jobs():
    """
    Mark jobs as inactive if they haven't been seen in 3+ consecutive crawl cycles.
    Runs periodically as part of job lifecycle tracking.
    """

    async def _run():
        from app.db.base import AsyncSessionLocal
        from app.models.job import Job
        from sqlalchemy import update

        cutoff = datetime.now(timezone.utc) - timedelta(days=7)

        async with AsyncSessionLocal() as db:
            result = await db.execute(
                update(Job)
                .where(Job.is_active == True, Job.last_seen_at < cutoff)
                .values(is_active=False)
                .returning(Job.id)
            )
            removed = len(result.fetchall())
            await db.commit()
            if removed:
                logger.info(f"Marked {removed} jobs as inactive (not seen in 7+ days)")

    _run_async(_run())
