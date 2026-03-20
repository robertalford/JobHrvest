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

                # Fallback: if discovery found nothing, check DB for existing pages
                # (company may already have pages from a prior crawl or seed data)
                if not career_pages:
                    from sqlalchemy import select as sa_select
                    from app.models.career_page import CareerPage
                    existing = list(await db.scalars(
                        sa_select(CareerPage).where(
                            CareerPage.company_id == company.id,
                            CareerPage.is_active == True,
                        )
                    ))
                    if existing:
                        career_pages = existing
                        logger.info(f"Using {len(existing)} existing career pages for {company.domain}")
                    else:
                        # Last resort: treat company root URL as the career page
                        # (handles cases where domain IS the careers site, e.g. careers.company.com)
                        fallback = await discoverer._upsert_career_page(company, company.root_url, {
                            "discovery_method": "root_fallback",
                            "confidence": 0.4,
                            "is_primary": True,
                            "page_type": "listing_page",
                        })
                        career_pages = [fallback]
                        logger.info(f"Root URL fallback for {company.domain}: {company.root_url}")

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
                company.next_crawl_at = datetime.now(timezone.utc) + timedelta(hours=company.crawl_frequency_hours or 24)

                # Run deduplication after extraction
                if total_jobs > 0:
                    from app.services.job_deduplicator import run_company_dedup
                    await run_company_dedup(db, company.id)

                # Score new jobs immediately after extraction + dedup
                if total_jobs > 0:
                    from app.services.quality_scorer import score_job, compute_site_quality
                    from sqlalchemy import select as sa_select, or_ as sa_or
                    from app.models.job import Job

                    unscored = list(await db.scalars(
                        sa_select(Job).where(
                            Job.company_id == company.id,
                            sa_or(Job.quality_score.is_(None), Job.quality_scored_at.is_(None)),
                        )
                    ))
                    job_scores = []
                    has_scam = False
                    has_disc = False
                    for j in unscored:
                        qr = score_job(
                            title=j.title, description=j.description,
                            location_raw=j.location_raw, employment_type=j.employment_type,
                            date_posted=j.date_posted, salary_raw=j.salary_raw,
                            requirements=j.requirements,
                        )
                        j.quality_score = qr.score
                        j.quality_completeness = qr.completeness_score
                        j.quality_description = qr.description_score
                        j.quality_issues = qr.issues
                        j.quality_flags = {
                            "scam_detected": qr.scam_detected,
                            "bad_words_detected": qr.bad_words_detected,
                            "discrimination_detected": qr.discrimination_detected,
                        }
                        j.quality_scored_at = datetime.now(timezone.utc)
                        job_scores.append(qr.score)
                        if qr.scam_detected:
                            has_scam = True
                        if qr.discrimination_detected:
                            has_disc = True

                    if job_scores:
                        company.quality_score = compute_site_quality(job_scores, has_scam, has_disc)
                        company.quality_scored_at = datetime.now(timezone.utc)
                    await db.commit()
                    logger.info(f"Scored {len(unscored)} jobs for {company.domain}")

                # Geocode new jobs inline (best-effort; beat task catches any misses)
                if total_jobs > 0:
                    try:
                        from app.services.geocoder import geocoder_service
                        from sqlalchemy import select as sa_select2
                        from app.models.job import Job as JobModel

                        ungeo = list(await db.scalars(
                            sa_select2(JobModel).where(
                                JobModel.company_id == company.id,
                                JobModel.geo_resolved.is_(None),
                            )
                        ))
                        for j in ungeo:
                            loc_text = (j.location_raw or j.location_city or "").strip()
                            if loc_text:
                                geo = await geocoder_service.geocode(
                                    db, loc_text, company.market_code or "AU"
                                )
                                if geo:
                                    j.geo_location_id = uuid.UUID(geo.geo_location_id)
                                    j.geo_level = geo.level
                                    j.geo_confidence = geo.confidence
                                    j.geo_resolution_method = geo.method
                                    j.geo_resolved = True
                                else:
                                    j.geo_resolved = False
                                    j.geo_resolution_method = "unresolvable"
                            else:
                                j.geo_resolved = False
                                j.geo_resolution_method = "no_location"
                        if ungeo:
                            await db.commit()
                            resolved = sum(1 for j in ungeo if j.geo_resolved)
                            logger.info(f"Geocoded {resolved}/{len(ungeo)} jobs for {company.domain}")
                    except Exception as geo_exc:
                        logger.warning(f"Geocoding failed for {company.domain}: {geo_exc}")

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
        from app.models.excluded_site import ExcludedSite
        from app.crawlers.domain_blocklist import refresh_from_db_async
        from sqlalchemy import select

        # Refresh blocklist from DB before scheduling so newly added exclusions take effect
        await refresh_from_db_async()

        async with AsyncSessionLocal() as db:
            # Exclude companies whose domain appears in excluded_sites
            excluded_domains_sq = select(ExcludedSite.domain)
            due_companies = await db.scalars(
                select(Company).where(
                    Company.is_active == True,
                    (Company.next_crawl_at <= datetime.now(timezone.utc)) | (Company.next_crawl_at.is_(None)),
                    ~Company.domain.in_(excluded_domains_sq),
                ).order_by(Company.crawl_priority.asc())
            )
            count = 0
            for company in due_companies:
                # No countdown — queue all immediately, let worker concurrency + per-domain
                # rate limiting in the HTTP client control actual throughput.
                crawl_company.apply_async(args=[str(company.id)])
                count += 1
            logger.info(f"Queued crawls for {count} companies")
            return count

    return _run_async(_run())


@celery_app.task(name="crawl.scheduled")
def scheduled_crawl_cycle():
    """Celery Beat periodic task."""
    full_crawl_cycle.delay()


@celery_app.task(bind=True, name="crawl.harvest_aggregators")
def harvest_aggregators(self, market_code: str = "AU", queries: list[str] = None):
    """
    Harvest company links from aggregator sites for the given market.
    Uses market configuration to determine queries and aggregator sources.
    """

    async def _run():
        from app.db.base import AsyncSessionLocal
        from app.crawlers.aggregator_harvester import IndeedAUHarvester, LinkedInHarvester
        from app.core.markets import get_market
        from app.models.crawl_log import CrawlLog

        market = get_market(market_code)
        if not market or not market.is_active:
            logger.info(f"Market {market_code} not active, skipping harvest")
            return

        async with AsyncSessionLocal() as db:
            log = CrawlLog(
                crawl_type="discovery",
                status="running",
                started_at=datetime.now(timezone.utc),
            )
            db.add(log)
            await db.commit()

            started_at = datetime.now(timezone.utc)
            total_discovered = 0

            for agg_config in market.aggregators:
                if not agg_config.enabled:
                    continue

                # Route to the correct harvester class
                if "indeed" in agg_config.name.lower():
                    harvester = IndeedAUHarvester()
                    search_queries = queries or agg_config.search_queries
                    for query in search_queries:
                        discovered = await harvester.harvest(
                            db, query=query,
                            location=agg_config.location_param,
                            max_pages=agg_config.max_pages_per_query,
                        )
                        total_discovered += len(discovered)
                        logger.info(f"{agg_config.name} '{query}': {len(discovered)} companies")

                elif "linkedin" in agg_config.name.lower():
                    harvester = LinkedInHarvester()
                    search_queries = queries or agg_config.search_queries
                    for query in search_queries:
                        discovered = await harvester.harvest(
                            db, query=query,
                            max_pages=agg_config.max_pages_per_query,
                        )
                        total_discovered += len(discovered)
                        logger.info(f"{agg_config.name} '{query}': {len(discovered)} companies")

            log.status = "success"
            log.jobs_found = total_discovered
            log.completed_at = datetime.now(timezone.utc)
            log.duration_seconds = (datetime.now(timezone.utc) - started_at).total_seconds()
            await db.commit()
            logger.info(f"Aggregator harvest complete ({market_code}): {total_discovered} discoveries")

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


@celery_app.task(name="crawl.seed_market_companies")
def seed_market_companies(market_code: str = "AU"):
    """
    Seed initial companies from a market's seed_domains list.
    Safe to run multiple times — idempotent upsert.
    """

    async def _run():
        from app.db.base import AsyncSessionLocal
        from app.models.company import Company
        from app.core.markets import get_market
        from sqlalchemy import select

        market = get_market(market_code)
        if not market:
            logger.error(f"Unknown market code: {market_code}")
            return

        added = 0
        async with AsyncSessionLocal() as db:
            for domain in market.seed_domains:
                domain = domain.lstrip("www.").lower()
                existing = await db.scalar(select(Company).where(Company.domain == domain))
                if existing:
                    continue
                company = Company(
                    name=domain.split(".")[0].capitalize(),
                    domain=domain,
                    root_url=f"https://{domain}",
                    market_code=market.code,
                    discovered_via="seed",
                    crawl_priority=5,  # Medium priority
                )
                db.add(company)
                added += 1

            await db.commit()
            logger.info(f"Seeded {added} companies for market {market_code}")

    _run_async(_run())


@celery_app.task(bind=True, max_retries=2, default_retry_delay=60, name="crawl.fix_company_sites")
def fix_company_sites(self, company_id: str, queue_item_id: str = None):
    """Run CompanySiteExtractor for a single company to find/fix career page URLs."""

    async def _run():
        from app.db.base import AsyncSessionLocal
        from app.models.company import Company
        from app.models.crawl_log import CrawlLog
        from app.services.company_site_extractor import CompanySiteExtractor
        from app.services import queue_manager

        async with AsyncSessionLocal() as db:
            company = await db.get(Company, uuid.UUID(company_id))
            if not company or not company.is_active:
                if queue_item_id:
                    await queue_manager.complete(db, uuid.UUID(queue_item_id))
                    await db.commit()
                return

            # Clean up zombie running logs from previously crashed runs
            from sqlalchemy import text as sa_text
            await db.execute(sa_text("""
                UPDATE crawl_logs SET status='failed',
                    error_message='zombie: task restarted before previous run completed',
                    completed_at=NOW()
                WHERE company_id=:cid AND status='running'
                  AND started_at < NOW() - INTERVAL '2 hours'
            """), {"cid": str(company.id)})
            await db.commit()

            log = CrawlLog(
                company_id=company.id,
                crawl_type="company_config",
                status="running",
                started_at=datetime.now(timezone.utc),
            )
            db.add(log)
            await db.commit()

            started_at = datetime.now(timezone.utc)
            try:
                extractor = CompanySiteExtractor(db)
                pages = await extractor.extract(company)

                # CASCADE: enqueue each discovered career page into site_config
                for page in pages:
                    await queue_manager.enqueue(
                        db, "site_config", page.id,
                        priority=company.crawl_priority or 5,
                        added_by="company_config_cascade",
                    )

                log.status = "success"
                log.pages_crawled = len(pages)
                log.completed_at = datetime.now(timezone.utc)
                log.duration_seconds = (datetime.now(timezone.utc) - started_at).total_seconds()
                if queue_item_id:
                    await queue_manager.complete(db, uuid.UUID(queue_item_id))
                await db.commit()
                logger.info(f"fix_company_sites complete for {company.domain}: {len(pages)} pages → queued {len(pages)} for site_config")
            except Exception as e:
                logger.error(f"fix_company_sites failed for {company_id}: {e}", exc_info=True)
                log.status = "failed"
                log.error_message = str(e)[:500]
                log.completed_at = datetime.now(timezone.utc)
                log.duration_seconds = (datetime.now(timezone.utc) - started_at).total_seconds()
                if queue_item_id:
                    await queue_manager.fail(db, uuid.UUID(queue_item_id), str(e))
                await db.commit()
                raise self.retry(exc=e)

    _run_async(_run())


@celery_app.task(bind=True, max_retries=2, default_retry_delay=60, name="crawl.fix_site_structure")
def fix_site_structure(self, career_page_id: str, queue_item_id: str = None):
    """Run SiteStructureExtractor for a single career page to map job listing structure."""

    async def _run():
        from app.db.base import AsyncSessionLocal
        from app.models.career_page import CareerPage
        from app.models.company import Company
        from app.models.crawl_log import CrawlLog
        from app.services.site_structure_extractor import SiteStructureExtractor
        from app.services import queue_manager

        async with AsyncSessionLocal() as db:
            page = await db.get(CareerPage, uuid.UUID(career_page_id))
            if not page or not page.is_active:
                if queue_item_id:
                    await queue_manager.complete(db, uuid.UUID(queue_item_id))
                    await db.commit()
                return
            company = await db.get(Company, page.company_id)
            if not company:
                if queue_item_id:
                    await queue_manager.complete(db, uuid.UUID(queue_item_id))
                    await db.commit()
                return

            # ── Fast-skip: company already has another ok page ────────────────
            # If another career_page for this company is already ok, skip this
            # one to focus site_config capacity on companies with zero coverage.
            from sqlalchemy import text as _text_skip
            has_ok = await db.scalar(_text_skip("""
                SELECT EXISTS(
                    SELECT 1 FROM career_pages
                    WHERE company_id = :cid AND site_status = 'ok'
                      AND is_active = true AND id != :pid
                )
            """), {"cid": str(page.company_id), "pid": str(page.id)})
            if has_ok:
                if queue_item_id:
                    await queue_manager.complete(db, uuid.UUID(queue_item_id))
                await db.commit()
                logger.debug(f"Skipping {page.url}: company already has ok career page")
                return

            # ── Fast-skip: known-bad URL patterns ─────────────────────────────
            # These URL patterns almost never contain job listings. Skip them
            # instead of spending 2+ minutes running all extraction layers.
            _url = page.url or ""
            _BAD_PATTERNS = [
                "/wechat/ShareJob", "/wechat/share", "pagestamp=",
                "/saved-jobs", "/job-alerts", "/sign-in", "/login",
                "/register", "/forgot-password", "/content/dam/",
                "mailto:", "javascript:", "#content",
            ]
            if any(p in _url for p in _BAD_PATTERNS):
                from sqlalchemy import text as _text_bad
                await db.execute(
                    _text_bad("UPDATE career_pages SET site_status = 'no_structure_broken' WHERE id = :id"),
                    {"id": str(page.id)},
                )
                if queue_item_id:
                    await queue_manager.complete(db, uuid.UUID(queue_item_id))
                await db.commit()
                logger.debug(f"Skipping bad URL pattern: {_url}")
                return

            # Clean up zombie running logs from previously crashed runs
            from sqlalchemy import text as sa_text
            await db.execute(sa_text("""
                UPDATE crawl_logs SET status='failed',
                    error_message='zombie: task restarted before previous run completed',
                    completed_at=NOW()
                WHERE company_id=:cid AND crawl_type='site_config' AND status='running'
                  AND started_at < NOW() - INTERVAL '2 hours'
            """), {"cid": str(company.id)})
            await db.commit()

            log = CrawlLog(
                company_id=company.id,
                crawl_type="site_config",
                status="running",
                started_at=datetime.now(timezone.utc),
            )
            db.add(log)
            await db.commit()

            started_at = datetime.now(timezone.utc)
            try:
                extractor = SiteStructureExtractor(db)
                success = await extractor.extract(page)

                # CASCADE: enqueue career page into job_crawling regardless of structure success
                # (job extractor uses fallback methods when structure mapping fails)
                await queue_manager.enqueue(
                    db, "job_crawling", page.id,
                    priority=company.crawl_priority or 5,
                    added_by="site_config_cascade",
                )

                log.status = "success"
                log.completed_at = datetime.now(timezone.utc)
                log.duration_seconds = (datetime.now(timezone.utc) - started_at).total_seconds()
                if queue_item_id:
                    await queue_manager.complete(db, uuid.UUID(queue_item_id))
                await db.commit()
                logger.info(f"fix_site_structure complete for {page.url}: mapped={success} → queued for job_crawling")
            except Exception as e:
                logger.error(f"fix_site_structure failed for {career_page_id}: {e}", exc_info=True)
                log.status = "failed"
                log.error_message = str(e)[:500]
                log.completed_at = datetime.now(timezone.utc)
                log.duration_seconds = (datetime.now(timezone.utc) - started_at).total_seconds()
                if queue_item_id:
                    await queue_manager.fail(db, uuid.UUID(queue_item_id), str(e))
                await db.commit()
                raise self.retry(exc=e)

    _run_async(_run())


@celery_app.task(name="queue.drain_company_config")
def drain_company_config():
    """Drain company_config queue: claim batch and run CompanySiteExtractor for each."""
    async def _run():
        from app.db.base import AsyncSessionLocal
        from app.services import queue_manager
        async with AsyncSessionLocal() as db:
            items = await queue_manager.claim_batch(db, "company_config", batch_size=60)
            await db.commit()
            for row in items:
                queue_item_id, company_id = row[0], row[1]
                if company_id:
                    fix_company_sites.apply_async(args=[str(company_id), str(queue_item_id)])
    _run_async(_run())


@celery_app.task(name="queue.drain_site_config")
def drain_site_config():
    """Drain site_config queue: claim batch and run SiteStructureExtractor for each."""
    async def _run():
        from app.db.base import AsyncSessionLocal
        from app.services import queue_manager
        async with AsyncSessionLocal() as db:
            items = await queue_manager.claim_batch(db, "site_config", batch_size=150)
            await db.commit()
            for row in items:
                queue_item_id, page_id = row[0], row[1]
                if page_id:
                    fix_site_structure.apply_async(args=[str(page_id), str(queue_item_id)])
    _run_async(_run())


@celery_app.task(name="queue.drain_job_crawling")
def drain_job_crawling():
    """Drain job_crawling queue: claim batch and send to Celery.

    Runs every 5 seconds (beat) with batch_size=250.
    At 160 workers × 5s/page = 32 pages/s throughput → 50k sites in ~26 min.
    """
    async def _run():
        from app.db.base import AsyncSessionLocal
        from app.services import queue_manager
        async with AsyncSessionLocal() as db:
            items = await queue_manager.claim_batch(db, "job_crawling", batch_size=500)
            await db.commit()
            for row in items:
                queue_item_id, page_id = row[0], row[1]
                if page_id:
                    crawl_career_page_from_queue.apply_async(
                        args=[str(page_id), str(queue_item_id)],
                        queue="crawl_jobs",
                    )
    _run_async(_run())


@celery_app.task(name="queue.drain_discovery")
def drain_discovery():
    """Drain discovery queue: claim batch and run aggregator harvest for each source."""
    async def _run():
        from app.db.base import AsyncSessionLocal
        from app.services import queue_manager
        async with AsyncSessionLocal() as db:
            items = await queue_manager.claim_batch(db, "discovery", batch_size=5)
            await db.commit()
            for row in items:
                queue_item_id, source_id = row[0], row[1]
                if source_id:
                    harvest_aggregator_source.apply_async(args=[str(source_id), str(queue_item_id)])
    _run_async(_run())


@celery_app.task(bind=True, max_retries=2, name="queue.crawl_career_page_from_queue")
def crawl_career_page_from_queue(self, career_page_id: str, queue_item_id: str):
    """Crawl a career page (from job_crawling queue)."""
    async def _run():
        from app.db.base import AsyncSessionLocal
        from app.models.career_page import CareerPage
        from app.models.company import Company
        from app.crawlers.job_extractor import JobExtractor
        from app.services import queue_manager
        async with AsyncSessionLocal() as db:
            page = await db.get(CareerPage, uuid.UUID(career_page_id))
            if not page or not page.is_active:
                await queue_manager.complete(db, uuid.UUID(queue_item_id))
                await db.commit()
                return
            company = await db.get(Company, page.company_id)
            if not company:
                await queue_manager.complete(db, uuid.UUID(queue_item_id))
                await db.commit()
                return
            try:
                extractor = JobExtractor(db)
                await extractor.extract(company, page)
                await queue_manager.complete(db, uuid.UUID(queue_item_id))
                await db.commit()
            except Exception as e:
                await queue_manager.fail(db, uuid.UUID(queue_item_id), str(e))
                await db.commit()
                raise self.retry(exc=e)
    _run_async(_run())


@celery_app.task(
    bind=True,
    max_retries=1,
    name="queue.harvest_aggregator_source",
    soft_time_limit=600,   # 10 min soft limit — raises SoftTimeLimitExceeded
    time_limit=660,        # 11 min hard kill
)
def harvest_aggregator_source(self, source_id: str, queue_item_id: str):
    """Harvest one aggregator source using the appropriate harvester class.
    Uses Playwright for JS-heavy sites; curl_cffi for static sites.
    Blank search + exhaustive pagination to maximise company discovery.
    Companies/sites are unique by domain/URL — ON CONFLICT DO NOTHING."""
    from billiard.exceptions import SoftTimeLimitExceeded

    async def _run():
        from app.db.base import AsyncSessionLocal
        from app.models.aggregator_source import AggregatorSource
        from app.crawlers.aggregator_harvester import get_harvester_for_source
        from app.services import queue_manager
        from app.models.crawl_log import CrawlLog
        async with AsyncSessionLocal() as db:
            source = await db.get(AggregatorSource, uuid.UUID(source_id))
            if not source or not source.is_active:
                await queue_manager.complete(db, uuid.UUID(queue_item_id))
                await db.commit()
                return

            log = CrawlLog(crawl_type="discovery", status="running",
                           started_at=datetime.now(timezone.utc))
            db.add(log)
            await db.commit()

            started_at = datetime.now(timezone.utc)
            err_msg = None
            try:
                harvester = get_harvester_for_source(source.name)
                if harvester is None:
                    logger.warning(f"No harvester registered for source: {source.name!r}")
                    err_msg = f"No harvester for: {source.name}"
                    log.status = "failed"
                    log.error_message = err_msg
                    log.completed_at = datetime.now(timezone.utc)
                    log.duration_seconds = 0
                    await queue_manager.fail(db, uuid.UUID(queue_item_id), err_msg)
                    await db.commit()
                    return

                discovered = await harvester.harvest(db)
                total = len(discovered)

                log.status = "success"
                log.jobs_found = total
                log.completed_at = datetime.now(timezone.utc)
                log.duration_seconds = (datetime.now(timezone.utc) - started_at).total_seconds()
                await queue_manager.complete(db, uuid.UUID(queue_item_id))
                await db.commit()
                # Re-add to queue for next cycle
                await queue_manager.enqueue(db, "discovery", uuid.UUID(source_id), added_by="auto_requeue")
                await db.commit()
                logger.info(f"Harvested {total} new companies from {source.name}")
            except (Exception, SoftTimeLimitExceeded) as e:
                err_msg = f"timeout" if isinstance(e, SoftTimeLimitExceeded) else str(e)[:500]
                logger.error(f"harvest_aggregator_source failed for {source_id}: {err_msg}")
                try:
                    log.status = "failed"
                    log.error_message = err_msg
                    log.completed_at = datetime.now(timezone.utc)
                    log.duration_seconds = (datetime.now(timezone.utc) - started_at).total_seconds()
                    await queue_manager.fail(db, uuid.UUID(queue_item_id), err_msg)
                    await db.commit()
                except Exception:
                    pass
                raise e

    try:
        _run_async(_run())
    except SoftTimeLimitExceeded:
        logger.error(f"harvest_aggregator_source timed out: source={source_id} item={queue_item_id}")
    except Exception as e:
        raise self.retry(exc=e)


@celery_app.task(name="queue.reset_stale_processing")
def reset_stale_processing():
    """Delete run_queue items stuck in 'processing' > 30 min, then flush matching Redis queues.

    Tasks that die mid-execution (worker restart, SIGKILL) leave DB rows in 'processing'
    AND leave their payloads re-queued in Redis (task_acks_late=True).  When new workers
    pick up those stale Redis tasks the queue_item_id no longer matches any DB row, so
    queue_manager.complete() silently updates 0 rows — completions are lost.
    Flushing the Redis queue forces the drain to re-dispatch with fresh, valid IDs.
    Runs every 15 min (beat schedule).
    """
    import redis as _redis
    from app.core.config import settings

    async def _run():
        from app.db.base import AsyncSessionLocal
        from app.services import queue_manager
        async with AsyncSessionLocal() as db:
            count = await queue_manager.reset_stale_processing(db, stale_after_minutes=30)
            await db.commit()
            return count

    count = _run_async(_run())
    if not count:
        return

    logger.warning(f"reset_stale_processing: deleted {count} stale items — flushing Redis queues")
    # Flush all work queues so stale Redis tasks (which have dead queue_item_ids) don't
    # continue running and silently failing to update the DB.
    try:
        r = _redis.from_url(settings.CELERY_BROKER_URL, socket_timeout=5)
        flushed = {q: r.delete(q) for q in ("company_config", "crawl_sites", "crawl_jobs", "crawl")}
        logger.warning(f"reset_stale_processing: flushed Redis queues: {flushed}")
    except Exception as exc:
        logger.error(f"reset_stale_processing: Redis flush failed: {exc}")


@celery_app.task(name="queue.populate_queues")
def populate_queues():
    """Populate all 4 queues with items that should be processed.
    Uses bulk SQL inserts for efficiency — avoids N+1 Python loops over 20k+ rows.
    Run every 1h as a safety net — auto-enqueue hooks handle real-time adds."""
    async def _run():
        from app.db.base import AsyncSessionLocal
        from sqlalchemy import text
        async with AsyncSessionLocal() as db:
            # company_config: non-ok companies not already pending
            r1 = await db.execute(text("""
                INSERT INTO run_queue (queue_type, item_id, item_type, priority, added_by)
                SELECT 'company_config', c.id, 'company', 5, 'scheduled_populate'
                FROM companies c
                WHERE c.is_active = true AND c.company_status != 'ok'
                  AND NOT EXISTS (
                    SELECT 1 FROM run_queue rq
                    WHERE rq.queue_type = 'company_config' AND rq.item_id = c.id AND rq.status = 'pending'
                  )
                ON CONFLICT DO NOTHING
            """))
            # site_config: non-ok sites not already pending
            # Priority: 1 = company has NO ok pages (critical), 8 = company already has ok pages (low priority)
            r2 = await db.execute(text("""
                INSERT INTO run_queue (queue_type, item_id, item_type, priority, added_by)
                SELECT 'site_config', cp.id, 'career_page',
                    CASE WHEN EXISTS(
                        SELECT 1 FROM career_pages cp2
                        WHERE cp2.company_id = cp.company_id AND cp2.site_status = 'ok'
                          AND cp2.is_active = true AND cp2.id != cp.id
                    ) THEN 8 ELSE 1 END,
                    'scheduled_populate'
                FROM career_pages cp
                WHERE cp.is_active = true AND cp.site_status != 'ok'
                  AND NOT EXISTS (
                    SELECT 1 FROM run_queue rq
                    WHERE rq.queue_type = 'site_config' AND rq.item_id = cp.id AND rq.status = 'pending'
                  )
                ON CONFLICT DO NOTHING
            """))
            # job_crawling: all active sites not already pending/processing
            # 24h cooldown: only re-queue pages not crawled in the last 24 hours
            r3 = await db.execute(text("""
                INSERT INTO run_queue (queue_type, item_id, item_type, priority, added_by)
                SELECT 'job_crawling', cp.id, 'career_page', 5, 'scheduled_populate'
                FROM career_pages cp
                WHERE cp.is_active = true
                  AND (cp.last_crawled_at IS NULL OR cp.last_crawled_at < NOW() - INTERVAL '24 hours')
                  AND NOT EXISTS (
                    SELECT 1 FROM run_queue rq
                    WHERE rq.queue_type = 'job_crawling' AND rq.item_id = cp.id
                      AND rq.status IN ('pending', 'processing')
                  )
                ON CONFLICT DO NOTHING
            """))
            # discovery: all active sources not already pending
            r4 = await db.execute(text("""
                INSERT INTO run_queue (queue_type, item_id, item_type, priority, added_by)
                SELECT 'discovery', s.id, 'aggregator_source', 5, 'scheduled_populate'
                FROM aggregator_sources s
                WHERE s.is_active = true
                  AND NOT EXISTS (
                    SELECT 1 FROM run_queue rq
                    WHERE rq.queue_type = 'discovery' AND rq.item_id = s.id AND rq.status = 'pending'
                  )
                ON CONFLICT DO NOTHING
            """))
            await db.commit()
            logger.info(f"Queue population complete: +{r1.rowcount} company_config, +{r2.rowcount} site_config, +{r3.rowcount} job_crawling, +{r4.rowcount} discovery")
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


@celery_app.task(name="crawl.rescue_job_locations", bind=True, max_retries=2)
def rescue_job_locations(self, limit: int = 300):
    """
    Fetch individual job detail pages for jobs that have no location_raw.

    Works through jobs in batches — for each job, fetches the source_url
    and re-runs schema.org + heuristic extraction to fill location
    (and employment_type if also missing).  Updates the job record in place.

    Runs automatically on schedule; can also be triggered via the API.
    """

    async def _run():
        from app.db.base import AsyncSessionLocal
        from app.models.job import Job
        from app.models.company import Company
        from app.crawlers.job_extractor import JobExtractor
        from app.crawlers.http_client import ResilientHTTPClient
        from app.utils.location_parser import location_normalizer
        from sqlalchemy import select

        async with AsyncSessionLocal() as db:
            jobs = list(await db.scalars(
                select(Job)
                .where(
                    Job.is_active == True,
                    Job.location_raw.is_(None),
                    Job.source_url.isnot(None),
                )
                .order_by(Job.first_seen_at.desc())
                .limit(limit)
            ))

            if not jobs:
                logger.info("rescue_job_locations: nothing to do")
                return 0

            client = ResilientHTTPClient()
            extractor = JobExtractor(db)
            updated = 0
            failed = 0

            import re
            from urllib.parse import urlparse
            # Regex to pull city from common ATS URL patterns like /job/{city}/ or /{city}/jobs/
            _URL_CITY_RE = re.compile(
                r'/jobs?/([a-z][a-z\-]{2,30})/[^/]',
                re.IGNORECASE,
            )
            _NOT_CITY = frozenset(["all", "search", "view", "apply", "new", "list", "category",
                                   "openings", "positions", "full", "part", "remote", "hybrid"])

            for job in jobs:
                try:
                    loc_raw = None
                    found = {}

                    # --- Fast path: extract city directly from the URL (no HTTP fetch needed) ---
                    url_path = urlparse(job.source_url).path
                    m = _URL_CITY_RE.search(url_path)
                    if m:
                        candidate = m.group(1).replace("-", " ").strip().title()
                        if candidate.lower() not in _NOT_CITY and len(candidate) > 2:
                            loc_raw = candidate

                    # --- Slow path: fetch the page and extract location ---
                    if not loc_raw:
                        try:
                            resp = await client.get(job.source_url, timeout=20)
                            if resp.status_code in (200, 203):
                                html = resp.text
                                # Schema.org first (highest accuracy)
                                structured = extractor._extract_structured_data(html, job.source_url)
                                for r in structured:
                                    if r.get("location_raw"):
                                        found = r
                                        loc_raw = r["location_raw"]
                                        break
                                # Heuristic fallback
                                if not loc_raw:
                                    heuristic = extractor._extract_heuristic_single_job(html, job.source_url)
                                    if heuristic:
                                        found = heuristic[0]
                                        loc_raw = found.get("location_raw") or ""
                        except Exception as fetch_err:
                            logger.debug(f"rescue fetch failed {job.source_url}: {fetch_err}")

                    # --- Apply extracted location ---
                    if loc_raw:
                        company = await db.get(Company, job.company_id)
                        market = (company.market_code if company else None) or "AU"
                        parsed = location_normalizer.normalize(loc_raw, market)
                        job.location_raw = loc_raw
                        if not job.location_city:
                            job.location_city = parsed.city
                        if not job.location_state:
                            job.location_state = parsed.state
                        if not job.location_country:
                            job.location_country = parsed.country
                        if job.is_remote is None:
                            job.is_remote = parsed.is_remote
                        updated += 1
                        job.quality_score = None
                        job.quality_scored_at = None

                    # Also fill employment_type from page extraction if missing
                    if found:
                        new_emp = extractor._normalize_employment_type(found.get("employment_type"))
                        if new_emp and not job.employment_type:
                            job.employment_type = new_emp

                except Exception as e:
                    failed += 1
                    logger.debug(f"rescue_job_locations: failed for {job.source_url}: {e}")

            await db.commit()
            logger.info(f"rescue_job_locations: updated {updated}/{len(jobs)} jobs (failed: {failed})")
            return updated

    return _run_async(_run())

    _run_async(_run())


@celery_app.task(name="crawl.score_unscored_jobs", bind=True, max_retries=1)
def score_unscored_jobs(self, batch_size: int = 2000):
    """Backfill quality scores for jobs that have never been scored.
    Runs on beat every 10 minutes to keep quality data up to date."""
    async def _run():
        from app.db.base import AsyncSessionLocal
        from app.models.job import Job
        from app.models.company import Company
        from app.services.quality_scorer import score_job, compute_site_quality
        from sqlalchemy import select, or_

        async with AsyncSessionLocal() as db:
            unscored = list(await db.scalars(
                select(Job)
                .where(Job.is_active == True, Job.quality_score.is_(None))
                .order_by(Job.created_at.desc())
                .limit(batch_size)
            ))
            if not unscored:
                logger.info("score_unscored_jobs: nothing to score")
                return 0

            scored_count = 0
            company_scores: dict = {}

            for j in unscored:
                try:
                    qr = score_job(
                        title=j.title, description=j.description,
                        location_raw=j.location_raw, employment_type=j.employment_type,
                        date_posted=j.date_posted, salary_raw=j.salary_raw,
                        requirements=j.requirements, source_url=j.source_url,
                    )
                    j.quality_score = qr.score
                    j.quality_completeness = qr.completeness_score
                    j.quality_description = qr.description_score
                    j.quality_issues = qr.issues
                    j.quality_flags = {
                        "scam_detected": qr.scam_detected,
                        "bad_words_detected": qr.bad_words_detected,
                        "discrimination_detected": qr.discrimination_detected,
                    }
                    j.quality_scored_at = datetime.now(timezone.utc)
                    scored_count += 1
                    cid = str(j.company_id)
                    company_scores.setdefault(cid, []).append(qr.score)
                except Exception as e:
                    logger.debug(f"score_unscored_jobs: failed for job {j.id}: {e}")

            # Update company quality scores
            for cid, scores in company_scores.items():
                try:
                    from app.services.quality_scorer import compute_site_quality
                    company = await db.get(Company, uuid.UUID(cid))
                    if company:
                        company.quality_score = compute_site_quality(scores, False, False)
                        company.quality_scored_at = datetime.now(timezone.utc)
                except Exception:
                    pass

            await db.commit()
            logger.info(f"score_unscored_jobs: scored {scored_count}/{len(unscored)} jobs")
            return scored_count

    return _run_async(_run())



# ─────────────────────────────────────────────────────────────────────────────
# Worker rebalancer — dynamically adjusts queue subscriptions based on backlog
# ─────────────────────────────────────────────────────────────────────────────

# Overflow queues: dynamically added to flex workers when their Redis depth
# exceeds the high threshold, removed when depth falls below the low threshold.
# "Flex" workers = all registered workers except those with 'ml' or 'company'
# in their hostname (those have dedicated roles that should not be disrupted).
_REBALANCE_OVERFLOW: dict[str, tuple[int, int]] = {
    # (add_when_depth_above, remove_when_depth_below)
    "company_config": (200, 20),
    "geocoder": (300, 30),
}


@celery_app.task(name="queue.rebalance_workers")
def rebalance_workers():
    """Automatically add/remove queue consumers on flex workers based on live queue depths.

    Scheduled every 3 minutes via beat.  Logic per overflow queue:
      - depth > high threshold → add_consumer on flex workers not already serving it.
      - depth <= low threshold → cancel_consumer on flex workers currently serving it.
    Dedicated workers (hostname contains 'ml' or 'company') are never touched.
    """
    import redis as _redis_lib
    from app.core.config import settings

    # ── 1. Measure Redis queue depths ────────────────────────────────────────
    try:
        r = _redis_lib.from_url(settings.CELERY_BROKER_URL, socket_timeout=5)
        depths: dict[str, int] = {q: (r.llen(q) or 0) for q in _REBALANCE_OVERFLOW}
    except Exception as exc:
        logger.error("rebalance_workers: failed to read Redis depths: %s", exc)
        return

    logger.info("rebalance_workers: queue depths = %s", depths)

    # ── 2. Discover current worker → active-queues mapping ───────────────────
    try:
        inspection = celery_app.control.inspect(timeout=8)
        active_queues_map: dict[str, list[dict]] = inspection.active_queues() or {}
    except Exception as exc:
        logger.error("rebalance_workers: inspect failed: %s", exc)
        return

    if not active_queues_map:
        logger.warning("rebalance_workers: no workers responded to inspect")
        return

    flex_workers = [
        hostname for hostname in active_queues_map
        if "ml" not in hostname and "company" not in hostname
    ]

    if not flex_workers:
        logger.warning("rebalance_workers: no flex workers found")
        return

    # ── 3. Apply threshold-based add/remove decisions ────────────────────────
    changes: list[str] = []

    for queue, (add_thresh, remove_thresh) in _REBALANCE_OVERFLOW.items():
        depth = depths.get(queue, 0)

        serving = [
            h for h in flex_workers
            if any(q.get("name") == queue for q in (active_queues_map.get(h) or []))
        ]
        not_serving = [h for h in flex_workers if h not in serving]

        if depth > add_thresh and not_serving:
            for h in not_serving:
                try:
                    celery_app.control.add_consumer(queue, destination=[h], reply=False)
                except Exception as exc:
                    logger.warning("rebalance_workers: add_consumer %s→%s failed: %s", queue, h, exc)
            changes.append(
                f"ADD {queue} to {len(not_serving)} workers (depth={depth})"
            )
        elif depth <= remove_thresh and serving:
            for h in serving:
                try:
                    celery_app.control.cancel_consumer(queue, destination=[h], reply=False)
                except Exception as exc:
                    logger.warning("rebalance_workers: cancel_consumer %s←%s failed: %s", queue, h, exc)
            changes.append(
                f"REMOVE {queue} from {len(serving)} workers (depth={depth})"
            )

    if changes:
        logger.info("rebalance_workers applied: %s", " | ".join(changes))
    else:
        logger.debug("rebalance_workers: no changes needed (depths=%s)", depths)

    return {"depths": depths, "changes": changes}


@celery_app.task(name="queue.deactivate_empty_pages")
def deactivate_empty_pages():
    """Deactivate career pages that have been crawled 3+ times with 0 jobs.

    These pages are confirmed to not yield jobs — continuing to crawl them
    wastes capacity. Pages can be reactivated manually if needed.
    Runs every 6 hours.
    """
    async def _run():
        from app.db.base import AsyncSessionLocal
        from sqlalchemy import text
        async with AsyncSessionLocal() as db:
            result = await db.execute(text("""
                UPDATE career_pages SET is_active = false
                WHERE is_active = true
                  AND site_status = 'ok'
                  AND last_crawled_at IS NOT NULL
                  AND id IN (
                      -- Pages crawled 3+ times (3+ done queue items) with 0 jobs
                      SELECT cp.id
                      FROM career_pages cp
                      LEFT JOIN jobs j ON j.career_page_id = cp.id
                      WHERE cp.is_active = true AND cp.site_status = 'ok'
                        AND cp.last_crawled_at IS NOT NULL
                      GROUP BY cp.id
                      HAVING COUNT(j.id) = 0
                        AND (
                            SELECT COUNT(*) FROM run_queue rq
                            WHERE rq.item_id = cp.id
                              AND rq.queue_type = 'job_crawling'
                              AND rq.status = 'done'
                        ) >= 3
                  )
            """))
            deactivated = result.rowcount
            await db.commit()
            if deactivated:
                logger.info(f"Deactivated {deactivated} empty career pages (crawled 3+ times, 0 jobs)")
    _run_async(_run())
