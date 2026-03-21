"""Celery application configuration."""

from celery import Celery
from app.core.config import settings

celery_app = Celery(
    "jobharvest",
    broker=settings.CELERY_BROKER_URL,
    backend=settings.CELERY_RESULT_BACKEND,
    include=[
        "app.tasks.crawl_tasks",
        "app.tasks.ml_tasks",
        "app.tasks.domain_import_tasks",
        "app.tasks.geocoder_tasks",
    ],
)

celery_app.conf.update(
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="Australia/Sydney",
    enable_utc=True,
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
    task_routes={
        "crawl.company": {"queue": "crawl_sites"},
        "crawl.career_page": {"queue": "crawl_jobs"},
        "crawl.full_cycle": {"queue": "default"},
        "crawl.harvest_aggregators": {"queue": "discovery"},
        "crawl.seed_market_companies": {"queue": "default"},
        "crawl.mark_inactive_jobs": {"queue": "default"},
        "crawl.validate_page_template": {"queue": "default"},
        "crawl.fix_company_sites": {"queue": "company_config"},
        "crawl.fix_site_structure": {"queue": "crawl_sites"},
        # Queue drain tasks
        "queue.drain_company_config": {"queue": "company_config"},
        "queue.drain_site_config": {"queue": "crawl_sites"},
        "queue.drain_job_crawling": {"queue": "crawl_jobs"},
        "queue.drain_discovery": {"queue": "discovery"},
        "queue.crawl_career_page_from_queue": {"queue": "crawl_jobs"},
        "queue.drain_job_crawling": {"queue": "crawl_jobs"},
        "queue.harvest_aggregator_source": {"queue": "discovery"},
        "queue.populate_queues": {"queue": "default"},
        "ml.*": {"queue": "ml"},
        "ml.llm_extract_page": {"queue": "ml"},
        "ml.enrich_job_descriptions": {"queue": "ml"},
        "ml.reprocess_company": {"queue": "ml"},
        "ml.batch_reprocess": {"queue": "ml"},
        # Domain import tasks
        "domain_import.tranco": {"queue": "default"},
        "domain_import.majestic": {"queue": "default"},
        "domain_import.asic": {"queue": "default"},
        "domain_import.wikidata": {"queue": "default"},
        # Geocoder tasks
        "geocoder.seed_geonames": {"queue": "geocoder"},
        "geocoder.geocode_new_jobs": {"queue": "geocoder"},
        "geocoder.retro_geocode_jobs": {"queue": "geocoder"},
        "geocoder.geocode_all_failed": {"queue": "geocoder"},
    },
    beat_schedule={
        # ── Queue drains ─────────────────────────────────────────────────────
        # job_crawling is the high-frequency recurring crawl — drain every 5s
        # so workers are never idle between batches at scale (50k sites).
        "drain-job-crawling":    {"task": "queue.drain_job_crawling",    "schedule": 5},
        # site_config is one-time per site — moderate drain rate is fine
        "drain-site-config":     {"task": "queue.drain_site_config",     "schedule": 10},
        # company_config is one-time per company — aggressive drain to clear 14k backlog
        "drain-company-config":  {"task": "queue.drain_company_config",  "schedule": 10},
        # discovery sources
        "drain-discovery":       {"task": "queue.drain_discovery",       "schedule": 60},
        # Populate queues every 30 min (clears backlog faster; hourly was too slow)
        "populate-queues": {"task": "queue.populate_queues", "schedule": 30 * 60},
        # Safety net: reset items stuck in 'processing' > 2h back to 'pending'
        "reset-stale-processing": {"task": "queue.reset_stale_processing", "schedule": 15 * 60},
        # Auto-rebalance worker queue subscriptions based on live queue depths
        "rebalance-workers": {"task": "queue.rebalance_workers", "schedule": 3 * 60, "options": {"queue": "default"}},
        # DEPRECATED: crawl.scheduled flooded the queue; now handled by drain tasks
        # "scheduled-crawl-cycle": { ... }  ← removed
        # Job lifecycle — mark stale jobs inactive daily
        "mark-inactive-jobs": {
            "task": "crawl.mark_inactive_jobs",
            "schedule": 24 * 3600,
        },
        # Deactivate career pages that consistently yield 0 jobs (3+ crawls)
        "deactivate-empty-pages": {
            "task": "queue.deactivate_empty_pages",
            "schedule": 6 * 3600,
        },
        # Enforce quality gate: rescore jobs, deactivate bad ones, trigger location rescue
        "enforce-quality-gate": {
            "task": "queue.enforce_quality_gate",
            "schedule": 5 * 60,
        },
        # Quality scoring backfill — every 30 min for newly crawled jobs
        "score-jobs-batch": {
            "task": "ml.score_jobs_batch",
            "schedule": 30 * 60,
            "kwargs": {"limit": 100},
            "options": {"queue": "ml"},
        },
        # Description enrichment — every 20 min
        "enrich-job-descriptions": {
            "task": "ml.enrich_job_descriptions",
            "schedule": 20 * 60,
            "kwargs": {"limit": 150},
            "options": {"queue": "ml"},
        },
        # Geocode newly crawled jobs — every 2 min
        "geocode-new-jobs": {
            "task": "geocoder.geocode_new_jobs",
            "schedule": 30,
            "kwargs": {"limit": 100},
        },
        # Location rescue — fetch individual job pages to fill missing location_raw.
        # Runs every 10 min until backlog is cleared, then becomes a no-op.
        "score-unscored-jobs": {
            "task": "crawl.score_unscored_jobs",
            "schedule": 600,
            "kwargs": {"batch_size": 2000},
            "options": {"queue": "default"},
        },
        "rescue-job-locations": {
            "task": "crawl.rescue_job_locations",
            "schedule": 2 * 60,
            "kwargs": {"limit": 200},
            "options": {"queue": "default"},
        },
    },
)
