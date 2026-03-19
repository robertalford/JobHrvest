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
        "crawl.company": {"queue": "crawl"},
        "crawl.career_page": {"queue": "crawl"},
        "crawl.full_cycle": {"queue": "default"},
        "crawl.harvest_aggregators": {"queue": "discovery"},
        "crawl.mark_inactive_jobs": {"queue": "default"},
        "crawl.validate_page_template": {"queue": "default"},
        "ml.*": {"queue": "ml"},
    },
    beat_schedule={
        # Main crawl cycle — every hour, picks up sites due for crawling
        "scheduled-crawl-cycle": {
            "task": "crawl.scheduled",
            "schedule": 3600,
        },
        # Aggregator discovery — every 6 hours, find new companies via Indeed AU
        "harvest-aggregators": {
            "task": "crawl.harvest_aggregators",
            "schedule": 6 * 3600,
        },
        # Job lifecycle — mark stale jobs inactive daily
        "mark-inactive-jobs": {
            "task": "crawl.mark_inactive_jobs",
            "schedule": 24 * 3600,
        },
    },
)
