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
    # Beat schedule for periodic crawling
    beat_schedule={
        "scheduled-crawl-cycle": {
            "task": "app.tasks.crawl_tasks.scheduled_crawl_cycle",
            "schedule": 3600,  # every hour — picks up sites whose next_crawl_at has passed
        },
    },
)
