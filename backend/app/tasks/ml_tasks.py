"""ML/classifier Celery tasks."""

import logging
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
