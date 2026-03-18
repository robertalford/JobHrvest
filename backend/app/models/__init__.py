# Import all models so Alembic can discover them
from app.models.market import Market
from app.models.blocked_domain import BlockedDomain
from app.models.company import Company
from app.models.career_page import CareerPage
from app.models.aggregator_source import AggregatorSource
from app.models.job import Job, JobTag
from app.models.crawl_log import CrawlLog
from app.models.site_template import SiteTemplate
from app.models.extraction_comparison import ExtractionComparison

__all__ = [
    "Market",
    "BlockedDomain",
    "Company",
    "CareerPage",
    "AggregatorSource",
    "Job",
    "JobTag",
    "CrawlLog",
    "SiteTemplate",
    "ExtractionComparison",
]
