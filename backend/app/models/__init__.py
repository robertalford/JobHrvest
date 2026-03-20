# Import all models so Alembic can discover them
from app.models.market import Market
from app.models.company import Company
from app.models.career_page import CareerPage
from app.models.aggregator_source import AggregatorSource
from app.models.job import Job, JobTag
from app.models.crawl_log import CrawlLog
from app.models.site_template import SiteTemplate
from app.models.extraction_comparison import ExtractionComparison
from app.models.lead_import import LeadImport
from app.models.lead_import_batch import LeadImportBatch
from app.models.excluded_site import ExcludedSite
from app.models.settings import WordFilter, SystemSetting
from app.models.geo_location import GeoLocation, GeocodeCache

__all__ = [
    "Market",
    "Company",
    "CareerPage",
    "AggregatorSource",
    "Job",
    "JobTag",
    "CrawlLog",
    "SiteTemplate",
    "ExtractionComparison",
    "LeadImport",
    "LeadImportBatch",
    "ExcludedSite",
    "WordFilter",
    "SystemSetting",
    "GeoLocation",
    "GeocodeCache",
]
