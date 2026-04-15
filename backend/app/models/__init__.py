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
from app.models.company_enrichment_run import CompanyEnrichmentRun
from app.models.company_enrichment_row import CompanyEnrichmentRow
from app.models.excluded_site import ExcludedSite
from app.models.settings import WordFilter, SystemSetting
from app.models.geo_location import GeoLocation, GeocodeCache
from app.models.test_data import CrawlerTestData, JobSiteTestData, SiteUrlTestData, SiteWrapperTestData
from app.models.ml_model import MLModel, MLModelTestRun, MLTestFeedback, CodexImprovementRun
from app.models.champion_challenger import (
    ModelVersion,
    GoldHoldoutSet,
    GoldHoldoutDomain,
    GoldHoldoutSnapshot,
    GoldHoldoutJob,
    Experiment,
    MetricSnapshot,
    AtsPatternProposal,
    DriftBaseline,
    InferenceMetricsHourly,
    EvoIndividual,
    EvoCycle,
    EvoPopulationEvent,
)
from app.models.universality import EverPassedSite, SiteResultHistory

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
    "CompanyEnrichmentRun",
    "CompanyEnrichmentRow",
    "ExcludedSite",
    "WordFilter",
    "SystemSetting",
    "GeoLocation",
    "GeocodeCache",
    "CrawlerTestData",
    "JobSiteTestData",
    "SiteUrlTestData",
    "SiteWrapperTestData",
    "MLModel",
    "MLModelTestRun",
    "MLTestFeedback",
    "CodexImprovementRun",
    "ModelVersion",
    "GoldHoldoutSet",
    "GoldHoldoutDomain",
    "GoldHoldoutSnapshot",
    "GoldHoldoutJob",
    "Experiment",
    "MetricSnapshot",
    "AtsPatternProposal",
    "DriftBaseline",
    "InferenceMetricsHourly",
    "EvoIndividual",
    "EvoCycle",
    "EvoPopulationEvent",
    "EverPassedSite",
    "SiteResultHistory",
]
