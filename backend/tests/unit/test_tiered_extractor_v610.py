from types import SimpleNamespace

import pytest

from app.crawlers.detail_enricher import EnrichmentReport
from app.crawlers.tiered_extractor_v610 import TieredExtractorV610


@pytest.mark.asyncio
async def test_v610_runs_v69_then_detail_enrichment(monkeypatch):
    extractor = TieredExtractorV610()
    listing_jobs = [
        {
            "title": "Platform Engineer",
            "source_url": "https://example.com/jobs/1",
            "description": "",
            "location_raw": "",
        }
    ]

    async def fake_listing_extract(career_page, company, html):
        return [dict(job) for job in listing_jobs]

    async def fake_enrich(self, jobs, *, ats=None, page_url=""):
        jobs[0]["description"] = "Rich detail description"
        jobs[0]["location_raw"] = "Melbourne, VIC"
        return jobs, EnrichmentReport(attempted=1, succeeded=1, fields_filled=2)

    monkeypatch.setattr(extractor._listing_delegate, "extract", fake_listing_extract)
    monkeypatch.setattr("app.crawlers.tiered_extractor_v610.DetailEnricher.enrich", fake_enrich)

    page = SimpleNamespace(url="https://example.com/careers")
    company = SimpleNamespace(name="Acme", ats_platform="workday")
    jobs = await extractor.extract(page, company, "<html></html>")

    assert jobs[0]["description"] == "Rich detail description"
    assert jobs[0]["location_raw"] == "Melbourne, VIC"
    assert extractor._enrichment_budget.max_pages == 10
    assert extractor._enrichment_budget.per_host_concurrency == 2
    assert extractor._enrichment_budget.total_deadline_s == 20
