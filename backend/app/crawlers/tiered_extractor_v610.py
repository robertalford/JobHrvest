"""Tiered Extraction Engine v6.10 — v6.9 listing extraction + detail enrichment."""

from __future__ import annotations

import logging

from app.crawlers.detail_enricher import DetailEnricher, EnrichmentBudget
from app.crawlers.tiered_extractor_v16 import TieredExtractorV16
from app.crawlers.tiered_extractor_v69 import TieredExtractorV69

logger = logging.getLogger(__name__)


class TieredExtractorV610(TieredExtractorV16):
    """Hotfix: preserve v6.9 behavior, then fill blank fields from detail pages."""

    def __init__(self) -> None:
        super().__init__()
        self._listing_delegate = TieredExtractorV69()
        self._enrichment_budget = EnrichmentBudget(
            max_pages=10,
            per_host_concurrency=2,
            total_deadline_s=20,
        )

    async def extract(self, career_page, company, html: str) -> list[dict]:
        jobs = await self._listing_delegate.extract(career_page, company, html)
        if not jobs:
            return jobs

        try:
            from app.crawlers.http_client import ResilientHTTPClient

            client = ResilientHTTPClient()

            async def _fetch(url: str) -> str:
                try:
                    response = await client.get(url)
                    return response.text if hasattr(response, "text") else ""
                except Exception:  # noqa: BLE001
                    return ""
        except Exception:  # noqa: BLE001
            async def _fetch(url: str) -> str:
                return ""

        ats_hint = getattr(company, "ats_platform", None)
        page_url = career_page.url if hasattr(career_page, "url") else str(career_page)
        try:
            enricher = DetailEnricher(http_fetch=_fetch, budget=self._enrichment_budget)
            jobs, _ = await enricher.enrich(jobs, ats=ats_hint, page_url=page_url)
        except Exception as exc:  # noqa: BLE001
            logger.warning("v6.10 detail enrichment failed for %s: %s", page_url, exc)
        return jobs
