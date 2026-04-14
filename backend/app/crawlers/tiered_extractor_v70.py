"""Tiered Extraction Engine v7.0 — stable extractor baseline.

This iteration targets volume misses from wrong career-page selection (finder-side).
Extractor behavior stays aligned with v6.9 to avoid introducing new extraction regressions.
"""

from app.crawlers.tiered_extractor_v16 import TieredExtractorV16
from app.crawlers.tiered_extractor_v69 import TieredExtractorV69


class TieredExtractorV70(TieredExtractorV16):
    """v7.0 extractor: delegate to proven v6.9 extraction behavior."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        return await TieredExtractorV69().extract(career_page, company, html)
