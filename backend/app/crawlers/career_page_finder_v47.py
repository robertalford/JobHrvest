"""
Career Page Finder v4.7 — direct from CareerPageFinderV4.

v4.7 keeps the proven v4.6 discovery behavior while extractor strategy is
updated in TieredExtractorV47.
"""

from __future__ import annotations

from app.crawlers.career_page_finder_v4 import CareerPageFinderV4
from app.crawlers.career_page_finder_v46 import CareerPageFinderV46


class CareerPageFinderV47(CareerPageFinderV4):
    """v4.7 finder wrapper preserving v4.6 discovery heuristics."""

    async def find(self, domain: str, company_name: str = "") -> dict:
        return await CareerPageFinderV46().find(domain, company_name)

