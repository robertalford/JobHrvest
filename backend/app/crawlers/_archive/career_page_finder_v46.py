"""
Career Page Finder v4.6 — direct from CareerPageFinderV4.

v4.6 keeps the proven v4.5 discovery behavior to avoid version-mismatch fallback
while extraction strategy is updated in TieredExtractorV46.
"""

from __future__ import annotations

from app.crawlers.career_page_finder_v4 import CareerPageFinderV4
from app.crawlers.career_page_finder_v45 import CareerPageFinderV45


class CareerPageFinderV46(CareerPageFinderV4):
    """v4.6 finder wrapper preserving v4.5 discovery heuristics."""

    async def find(self, domain: str, company_name: str = "") -> dict:
        return await CareerPageFinderV45().find(domain, company_name)
