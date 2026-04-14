"""
Career Page Finder v4.9 — direct from CareerPageFinderV4.

v4.9 keeps the proven v4.8 discovery behavior as a version-matched finder so
new extractor versions do not regress to generic discovery fallbacks.
"""

from __future__ import annotations

from app.crawlers.career_page_finder_v4 import CareerPageFinderV4
from app.crawlers.career_page_finder_v48 import CareerPageFinderV48


class CareerPageFinderV49(CareerPageFinderV4):
    """v4.9 finder wrapper preserving v4.8 discovery logic."""

    async def find(self, domain: str, company_name: str = "") -> dict:
        return await CareerPageFinderV48().find(domain, company_name)
