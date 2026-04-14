"""
Career Page Finder v3.2 — direct from CareerPageFinderV4.

v3.2 keeps the proven v3.1 discovery behavior while extractor-side logic changes.
"""

from app.crawlers.career_page_finder_v4 import CareerPageFinderV4
from app.crawlers.career_page_finder_v31 import CareerPageFinderV31


class CareerPageFinderV32(CareerPageFinderV4):
    """v3.2 finder wrapper preserving v3.1 discovery behavior."""

    async def find(self, domain: str, company_name: str = "") -> dict:
        return await CareerPageFinderV31().find(domain, company_name)

