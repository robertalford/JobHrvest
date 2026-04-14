"""
Career Page Finder v3.1 — direct from CareerPageFinderV4.

v3.1 keeps the proven v3.0 discovery behavior to avoid versioned finder fallback
regressions while extractor-side changes are being evaluated.
"""

from app.crawlers.career_page_finder_v4 import CareerPageFinderV4
from app.crawlers.career_page_finder_v30 import CareerPageFinderV30


class CareerPageFinderV31(CareerPageFinderV4):
    """v3.1 finder wrapper preserving v3.0 discovery behavior."""

    async def find(self, domain: str, company_name: str = "") -> dict:
        return await CareerPageFinderV30().find(domain, company_name)

