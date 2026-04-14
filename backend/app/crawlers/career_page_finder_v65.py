"""
Career Page Finder v6.5 — Inherits v6.4 finder with Manatal/careers-page.com
awareness.

No changes needed to the finder logic — v6.4's hint support, listing-dense
scoring, and subdomain detection already handle these sites well. This file
exists to maintain version parity with the v6.5 extractor.
"""

from __future__ import annotations

from app.crawlers.career_page_finder_v64 import CareerPageFinderV64


class CareerPageFinderV65(CareerPageFinderV64):
    """v6.5 finder: same as v6.4 — version parity with extractor."""
    pass
