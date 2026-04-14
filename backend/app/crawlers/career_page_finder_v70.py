"""Career Page Finder v7.0 — listing-intent boost for hub-vs-listing ambiguity.

Targets a recurring bespoke pattern where discovery lands on editorial career hubs
(e.g. internships/culture pages) instead of the true job-listing endpoint.
"""

from __future__ import annotations

import re

from app.crawlers.career_page_finder_v26 import CareerPageFinderV26

_STRONG_LISTING_PATH_V70 = re.compile(
    r"/(?:job-search|search-jobs?|jobs?/search|job-openings?|open-positions?|vacancies|current-openings)(?:/|$|\?)",
    re.IGNORECASE,
)

_EDITORIAL_HUB_PATH_V70 = re.compile(
    r"/(?:students?|internships?|graduates?|early-career|life-at|culture|benefits|teams?|overview|about)(?:/|$|\?)",
    re.IGNORECASE,
)

_LISTING_SHELL_MARKERS_V70 = (
    "job-search-results",
    "resultscount",
    "job-content",
    "load more",
    "pagination-list",
    "showing 1 -",
)

_HUB_CONTENT_MARKERS_V70 = (
    "internship",
    "graduate opportunities",
    "early career",
    "life at",
    "our culture",
    "employee benefits",
)


class CareerPageFinderV70(CareerPageFinderV26):
    """v7.0 finder: prefer listing-intent pages over editorial career hubs."""

    def _listing_page_score(self, url: str, html: str) -> int:
        score = super()._listing_page_score(url, html)

        lower_url = (url or "").lower()
        strong_listing = bool(_STRONG_LISTING_PATH_V70.search(lower_url))

        if strong_listing:
            score += 12

        if _EDITORIAL_HUB_PATH_V70.search(lower_url) and not strong_listing:
            score -= 12

        lower_html = (html or "").lower()[:200000]
        if lower_html:
            listing_hits = sum(1 for marker in _LISTING_SHELL_MARKERS_V70 if marker in lower_html)
            hub_hits = sum(1 for marker in _HUB_CONTENT_MARKERS_V70 if marker in lower_html)

            if listing_hits >= 2:
                score += 6
            if hub_hits >= 3 and listing_hits == 0:
                score -= 6

        return score
