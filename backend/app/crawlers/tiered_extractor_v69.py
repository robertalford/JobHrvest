"""
Tiered Extraction Engine v6.9 — Jobs2Web probe ordering + heading FP guard.

Builds on v6.8 with two focused improvements:
1. Prioritize same-host Jobs2Web search endpoints inside the existing bounded
   probe budget so config-shell pages are less likely to return 0 jobs.
2. Reject generic vacancy headings (for example menu labels) as job titles.
"""

from __future__ import annotations

import re
from urllib.parse import quote_plus, urlparse

from app.crawlers.tiered_extractor_v68 import TieredExtractorV68

_NON_JOB_VACANCY_HEADING_V69 = re.compile(
    r"^(?:job\s+vacancies|current\s+vacancies|vacancies)$",
    re.IGNORECASE,
)


class TieredExtractorV69(TieredExtractorV68):
    """v6.9 extractor: tighter heading rejection + better Jobs2Web probe ordering."""

    def _is_valid_title_v60(self, title: str) -> bool:
        if not super()._is_valid_title_v60(title):
            return False
        return not _NON_JOB_VACANCY_HEADING_V69.match((title or "").strip())

    def _jobs2web_endpoint_candidates_v66(self, page_url: str, cfg: dict[str, str | None]) -> list[str]:
        candidates = list(super()._jobs2web_endpoint_candidates_v66(page_url, cfg))
        if not candidates:
            return []

        locale = (cfg.get("locale") or "").strip()
        lower_page = (page_url or "").lower()
        if "/search/" in lower_page:
            if page_url not in candidates:
                candidates.append(page_url)
            if locale and "locale=" not in lower_page:
                sep = "&" if "?" in page_url else "?"
                candidates.append(f"{page_url}{sep}locale={quote_plus(locale)}")

        page_host = (urlparse(page_url).netloc or "").lower()
        page_norm = (page_url or "").rstrip("/")

        def _score(endpoint: str) -> int:
            parsed = urlparse(endpoint)
            host = (parsed.netloc or "").lower()
            path = (parsed.path or "").lower()
            lower = endpoint.lower()

            score = 0
            if host == page_host and "/search/" in path:
                score += 120
            if endpoint.rstrip("/") == page_norm:
                score += 40
            if "/career/jobsearch" in path:
                score += 70
            if "/career/jobreqcareersite" in path:
                score += 60
            if host == page_host:
                score += 20
            if "skillssearch=false" in lower:
                score += 20
            if "locale=" in lower:
                score += 6
            return score

        ranked = sorted(enumerate(candidates), key=lambda pair: (-_score(pair[1]), pair[0]))

        ordered: list[str] = []
        seen: set[str] = set()
        for _, endpoint in ranked:
            if not endpoint or endpoint in seen:
                continue
            seen.add(endpoint)
            ordered.append(endpoint)
        return ordered
