"""Tiered Extraction Engine v6.10 — v6.9 listing extraction + detail enrichment."""

from __future__ import annotations

import logging
import re

from app.crawlers.detail_enricher import DetailEnricher, EnrichmentBudget
from app.crawlers.tiered_extractor_v16 import TieredExtractorV16, _title_has_job_noun
from app.crawlers.tiered_extractor_v69 import TieredExtractorV69

logger = logging.getLogger(__name__)

_V610_URL_HINT = re.compile(
    r"(?:/job|/jobs|/career|/careers|/position|/vacanc|/opening|/requisition|jobid=|vacancyid=)",
    re.IGNORECASE,
)
_V610_NON_ROLE_TITLES = {
    "start",
    "search jobs",
    "jobdetail",
    "refer a friend",
    "view all jobs",
    "view jobs",
    "open positions",
    "explore jobs",
}
_V610_MAX_FALLBACK_ADDITIONS = 8


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
        return await self._finalize_with_enrichment(career_page, company, html)

    async def _finalize_with_enrichment(self, career_page, company, html: str) -> list[dict]:
        jobs = await self._listing_delegate.extract(career_page, company, html)
        if not jobs:
            return jobs
        page_url = career_page.url if hasattr(career_page, "url") else str(career_page)

        # Volume guardrail: recover extra valid rows from the stable v1.6 tier-2 parser
        # when v6.9 under-captures bespoke/repeating-row pages.
        if len(jobs) <= 60:
            fallback_jobs = self._extract_tier2_v16(page_url, html) or []
            if fallback_jobs:
                jobs = self._merge_volume_candidates_v610(jobs, fallback_jobs, page_url)

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
        try:
            enricher = DetailEnricher(http_fetch=_fetch, budget=self._enrichment_budget)
            jobs, _ = await enricher.enrich(jobs, ats=ats_hint, page_url=page_url)
        except Exception as exc:  # noqa: BLE001
            logger.warning("v6.10 detail enrichment failed for %s: %s", page_url, exc)
        return jobs

    def _merge_volume_candidates_v610(
        self,
        primary_jobs: list[dict],
        fallback_jobs: list[dict],
        page_url: str,
    ) -> list[dict]:
        if not primary_jobs or not fallback_jobs:
            return primary_jobs

        page_norm = self._normalize_source_url_v610(page_url, page_url)
        seen: set[tuple[str, str]] = set()
        for job in primary_jobs:
            title = " ".join(str(job.get("title") or "").split()).strip()
            source_url = self._normalize_source_url_v610(str(job.get("source_url") or page_url), page_url)
            if title and source_url:
                seen.add((title.lower(), source_url.lower()))

        additions: list[dict] = []
        for cand in fallback_jobs:
            title = " ".join(str(cand.get("title") or "").split()).strip()
            if not title:
                continue
            title_key = title.lower().strip(" .")
            if title_key in _V610_NON_ROLE_TITLES:
                continue
            if not self._is_valid_title_v16(title):
                continue

            source_url = self._normalize_source_url_v610(str(cand.get("source_url") or page_url), page_url)
            key = (title_key, source_url.lower())
            if key in seen:
                continue

            noun_hint = _title_has_job_noun(title)
            url_hint = bool(_V610_URL_HINT.search(source_url))
            if not (noun_hint or url_hint):
                continue
            if source_url.rstrip("/") == page_norm.rstrip("/") and not noun_hint:
                continue

            merged = dict(cand)
            merged["title"] = title
            merged["source_url"] = source_url
            merged["extraction_method"] = "tier2_heuristic_v16_merge_v610"
            merged["extraction_confidence"] = min(float(cand.get("extraction_confidence") or 0.62), 0.72)
            additions.append(merged)
            seen.add(key)
            if len(additions) >= _V610_MAX_FALLBACK_ADDITIONS:
                break

        if not additions:
            return primary_jobs
        return primary_jobs + additions

    @staticmethod
    def _normalize_source_url_v610(source_url: str, page_url: str) -> str:
        src = (source_url or "").strip() or (page_url or "").strip()
        if "#" in src:
            src = src.split("#", 1)[0]
        return src
