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
_V610_TEMPLATE_TOKEN = re.compile(r"\{\{|\}\}|v-bind:|entry\.", re.IGNORECASE)
_V610_EMAIL = re.compile(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", re.IGNORECASE)
_V610_NON_ROLE_PREFIX = re.compile(
    r"^(?:please|did you mean|sorry, we could not find|already applied|thank you for your inquiry)\b",
    re.IGNORECASE,
)
_V610_CTA_TITLE = re.compile(
    r"^(?:browse|search|explore|view|see|find|show|current)\s+"
    r"(?:(?:all|our|open)\s+){0,2}(?:jobs?|vacanc(?:y|ies)|openings?|positions?)\b",
    re.IGNORECASE,
)
_V610_NON_ROLE_TITLES = {
    "start",
    "browse jobs",
    "search jobs",
    "search all jobs",
    "jobdetail",
    "refer a friend",
    "view all jobs",
    "view jobs",
    "current vacancies",
    "open positions",
    "explore jobs",
    "explore jobs for all locations",
}
_V610_MAX_FALLBACK_ADDITIONS = 8
_V610_MAX_FALLBACK_ADDITIONS_WITH_GAP = 12


def _is_role_shape_v610(title: str) -> bool:
    words = re.findall(r"[A-Za-z]{3,}", title)
    if len(words) < 2:
        return False
    return len(set(w.lower() for w in words)) >= 2


def _is_non_role_title_v610(title: str) -> bool:
    normalized = " ".join((title or "").split()).strip()
    if not normalized:
        return True
    title_key = normalized.lower().strip(" .")
    if title_key in _V610_NON_ROLE_TITLES:
        return True
    if _V610_TEMPLATE_TOKEN.search(normalized):
        return True
    if _V610_EMAIL.search(normalized):
        return True
    if _V610_NON_ROLE_PREFIX.match(normalized):
        return True
    if _V610_CTA_TITLE.match(normalized):
        return True
    return False


def _normalize_source_url_v610(source_url: str, page_url: str) -> str:
    src = (source_url or "").strip() or (page_url or "").strip()
    if "#" in src:
        src = src.split("#", 1)[0]
    return src


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
        jobs = self._filter_non_role_jobs_v610(jobs)
        if not jobs:
            return jobs
        page_url = career_page.url if hasattr(career_page, "url") else str(career_page)

        # Volume guardrail: recover extra valid rows from the stable v1.6 tier-2 parser
        # when v6.9 under-captures bespoke/repeating-row pages.
        if len(jobs) <= 60:
            fallback_jobs = self._extract_tier2_v16(page_url, html) or []
            if fallback_jobs:
                jobs = self._merge_volume_candidates_v610(jobs, fallback_jobs, page_url)
                jobs = self._filter_non_role_jobs_v610(jobs)
                if not jobs:
                    return jobs

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
        return self._filter_non_role_jobs_v610(jobs)

    def _merge_volume_candidates_v610(
        self,
        primary_jobs: list[dict],
        fallback_jobs: list[dict],
        page_url: str,
    ) -> list[dict]:
        if not primary_jobs or not fallback_jobs:
            return primary_jobs

        page_norm = _normalize_source_url_v610(page_url, page_url)
        coverage_gap_mode = len(fallback_jobs) >= max(4, len(primary_jobs) + 2)
        max_additions = _V610_MAX_FALLBACK_ADDITIONS_WITH_GAP if coverage_gap_mode else _V610_MAX_FALLBACK_ADDITIONS
        seen: set[tuple[str, str]] = set()
        for job in primary_jobs:
            title = " ".join(str(job.get("title") or "").split()).strip()
            source_url = _normalize_source_url_v610(str(job.get("source_url") or page_url), page_url)
            if title and source_url:
                seen.add((title.lower(), source_url.lower()))

        additions: list[dict] = []
        for cand in fallback_jobs:
            title = " ".join(str(cand.get("title") or "").split()).strip()
            if not title:
                continue
            if _is_non_role_title_v610(title):
                continue
            title_key = title.lower().strip(" .")
            if not self._is_valid_title_v16(title):
                continue

            source_url = _normalize_source_url_v610(str(cand.get("source_url") or page_url), page_url)
            key = (title_key, source_url.lower())
            if key in seen:
                continue

            noun_hint = _title_has_job_noun(title)
            url_hint = bool(_V610_URL_HINT.search(source_url))
            metadata_hint = bool(
                str(cand.get("location_raw") or "").strip()
                or str(cand.get("employment_type") or "").strip()
                or str(cand.get("description") or "").strip()
            )
            role_shape_hint = _is_role_shape_v610(title)
            if not (noun_hint or url_hint or (coverage_gap_mode and role_shape_hint and metadata_hint)):
                continue
            if source_url.rstrip("/") == page_norm.rstrip("/") and not (
                noun_hint or (coverage_gap_mode and role_shape_hint and metadata_hint)
            ):
                continue

            merged = dict(cand)
            merged["title"] = title
            merged["source_url"] = source_url
            merged["extraction_method"] = "tier2_heuristic_v16_merge_v610"
            merged["extraction_confidence"] = min(float(cand.get("extraction_confidence") or 0.62), 0.72)
            additions.append(merged)
            seen.add(key)
            if len(additions) >= max_additions:
                break

        if not additions:
            return primary_jobs
        return primary_jobs + additions

    def _filter_non_role_jobs_v610(self, jobs: list[dict]) -> list[dict]:
        return [job for job in jobs if not _is_non_role_title_v610(str(job.get("title") or ""))]
