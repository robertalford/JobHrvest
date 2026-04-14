"""Tiered Extraction Engine v7.0 — v6.9 + guarded ATS handoff fallback."""

from __future__ import annotations

import logging
import re
from urllib.parse import urljoin, urlparse

import httpx

from app.crawlers.tiered_extractor import MAX_JOBS_PER_PAGE, _parse_html, _resolve_url
from app.crawlers.tiered_extractor_v16 import TieredExtractorV16
from app.crawlers.tiered_extractor_v69 import TieredExtractorV69

logger = logging.getLogger(__name__)

_ATS_HOST_HINT_V70 = re.compile(
    r"(?:^|\.)(?:"
    r"icims\.com|"
    r"myworkdayjobs\.com|"
    r"workdayjobs\.com"
    r")$",
    re.IGNORECASE,
)


class TieredExtractorV70(TieredExtractorV16):
    """v7.0 extractor: v6.9 baseline + guarded off-site ATS handoff."""

    def __init__(self) -> None:
        super().__init__()
        self._delegate = TieredExtractorV69()

    async def extract(self, career_page, company, html: str) -> list[dict]:
        base_jobs = await self._delegate.extract(career_page, company, html)

        page_url = career_page.url if hasattr(career_page, "url") else str(career_page)
        if not self._should_try_external_ats_handoff(page_url, html or "", base_jobs):
            return base_jobs

        handoff_jobs = await self._extract_external_ats_jobs(
            page_url=page_url,
            html=html or "",
            company_name=getattr(company, "name", "") or "",
            company=company,
            baseline_count=len(base_jobs),
        )
        if handoff_jobs and len(handoff_jobs) >= max(3, len(base_jobs) + 3):
            logger.info("v7.0 ATS handoff improved volume %s: %d -> %d jobs", page_url, len(base_jobs), len(handoff_jobs))
            return handoff_jobs[:MAX_JOBS_PER_PAGE]

        return base_jobs

    def _should_try_external_ats_handoff(self, page_url: str, html: str, jobs: list[dict]) -> bool:
        if len(jobs) >= 6:
            return False

        host = (urlparse(page_url).hostname or "").lower()
        if _ATS_HOST_HINT_V70.search(host):
            return True

        lower = (html or "").lower()
        if not lower:
            return False
        return any(marker in lower for marker in (".icims.com", "myworkdayjobs.com", "workdayjobs.com"))

    async def _extract_external_ats_jobs(
        self,
        page_url: str,
        html: str,
        company_name: str,
        company,
        baseline_count: int,
    ) -> list[dict]:
        seed_urls = self._external_ats_seed_urls(page_url, html, company_name)
        if not seed_urls:
            return []
        best_jobs: list[dict] = []
        best_score = baseline_count

        async with httpx.AsyncClient(
            timeout=httpx.Timeout(3.0, connect=1.5),
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0 (JobHarvest v7.0 ATS handoff)"},
        ) as client:
            for candidate_url in seed_urls[:4]:
                try:
                    resp = await client.get(candidate_url)
                except Exception:
                    continue

                body = resp.text or ""
                if resp.status_code != 200 or len(body) < 250:
                    continue

                probe_url = str(resp.url)
                probe_page = type("CareerPageProxyV70", (), {"url": probe_url, "requires_js_rendering": False})()
                probe_jobs = await self._delegate.extract(probe_page, company, body)
                if len(probe_jobs) <= best_score:
                    continue
                if not self._delegate._passes_jobset_validation(probe_jobs, probe_url):
                    continue
                best_jobs = probe_jobs
                best_score = len(probe_jobs)

        return best_jobs

    def _external_ats_seed_urls(self, page_url: str, html: str, company_name: str) -> list[str]:
        seeds: list[str] = []
        seen: set[str] = set()

        def _add(url: str) -> None:
            if not url:
                return
            norm = url.rstrip("/")
            if norm in seen:
                return
            seen.add(norm)
            seeds.append(url)

        for url in self._expand_ats_probe_urls(page_url, company_name):
            _add(url)

        root = _parse_html(html)
        if root is None:
            return seeds

        for a_el in root.xpath("//a[@href]"):
            href = (a_el.get("href") or "").strip()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue
            full = _resolve_url(href, page_url) or urljoin(page_url, href)
            if not full:
                continue
            host = (urlparse(full).hostname or "").lower()
            if not _ATS_HOST_HINT_V70.search(host):
                continue
            for url in self._expand_ats_probe_urls(full, company_name):
                _add(url)

        return seeds

    def _expand_ats_probe_urls(self, seed_url: str, company_name: str) -> list[str]:
        parsed = urlparse(seed_url)
        host = (parsed.hostname or "").lower()
        if not host:
            return []
        base = f"{parsed.scheme or 'https'}://{host}"
        candidates: list[str] = []

        if "icims.com" in host:
            candidates.extend(
                [
                    f"{base}/jobs/search?ss=1",
                    f"{base}/jobs/search",
                    f"{base}/jobs",
                ]
            )

        if "myworkdayjobs.com" in host or "workdayjobs.com" in host:
            path = parsed.path or ""
            if "/job/" in path:
                listing_root = path.split("/job/", 1)[0].rstrip("/")
                if listing_root:
                    candidates.append(f"{base}{listing_root}")
                    candidates.append(f"{base}{listing_root}/jobs")
            for slug in self._workday_slug_variants(company_name):
                candidates.append(f"{base}/en-US/{slug}")
                candidates.append(f"{base}/{slug}")

        candidates.append(seed_url)
        deduped: list[str] = []
        seen: set[str] = set()
        for url in candidates:
            norm = url.rstrip("/")
            if not norm or norm in seen:
                continue
            seen.add(norm)
            deduped.append(url)
        return deduped

    @staticmethod
    def _workday_slug_variants(company_name: str) -> list[str]:
        text = (company_name or "").strip().lower()
        if not text:
            return []
        words = re.findall(r"[a-z0-9]+", text)
        if not words:
            return []
        base = "_".join(words[:4])
        compact = "".join(words[:4])
        variants = [
            f"{base}_careers",
            base,
            f"{compact}_careers",
            compact,
        ]
        out: list[str] = []
        seen: set[str] = set()
        for v in variants:
            if v and v not in seen:
                seen.add(v)
                out.append(v)
        return out
