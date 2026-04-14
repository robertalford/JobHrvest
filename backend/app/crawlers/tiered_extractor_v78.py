"""
Tiered Extraction Engine v7.8 — careers-page API recovery + Connx URL repair.

Strategy:
1. Recover complete job sets from careers-page.com Vue shells via their own JSON API.
2. Repair same-page Connx heuristic outputs by backfilling detail URLs from inline
   `/job/details/...` path evidence when available.
3. Preserve v7.7 quality behavior and keep strict title/jobset validation.
"""

from __future__ import annotations

import re
from typing import Any, Optional
from urllib.parse import urljoin, urlparse

import httpx

from app.crawlers.tiered_extractor import MAX_JOBS_PER_PAGE, _resolve_url
from app.crawlers.tiered_extractor_v77 import TieredExtractorV77

_V78_CAREERS_PAGE_BASE = re.compile(r'const\s+baseUrl\s*=\s*["\']([^"\']+)["\']', re.IGNORECASE)
_V78_CAREERS_PAGE_SLUG = re.compile(r'const\s+clientSlug\s*=\s*["\']([a-z0-9][a-z0-9-]{1,80})["\']', re.IGNORECASE)
_V78_CAREERS_PAGE_DIRECT_API = re.compile(r"apiBaseURL\s*:\s*`([^`]+)`", re.IGNORECASE)
_V78_TEMPLATE_TITLE = re.compile(r"(?:\[\[|{{)\s*job\.", re.IGNORECASE)
_V78_CONNX_DETAIL_PATH = re.compile(r"/job/details/[^\s\"'<>)]{3,}", re.IGNORECASE)


class TieredExtractorV78(TieredExtractorV77):
    """v7.8 extractor: careers-page API fallback + Connx same-page URL repair."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        page_url = career_page.url if hasattr(career_page, "url") else str(career_page)
        working_html = html or ""

        careers_page_jobs = await self._extract_careers_page_api_jobs_v78(page_url, working_html)
        if len(careers_page_jobs) >= 3 and self._passes_jobset_validation(careers_page_jobs, page_url):
            return await self._finalize_jobs_v76(careers_page_jobs, working_html, page_url)

        jobs = await super().extract(career_page, company, html)

        repaired = self._repair_connx_same_page_urls_v78(jobs, working_html, page_url)
        if repaired is not jobs:
            repaired = self._dedupe(repaired, page_url)
            repaired = self._clean_jobs_v73(repaired)[:MAX_JOBS_PER_PAGE]
            return repaired

        return jobs

    async def _extract_careers_page_api_jobs_v78(self, page_url: str, html: str) -> list[dict]:
        host = (urlparse(page_url).netloc or "").lower()
        if host in {"example.com", "localhost", "127.0.0.1"}:
            return []

        marker = "api/v1.0/c/" in (html or "") and "clientSlug" in (html or "")
        if "careers-page.com" not in host and not marker:
            return []

        api_bases = self._careers_page_api_bases_v78(page_url, html)
        if not api_bases:
            return []

        jobs: list[dict] = []
        seen_urls: set[str] = set()

        try:
            async with httpx.AsyncClient(
                timeout=4.0,
                follow_redirects=True,
                headers={
                    "User-Agent": "Mozilla/5.0",
                    "Accept": "application/json,text/plain,*/*",
                },
            ) as client:
                for api_base, base_url, slug in api_bases:
                    next_url = f"{api_base}jobs/?page_size=100&page=1&ordering=-is_pinned_in_career_page,-last_published_at"
                    for _ in range(4):
                        if not next_url:
                            break
                        try:
                            resp = await client.get(next_url)
                        except Exception:
                            break
                        if resp.status_code != 200:
                            break

                        payload = resp.json() if "json" in (resp.headers.get("content-type") or "").lower() else None
                        if not isinstance(payload, dict):
                            break

                        items = payload.get("results")
                        if not isinstance(items, list) or not items:
                            break

                        for item in items[:200]:
                            if not isinstance(item, dict):
                                continue
                            job = self._careers_page_item_to_job_v78(item, page_url, base_url, slug)
                            if not job:
                                continue
                            source_url = str(job.get("source_url") or "")
                            if not source_url or source_url in seen_urls:
                                continue
                            seen_urls.add(source_url)
                            jobs.append(job)
                            if len(jobs) >= MAX_JOBS_PER_PAGE:
                                return self._dedupe_basic_v66(jobs)

                        next_raw = payload.get("next")
                        next_url = next_raw.strip() if isinstance(next_raw, str) else ""
                        if not next_url:
                            break
                        next_url = urljoin(next_url if next_url.startswith("http") else api_base, next_url)
        except Exception:
            return []

        return self._dedupe_basic_v66(jobs)

    def _careers_page_api_bases_v78(self, page_url: str, html: str) -> list[tuple[str, str, str]]:
        base_match = _V78_CAREERS_PAGE_BASE.search(html or "")
        slug_match = _V78_CAREERS_PAGE_SLUG.search(html or "")
        if not base_match or not slug_match:
            return []

        base_url = base_match.group(1).strip().rstrip("/")
        slug = slug_match.group(1).strip()
        if not base_url or not slug:
            return []

        candidate_bases: list[str] = []
        direct_api_match = _V78_CAREERS_PAGE_DIRECT_API.search(html or "")
        if direct_api_match:
            api_tpl = direct_api_match.group(1)
            api_tpl = api_tpl.replace("${baseUrl}", base_url).replace("${clientSlug}", slug)
            api_tpl = api_tpl.strip().rstrip("/")
            if api_tpl:
                candidate_bases.append(f"{api_tpl}/")

        candidate_bases.append(f"{base_url}/api/v1.0/c/{slug}/")

        out: list[tuple[str, str, str]] = []
        seen: set[str] = set()
        for api_base in candidate_bases:
            norm = api_base.strip()
            if not norm or norm in seen:
                continue
            seen.add(norm)
            out.append((norm, base_url, slug))
        return out

    def _careers_page_item_to_job_v78(
        self,
        item: dict[str, Any],
        page_url: str,
        base_url: str,
        slug: str,
    ) -> Optional[dict]:
        raw_title = ""
        for key in ("position_name", "title", "name", "job_title"):
            value = item.get(key)
            if isinstance(value, str) and value.strip():
                raw_title = value.strip()
                break

        title = self._normalize_title(raw_title)
        if not title or _V78_TEMPLATE_TITLE.search(title):
            return None
        if not self._is_valid_title_v60(title):
            return None

        source_url = ""
        for key in ("url", "job_url", "jobUrl", "detail_url", "absolute_url"):
            value = item.get(key)
            if isinstance(value, str) and value.strip():
                source_url = value.strip()
                break

        if not source_url:
            hash_value = item.get("hash")
            if isinstance(hash_value, str) and hash_value.strip():
                source_url = f"{base_url}/{slug}/job/{hash_value.strip()}"

        source_url = (_resolve_url(source_url, page_url) or "").split("#", 1)[0]
        if not source_url:
            return None
        if self._is_non_job_url(source_url):
            return None

        parsed = urlparse(source_url)
        if "/job/" not in parsed.path.lower() and not self._is_job_like_url(source_url):
            return None

        location = self._careers_page_location_v78(item)
        description = self._clean_description_v73(
            item.get("description")
            or item.get("summary")
            or item.get("short_description")
            or item.get("overview")
        )

        employment_type = None
        for key in ("employment_type", "job_type", "work_type"):
            value = item.get(key)
            if isinstance(value, str) and value.strip():
                employment_type = value.strip()[:80]
                break

        return {
            "title": title,
            "source_url": source_url,
            "location_raw": location,
            "description": description,
            "salary_raw": None,
            "employment_type": employment_type,
            "extraction_method": "ats_careers_page_api_v78",
            "extraction_confidence": 0.92,
        }

    def _repair_connx_same_page_urls_v78(self, jobs: list[dict], html: str, page_url: str) -> list[dict]:
        host = (urlparse(page_url).netloc or "").lower()
        if "connxcareers.com" not in host or not jobs:
            return jobs

        page_norm = page_url.rstrip("/")
        weak_indexes = [
            idx
            for idx, job in enumerate(jobs)
            if not str(job.get("source_url") or "").strip()
            or str(job.get("source_url") or "").rstrip("/") == page_norm
        ]
        if not weak_indexes:
            return jobs

        strong_seen = {
            str(job.get("source_url") or "").split("#", 1)[0]
            for job in jobs
            if str(job.get("source_url") or "").rstrip("/") != page_norm
        }
        detail_urls: list[str] = []
        for match in _V78_CONNX_DETAIL_PATH.finditer(html or ""):
            candidate = (_resolve_url(match.group(0), page_url) or "").split("#", 1)[0]
            if not candidate or candidate in strong_seen or candidate in detail_urls:
                continue
            detail_urls.append(candidate)
            if len(detail_urls) >= 200:
                break

        if not detail_urls:
            return jobs

        repaired = [dict(job) for job in jobs]
        remaining = list(detail_urls)
        for idx in weak_indexes:
            if not remaining:
                break
            repaired[idx]["source_url"] = remaining.pop(0)

        return repaired

    def _careers_page_location_v78(self, item: dict[str, Any]) -> Optional[str]:
        parts: list[str] = []
        for key in ("city", "state", "country"):
            value = item.get(key)
            if isinstance(value, str):
                cleaned = value.strip(" ,|")
                if cleaned:
                    parts.append(cleaned)
        if not parts:
            return None
        return ", ".join(parts)[:120]
