"""
ATS-specific job extractors.

Each known ATS has a dedicated extractor class that knows the exact page structure.
These are the highest-accuracy extractors in the pipeline.

Implemented: Greenhouse, Lever, Workday (Phase 2/3)
"""

import logging
from abc import ABC, abstractmethod
from typing import Optional
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup

from app.core.config import settings

logger = logging.getLogger(__name__)


class BaseATSExtractor(ABC):
    headers = {"User-Agent": settings.CRAWL_USER_AGENT}

    @abstractmethod
    async def extract(self, url: str, html: str) -> list[dict]:
        """Extract job listings from the given page. Returns list of job dicts."""


class GreenhouseExtractor(BaseATSExtractor):
    """
    Extracts jobs from Greenhouse boards (boards.greenhouse.io/{company}).
    Greenhouse provides a public JSON API: https://boards-api.greenhouse.io/v1/boards/{company}/jobs
    """

    async def extract(self, url: str, html: str) -> list[dict]:
        # Try the JSON API first
        slug = self._extract_slug(url)
        if slug:
            api_jobs = await self._extract_api(slug)
            if api_jobs:
                return api_jobs

        # Fallback: parse HTML
        return self._extract_html(html, url)

    def _extract_slug(self, url: str) -> Optional[str]:
        path = urlparse(url).path
        parts = [p for p in path.split("/") if p]
        return parts[0] if parts else None

    async def _extract_api(self, slug: str) -> list[dict]:
        api_url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true"
        try:
            async with httpx.AsyncClient(headers=self.headers, timeout=30) as client:
                resp = await client.get(api_url)
                resp.raise_for_status()
                data = resp.json()
        except Exception as e:
            logger.warning(f"Greenhouse API failed for {slug}: {e}")
            return []

        jobs = []
        for item in data.get("jobs", []):
            location = item.get("location", {})
            jobs.append({
                "external_id": str(item.get("id")),
                "title": item.get("title", ""),
                "description": item.get("content", ""),
                "source_url": item.get("absolute_url", ""),
                "application_url": item.get("absolute_url", ""),
                "location_raw": location.get("name", ""),
                "department": item.get("departments", [{}])[0].get("name") if item.get("departments") else None,
                "date_posted": item.get("updated_at", "")[:10] if item.get("updated_at") else None,
                "extraction_method": "ats_api",
                "extraction_confidence": 0.98,
                "raw_data": item,
            })
        return jobs

    def _extract_html(self, html: str, base_url: str) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        jobs = []
        for div in soup.select(".opening"):
            link = div.find("a")
            if not link:
                continue
            from urllib.parse import urljoin
            jobs.append({
                "title": link.get_text(strip=True),
                "source_url": urljoin(base_url, link.get("href", "")),
                "location_raw": div.select_one(".location") and div.select_one(".location").get_text(strip=True),
                "department": div.parent.find_previous_sibling("h3") and div.parent.find_previous_sibling("h3").get_text(strip=True),
                "extraction_method": "ats_html",
                "extraction_confidence": 0.88,
                "raw_data": {"html_snippet": str(div)[:500]},
            })
        return jobs


class LeverExtractor(BaseATSExtractor):
    """
    Extracts jobs from Lever boards (jobs.lever.co/{company}).
    Lever provides a public JSON API: https://api.lever.co/v0/postings/{company}?mode=json
    """

    async def extract(self, url: str, html: str) -> list[dict]:
        slug = urlparse(url).path.strip("/").split("/")[0]
        if slug:
            api_jobs = await self._extract_api(slug)
            if api_jobs:
                return api_jobs
        return self._extract_html(html, url)

    async def _extract_api(self, slug: str) -> list[dict]:
        api_url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
        try:
            async with httpx.AsyncClient(headers=self.headers, timeout=30) as client:
                resp = await client.get(api_url)
                resp.raise_for_status()
                data = resp.json()
        except Exception as e:
            logger.warning(f"Lever API failed for {slug}: {e}")
            return []

        jobs = []
        for item in data if isinstance(data, list) else []:
            jobs.append({
                "external_id": item.get("id"),
                "title": item.get("text", ""),
                "description": item.get("descriptionPlain", ""),
                "source_url": item.get("hostedUrl", ""),
                "application_url": item.get("applyUrl", ""),
                "location_raw": item.get("workplaceType", ""),
                "department": item.get("categories", {}).get("department"),
                "team": item.get("categories", {}).get("team"),
                "employment_type": item.get("categories", {}).get("commitment"),
                "extraction_method": "ats_api",
                "extraction_confidence": 0.98,
                "raw_data": item,
            })
        return jobs

    def _extract_html(self, html: str, base_url: str) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        jobs = []
        for posting in soup.select(".posting"):
            link = posting.select_one("a.posting-title")
            if not link:
                continue
            from urllib.parse import urljoin
            jobs.append({
                "title": posting.select_one("h5") and posting.select_one("h5").get_text(strip=True) or "",
                "source_url": urljoin(base_url, link.get("href", "")),
                "location_raw": posting.select_one(".sort-by-location") and posting.select_one(".sort-by-location").get_text(strip=True),
                "department": posting.select_one(".sort-by-department") and posting.select_one(".sort-by-department").get_text(strip=True),
                "employment_type": posting.select_one(".sort-by-commitment") and posting.select_one(".sort-by-commitment").get_text(strip=True),
                "extraction_method": "ats_html",
                "extraction_confidence": 0.88,
            })
        return jobs


class WorkdayExtractor(BaseATSExtractor):
    """
    Extracts jobs from Workday career pages.
    Workday requires JS rendering — this is a basic HTML fallback.
    Full implementation requires Playwright (Phase 3).
    """

    async def extract(self, url: str, html: str) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        jobs = []
        # Workday renders job listings in a list with data-automation-id attributes
        for item in soup.select("[data-automation-id='jobTitle']"):
            link = item.find_parent("a") or item.find("a")
            from urllib.parse import urljoin
            jobs.append({
                "title": item.get_text(strip=True),
                "source_url": urljoin(url, link.get("href", "")) if link else url,
                "extraction_method": "ats_html",
                "extraction_confidence": 0.75,  # Lower confidence — Workday needs JS
                "raw_data": {"note": "Workday requires JS rendering for full extraction"},
            })
        return jobs


# Registry mapping platform name → extractor class
REGISTRY: dict[str, type[BaseATSExtractor]] = {
    "greenhouse": GreenhouseExtractor,
    "lever": LeverExtractor,
    "workday": WorkdayExtractor,
}
