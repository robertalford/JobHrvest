"""
ATS-specific job extractors.

Each known ATS has a dedicated extractor class that knows the exact page structure
or public API. These are the highest-accuracy extractors in the pipeline.

Implemented: Greenhouse, Lever, Workday, BambooHR, Ashby, SmartRecruiters, Jobvite, iCIMS
"""

import logging
from abc import ABC, abstractmethod
from typing import Optional
from urllib.parse import urljoin, urlparse

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
    Greenhouse boards (boards.greenhouse.io/{company}).
    Public API: https://boards-api.greenhouse.io/v1/boards/{company}/jobs?content=true
    """

    async def extract(self, url: str, html: str) -> list[dict]:
        slug = self._extract_slug(url)
        if slug:
            api_jobs = await self._extract_api(slug)
            if api_jobs:
                return api_jobs
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
            # Employment type lives in metadata fields
            emp_type = None
            for meta in item.get("metadata", []):
                if isinstance(meta, dict) and meta.get("name", "").lower() in ("employment type", "employment_type", "type"):
                    emp_type = meta.get("value")
                    break
            jobs.append({
                "external_id": str(item.get("id")),
                "title": item.get("title", ""),
                "description": item.get("content", ""),
                "source_url": item.get("absolute_url", ""),
                "application_url": item.get("absolute_url", ""),
                "location_raw": location.get("name", "") if isinstance(location, dict) else "",
                "department": item.get("departments", [{}])[0].get("name") if item.get("departments") else None,
                "employment_type": emp_type,
                "date_posted": item.get("updated_at", "")[:10] if item.get("updated_at") else None,
                "extraction_method": "ats_api",
                "extraction_confidence": 0.98,
                "raw_data": item,
            })
        return jobs

    def _extract_html(self, html: str, base_url: str) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        jobs = []
        for div in soup.select(".opening, [class*='job-post']"):
            link = div.find("a")
            if not link:
                continue
            title_el = div.find(["h3", "h4", "h5", "span"]) or link
            loc_el = div.select_one(".location, [class*='location']")
            jobs.append({
                "title": title_el.get_text(strip=True),
                "source_url": urljoin(base_url, link.get("href", "")),
                "location_raw": loc_el.get_text(strip=True) if loc_el else None,
                "extraction_method": "ats_html",
                "extraction_confidence": 0.85,
            })
        return jobs


class LeverExtractor(BaseATSExtractor):
    """
    Lever boards (jobs.lever.co/{company}).
    Public API: https://api.lever.co/v0/postings/{company}?mode=json
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
        for item in (data if isinstance(data, list) else []):
            cats = item.get("categories", {})
            # workplaceType = "on-site"/"remote" (work style), NOT the actual location.
            # Real location is in categories.location (e.g. "Sydney, NSW").
            location_raw = cats.get("location") or ""
            # Fall back to workplaceType only if it looks like a real location (not just "on-site")
            if not location_raw:
                wp = item.get("workplaceType", "")
                if wp and wp.lower() not in ("on-site", "onsite", "remote", "hybrid", ""):
                    location_raw = wp
            jobs.append({
                "external_id": item.get("id"),
                "title": item.get("text", ""),
                "description": item.get("descriptionPlain", ""),
                "source_url": item.get("hostedUrl", ""),
                "application_url": item.get("applyUrl", ""),
                "location_raw": location_raw,
                "department": cats.get("department"),
                "team": cats.get("team"),
                "employment_type": cats.get("commitment"),
                "extraction_method": "ats_api",
                "extraction_confidence": 0.98,
                "raw_data": item,
            })
        return jobs

    def _extract_html(self, html: str, base_url: str) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        jobs = []
        for posting in soup.select(".posting, [class*='posting']"):
            link = posting.select_one("a.posting-title, a[class*='posting-title']")
            if not link:
                link = posting.find("a")
            if not link:
                continue
            title_el = posting.select_one("h5, h4, [class*='posting-name']") or link
            loc_el = posting.select_one(".sort-by-location, [class*='location']")
            dept_el = posting.select_one(".sort-by-department, [class*='department']")
            jobs.append({
                "title": title_el.get_text(strip=True),
                "source_url": urljoin(base_url, link.get("href", "")),
                "location_raw": loc_el.get_text(strip=True) if loc_el else None,
                "department": dept_el.get_text(strip=True) if dept_el else None,
                "extraction_method": "ats_html",
                "extraction_confidence": 0.85,
            })
        return jobs


class WorkdayExtractor(BaseATSExtractor):
    """
    Workday career pages (myworkdayjobs.com).
    Workday requires JS rendering — this is a basic HTML fallback.
    Full extraction requires Playwright (Phase 3).
    """

    async def extract(self, url: str, html: str) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        jobs = []
        # Workday renders with data-automation-id attributes
        for item in soup.select("[data-automation-id='jobTitle'], [class*='jobTitle']"):
            link = item.find_parent("a") or item.find("a")
            loc_el = item.find_next_sibling(lambda t: t and any(
                kw in (t.get("class") or []) for kw in ["location", "jobLocation"]
            ))
            jobs.append({
                "title": item.get_text(strip=True),
                "source_url": urljoin(url, link.get("href", "")) if link else url,
                "location_raw": loc_el.get_text(strip=True) if loc_el else None,
                "extraction_method": "ats_html",
                "extraction_confidence": 0.72,
                "raw_data": {"note": "Workday requires JS rendering for full extraction"},
            })
        return jobs


class BambooHRExtractor(BaseATSExtractor):
    """
    BambooHR career pages ({company}.bamboohr.com/careers).
    Public list API: GET /careers/list
    """

    async def extract(self, url: str, html: str) -> list[dict]:
        # Try the JSON list API
        parsed = urlparse(url)
        api_url = f"{parsed.scheme}://{parsed.netloc}/careers/list"
        try:
            async with httpx.AsyncClient(
                headers={**self.headers, "Accept": "application/json"},
                timeout=30,
                follow_redirects=True,
            ) as client:
                resp = await client.get(api_url)
                resp.raise_for_status()
                data = resp.json()
            return self._parse_api(data, url)
        except Exception:
            pass

        # Fallback: HTML parsing
        return self._extract_html(html, url)

    def _parse_api(self, data: dict, base_url: str) -> list[dict]:
        jobs = []
        for item in data.get("result", []):
            job_url = urljoin(base_url, f"/careers/{item.get('id')}")
            jobs.append({
                "external_id": str(item.get("id", "")),
                "title": item.get("jobOpeningName", "") or item.get("title", ""),
                "location_raw": item.get("location", {}).get("city", "") if isinstance(item.get("location"), dict) else item.get("location", ""),
                "department": item.get("department", {}).get("label", "") if isinstance(item.get("department"), dict) else item.get("department", ""),
                "employment_type": item.get("employmentStatusLabel", ""),
                "source_url": job_url,
                "extraction_method": "ats_api",
                "extraction_confidence": 0.96,
                "raw_data": item,
            })
        return jobs

    def _extract_html(self, html: str, base_url: str) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        jobs = []
        for row in soup.select(".BambooHR-ATS-board-item, [class*='careers-item'], [class*='job-listing']"):
            link = row.find("a")
            title_el = row.find(["h2", "h3", "h4", "strong"]) or link
            if not title_el:
                continue
            loc_el = row.select_one("[class*='location'], [class*='city']")
            dept_el = row.select_one("[class*='department']")
            jobs.append({
                "title": title_el.get_text(strip=True),
                "source_url": urljoin(base_url, link.get("href", "")) if link else base_url,
                "location_raw": loc_el.get_text(strip=True) if loc_el else None,
                "department": dept_el.get_text(strip=True) if dept_el else None,
                "extraction_method": "ats_html",
                "extraction_confidence": 0.82,
            })
        return jobs


class AshbyExtractor(BaseATSExtractor):
    """
    Ashby HQ job boards (jobs.ashbyhq.com/{company}).
    Public API: GET https://api.ashbyhq.com/posting-api/job-board/{company}
    """

    async def extract(self, url: str, html: str) -> list[dict]:
        slug = urlparse(url).path.strip("/").split("/")[0]
        if slug:
            try:
                api_url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}"
                async with httpx.AsyncClient(headers=self.headers, timeout=30) as client:
                    resp = await client.get(api_url)
                    resp.raise_for_status()
                    data = resp.json()
                jobs = []
                for item in data.get("jobPostings", []):
                    jobs.append({
                        "external_id": item.get("id"),
                        "title": item.get("title", ""),
                        "description": item.get("descriptionHtml", ""),
                        "source_url": item.get("jobUrl", url),
                        "location_raw": item.get("locationName", ""),
                        "department": item.get("departmentName", ""),
                        "employment_type": item.get("employmentType", ""),
                        "extraction_method": "ats_api",
                        "extraction_confidence": 0.97,
                        "raw_data": item,
                    })
                if jobs:
                    return jobs
            except Exception as e:
                logger.warning(f"Ashby API failed for {slug}: {e}")

        # HTML fallback
        return self._extract_html(html, url)

    def _extract_html(self, html: str, base_url: str) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        jobs = []
        for row in soup.select("[class*='ashby-job'], [class*='job-posting']"):
            link = row.find("a")
            title_el = row.find(["h3", "h4", "h5", "strong"]) or link
            if not title_el:
                continue
            loc_el = row.select_one("[class*='location']")
            jobs.append({
                "title": title_el.get_text(strip=True),
                "source_url": urljoin(base_url, link.get("href", "")) if link else base_url,
                "location_raw": loc_el.get_text(strip=True) if loc_el else None,
                "extraction_method": "ats_html",
                "extraction_confidence": 0.83,
            })
        return jobs


class SmartRecruitersExtractor(BaseATSExtractor):
    """
    SmartRecruiters career pages (careers.smartrecruiters.com/{company}).
    Public API: GET https://api.smartrecruiters.com/v1/companies/{company}/postings
    """

    async def extract(self, url: str, html: str) -> list[dict]:
        slug = urlparse(url).path.strip("/").split("/")[0]
        if slug:
            try:
                api_url = f"https://api.smartrecruiters.com/v1/companies/{slug}/postings"
                async with httpx.AsyncClient(headers=self.headers, timeout=30) as client:
                    resp = await client.get(api_url)
                    resp.raise_for_status()
                    data = resp.json()
                jobs = []
                for item in data.get("content", []):
                    loc = item.get("location", {})
                    jobs.append({
                        "external_id": item.get("id"),
                        "title": item.get("name", ""),
                        "source_url": f"https://careers.smartrecruiters.com/{slug}/{item.get('id')}",
                        "location_raw": f"{loc.get('city', '')} {loc.get('country', '')}".strip(),
                        "department": item.get("department", {}).get("label") if item.get("department") else None,
                        "employment_type": item.get("typeOfEmployment", {}).get("label") if item.get("typeOfEmployment") else None,
                        "date_posted": item.get("releasedDate", "")[:10] if item.get("releasedDate") else None,
                        "extraction_method": "ats_api",
                        "extraction_confidence": 0.96,
                        "raw_data": item,
                    })
                if jobs:
                    return jobs
            except Exception as e:
                logger.warning(f"SmartRecruiters API failed for {slug}: {e}")

        return self._extract_html(html, url)

    def _extract_html(self, html: str, base_url: str) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        jobs = []
        for row in soup.select("[class*='job-ad'], [class*='opening-item'], li[class*='job']"):
            link = row.find("a")
            title_el = row.find(["h4", "h3", "strong", "span"]) or link
            if not title_el:
                continue
            loc_el = row.select_one("[class*='location'], [class*='city']")
            jobs.append({
                "title": title_el.get_text(strip=True),
                "source_url": urljoin(base_url, link.get("href", "")) if link else base_url,
                "location_raw": loc_el.get_text(strip=True) if loc_el else None,
                "extraction_method": "ats_html",
                "extraction_confidence": 0.80,
            })
        return jobs


class JobviteExtractor(BaseATSExtractor):
    """
    Jobvite career pages (jobs.jobvite.com/{company}).
    HTML-based — Jobvite has no reliable public API.
    """

    async def extract(self, url: str, html: str) -> list[dict]:
        # Try Jobvite's feed endpoint
        parsed = urlparse(url)
        slug = parsed.path.strip("/").split("/")[0]
        try:
            feed_url = f"https://jobs.jobvite.com/{slug}/jobs/feed.json"
            async with httpx.AsyncClient(headers=self.headers, timeout=20) as client:
                resp = await client.get(feed_url)
                if resp.status_code == 200:
                    data = resp.json()
                    jobs = []
                    for item in data.get("requisitions", []):
                        jobs.append({
                            "external_id": item.get("id"),
                            "title": item.get("title", ""),
                            "location_raw": item.get("location", ""),
                            "department": item.get("category", ""),
                            "source_url": item.get("applyUrl", url),
                            "date_posted": item.get("date", "")[:10] if item.get("date") else None,
                            "extraction_method": "ats_api",
                            "extraction_confidence": 0.93,
                            "raw_data": item,
                        })
                    if jobs:
                        return jobs
        except Exception:
            pass

        return self._extract_html(html, url)

    def _extract_html(self, html: str, base_url: str) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        jobs = []
        for row in soup.select("tr.jv-job-list-row, [class*='job-post'], [class*='jv-job']"):
            link = row.find("a")
            title_el = row.select_one(".jv-job-list-name, [class*='job-title'], td:first-child") or link
            if not title_el:
                continue
            loc_el = row.select_one(".jv-job-list-location, [class*='location']")
            jobs.append({
                "title": title_el.get_text(strip=True),
                "source_url": urljoin(base_url, link.get("href", "")) if link else base_url,
                "location_raw": loc_el.get_text(strip=True) if loc_el else None,
                "extraction_method": "ats_html",
                "extraction_confidence": 0.80,
            })
        return jobs


class ICIMSExtractor(BaseATSExtractor):
    """
    iCIMS career pages.
    iCIMS has a JSON API: /api/job/search
    """

    async def extract(self, url: str, html: str) -> list[dict]:
        parsed = urlparse(url)
        # iCIMS API endpoint pattern
        try:
            api_url = f"{parsed.scheme}://{parsed.netloc}/api/job/search?applyCountry=&applyState=&applyCity=&page=0&pageSize=50&portfolioId=-1&isFeatured=false"
            async with httpx.AsyncClient(headers=self.headers, timeout=20) as client:
                resp = await client.get(api_url)
                if resp.status_code == 200:
                    data = resp.json()
                    jobs = []
                    for item in data.get("searchResults", []):
                        jobs.append({
                            "external_id": str(item.get("jobId", "")),
                            "title": item.get("title", ""),
                            "location_raw": item.get("city", ""),
                            "department": item.get("category", ""),
                            "source_url": item.get("jobDetailUrl", url),
                            "date_posted": item.get("datePosted", "")[:10] if item.get("datePosted") else None,
                            "extraction_method": "ats_api",
                            "extraction_confidence": 0.92,
                            "raw_data": item,
                        })
                    if jobs:
                        return jobs
        except Exception:
            pass

        return self._extract_html(html, url)

    def _extract_html(self, html: str, base_url: str) -> list[dict]:
        soup = BeautifulSoup(html, "lxml")
        jobs = []
        for row in soup.select("[class*='iCIMS_JobsTable'] tr, [class*='job-tile'], [id*='iCIMS']"):
            link = row.find("a")
            title_el = row.select_one("[class*='title'], [class*='job-title']") or link
            if not title_el or not title_el.get_text(strip=True):
                continue
            loc_el = row.select_one("[class*='location'], [class*='city']")
            jobs.append({
                "title": title_el.get_text(strip=True),
                "source_url": urljoin(base_url, link.get("href", "")) if link else base_url,
                "location_raw": loc_el.get_text(strip=True) if loc_el else None,
                "extraction_method": "ats_html",
                "extraction_confidence": 0.78,
            })
        return jobs


# Registry mapping platform name → extractor class
REGISTRY: dict[str, type[BaseATSExtractor]] = {
    "greenhouse": GreenhouseExtractor,
    "lever": LeverExtractor,
    "workday": WorkdayExtractor,
    "bamboohr": BambooHRExtractor,
    "ashby": AshbyExtractor,
    "smartrecruiters": SmartRecruitersExtractor,
    "jobvite": JobviteExtractor,
    "icims": ICIMSExtractor,
}
