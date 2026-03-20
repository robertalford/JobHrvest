"""
ATS Bulk Extractor — one API call per company to get ALL open jobs.

This is the highest-impact optimisation in the pipeline. For companies with a
known ATS platform, we bypass per-page crawling entirely and call the ATS
platform's public bulk API to retrieve every open position in a single request
(or paginated sequence).

Supported platforms:
  - Greenhouse (boards-api.greenhouse.io)
  - Lever (api.lever.co)
  - SmartRecruiters (api.smartrecruiters.com)
  - Ashby (api.ashbyhq.com)
  - BambooHR ({company}.bamboohr.com/careers/list)
  - Workday (CXS JSON API)
  - Jobvite (feed.json)
  - iCIMS (/api/job/search)

Each extractor:
  1. Determines the board/company identifier from career page URLs or root_url
  2. Makes one (or paginated) API call(s) to get all jobs
  3. Returns a list of normalised job dicts ready for upsert

Usage from Celery:
  from app.crawlers.ats_bulk_extractor import ATSBulkExtractor
  extractor = ATSBulkExtractor()
  jobs = await extractor.extract_all(company, career_pages)
"""

import logging
import re
from typing import Optional
from urllib.parse import urlparse

import httpx

from app.core.config import settings

logger = logging.getLogger(__name__)

# Shared headers for all API requests
_HEADERS = {"User-Agent": settings.CRAWL_USER_AGENT}
_TIMEOUT = 45  # generous timeout for bulk responses


class ATSBulkExtractor:
    """Orchestrator: picks the right platform extractor and runs it."""

    def __init__(self):
        self._extractors = {
            "greenhouse": GreenhouseBulk(),
            "lever": LeverBulk(),
            "smartrecruiters": SmartRecruitersBulk(),
            "ashby": AshbyBulk(),
            "bamboohr": BambooHRBulk(),
            "workday": WorkdayBulk(),
            "jobvite": JobviteBulk(),
            "icims": ICIMSBulk(),
        }

    def supports(self, platform: str) -> bool:
        return platform in self._extractors

    async def extract_all(self, company, career_pages: list) -> list[dict]:
        """
        Extract all jobs for a company via its ATS bulk API.

        Args:
            company: Company ORM object with ats_platform, root_url, domain, etc.
            career_pages: List of CareerPage ORM objects (used for slug extraction)

        Returns:
            List of job dicts ready for JobExtractor._upsert_job()
        """
        platform = (company.ats_platform or "").lower().strip()
        extractor = self._extractors.get(platform)
        if not extractor:
            logger.debug(f"No bulk extractor for platform: {platform}")
            return []

        # Gather candidate URLs for slug extraction
        urls = []
        for page in career_pages:
            if page.url:
                urls.append(page.url)
        if company.root_url:
            urls.append(company.root_url)

        try:
            jobs = await extractor.extract(urls, company)
            if jobs:
                logger.info(
                    f"ATS bulk extract ({platform}): {len(jobs)} jobs for "
                    f"{company.domain}"
                )
            else:
                logger.info(
                    f"ATS bulk extract ({platform}): 0 jobs for {company.domain}"
                )
            return jobs
        except Exception as e:
            logger.error(
                f"ATS bulk extract ({platform}) failed for {company.domain}: {e}",
                exc_info=True,
            )
            return []


# ─── Platform-specific bulk extractors ────────────────────────────────────────


class GreenhouseBulk:
    """
    Greenhouse: GET https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true
    Slug is the board token from boards.greenhouse.io/{slug} or gh_jid/gh_src params.
    """

    _SLUG_PATTERNS = [
        # boards.greenhouse.io/{slug}
        re.compile(r"boards\.greenhouse\.io/([a-zA-Z0-9_-]+)", re.IGNORECASE),
        # job-boards.greenhouse.io/{slug}
        re.compile(r"job-boards\.greenhouse\.io/([a-zA-Z0-9_-]+)", re.IGNORECASE),
    ]

    def _find_slug(self, urls: list[str]) -> Optional[str]:
        for url in urls:
            for pat in self._SLUG_PATTERNS:
                m = pat.search(url)
                if m:
                    return m.group(1)
        # Fallback: look for greenhouse embed params (gh_src) in any URL —
        # the company domain itself might be the slug
        return None

    async def extract(self, urls: list[str], company) -> list[dict]:
        slug = self._find_slug(urls)
        if not slug:
            # Try company domain as slug (some companies use their name)
            slug = company.domain.split(".")[0].replace("-", "").lower()
            logger.debug(f"Greenhouse: no slug in URLs, trying domain-derived: {slug}")

        api_url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true"
        try:
            async with httpx.AsyncClient(headers=_HEADERS, timeout=_TIMEOUT) as client:
                resp = await client.get(api_url)
                if resp.status_code == 404:
                    # Slug didn't work — try alternate slugs
                    for alt in self._alternate_slugs(company):
                        resp = await client.get(
                            f"https://boards-api.greenhouse.io/v1/boards/{alt}/jobs?content=true"
                        )
                        if resp.status_code == 200:
                            slug = alt
                            break
                    else:
                        logger.info(f"Greenhouse: no board found for {company.domain}")
                        return []
                resp.raise_for_status()
                data = resp.json()
        except httpx.HTTPStatusError:
            logger.warning(f"Greenhouse API HTTP error for {slug}")
            return []
        except Exception as e:
            logger.warning(f"Greenhouse API failed for {slug}: {e}")
            return []

        jobs = []
        for item in data.get("jobs", []):
            location = item.get("location", {})
            emp_type = None
            for meta in item.get("metadata", []):
                if isinstance(meta, dict) and meta.get("name", "").lower() in (
                    "employment type", "employment_type", "type",
                ):
                    emp_type = meta.get("value")
                    break
            jobs.append({
                "external_id": str(item.get("id")),
                "title": item.get("title", ""),
                "description": item.get("content", ""),
                "source_url": item.get("absolute_url", ""),
                "application_url": item.get("absolute_url", ""),
                "location_raw": location.get("name", "") if isinstance(location, dict) else "",
                "department": (
                    item.get("departments", [{}])[0].get("name")
                    if item.get("departments")
                    else None
                ),
                "employment_type": emp_type,
                "date_posted": (
                    item.get("updated_at", "")[:10] if item.get("updated_at") else None
                ),
                "extraction_method": "ats_bulk_api",
                "extraction_confidence": 0.98,
                "raw_data": item,
            })
        return jobs

    def _alternate_slugs(self, company) -> list[str]:
        """Generate alternate board slugs from company info."""
        slugs = []
        # Try domain without TLD
        base = company.domain.split(".")[0].lower()
        slugs.append(base)
        # Try with hyphens removed
        if "-" in base:
            slugs.append(base.replace("-", ""))
        # Try company name lowered
        if company.name:
            name_slug = re.sub(r"[^a-z0-9]", "", company.name.lower())
            if name_slug and name_slug != base:
                slugs.append(name_slug)
        return slugs


class LeverBulk:
    """
    Lever: GET https://api.lever.co/v0/postings/{slug}?mode=json
    Slug is from jobs.lever.co/{slug}
    """

    _SLUG_PATTERN = re.compile(r"jobs\.lever\.co/([a-zA-Z0-9_-]+)", re.IGNORECASE)

    def _find_slug(self, urls: list[str]) -> Optional[str]:
        for url in urls:
            m = self._SLUG_PATTERN.search(url)
            if m:
                return m.group(1)
        return None

    async def extract(self, urls: list[str], company) -> list[dict]:
        slug = self._find_slug(urls)
        if not slug:
            slug = company.domain.split(".")[0].lower()
            logger.debug(f"Lever: no slug in URLs, trying domain-derived: {slug}")

        api_url = f"https://api.lever.co/v0/postings/{slug}?mode=json"
        try:
            async with httpx.AsyncClient(headers=_HEADERS, timeout=_TIMEOUT) as client:
                resp = await client.get(api_url)
                resp.raise_for_status()
                data = resp.json()
        except Exception as e:
            logger.warning(f"Lever API failed for {slug}: {e}")
            return []

        if not isinstance(data, list):
            return []

        jobs = []
        for item in data:
            cats = item.get("categories", {})
            location_raw = cats.get("location") or ""
            if not location_raw:
                wp = item.get("workplaceType", "")
                if wp and wp.lower() not in ("on-site", "onsite", "remote", "hybrid", ""):
                    location_raw = wp

            sal = item.get("salaryRange") or {}
            sal_raw = None
            if sal.get("min") or sal.get("max"):
                sal_raw = (
                    f"{sal.get('currency', '')} {sal.get('min', '')}-"
                    f"{sal.get('max', '')} {sal.get('interval', '')}"
                ).strip()

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
                "salary_raw": sal_raw,
                "salary_min": sal.get("min"),
                "salary_max": sal.get("max"),
                "salary_currency": sal.get("currency"),
                "extraction_method": "ats_bulk_api",
                "extraction_confidence": 0.98,
                "raw_data": item,
            })
        return jobs


class SmartRecruitersBulk:
    """
    SmartRecruiters: GET https://api.smartrecruiters.com/v1/companies/{slug}/postings
    Supports pagination via offset parameter. Slug from careers.smartrecruiters.com/{slug}
    """

    _SLUG_PATTERN = re.compile(
        r"careers\.smartrecruiters\.com/([a-zA-Z0-9_.-]+)", re.IGNORECASE
    )

    def _find_slug(self, urls: list[str]) -> Optional[str]:
        for url in urls:
            m = self._SLUG_PATTERN.search(url)
            if m:
                slug = m.group(1).split("/")[0]
                return slug
        return None

    async def extract(self, urls: list[str], company) -> list[dict]:
        slug = self._find_slug(urls)
        if not slug:
            slug = company.domain.split(".")[0].lower()

        all_jobs: list[dict] = []
        offset = 0
        limit = 100

        try:
            async with httpx.AsyncClient(headers=_HEADERS, timeout=_TIMEOUT) as client:
                while True:
                    api_url = (
                        f"https://api.smartrecruiters.com/v1/companies/{slug}/postings"
                        f"?offset={offset}&limit={limit}"
                    )
                    resp = await client.get(api_url)
                    if resp.status_code != 200:
                        if offset == 0:
                            logger.warning(
                                f"SmartRecruiters API {resp.status_code} for {slug}"
                            )
                        break
                    data = resp.json()
                    items = data.get("content", [])
                    if not items:
                        break

                    for item in items:
                        loc = item.get("location", {})
                        all_jobs.append({
                            "external_id": item.get("id"),
                            "title": item.get("name", ""),
                            "source_url": (
                                f"https://careers.smartrecruiters.com/{slug}/"
                                f"{item.get('id')}"
                            ),
                            "location_raw": (
                                f"{loc.get('city', '')} {loc.get('country', '')}".strip()
                            ),
                            "department": (
                                item.get("department", {}).get("label")
                                if item.get("department")
                                else None
                            ),
                            "employment_type": (
                                item.get("typeOfEmployment", {}).get("label")
                                if item.get("typeOfEmployment")
                                else None
                            ),
                            "date_posted": (
                                item.get("releasedDate", "")[:10]
                                if item.get("releasedDate")
                                else None
                            ),
                            "extraction_method": "ats_bulk_api",
                            "extraction_confidence": 0.96,
                            "raw_data": item,
                        })

                    total = data.get("totalFound", 0)
                    offset += limit
                    if offset >= total or len(items) < limit:
                        break
        except Exception as e:
            logger.warning(f"SmartRecruiters API failed for {slug}: {e}")
            return []

        return all_jobs


class AshbyBulk:
    """
    Ashby: GET https://api.ashbyhq.com/posting-api/job-board/{slug}?includeCompensation=true
    Slug from jobs.ashbyhq.com/{slug}
    """

    _SLUG_PATTERN = re.compile(r"jobs\.ashbyhq\.com/([a-zA-Z0-9_-]+)", re.IGNORECASE)

    def _find_slug(self, urls: list[str]) -> Optional[str]:
        for url in urls:
            m = self._SLUG_PATTERN.search(url)
            if m:
                return m.group(1)
        return None

    async def extract(self, urls: list[str], company) -> list[dict]:
        slug = self._find_slug(urls)
        if not slug:
            slug = company.domain.split(".")[0].lower()

        api_url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}?includeCompensation=true"
        try:
            async with httpx.AsyncClient(headers=_HEADERS, timeout=_TIMEOUT) as client:
                resp = await client.get(api_url)
                resp.raise_for_status()
                data = resp.json()
        except Exception as e:
            logger.warning(f"Ashby API failed for {slug}: {e}")
            return []

        jobs = []
        for item in data.get("jobPostings", []):
            jobs.append({
                "external_id": item.get("id"),
                "title": item.get("title", ""),
                "description": item.get("descriptionHtml", ""),
                "source_url": item.get("jobUrl", ""),
                "location_raw": item.get("locationName", ""),
                "department": item.get("departmentName", ""),
                "employment_type": item.get("employmentType", ""),
                "salary_raw": item.get("compensationTierSummary") or None,
                "extraction_method": "ats_bulk_api",
                "extraction_confidence": 0.97,
                "raw_data": item,
            })
        return jobs


class BambooHRBulk:
    """
    BambooHR: GET https://{subdomain}.bamboohr.com/careers/list
    Subdomain from {subdomain}.bamboohr.com career pages.
    """

    _SUBDOMAIN_PATTERN = re.compile(
        r"([a-zA-Z0-9_-]+)\.bamboohr\.com", re.IGNORECASE
    )

    def _find_subdomain(self, urls: list[str]) -> Optional[str]:
        for url in urls:
            m = self._SUBDOMAIN_PATTERN.search(url)
            if m:
                sub = m.group(1)
                if sub.lower() not in ("www", "api"):
                    return sub
        return None

    async def extract(self, urls: list[str], company) -> list[dict]:
        subdomain = self._find_subdomain(urls)
        if not subdomain:
            logger.info(f"BambooHR: no subdomain found for {company.domain}")
            return []

        api_url = f"https://{subdomain}.bamboohr.com/careers/list"
        try:
            async with httpx.AsyncClient(
                headers={**_HEADERS, "Accept": "application/json"},
                timeout=_TIMEOUT,
                follow_redirects=True,
            ) as client:
                resp = await client.get(api_url)
                resp.raise_for_status()
                data = resp.json()
        except Exception as e:
            logger.warning(f"BambooHR API failed for {subdomain}: {e}")
            return []

        base_url = f"https://{subdomain}.bamboohr.com"
        jobs = []
        for item in data.get("result", []):
            loc = item.get("location", {})
            location_raw = (
                loc.get("city", "") if isinstance(loc, dict) else str(loc or "")
            )
            dept = item.get("department", {})
            department = (
                dept.get("label", "") if isinstance(dept, dict) else str(dept or "")
            )
            jobs.append({
                "external_id": str(item.get("id", "")),
                "title": item.get("jobOpeningName", "") or item.get("title", ""),
                "location_raw": location_raw,
                "department": department,
                "employment_type": item.get("employmentStatusLabel", ""),
                "source_url": f"{base_url}/careers/{item.get('id')}",
                "extraction_method": "ats_bulk_api",
                "extraction_confidence": 0.96,
                "raw_data": item,
            })
        return jobs


class WorkdayBulk:
    """
    Workday: POST {base}/wday/cxs/{tenant}/{board}/jobs
    Paginated. Extracts tenant/board from myworkdayjobs.com URLs.
    """

    def _parse_url(self, url: str) -> Optional[tuple[str, str, str]]:
        """Extract (base_url, tenant, job_board) from a Workday URL."""
        parsed = urlparse(url)
        if "myworkdayjobs.com" not in parsed.netloc:
            return None
        path_parts = [p for p in parsed.path.split("/") if p]
        if not path_parts:
            return None
        tenant = parsed.netloc.split(".")[0]
        job_board = path_parts[0]
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        return base_url, tenant, job_board

    def _find_workday_info(self, urls: list[str]) -> Optional[tuple[str, str, str]]:
        for url in urls:
            info = self._parse_url(url)
            if info:
                return info
        return None

    async def extract(self, urls: list[str], company) -> list[dict]:
        info = self._find_workday_info(urls)
        if not info:
            logger.info(f"Workday: no myworkdayjobs.com URL for {company.domain}")
            return []

        base_url, tenant, job_board = info
        api_url = f"{base_url}/wday/cxs/{tenant}/{job_board}/jobs"

        all_jobs: list[dict] = []
        offset = 0
        limit = 20

        try:
            async with httpx.AsyncClient(
                headers={
                    **_HEADERS,
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                },
                timeout=_TIMEOUT,
                follow_redirects=True,
            ) as client:
                while True:
                    resp = await client.post(
                        api_url,
                        json={
                            "limit": limit,
                            "offset": offset,
                            "searchText": "",
                            "appliedFacets": {},
                        },
                    )
                    if resp.status_code != 200:
                        if offset == 0:
                            logger.warning(
                                f"Workday CXS API {resp.status_code} for {api_url}"
                            )
                        break

                    data = resp.json()
                    postings = data.get("jobPostings", [])
                    if not postings:
                        break

                    for item in postings:
                        ext_path = item.get("externalPath", "")
                        job_url = f"{base_url}{ext_path}" if ext_path else ""
                        all_jobs.append({
                            "external_id": (
                                ext_path.split("/")[-1] if ext_path else None
                            ),
                            "title": item.get("title", ""),
                            "location_raw": item.get("locationsText", ""),
                            "employment_type": item.get("timeType", "") or None,
                            "source_url": job_url,
                            "application_url": job_url,
                            "extraction_method": "ats_bulk_api",
                            "extraction_confidence": 0.95,
                            "raw_data": item,
                        })

                    total = data.get("total", 0)
                    offset += limit
                    if offset >= total or len(postings) < limit:
                        break
                    # Safety cap: Workday can have thousands of jobs
                    if offset >= 2000:
                        logger.info(
                            f"Workday: capping at {offset} jobs for {company.domain}"
                        )
                        break
        except Exception as e:
            logger.warning(f"Workday CXS API failed for {company.domain}: {e}")
            return []

        return all_jobs


class JobviteBulk:
    """
    Jobvite: GET https://jobs.jobvite.com/{slug}/jobs/feed.json
    Slug from jobs.jobvite.com/{slug}
    """

    _SLUG_PATTERN = re.compile(r"jobs\.jobvite\.com/([a-zA-Z0-9_-]+)", re.IGNORECASE)

    def _find_slug(self, urls: list[str]) -> Optional[str]:
        for url in urls:
            m = self._SLUG_PATTERN.search(url)
            if m:
                return m.group(1)
        return None

    async def extract(self, urls: list[str], company) -> list[dict]:
        slug = self._find_slug(urls)
        if not slug:
            return []

        feed_url = f"https://jobs.jobvite.com/{slug}/jobs/feed.json"
        try:
            async with httpx.AsyncClient(headers=_HEADERS, timeout=_TIMEOUT) as client:
                resp = await client.get(feed_url)
                if resp.status_code != 200:
                    return []
                data = resp.json()
        except Exception as e:
            logger.warning(f"Jobvite feed failed for {slug}: {e}")
            return []

        jobs = []
        for item in data.get("requisitions", []):
            jobs.append({
                "external_id": item.get("id"),
                "title": item.get("title", ""),
                "location_raw": item.get("location", ""),
                "department": item.get("category", ""),
                "source_url": item.get("applyUrl", ""),
                "date_posted": (
                    item.get("date", "")[:10] if item.get("date") else None
                ),
                "extraction_method": "ats_bulk_api",
                "extraction_confidence": 0.93,
                "raw_data": item,
            })
        return jobs


class ICIMSBulk:
    """
    iCIMS: GET {base}/api/job/search with pagination.
    Base from career page URL on icims.com domain.
    """

    def _find_base_url(self, urls: list[str]) -> Optional[str]:
        for url in urls:
            parsed = urlparse(url)
            if "icims.com" in parsed.netloc:
                return f"{parsed.scheme}://{parsed.netloc}"
        return None

    async def extract(self, urls: list[str], company) -> list[dict]:
        base = self._find_base_url(urls)
        if not base:
            return []

        all_jobs: list[dict] = []
        page_num = 0
        page_size = 50

        try:
            async with httpx.AsyncClient(headers=_HEADERS, timeout=_TIMEOUT) as client:
                while True:
                    api_url = (
                        f"{base}/api/job/search?"
                        f"applyCountry=&applyState=&applyCity="
                        f"&page={page_num}&pageSize={page_size}"
                        f"&portfolioId=-1&isFeatured=false"
                    )
                    resp = await client.get(api_url)
                    if resp.status_code != 200:
                        break

                    data = resp.json()
                    items = data.get("searchResults", [])
                    if not items:
                        break

                    for item in items:
                        all_jobs.append({
                            "external_id": str(item.get("jobId", "")),
                            "title": item.get("title", ""),
                            "location_raw": item.get("city", ""),
                            "department": item.get("category", ""),
                            "source_url": item.get("jobDetailUrl", ""),
                            "date_posted": (
                                item.get("datePosted", "")[:10]
                                if item.get("datePosted")
                                else None
                            ),
                            "extraction_method": "ats_bulk_api",
                            "extraction_confidence": 0.92,
                            "raw_data": item,
                        })

                    total = data.get("totalCount", 0)
                    page_num += 1
                    if page_num * page_size >= total or len(items) < page_size:
                        break
                    if page_num >= 20:  # Safety cap
                        break
        except Exception as e:
            logger.warning(f"iCIMS API failed for {company.domain}: {e}")
            return []

        return all_jobs
