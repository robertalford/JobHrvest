"""
Career Page Finder v3.6 — direct from CareerPageFinderV4.

v3.6 keeps the proven v3.0 discovery behavior and adds:
1. Query-variant upgrade for search-form listing pages (`/search` -> `?search=`).
2. Targeted Oracle tenant recovery to prefer richer tenant-specific
   CandidateExperience paths (e.g. CX_1001 requisitions) over generic CX shells.
"""

from __future__ import annotations

import logging
import re
from urllib.parse import parse_qsl, urljoin, urlparse

import httpx

from app.crawlers.career_page_finder_v2 import _CLIENT_HEADERS
from app.crawlers.career_page_finder_v4 import CareerPageFinderV4
from app.crawlers.career_page_finder_v30 import CareerPageFinderV30

logger = logging.getLogger(__name__)


class CareerPageFinderV36(CareerPageFinderV4):
    """v3.6 finder wrapper with query-variant and Oracle tenant-path recovery."""

    async def find(self, domain: str, company_name: str = "") -> dict:
        delegate = CareerPageFinderV30()
        disc = await delegate.find(domain, company_name)
        if not disc.get("url"):
            return disc

        canonical_upgrade = await self._canonical_listing_upgrade_v36(delegate, disc)
        if canonical_upgrade:
            disc = canonical_upgrade

        query_upgrade = await self._query_listing_upgrade_v36(delegate, disc)
        if query_upgrade:
            disc = query_upgrade

        url = disc.get("url", "")
        html = disc.get("html") or ""
        url_lower = url.lower()
        if "oraclecloud.com" not in url_lower and "candidateexperience" not in html.lower():
            return disc

        improved = await self._oracle_tenant_recovery_v36(delegate, disc)
        return improved or disc

    async def _canonical_listing_upgrade_v36(self, delegate: CareerPageFinderV30, disc: dict) -> dict | None:
        current_url = disc.get("url", "")
        current_html = disc.get("html") or ""
        if not current_url or not current_html:
            return None

        canonical_match = re.search(
            r"<link[^>]+rel=['\"]canonical['\"][^>]+href=['\"]([^\"']+)['\"]",
            current_html,
            re.IGNORECASE,
        )
        if not canonical_match:
            return None

        canonical_url = urljoin(current_url, canonical_match.group(1).strip())
        if not canonical_url or canonical_url.rstrip("/") == current_url.rstrip("/"):
            return None

        current_host = urlparse(current_url).netloc.lower()
        canonical_host = urlparse(canonical_url).netloc.lower()
        if not canonical_host or canonical_host != current_host:
            return None
        if not re.search(r"(?:career|job|vacanc|position|opening|requisition)", canonical_url, re.IGNORECASE):
            return None

        try:
            async with httpx.AsyncClient(timeout=8, follow_redirects=True, headers=_CLIENT_HEADERS) as client:
                resp = await client.get(canonical_url)
        except Exception:
            return None

        body = resp.text or ""
        if resp.status_code != 200 or len(body) < 200 or delegate._is_non_html_payload(body):
            return None

        baseline_score = delegate._listing_page_score(current_url, current_html)
        canonical_score = delegate._listing_page_score(str(resp.url), body)
        if canonical_score <= baseline_score + 0.15:
            return None

        return {
            "url": str(resp.url),
            "method": disc.get("method", "") + "+canonical_v36",
            "candidates": disc.get("candidates", []) + [canonical_url],
            "html": body,
        }

    async def _query_listing_upgrade_v36(self, delegate: CareerPageFinderV30, disc: dict) -> dict | None:
        current_url = disc.get("url", "")
        current_html = disc.get("html") or ""
        parsed = urlparse(current_url or "")
        if not parsed.netloc or parsed.query:
            return None
        if not re.search(r"(?:/search(?:/|$)|/jobs(?:/|$)|/vacanc|/opening|/career)", parsed.path or "", re.IGNORECASE):
            return None

        lower = current_html.lower()
        if not any(token in lower for token in ("jobsearchbutton", "name=\"keywords\"", "current vacancies", "show all jobs")):
            return None

        base = current_url.rstrip("/")
        candidate_urls = [
            f"{base}?search=",
            f"{base}?search=&keywords=",
            f"{base}?keyword=",
            f"{base}?keywords=",
            f"{base}?q=",
        ]

        best: dict | None = None
        best_score = delegate._listing_page_score(current_url, current_html)

        try:
            async with httpx.AsyncClient(timeout=8, follow_redirects=True, headers=_CLIENT_HEADERS) as client:
                for candidate_url in candidate_urls:
                    try:
                        resp = await client.get(candidate_url)
                    except Exception:
                        continue
                    body = resp.text or ""
                    if resp.status_code != 200 or len(body) < 200 or delegate._is_non_html_payload(body):
                        continue

                    score = delegate._listing_page_score(str(resp.url), body)
                    if self._has_listing_markers_v36(body):
                        score += 0.8
                    if "search=" in str(resp.url).lower():
                        score += 0.4

                    if score <= best_score + 0.15:
                        continue
                    best_score = score
                    best = {
                        "url": str(resp.url),
                        "method": disc.get("method", "") + "+query_variant_v36",
                        "candidates": disc.get("candidates", []) + [candidate_url],
                        "html": body,
                    }
        except Exception:
            logger.debug("v3.6 query listing upgrade failed for %s", current_url)

        return best

    @staticmethod
    def _has_listing_markers_v36(html_body: str) -> bool:
        body = html_body or ""
        return bool(
            re.search(
                r"(?:class=['\"][^'\"]*(?:jobitem|joblist|vacanc|requisition|position|opening)[^'\"]*['\"]|"
                r"/jobdetails\?|ajid=|jobid=|requisitionid=)",
                body,
                re.IGNORECASE,
            )
        )

    async def _oracle_tenant_recovery_v36(self, delegate: CareerPageFinderV30, disc: dict) -> dict | None:
        current_url = disc.get("url", "")
        current_html = disc.get("html") or ""
        parsed = urlparse(current_url)
        if not parsed.netloc:
            return None

        current_site = ""
        site_match = re.search(r"/sites/([A-Za-z0-9_]+)/", parsed.path, flags=re.IGNORECASE)
        if site_match:
            current_site = site_match.group(1)

        site_ids = self._oracle_site_ids_v36(current_url, current_html)
        if not site_ids:
            return None

        candidate_urls: list[tuple[str, str]] = []
        base_url = f"{parsed.scheme or 'https'}://{parsed.netloc}"
        for site_id in site_ids[:10]:
            candidate_urls.append(
                (urljoin(base_url, f"/hcmUI/CandidateExperience/en/sites/{site_id}/requisitions"), site_id)
            )
            candidate_urls.append(
                (urljoin(base_url, f"/hcmUI/CandidateExperience/en/sites/{site_id}/jobs"), site_id)
            )

        best: dict | None = None
        best_score = delegate._listing_page_score(current_url, current_html)
        force_upgrade = current_site.upper() == "CX"

        try:
            async with httpx.AsyncClient(timeout=8, follow_redirects=True, headers=_CLIENT_HEADERS) as client:
                for candidate_url, candidate_site in candidate_urls:
                    try:
                        resp = await client.get(candidate_url)
                    except Exception:
                        continue
                    body = resp.text or ""
                    if resp.status_code != 200 or len(body) < 200 or delegate._is_non_html_payload(body):
                        continue

                    score = delegate._listing_page_score(str(resp.url), body)
                    if "/requisitions" in candidate_url:
                        score += 0.8
                    if re.search(r"_[0-9]+$", candidate_site):
                        score += 0.8
                    if re.search(rf"siteNumber['\"=: ]+{re.escape(candidate_site)}", body, re.IGNORECASE):
                        score += 0.6

                    should_upgrade = False
                    if score > best_score + 0.2:
                        should_upgrade = True
                    elif (
                        force_upgrade
                        and re.search(r"_[0-9]+$", candidate_site)
                        and score >= best_score - 0.1
                    ):
                        should_upgrade = True

                    if should_upgrade:
                        best_score = score
                        best = {
                            "url": str(resp.url),
                            "method": disc.get("method", "") + f"+oracle_tenant_v36:{candidate_site}",
                            "candidates": disc.get("candidates", []) + [candidate_url],
                            "html": body,
                        }
        except Exception:
            logger.debug("v3.6 oracle tenant recovery failed for %s", current_url)

        return best

    @staticmethod
    def _oracle_site_ids_v36(page_url: str, html_body: str) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []

        def _add(value: str) -> None:
            site = (value or "").strip()
            if not site or site in seen:
                return
            seen.add(site)
            ordered.append(site)

        for match in re.finditer(r"/sites/([A-Za-z0-9_]+)/", page_url or "", re.IGNORECASE):
            _add(match.group(1))
        for match in re.finditer(r"/sites/([A-Za-z0-9_]+)/", html_body or "", re.IGNORECASE):
            _add(match.group(1))
        for match in re.finditer(r"siteNumber\s*[:=]\s*['\"]([A-Za-z0-9_]+)", html_body or "", re.IGNORECASE):
            _add(match.group(1))
        for match in re.finditer(r"siteNumber=([A-Za-z0-9_]+)", html_body or "", re.IGNORECASE):
            _add(match.group(1))

        query_pairs = dict(parse_qsl(urlparse(page_url or "").query))
        if query_pairs.get("siteNumber"):
            _add(query_pairs.get("siteNumber", ""))

        base_ids = list(ordered)
        for site_id in base_ids:
            if re.fullmatch(r"CX(?:_\d+)?", site_id, flags=re.IGNORECASE):
                root = site_id.split("_", 1)[0]
                _add(root)
                for suffix in ("1001", "1002", "1003", "1004"):
                    _add(f"{root}_{suffix}")

        if not ordered:
            for fallback in ("CX_1001", "CX_1002", "CX"):
                _add(fallback)

        ordered.sort(
            key=lambda site: (
                0 if re.search(r"_[0-9]+$", site) else 1,
                0 if site.upper().endswith("_1001") else 1,
                site.lower(),
            )
        )
        return ordered[:12]
