"""
Career Page Finder v6.2 — Better discovery URL preferences.

Changes from v6.1:
  - Prefer URLs containing /jobs over /departments or /teams
  - Penalize blog post URLs (/career-spotlight, /blog/, /news/)
  - When domain has careers subdomain, try /jobs path on it directly
  - Add /career/ (singular) AND /careers/ to initial probes
"""

from __future__ import annotations

import logging
import re
from typing import Optional
from urllib.parse import urljoin, urlparse

import httpx
from lxml import etree

from app.crawlers.career_page_finder_v61 import CareerPageFinderV61
from app.crawlers.career_page_finder_v2 import _CLIENT_HEADERS

logger = logging.getLogger(__name__)

# Patterns for URL preference scoring
_PREFERRED_PATH = re.compile(r"/jobs(?:/|$|\?)", re.IGNORECASE)
_PENALIZED_DEPARTMENTS = re.compile(r"/(?:departments?|teams?)(?:/|$|\?)", re.IGNORECASE)
_PENALIZED_BLOG = re.compile(
    r"/(?:career-spotlight|blog|news)(?:/|$|\?)", re.IGNORECASE,
)


class CareerPageFinderV62(CareerPageFinderV61):
    """v6.2 finder: v6.1 discovery + URL preference scoring, careers subdomain
    /jobs probing, and extra initial probe paths."""

    async def find(self, domain: str, company_name: str = "") -> dict:
        # Try careers subdomain /jobs path directly if applicable
        host_parts = domain.lower().split(".")
        careers_subdomain_result = None
        if host_parts[0] != "careers":
            # Check if a careers subdomain exists
            base_domain = ".".join(host_parts) if host_parts[0] == "www" else domain
            if host_parts[0] == "www":
                base_domain = ".".join(host_parts[1:])
            careers_domain = f"careers.{base_domain}"
            careers_result = await self._probe_careers_subdomain_jobs(careers_domain)
            if careers_result:
                careers_subdomain_result = careers_result
        elif host_parts[0] == "careers":
            # Already on careers subdomain, try /jobs directly
            careers_result = await self._probe_careers_subdomain_jobs(domain)
            if careers_result:
                careers_subdomain_result = careers_result

        # Run parent discovery (v6.1 chain)
        disc = await super().find(domain, company_name)

        # If parent found nothing but careers subdomain worked, use that
        if not disc.get("url") and careers_subdomain_result:
            return careers_subdomain_result

        # If both found results, pick the better one based on URL preference
        if disc.get("url") and careers_subdomain_result:
            disc_score = self._url_preference_score(disc["url"])
            careers_score = self._url_preference_score(careers_subdomain_result["url"])
            # Also factor in listing page score
            disc_listing = self._listing_page_score(disc["url"], disc.get("html") or "")
            careers_listing = self._listing_page_score(
                careers_subdomain_result["url"],
                careers_subdomain_result.get("html") or "",
            )
            if (careers_score + careers_listing) > (disc_score + disc_listing):
                return careers_subdomain_result

        if disc.get("url"):
            # Check if we should try extra probe paths
            extra = await self._probe_extra_paths_v62(disc)
            if extra:
                return extra
            return disc

        # Last resort: try /career/ and /careers/ on the base domain
        base_url = f"https://{domain}"
        extra = await self._probe_career_paths(base_url)
        if extra:
            return extra

        return disc

    async def _probe_careers_subdomain_jobs(self, careers_domain: str) -> Optional[dict]:
        """Try /jobs path on the careers subdomain directly."""
        paths_to_try = ["/jobs", "/jobs/", "/"]
        base_url = f"https://{careers_domain}"

        try:
            async with httpx.AsyncClient(
                timeout=8, follow_redirects=True, headers=_CLIENT_HEADERS,
            ) as client:
                for path in paths_to_try:
                    probe_url = base_url.rstrip("/") + path
                    try:
                        resp = await client.get(probe_url)
                    except Exception:
                        continue
                    if resp.status_code != 200 or len(resp.text or "") < 200:
                        continue
                    if self._is_non_html_payload(resp.text):
                        continue

                    score = self._listing_page_score(str(resp.url), resp.text)
                    if score >= 3:
                        return {
                            "url": str(resp.url),
                            "method": f"careers_subdomain_probe_v62:{path}",
                            "html": resp.text,
                        }
        except Exception:
            pass

        return None

    async def _probe_extra_paths_v62(self, disc: dict) -> Optional[dict]:
        """If the discovered URL is a /departments or /teams page, try /jobs instead."""
        current_url = disc.get("url", "")
        if not _PENALIZED_DEPARTMENTS.search(current_url) and not _PENALIZED_BLOG.search(current_url):
            return None

        parsed = urlparse(current_url)
        base = f"{parsed.scheme}://{parsed.netloc}"
        preferred_paths = ["/jobs", "/jobs/", "/careers/jobs", "/careers/"]

        try:
            async with httpx.AsyncClient(
                timeout=8, follow_redirects=True, headers=_CLIENT_HEADERS,
            ) as client:
                for path in preferred_paths:
                    probe_url = base + path
                    try:
                        resp = await client.get(probe_url)
                    except Exception:
                        continue
                    if resp.status_code != 200 or len(resp.text or "") < 200:
                        continue
                    if self._is_non_html_payload(resp.text):
                        continue

                    score = self._listing_page_score(str(resp.url), resp.text)
                    if score >= 3:
                        return {
                            "url": str(resp.url),
                            "method": disc.get("method", "") + f"+prefer_jobs_v62:{path}",
                            "html": resp.text,
                        }
        except Exception:
            pass

        return None

    async def _probe_career_paths(self, base_url: str) -> Optional[dict]:
        """Probe /career/ (singular) and /careers/ as last resort."""
        paths = ["/career/", "/careers/", "/career", "/careers"]

        try:
            async with httpx.AsyncClient(
                timeout=8, follow_redirects=True, headers=_CLIENT_HEADERS,
            ) as client:
                for path in paths:
                    probe_url = base_url.rstrip("/") + path
                    try:
                        resp = await client.get(probe_url)
                    except Exception:
                        continue
                    if resp.status_code != 200 or len(resp.text or "") < 200:
                        continue
                    if self._is_non_html_payload(resp.text):
                        continue

                    score = self._listing_page_score(str(resp.url), resp.text)
                    if score >= 3:
                        return {
                            "url": str(resp.url),
                            "method": f"probe_career_v62:{path}",
                            "html": resp.text,
                        }
        except Exception:
            pass

        return None

    @staticmethod
    def _url_preference_score(url: str) -> int:
        """Score a URL based on how likely it is to be a good jobs listing page.
        Higher = better."""
        score = 0

        # Prefer /jobs paths
        if _PREFERRED_PATH.search(url):
            score += 3

        # Penalize /departments, /teams
        if _PENALIZED_DEPARTMENTS.search(url):
            score -= 2

        # Penalize blog/news/spotlight
        if _PENALIZED_BLOG.search(url):
            score -= 3

        # Prefer careers subdomain
        parsed = urlparse(url)
        host = parsed.netloc.lower()
        if host.startswith("careers."):
            score += 1

        return score
