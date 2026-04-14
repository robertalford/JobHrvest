"""
Career Page Finder v6.3 — URL hint support, listing-dense page preference,
and subdomain career detection.

Changes from v6.2:
  1. Accept URL hints via set_hint(url) — if a known career page URL is
     available, fetch it directly and skip discovery if it returns valid HTML.
  2. Prefer listing-dense pages: add scoring bonus for candidate pages that
     have many job-like links (>5 links matching /job|/career|/position).
  3. Subdomain career detection: try careers.X.com and jobs.X.com roots
     directly before doing broader discovery.
"""

from __future__ import annotations

import logging
import re
from collections import defaultdict
from typing import Optional
from urllib.parse import urljoin, urlparse

import httpx
from lxml import etree

from app.crawlers.career_page_finder_v62 import CareerPageFinderV62
from app.crawlers.career_page_finder_v2 import _CLIENT_HEADERS

logger = logging.getLogger(__name__)

# Patterns for job-like link detection (listing density scoring)
_JOB_LINK_HREF_PATTERN = re.compile(
    r"/(?:job|career|position|opening|vacanc|posting|opportunity|rolle|stelle)",
    re.IGNORECASE,
)

# Career subdomain prefixes to try
_CAREER_SUBDOMAINS = ["careers", "jobs", "career"]


class CareerPageFinderV63(CareerPageFinderV62):
    """v6.3 finder: URL hint support, listing-dense page preference,
    subdomain career detection."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._hint_url: Optional[str] = None

    # ==================================================================
    # Change 1: URL hint support
    # ==================================================================

    def set_hint(self, url: str) -> None:
        """Store a URL hint for the next find() call.

        If a known career page URL is available (e.g., from a company record),
        store it here and find() will try it first before discovery.
        """
        self._hint_url = url.strip() if url else None

    async def find(self, domain: str, company_name: str = "") -> dict:
        # --- Change 1: Try hint URL first ---
        if self._hint_url:
            hint_result = await self._try_hint_url(self._hint_url)
            if hint_result:
                logger.info(
                    "v6.3 hint URL successful: %s", hint_result.get("url")
                )
                return hint_result
            logger.info(
                "v6.3 hint URL failed (%s), falling back to discovery",
                self._hint_url,
            )

        # --- Change 3: Try career/jobs subdomains directly ---
        subdomain_result = await self._try_career_subdomains(domain)
        if subdomain_result:
            # Don't return immediately — compare with discovery results below
            pass
        else:
            subdomain_result = None

        # --- Run parent v6.2 discovery ---
        disc = await super().find(domain, company_name)

        # If we got a subdomain result, compare with discovery result
        if subdomain_result:
            if not disc.get("url"):
                return subdomain_result

            # Compare: factor in listing density bonus (Change 2)
            disc_score = (
                self._url_preference_score(disc["url"])
                + self._listing_page_score(disc["url"], disc.get("html") or "")
                + self._listing_density_bonus(disc.get("html") or "")
            )
            sub_score = (
                self._url_preference_score(subdomain_result["url"])
                + self._listing_page_score(
                    subdomain_result["url"],
                    subdomain_result.get("html") or "",
                )
                + self._listing_density_bonus(subdomain_result.get("html") or "")
            )
            if sub_score > disc_score:
                return subdomain_result

        # --- Change 2: Re-evaluate discovered URL with listing density ---
        # If discovery found something, check if listing density suggests a better
        # candidate was missed. The listing density bonus is already factored into
        # the scoring above when comparing with subdomain results.
        if disc.get("url"):
            return disc

        return disc

    # ==================================================================
    # Change 1 helper: Try a hint URL directly
    # ==================================================================

    async def _try_hint_url(self, hint_url: str) -> Optional[dict]:
        """Fetch the hint URL and return result if it's valid HTML."""
        try:
            async with httpx.AsyncClient(
                timeout=12,
                follow_redirects=True,
                headers=_CLIENT_HEADERS,
            ) as client:
                resp = await client.get(hint_url)

                if resp.status_code != 200:
                    return None

                html = resp.text or ""
                if len(html) < 200:
                    return None

                # Check it's not an error page or non-HTML
                if self._is_non_html_payload(html):
                    return None

                # Validate it looks like a real page (not a redirect/error page)
                score = self._listing_page_score(str(resp.url), html)
                if score < -10:
                    # Clearly an error or login page
                    return None

                return {
                    "url": str(resp.url),
                    "method": "hint_url_v63",
                    "html": html,
                }

        except Exception as e:
            logger.debug("v6.3 hint URL fetch failed for %s: %s", hint_url, e)
            return None

    # ==================================================================
    # Change 2: Listing density bonus scoring
    # ==================================================================

    @staticmethod
    def _listing_density_bonus(html: str) -> int:
        """Score bonus for pages with many job-like links.

        Pages that have >5 links matching /job|/career|/position patterns
        are more likely to be the actual listing page (vs. a hub/about page).
        """
        if not html or len(html) < 500:
            return 0

        try:
            parser = etree.HTMLParser(encoding="utf-8")
            root = etree.fromstring(
                html.encode("utf-8", errors="replace"), parser
            )
        except Exception:
            return 0

        job_link_count = 0
        for a_el in root.iter("a"):
            href = (a_el.get("href") or "").strip()
            if not href:
                continue
            if _JOB_LINK_HREF_PATTERN.search(href):
                job_link_count += 1

        # Scoring tiers
        if job_link_count >= 20:
            return 6
        elif job_link_count >= 10:
            return 4
        elif job_link_count >= 5:
            return 2
        return 0

    # ==================================================================
    # Change 3: Subdomain career detection
    # ==================================================================

    async def _try_career_subdomains(self, domain: str) -> Optional[dict]:
        """Try careers.X.com, jobs.X.com, career.X.com root pages directly.

        If the domain already starts with one of these prefixes, skip.
        """
        host_parts = domain.lower().split(".")
        current_prefix = host_parts[0]

        # Skip if we're already on a career subdomain
        if current_prefix in _CAREER_SUBDOMAINS:
            return None

        # Derive the base domain (strip www if present)
        if current_prefix == "www":
            base_domain = ".".join(host_parts[1:])
        else:
            base_domain = domain

        # Don't try subdomains if domain is too short (e.g., co.uk)
        if len(base_domain.split(".")) < 2:
            return None

        best_result: Optional[dict] = None
        best_score: int = -100

        try:
            async with httpx.AsyncClient(
                timeout=8,
                follow_redirects=True,
                headers=_CLIENT_HEADERS,
            ) as client:
                for prefix in _CAREER_SUBDOMAINS:
                    subdomain = f"{prefix}.{base_domain}"

                    # Try root and /jobs path
                    for path in ["/", "/jobs", "/jobs/"]:
                        probe_url = f"https://{subdomain}{path}"
                        try:
                            resp = await client.get(probe_url)
                        except Exception:
                            continue

                        if resp.status_code != 200:
                            continue

                        html = resp.text or ""
                        if len(html) < 200:
                            continue

                        if self._is_non_html_payload(html):
                            continue

                        final_url = str(resp.url)
                        score = (
                            self._listing_page_score(final_url, html)
                            + self._listing_density_bonus(html)
                        )

                        if score >= 3 and score > best_score:
                            best_score = score
                            best_result = {
                                "url": final_url,
                                "method": f"subdomain_probe_v63:{prefix}{path}",
                                "html": html,
                            }

                    # If we found a good result on this subdomain, don't try others
                    if best_result and best_score >= 5:
                        break

        except Exception:
            pass

        return best_result
