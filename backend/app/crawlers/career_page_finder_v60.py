"""
Career Page Finder v6.0 — Delegates to proven v2.6 discovery.

v2.6 discovery achieved 82% accuracy. No changes needed to the discovery logic itself.
This version exists as a version-matched finder for TieredExtractorV60.

Added: Salesforce fRecruit path probing (the one ATS path v26 didn't cover).
"""

from __future__ import annotations

import logging
import re
from typing import Optional
from urllib.parse import urljoin, urlparse

import httpx

from app.crawlers.career_page_finder_v26 import CareerPageFinderV26
from app.crawlers.career_page_finder_v2 import _CLIENT_HEADERS

logger = logging.getLogger(__name__)


class CareerPageFinderV60(CareerPageFinderV26):
    """v6.0 finder: v2.6 discovery + Salesforce fRecruit path probing."""

    async def find(self, domain: str, company_name: str = "") -> dict:
        disc = await super().find(domain, company_name)
        if not disc.get("url"):
            return disc

        # If we landed on a Salesforce site, try the fRecruit listing path
        current_url = disc.get("url", "")
        current_html = disc.get("html") or ""
        if "salesforce-sites.com" in current_url.lower() or "frecruit" in current_html.lower()[:5000]:
            sf_candidate = await self._probe_salesforce_paths(current_url, disc)
            if sf_candidate:
                sf_score = self._listing_page_score(sf_candidate["url"], sf_candidate.get("html") or "")
                cur_score = self._listing_page_score(current_url, current_html)
                if sf_score > cur_score:
                    return sf_candidate

        return disc

    async def _probe_salesforce_paths(self, current_url: str, disc: dict) -> Optional[dict]:
        parsed = urlparse(current_url)
        base = f"{parsed.scheme}://{parsed.netloc}"

        paths = [
            "/careers/fRecruit__ApplyJobList",
            "/careers/fRecruit__ApplyJobList?portal=English",
            "/careers",
        ]

        try:
            async with httpx.AsyncClient(
                timeout=8, follow_redirects=True, headers=_CLIENT_HEADERS,
            ) as client:
                for path in paths:
                    probe_url = base + path
                    try:
                        resp = await client.get(probe_url)
                    except Exception:
                        continue
                    if resp.status_code != 200 or len(resp.text or "") < 200:
                        continue
                    if self._is_non_html_payload(resp.text):
                        continue
                    # Check if this page has actual job content
                    if "frecruit" in resp.text.lower() or "datarow" in resp.text.lower():
                        return {
                            "url": str(resp.url),
                            "method": disc.get("method", "") + "+salesforce_probe",
                            "html": resp.text,
                        }
        except Exception:
            pass

        return None
