"""
Career Page Finder v6.1 — Extended discovery for v6.1 extractor.

Changes from v6.0:
  - Add /career/ (singular) to probe paths
  - Add /vacantes/ and /ofertas/ for Spanish sites
  - Follow nav links matching "lowongan", "vacantes", "ofertas"
  - Handle vacantes. subdomain as career subdomain
"""

from __future__ import annotations

import logging
import re
from typing import Optional
from urllib.parse import urljoin, urlparse

import httpx
from lxml import etree

from app.crawlers.career_page_finder_v60 import CareerPageFinderV60
from app.crawlers.career_page_finder_v2 import _CLIENT_HEADERS

logger = logging.getLogger(__name__)

# Nav link patterns for Spanish and Indonesian career pages
_EXTRA_NAV_PATTERN = re.compile(
    r"\b(?:lowongan|vacantes?|ofertas?(?:\s+de\s+empleo)?)\b",
    re.IGNORECASE,
)


class CareerPageFinderV61(CareerPageFinderV60):
    """v6.1 finder: v6.0 discovery + extra probe paths and nav link patterns."""

    async def find(self, domain: str, company_name: str = "") -> dict:
        # Handle vacantes. subdomain (like careers.)
        host_parts = domain.lower().split(".")
        if host_parts[0] == "vacantes":
            base_url = f"https://{domain}"
            return await self._fetch_and_return(base_url, f"careers_subdomain:vacantes")

        # Run parent discovery
        disc = await super().find(domain, company_name)
        if disc.get("url"):
            return disc

        # If parent found nothing, try extra probe paths
        base_url = f"https://{domain}"
        extra = await self._probe_extra_paths_v61(base_url, disc)
        if extra:
            return extra

        return disc

    async def _probe_extra_paths_v61(self, base_url: str, disc: dict) -> Optional[dict]:
        """Probe additional career paths not covered by parent."""
        extra_paths = [
            "/career/",
            "/vacantes/",
            "/ofertas/",
        ]

        try:
            async with httpx.AsyncClient(
                timeout=8, follow_redirects=True, headers=_CLIENT_HEADERS,
            ) as client:
                for path in extra_paths:
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
                            "method": disc.get("method", "") + f"+probe_v61:{path}",
                            "html": resp.text,
                        }

                    # Also follow nav links on the probed page for
                    # "lowongan", "vacantes", "ofertas"
                    nav_result = await self._follow_extra_nav_links_v61(
                        client, str(resp.url), resp.text, disc,
                    )
                    if nav_result:
                        return nav_result
        except Exception:
            pass

        # Also try following extra nav links on the original discovery page
        if disc.get("html"):
            try:
                async with httpx.AsyncClient(
                    timeout=8, follow_redirects=True, headers=_CLIENT_HEADERS,
                ) as client:
                    nav_result = await self._follow_extra_nav_links_v61(
                        client, disc.get("url", base_url), disc["html"], disc,
                    )
                    if nav_result:
                        return nav_result
            except Exception:
                pass

        return None

    async def _follow_extra_nav_links_v61(
        self,
        client: httpx.AsyncClient,
        page_url: str,
        html: str,
        disc: dict,
    ) -> Optional[dict]:
        """Follow nav links matching lowongan/vacantes/ofertas."""
        try:
            parser = etree.HTMLParser(encoding="utf-8")
            root = etree.fromstring(html.encode("utf-8", errors="replace"), parser)
        except Exception:
            return None

        candidates: list[str] = []
        for a_el in root.iter("a"):
            href = (a_el.get("href") or "").strip()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue
            text = self._safe_text_v26(a_el) if hasattr(self, "_safe_text_v26") else (a_el.text or "").strip()
            if _EXTRA_NAV_PATTERN.search(text) or _EXTRA_NAV_PATTERN.search(href):
                full = urljoin(page_url, href)
                if self._is_related_host(page_url, full):
                    candidates.append(full)

        for candidate_url in candidates[:5]:
            try:
                resp = await client.get(candidate_url)
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
                    "method": disc.get("method", "") + "+nav_v61",
                    "html": resp.text,
                }

        return None
