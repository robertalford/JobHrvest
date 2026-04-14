"""
Career Page Finder v6.7 — resilient hint fetching for TLS-mismatch sites.

Builds on v6.6 with one targeted discovery improvement:
1. If hint URL fetch fails with TLS/certificate issues, retry with `verify=False`
   and alternate scheme (http<->https) before falling back to full discovery.
"""

from __future__ import annotations

import logging
from typing import Optional
from urllib.parse import urlparse

import httpx

from app.crawlers.career_page_finder_v66 import CareerPageFinderV66
from app.crawlers.career_page_finder_v2 import _CLIENT_HEADERS

logger = logging.getLogger(__name__)


class CareerPageFinderV67(CareerPageFinderV66):
    """v6.7 finder: v6.6 behavior + insecure hint retry fallback."""

    async def _try_hint_url_v64(self, hint_url: str) -> Optional[dict]:
        # Keep the proven secure path first.
        result = await super()._try_hint_url_v64(hint_url)
        if result:
            return result

        for candidate in self._hint_candidates_v67(hint_url):
            try:
                async with httpx.AsyncClient(
                    timeout=10,
                    follow_redirects=True,
                    verify=False,
                    headers=_CLIENT_HEADERS,
                ) as client:
                    resp = await client.get(candidate)
            except Exception as exc:
                logger.debug("v6.7 insecure hint fetch failed for %s: %s", candidate, exc)
                continue

            html = resp.text or ""
            if resp.status_code != 200 or len(html) < 200:
                continue
            if self._is_non_html_payload(html):
                continue

            score = self._listing_page_score(str(resp.url), html)
            if score < -10:
                continue

            return {
                "url": str(resp.url),
                "method": "hint_url_v67_insecure",
                "html": html,
            }

        return None

    @staticmethod
    def _hint_candidates_v67(hint_url: str) -> list[str]:
        if not hint_url:
            return []

        parsed = urlparse(hint_url)
        base = parsed.netloc or parsed.path
        path = parsed.path or ""
        if parsed.query:
            path = f"{path}?{parsed.query}"

        candidates: list[str] = []

        if parsed.scheme in {"http", "https"} and parsed.netloc:
            candidates.append(hint_url)
            alt_scheme = "http" if parsed.scheme == "https" else "https"
            candidates.append(f"{alt_scheme}://{parsed.netloc}{path}")
        elif base:
            normalized = base if "/" in base else f"{base}{path}"
            candidates.append(f"https://{normalized}")
            candidates.append(f"http://{normalized}")
        else:
            return []

        deduped: list[str] = []
        seen: set[str] = set()
        for candidate in candidates:
            if candidate in seen:
                continue
            seen.add(candidate)
            deduped.append(candidate)
        return deduped
