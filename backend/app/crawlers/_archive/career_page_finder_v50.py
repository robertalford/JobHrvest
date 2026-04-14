"""
Career Page Finder v5.0 — direct from CareerPageFinderV4.

v5.0 keeps v4 behavior but adds:
1. Timeout-safe guard around parent discovery.
2. Fast multilingual listing-path recovery (jobs/lowongan/loker/karir/kerjaya).
3. Homepage multilingual hub-link scoring as a fallback when parent discovery fails.
"""

from __future__ import annotations

import asyncio
import logging
import re
from urllib.parse import urljoin, urlparse

import httpx
from lxml import etree

from app.crawlers.career_page_finder_v2 import _CLIENT_HEADERS
from app.crawlers.career_page_finder_v4 import CareerPageFinderV4

logger = logging.getLogger(__name__)


_LOCALIZED_CAREER_PATHS_V50 = [
    "/jobs",
    "/careers",
    "/career",
    "/job-openings",
    "/join-our-team",
    "/openings",
    "/vacancies",
    "/positions",
    "/recruitment",
    "/jobs/search",
    "/careers/jobs",
    "/lowongan",
    "/loker",
    "/karir",
    "/kerjaya",
    "/peluang-karir",
]

_LISTING_TEXT_PATTERN_V50 = re.compile(
    r"\b(?:careers?|jobs?|job\s+openings?|open\s+positions?|vacanc(?:y|ies)|"
    r"join\s+our\s+team|search\s+jobs|current\s+jobs?|current\s+vacancies|"
    r"lowongan|loker|karir|kerjaya|peluang\s+karir|info\s+lengkap)\b",
    re.IGNORECASE,
)

_LISTING_HREF_PATTERN_V50 = re.compile(
    r"/(?:career|careers|jobs?|openings?|vacanc|position|recruit|"
    r"lowongan|loker|karir|kerjaya|peluang-karir)",
    re.IGNORECASE,
)


class CareerPageFinderV50(CareerPageFinderV4):
    """v5.0 finder with timeout-safe multilingual fallback discovery."""

    async def find(self, domain: str, company_name: str = "") -> dict:
        try:
            parent_disc = await asyncio.wait_for(super().find(domain, company_name), timeout=15.0)
        except asyncio.TimeoutError:
            logger.warning("v5.0 parent finder timeout for %s", domain)
            parent_disc = {"url": None, "method": "parent_timeout", "candidates": [], "html": None}
        except Exception:
            logger.exception("v5.0 parent finder failed for %s", domain)
            parent_disc = {"url": None, "method": "parent_error", "candidates": [], "html": None}

        if parent_disc.get("url") and parent_disc.get("html") and len(parent_disc.get("html") or "") > 200:
            return parent_disc

        recovered = await self._probe_localized_paths_v50(domain)
        if recovered:
            return recovered

        hub_recovered = await self._homepage_hub_recovery_v50(domain)
        if hub_recovered:
            return hub_recovered

        return parent_disc

    async def _probe_localized_paths_v50(self, domain: str) -> dict | None:
        base_url = f"https://{domain}"

        async with httpx.AsyncClient(timeout=4.5, follow_redirects=True, headers=_CLIENT_HEADERS) as client:

            async def _try(path: str) -> tuple[str, str | None, str | None, int]:
                url = base_url + path
                try:
                    resp = await client.get(url)
                except Exception:
                    return path, None, None, -999

                body = resp.text or ""
                if resp.status_code != 200 or len(body) < 180:
                    return path, None, None, -999

                resolved = str(resp.url)
                score = self._score_listing_page_v50(resolved, body)
                return path, resolved, body, score

            results = await asyncio.gather(*[_try(path) for path in _LOCALIZED_CAREER_PATHS_V50])

        valid = [(path, url, body, score) for path, url, body, score in results if url and body]
        if not valid:
            return None

        best = max(valid, key=lambda item: item[3])
        path, url, body, score = best

        if score < 2:
            return None

        return {
            "url": url,
            "method": f"probe_v50:{path}",
            "candidates": [u for _, u, _, _ in sorted(valid, key=lambda item: item[3], reverse=True)[:5]],
            "html": body,
        }

    async def _homepage_hub_recovery_v50(self, domain: str) -> dict | None:
        base_url = f"https://{domain}"

        try:
            async with httpx.AsyncClient(timeout=4.5, follow_redirects=True, headers=_CLIENT_HEADERS) as client:
                resp = await client.get(base_url)
                if resp.status_code != 200 or len(resp.text or "") < 200:
                    return None

                root = etree.fromstring((resp.text or "").encode("utf-8", errors="replace"), etree.HTMLParser(encoding="utf-8"))

                scored_links: list[tuple[int, str, str]] = []
                seen: set[str] = set()

                for a_el in root.xpath("//a[@href]")[:1800]:
                    href = (a_el.get("href") or "").strip()
                    if not href or href.startswith("#") or href.startswith("javascript:"):
                        continue

                    full_url = urljoin(base_url, href)
                    parsed = urlparse(full_url)
                    if not parsed.netloc:
                        continue

                    home_host = urlparse(base_url).netloc.lower()
                    link_host = parsed.netloc.lower()
                    if link_host != home_host:
                        base_a = ".".join(home_host.split(".")[-2:])
                        base_b = ".".join(link_host.split(".")[-2:])
                        if base_a != base_b:
                            continue

                    norm = full_url.rstrip("/")
                    if norm in seen:
                        continue
                    seen.add(norm)

                    text = self._safe_text_v50(a_el)
                    score = 0
                    if _LISTING_TEXT_PATTERN_V50.search(text):
                        score += 10
                    if _LISTING_HREF_PATTERN_V50.search(parsed.path or ""):
                        score += 8
                    if any(tok in (parsed.path or "").lower() for tok in ("lowongan", "loker", "karir", "kerjaya")):
                        score += 8
                    if score > 0:
                        scored_links.append((score, full_url, text[:60]))

                if not scored_links:
                    return None

                scored_links.sort(key=lambda item: item[0], reverse=True)

                for score, candidate_url, label in scored_links[:3]:
                    try:
                        sub_resp = await client.get(candidate_url)
                    except Exception:
                        continue
                    body = sub_resp.text or ""
                    if sub_resp.status_code != 200 or len(body) < 180:
                        continue

                    sub_score = self._score_listing_page_v50(str(sub_resp.url), body)
                    if sub_score < 2:
                        continue

                    return {
                        "url": str(sub_resp.url),
                        "method": f"homepage_hub_v50:{label}",
                        "candidates": [u for _, u, _ in scored_links[:5]],
                        "html": body,
                    }

        except Exception:
            logger.debug("v5.0 homepage hub recovery failed for %s", domain)

        return None

    def _score_listing_page_v50(self, page_url: str, html_body: str) -> int:
        lower = (html_body or "").lower()
        score = 0

        if any(tok in (page_url or "").lower() for tok in ("jobs", "careers", "lowongan", "loker", "karir", "kerjaya")):
            score += 2

        score += min(lower.count("apply"), 6)
        score += min(lower.count("job"), 8) // 2
        score += min(lower.count("vacanc"), 5)
        score += min(lower.count("lowongan"), 5)

        if "elementor-heading-title" in lower and "elementor-inner-column" in lower:
            score += 3
        if "__next_data__" in lower and ("clientcode" in lower or "recruiterid" in lower):
            score += 3
        if "jobadid" in lower or "requisition" in lower:
            score += 2

        if lower.count("<a ") <= 2:
            score -= 1

        return score

    @staticmethod
    def _safe_text_v50(el: etree._Element) -> str:
        try:
            txt = el.text_content()
            if txt:
                return " ".join(txt.split())
        except Exception:
            pass
        try:
            txt = etree.tostring(el, method="text", encoding="unicode")
            return " ".join((txt or "").split())
        except Exception:
            return ""
