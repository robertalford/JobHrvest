"""
Career Page Finder v4 — extends v3 with sub-page link following and SPA handling.

Fixes applied:
  Fix 3: After finding a careers page URL, fetch it and scan for deeper job listing
         sub-pages (e.g. /careers/openings, /careers/search-jobs). If a sub-page has
         MORE job-like content than the parent, use the sub-page URL instead.
  Fix 4: Verify that query parameters from homepage links are preserved (urljoin
         already handles this in v2/v3, but we add explicit verification logging).
  Fix 5: For careers-page.com domains (Angular SPA), always try Playwright rendering
         after discovery to ensure we get the fully-rendered HTML.
"""

import logging
import re
from typing import Optional
from urllib.parse import urljoin, urlparse

import httpx
from lxml import etree

from app.crawlers.career_page_finder_v3 import CareerPageFinderV3
from app.crawlers.career_page_finder_v2 import _CLIENT_HEADERS

logger = logging.getLogger(__name__)

# Sub-page URL patterns that indicate deeper job listing pages
_SUBPAGE_HREF_PATTERN = re.compile(
    r"/(?:jobs|openings|opportunities|vacancies|career-opportunities|"
    r"search|search-jobs|job-search|job-list|all-jobs|current-openings|"
    r"open-positions|available-positions|positions)",
    re.IGNORECASE,
)

# Link text patterns that indicate "view all jobs" type links
_SUBPAGE_TEXT_PATTERN = re.compile(
    r"(?:view\s+all|see\s+all|all\s+jobs|current\s+openings|job\s+openings|"
    r"browse\s+all|browse\s+jobs|search\s+jobs|open\s+positions|"
    r"available\s+positions|view\s+positions|all\s+openings|"
    r"all\s+opportunities|view\s+opportunities)",
    re.IGNORECASE,
)

# Signals that indicate job-like content density
_JOB_CONTENT_SIGNALS = [
    "apply now", "apply here", "apply today", "apply online",
    "job-card", "job-listing", "job-post", "job-item", "job-row",
    "job_card", "job_listing", "job_post", "job_item",
    "position-card", "position-item", "vacancy-card", "vacancy-item",
    "opening-card", "opening-item",
]


def _count_job_signals(html: str) -> int:
    """Count the number of job-like content signals in HTML."""
    html_lower = html.lower()
    count = 0

    # Count apply buttons/links
    count += len(re.findall(
        r"(?:apply\s*(?:now|here|today|online))",
        html_lower,
    ))

    # Count job-class elements
    for signal in _JOB_CONTENT_SIGNALS:
        count += html_lower.count(signal)

    return count


class CareerPageFinderV4(CareerPageFinderV3):
    """Discover career page URLs — v4 with sub-page following and SPA handling."""

    async def find(self, domain: str, company_name: str = "") -> dict:
        """Find career page for a domain.

        After v3 discovers the careers page, v4 additionally:
        1. Scans the discovered page for links to deeper job listing sub-pages
        2. If a sub-page has more job content, uses that URL instead
        3. For careers-page.com (Angular SPA), always tries Playwright rendering
        """
        # Run v3 discovery first (which includes v2 homepage crawl, ATS slug, etc.)
        disc = await super().find(domain, company_name)

        if not disc.get("url"):
            return disc

        # Fix 4: Log query parameter preservation for debugging
        parsed = urlparse(disc["url"])
        if parsed.query:
            logger.info(
                "v4 preserved query params: %s?%s", parsed.path, parsed.query
            )

        # Fix 5: Always use Playwright for careers-page.com (Angular SPA)
        if "careers-page.com" in (disc["url"] or ""):
            rendered = await self._try_playwright(disc["url"])
            if rendered and len(rendered) > 200:
                disc["html"] = rendered
                disc["method"] += "+playwright_angular"
                logger.info(
                    "v4 Playwright rendered careers-page.com: %s (%d bytes)",
                    disc["url"], len(rendered),
                )

        # Fix 3: Follow sub-page links to find deeper job listing pages
        if disc.get("html") and len(disc["html"]) > 200:
            improved = await self._try_subpage_discovery(disc)
            if improved:
                return improved

        return disc

    async def _try_subpage_discovery(self, disc: dict) -> Optional[dict]:
        """Scan the discovered careers page for links to deeper job listing sub-pages.

        If a sub-page has MORE job-like content than the parent page, return a new
        discovery dict pointing to the sub-page instead.
        """
        parent_url = disc["url"]
        parent_html = disc["html"]
        parent_signal_count = _count_job_signals(parent_html)

        # Parse the parent page to find sub-page links
        try:
            parser = etree.HTMLParser(encoding="utf-8")
            root = etree.fromstring(
                parent_html.encode("utf-8", errors="replace"), parser
            )
        except Exception:
            return None

        subpage_candidates: list[tuple[str, int, str]] = []  # (url, score, reason)

        for a_el in root.iter("a"):
            href = (a_el.get("href") or "").strip()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue

            try:
                text = a_el.text_content().strip()
            except AttributeError:
                text = etree.tostring(a_el, method="text", encoding="unicode").strip()

            full_url = urljoin(parent_url, href)
            parsed = urlparse(full_url)

            # Skip links to entirely different domains (unless known ATS)
            parent_parsed = urlparse(parent_url)
            if parsed.hostname and parent_parsed.hostname:
                if parsed.hostname != parent_parsed.hostname:
                    # Allow same base domain (e.g. careers.example.com -> jobs.example.com)
                    parent_parts = parent_parsed.hostname.rsplit(".", 2)
                    child_parts = parsed.hostname.rsplit(".", 2)
                    parent_base = ".".join(parent_parts[-2:]) if len(parent_parts) >= 2 else parent_parsed.hostname
                    child_base = ".".join(child_parts[-2:]) if len(child_parts) >= 2 else parsed.hostname
                    if parent_base != child_base:
                        continue

            # Skip self-links
            if full_url.rstrip("/") == parent_url.rstrip("/"):
                continue

            score = 0
            reason_parts: list[str] = []

            # Score based on URL path
            if _SUBPAGE_HREF_PATTERN.search(href):
                score += 5
                reason_parts.append("href_match")

            # Score based on link text
            if text and _SUBPAGE_TEXT_PATTERN.search(text):
                score += 5
                reason_parts.append(f"text_match:{text[:40]}")

            if score > 0:
                subpage_candidates.append((
                    full_url,
                    score,
                    "+".join(reason_parts),
                ))

        if not subpage_candidates:
            return None

        # Sort by score descending
        subpage_candidates.sort(key=lambda c: c[1], reverse=True)

        logger.info(
            "v4 sub-page discovery for %s: %d candidates (top: %s)",
            parent_url,
            len(subpage_candidates),
            subpage_candidates[0][0] if subpage_candidates else "none",
        )

        # Try fetching the top candidates (up to 3)
        async with httpx.AsyncClient(
            timeout=8, follow_redirects=True, headers=_CLIENT_HEADERS,
        ) as client:
            for candidate_url, candidate_score, reason in subpage_candidates[:3]:
                try:
                    resp = await client.get(candidate_url)
                    if resp.status_code != 200 or len(resp.text) < 200:
                        continue

                    subpage_html = resp.text
                    subpage_signal_count = _count_job_signals(subpage_html)

                    logger.info(
                        "v4 sub-page %s: %d job signals (parent: %d) — %s",
                        candidate_url, subpage_signal_count,
                        parent_signal_count, reason,
                    )

                    # Use the sub-page if it has MORE job content than the parent
                    if subpage_signal_count > parent_signal_count:
                        logger.info(
                            "v4 sub-page wins: %s (%d > %d signals)",
                            candidate_url, subpage_signal_count, parent_signal_count,
                        )

                        # Fix 5: If sub-page is on careers-page.com, try Playwright
                        final_html = subpage_html
                        method_suffix = ""
                        if "careers-page.com" in candidate_url:
                            rendered = await self._try_playwright(candidate_url)
                            if rendered and len(rendered) > 200:
                                final_html = rendered
                                method_suffix = "+playwright_angular"

                        return {
                            "url": str(resp.url),
                            "method": disc["method"] + f"+subpage:{reason}" + method_suffix,
                            "candidates": disc.get("candidates", []) + [candidate_url],
                            "html": final_html,
                        }

                except Exception as e:
                    logger.debug("v4 sub-page fetch failed for %s: %s", candidate_url, e)
                    continue

        return None
