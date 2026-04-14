"""
Career Page Finder v5 — extends v4 with broader discovery quality controls:

1. Feed/document/login URL suppression so homepage crawl does not choose RSS/download links.
2. Multilingual careers hints (e.g. lowongan/karir/vacature/empleo) for non-English pages.
3. ATS path probing for common hosted platforms where valid listing pages are on
   non-root paths (e.g. /careers/, /recruit/Portal.na, /search?search=).
4. Sub-page promotion by structural listing score (repeated job rows/anchors), not only
   keyword density, to choose deeper listing pages like /career/job-openings.
"""

from __future__ import annotations

import logging
import re
from collections import defaultdict
from typing import Optional
from urllib.parse import urljoin, urlparse

import httpx
from lxml import etree

from app.crawlers.career_page_finder_v4 import CareerPageFinderV4
from app.crawlers.career_page_finder_v2 import _CLIENT_HEADERS

logger = logging.getLogger(__name__)

_MULTILINGUAL_CAREER_HREF_PATTERN_V5 = re.compile(
    r"/(?:career|careers|jobs?|vacanc|hiring|opening|position|opportunit|"
    r"employment|recruit|talent|search(?:-jobs?)?|"
    r"lowongan|karir|karier|vacature|vacatures|empleo|trabajo|"
    r"trabalho|vaga|vagas|stellen|jobsuche)",
    re.IGNORECASE,
)

_MULTILINGUAL_CAREER_TEXT_PATTERN_V5 = re.compile(
    r"\b(?:career|careers|jobs?|vacanc(?:y|ies)|hiring|open(?:\s+)?positions?|"
    r"current\s+vacancies|join\s+our\s+team|work\s+with\s+us|"
    r"lowongan|karir|karier|vacature|vacatures|empleo|trabajo|"
    r"trabalho|vaga|vagas|stellenangebote|jobsuche)\b",
    re.IGNORECASE,
)

_REJECT_DISCOVERY_URL_PATTERN_V5 = re.compile(
    r"(?:rss|feed|xml|download|calendar|ics|pdf|docx?|xlsx?|zip|"
    r"wp-json|login|logout|register|sign(?:in|up)|account|profile|"
    r"privacy|terms|cookie|news|blog)",
    re.IGNORECASE,
)

_REJECT_DISCOVERY_TEXT_PATTERN_V5 = re.compile(
    r"\b(?:rss|feed|download|privacy|terms|cookie|news|blog|login|"
    r"sign\s*in|sign\s*up|register|my\s+account)\b",
    re.IGNORECASE,
)

_COMMON_ATS_PATHS_V5: dict[str, list[str]] = {
    # Zoho Recruit customer portals commonly expose listings on this path.
    "zohorecruit.": ["/recruit/Portal.na", "/jobs/Careers", "/careers", "/jobs"],
    # Salesforce hosted career sites often use /careers.
    "salesforce-sites.com": ["/careers/", "/careers", "/jobs", "/career"],
    # Recruiting microsites frequently index all roles at /search?search=
    "jobs.co.nz": ["/search?search=", "/search", "/jobs", "/careers"],
}


class CareerPageFinderV5(CareerPageFinderV4):
    """v5 finder with better candidate suppression and deeper listing-page promotion."""

    async def find(self, domain: str, company_name: str = "") -> dict:
        disc = await super().find(domain, company_name)
        if not disc.get("url"):
            return disc

        current = dict(disc)
        current_score = self._listing_structure_score(current.get("html") or "")

        ats_candidate = await self._try_common_ats_paths(domain, current)
        if ats_candidate:
            ats_score = self._listing_structure_score(ats_candidate.get("html") or "")
            if ats_score > current_score + 1 or self._is_bad_discovery_target(current.get("url", "")):
                current = ats_candidate
                current_score = ats_score

        subpage_candidate = await self._try_subpage_discovery_v5(current)
        if subpage_candidate:
            sub_score = self._listing_structure_score(subpage_candidate.get("html") or "")
            if sub_score > current_score + 1 or self._is_bad_discovery_target(current.get("url", "")):
                current = subpage_candidate

        return current

    async def _try_common_ats_paths(self, domain: str, disc: dict) -> Optional[dict]:
        """Probe common ATS listing paths when root/homepage target is weak."""
        parsed = urlparse(disc.get("url") or f"https://{domain}")
        host = (parsed.hostname or domain).lower()
        base_url = f"https://{host}"

        path_candidates: list[str] = []
        for pattern, paths in _COMMON_ATS_PATHS_V5.items():
            if pattern in host:
                path_candidates.extend(paths)

        if not path_candidates:
            # Generic fallback paths for job-hosting subdomains.
            if any(key in host for key in ("jobs.", "careers.", "career.", "recruit")):
                path_candidates.extend(["/jobs", "/careers", "/career", "/search?search="])

        if not path_candidates:
            return None

        seen: set[str] = set()
        best: Optional[dict] = None
        best_score = -1

        async with httpx.AsyncClient(
            timeout=8, follow_redirects=True, headers=_CLIENT_HEADERS,
        ) as client:
            for path in path_candidates:
                candidate_url = urljoin(base_url, path)
                if candidate_url in seen:
                    continue
                seen.add(candidate_url)

                try:
                    resp = await client.get(candidate_url)
                    if resp.status_code != 200 or len(resp.text) < 200:
                        continue

                    html = resp.text
                    score = self._listing_structure_score(html)
                    if score <= 0:
                        continue

                    if score > best_score:
                        best_score = score
                        best = {
                            "url": str(resp.url),
                            "method": disc.get("method", "") + f"+ats_path:{path}",
                            "candidates": disc.get("candidates", []) + [candidate_url],
                            "html": html,
                        }
                except Exception:
                    continue

        return best

    async def _try_subpage_discovery_v5(self, disc: dict) -> Optional[dict]:
        """Follow stronger sub-page candidates and pick by structural listing score."""
        parent_url = disc.get("url")
        parent_html = disc.get("html")
        if not parent_url or not parent_html:
            return None

        try:
            parser = etree.HTMLParser(encoding="utf-8")
            root = etree.fromstring(parent_html.encode("utf-8", errors="replace"), parser)
        except Exception:
            return None

        parent_score = self._listing_structure_score(parent_html)

        candidates: list[tuple[str, int, str]] = []
        seen: set[str] = set()

        for a_el in root.iter("a"):
            href = (a_el.get("href") or "").strip()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue

            text = ""
            try:
                text = a_el.text_content().strip()
            except Exception:
                text = etree.tostring(a_el, method="text", encoding="unicode").strip()

            if self._is_rejected_link(href, text):
                continue

            full_url = urljoin(parent_url, href)
            if full_url in seen:
                continue
            seen.add(full_url)

            if not self._is_related_host(parent_url, full_url):
                continue

            score = 0
            reason: list[str] = []

            if _MULTILINGUAL_CAREER_HREF_PATTERN_V5.search(href):
                score += 5
                reason.append("href")

            if text and _MULTILINGUAL_CAREER_TEXT_PATTERN_V5.search(text):
                score += 5
                reason.append("text")

            if "search=" in full_url or "?" in full_url and "job" in full_url.lower():
                score += 2
                reason.append("query")

            if score > 0:
                candidates.append((full_url, score, "+".join(reason) or "candidate"))

        if not candidates:
            return None

        candidates.sort(key=lambda x: x[1], reverse=True)

        best: Optional[dict] = None
        best_score = parent_score

        async with httpx.AsyncClient(
            timeout=8, follow_redirects=True, headers=_CLIENT_HEADERS,
        ) as client:
            for candidate_url, _, reason in candidates[:8]:
                try:
                    resp = await client.get(candidate_url)
                    if resp.status_code != 200 or len(resp.text) < 200:
                        continue

                    candidate_html = resp.text
                    candidate_score = self._listing_structure_score(candidate_html)
                    if candidate_score <= 0:
                        continue

                    if candidate_score > best_score:
                        best_score = candidate_score
                        best = {
                            "url": str(resp.url),
                            "method": disc.get("method", "") + f"+subpage_v5:{reason}",
                            "candidates": disc.get("candidates", []) + [candidate_url],
                            "html": candidate_html,
                        }
                except Exception:
                    continue

        return best

    def _listing_structure_score(self, html: str) -> int:
        """Estimate how likely a page is a job listing page using structure signals."""
        if not html or len(html) < 200:
            return 0

        html_lower = html.lower()

        score = 0

        # Penalize feed/document pages aggressively.
        if "<rss" in html_lower or "<feed" in html_lower:
            score -= 30

        score += min(html_lower.count("jobitem"), 15)
        score += min(html_lower.count("job-post"), 15)
        score += min(html_lower.count("position"), 10)
        score += min(html_lower.count("vacanc"), 10)

        try:
            parser = etree.HTMLParser(encoding="utf-8")
            root = etree.fromstring(html.encode("utf-8", errors="replace"), parser)
        except Exception:
            return score

        jobish_anchor_count = 0
        row_class_counts: dict[str, int] = defaultdict(int)

        for a_el in root.iter("a"):
            href = (a_el.get("href") or "")
            try:
                txt_raw = a_el.text_content()
            except Exception:
                txt_raw = etree.tostring(a_el, method="text", encoding="unicode")
            txt = " ".join((txt_raw or "").split())
            if 6 <= len(txt) <= 140 and (
                _MULTILINGUAL_CAREER_HREF_PATTERN_V5.search(href)
                or _MULTILINGUAL_CAREER_TEXT_PATTERN_V5.search(txt)
            ):
                jobish_anchor_count += 1

        for el in root.iter():
            if not isinstance(el.tag, str):
                continue
            tag = el.tag.lower()
            if tag not in {"li", "div", "tr", "article", "section"}:
                continue
            classes = (el.get("class") or "").lower()
            if not classes:
                continue
            if re.search(r"job|career|vacanc|position|opening|posting", classes):
                key = f"{tag}:{classes.split()[0]}"
                row_class_counts[key] += 1

        repeated_rows = sum(c for c in row_class_counts.values() if c >= 3)
        table_rows_with_links = len(root.xpath("//tr[.//a[@href]]"))

        score += min(jobish_anchor_count, 25)
        score += min(repeated_rows * 2, 30)
        score += min(table_rows_with_links, 20)

        return score

    @staticmethod
    def _is_related_host(parent_url: str, child_url: str) -> bool:
        p = urlparse(parent_url).hostname or ""
        c = urlparse(child_url).hostname or ""
        if not p or not c:
            return False
        if p == c:
            return True
        p_parts = p.rsplit(".", 2)
        c_parts = c.rsplit(".", 2)
        p_base = ".".join(p_parts[-2:]) if len(p_parts) >= 2 else p
        c_base = ".".join(c_parts[-2:]) if len(c_parts) >= 2 else c
        return p_base == c_base

    @staticmethod
    def _is_rejected_link(href: str, text: str) -> bool:
        href_l = (href or "").lower()
        text_l = (text or "").lower()

        if href_l.startswith("mailto:") or href_l.startswith("tel:"):
            return True

        if _REJECT_DISCOVERY_URL_PATTERN_V5.search(href_l):
            return True

        if _REJECT_DISCOVERY_TEXT_PATTERN_V5.search(text_l):
            return True

        return False

    @staticmethod
    def _is_bad_discovery_target(url: str) -> bool:
        u = (url or "").lower()
        return bool(_REJECT_DISCOVERY_URL_PATTERN_V5.search(u))
