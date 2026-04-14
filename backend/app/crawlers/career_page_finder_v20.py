"""
Career Page Finder v2.0 — direct from v1.6 finder base (CareerPageFinderV4).

High-impact discovery changes:
1. Suppress feed/document/login/privacy targets selected from homepage links.
2. Probe common ATS listing paths for hosted platforms (Zoho, Salesforce sites,
   jobs subdomains, etc.).
3. Promote deeper sub-pages using structural listing score (rows/anchors/signals),
   not only keyword density.
4. Preserve/introduce search query variants for job sites that require
   query parameters (e.g. /search?search=).
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


_MULTILINGUAL_CAREER_HREF_PATTERN_V20 = re.compile(
    r"/(?:career|careers|jobs?|vacanc|hiring|opening|position|opportunit|"
    r"employment|recruit|talent|search(?:-jobs?)?|"
    r"lowongan|karir|karier|vacature|vacatures|empleo|trabajo|"
    r"trabalho|vaga|vagas|stellen|jobsuche)",
    re.IGNORECASE,
)

_MULTILINGUAL_CAREER_TEXT_PATTERN_V20 = re.compile(
    r"\b(?:career|careers|jobs?|vacanc(?:y|ies)|hiring|open(?:\s+)?positions?|"
    r"current\s+vacancies|join\s+our\s+team|work\s+with\s+us|"
    r"lowongan|karir|karier|vacature|vacatures|empleo|trabajo|"
    r"trabalho|vaga|vagas|stellenangebote|jobsuche|current\s+opportunities)\b",
    re.IGNORECASE,
)

_REJECT_DISCOVERY_URL_PATTERN_V20 = re.compile(
    r"(?:rss|feed|xml|download|calendar|ics|pdf|docx?|xlsx?|zip|"
    r"wp-json|login|logout|register|sign(?:in|up)|account|profile|"
    r"privacy|terms|cookie|news|blog)",
    re.IGNORECASE,
)

_REJECT_DISCOVERY_TEXT_PATTERN_V20 = re.compile(
    r"\b(?:rss|feed|download|privacy|terms|cookie|news|blog|login|"
    r"sign\s*in|sign\s*up|register|my\s+account)\b",
    re.IGNORECASE,
)

_COMMON_ATS_PATHS_V20: dict[str, list[str]] = {
    # Zoho Recruit customer portals.
    "zohorecruit.": ["/recruit/Portal.na", "/jobs/Careers", "/careers", "/jobs"],
    # Salesforce hosted career sites.
    "salesforce-sites.com": ["/careers/", "/careers", "/jobs", "/career"],
    # Job microsites that commonly need explicit search query.
    "jobs.co.nz": ["/search?search=", "/search", "/jobs", "/careers"],
    "breezy.hr": ["/", "/#positions"],
}


class CareerPageFinderV20(CareerPageFinderV4):
    """v2.0 finder with stronger target quality and subpage selection."""

    async def find(self, domain: str, company_name: str = "") -> dict:
        disc = await super().find(domain, company_name)
        if not disc.get("url"):
            return disc

        current = dict(disc)
        current_score = self._listing_structure_score(current.get("html") or "")

        # If current target is obviously wrong (feed/doc/login or non-HTML), force re-evaluation.
        current_bad = self._is_bad_discovery_target(current.get("url", "")) or self._is_non_html_payload(
            current.get("html")
        )

        ats_candidate = await self._try_common_ats_paths(domain, current)
        if ats_candidate:
            ats_score = self._listing_structure_score(ats_candidate.get("html") or "")
            if current_bad or ats_score > current_score + 1:
                current = ats_candidate
                current_score = ats_score
                current_bad = False

        sub_candidate = await self._try_subpage_discovery_v20(current)
        if sub_candidate:
            sub_score = self._listing_structure_score(sub_candidate.get("html") or "")
            if current_bad or sub_score > current_score + 1:
                current = sub_candidate

        return current

    async def _try_common_ats_paths(self, domain: str, disc: dict) -> Optional[dict]:
        parsed = urlparse(disc.get("url") or f"https://{domain}")
        host = (parsed.hostname or domain).lower()
        base_url = f"https://{host}"

        candidates: list[str] = []
        for marker, paths in _COMMON_ATS_PATHS_V20.items():
            if marker in host:
                candidates.extend(paths)

        if not candidates and any(k in host for k in ("jobs.", "careers.", "career.", "recruit")):
            candidates.extend(["/jobs", "/careers", "/career", "/search?search=", "/search"])

        if not candidates:
            return None

        best: Optional[dict] = None
        best_score = -1
        seen: set[str] = set()

        async with httpx.AsyncClient(timeout=8, follow_redirects=True, headers=_CLIENT_HEADERS) as client:
            for path in candidates:
                candidate_url = urljoin(base_url, path)
                if candidate_url in seen:
                    continue
                seen.add(candidate_url)

                try:
                    resp = await client.get(candidate_url)
                except Exception:
                    continue

                body = resp.text or ""
                if resp.status_code != 200 or len(body) < 200 or self._is_non_html_payload(body):
                    continue

                score = self._listing_structure_score(body)
                if score <= 0:
                    continue

                if score > best_score:
                    best_score = score
                    best = {
                        "url": str(resp.url),
                        "method": disc.get("method", "") + f"+ats_path:{path}",
                        "candidates": disc.get("candidates", []) + [candidate_url],
                        "html": body,
                    }

        return best

    async def _try_subpage_discovery_v20(self, disc: dict) -> Optional[dict]:
        parent_url = disc.get("url")
        parent_html = disc.get("html")
        if not parent_url or not parent_html:
            return None

        if self._is_non_html_payload(parent_html):
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
            reason = []

            if _MULTILINGUAL_CAREER_HREF_PATTERN_V20.search(href):
                score += 5
                reason.append("href")

            if text and _MULTILINGUAL_CAREER_TEXT_PATTERN_V20.search(text):
                score += 5
                reason.append("text")

            # Prefer explicit job-opening/search style paths.
            if re.search(r"/(?:job-openings|openings|search|jobs?)(?:/|$|\?)", full_url, re.IGNORECASE):
                score += 4
                reason.append("job_path")

            if "search=" in full_url or ("?" in full_url and "job" in full_url.lower()):
                score += 2
                reason.append("query")

            if score > 0:
                candidates.append((full_url, score, "+".join(reason) or "candidate"))

        if not candidates:
            return None

        candidates.sort(key=lambda x: x[1], reverse=True)

        best: Optional[dict] = None
        best_score = parent_score

        async with httpx.AsyncClient(timeout=8, follow_redirects=True, headers=_CLIENT_HEADERS) as client:
            for candidate_url, _, reason in candidates[:8]:
                try:
                    resp = await client.get(candidate_url)
                except Exception:
                    continue

                body = resp.text or ""
                if resp.status_code != 200 or len(body) < 200 or self._is_non_html_payload(body):
                    continue

                score = self._listing_structure_score(body)
                if score <= 0:
                    continue

                if score > best_score:
                    best_score = score
                    best = {
                        "url": str(resp.url),
                        "method": disc.get("method", "") + f"+subpage_v20:{reason}",
                        "candidates": disc.get("candidates", []) + [candidate_url],
                        "html": body,
                    }

        return best

    def _listing_structure_score(self, html: str) -> int:
        if not html or len(html) < 200 or self._is_non_html_payload(html):
            return 0

        html_lower = html.lower()
        score = 0

        # Aggressive penalties for feed/document payloads.
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
                _MULTILINGUAL_CAREER_HREF_PATTERN_V20.search(href)
                or _MULTILINGUAL_CAREER_TEXT_PATTERN_V20.search(txt)
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
        if _REJECT_DISCOVERY_URL_PATTERN_V20.search(href_l):
            return True
        if _REJECT_DISCOVERY_TEXT_PATTERN_V20.search(text_l):
            return True

        return False

    @staticmethod
    def _is_bad_discovery_target(url: str) -> bool:
        return bool(_REJECT_DISCOVERY_URL_PATTERN_V20.search((url or "").lower()))

    @staticmethod
    def _is_non_html_payload(body: Optional[str]) -> bool:
        if not body:
            return True
        sample = body[:400].lstrip()
        if sample.startswith("%PDF-"):
            return True
        return False
