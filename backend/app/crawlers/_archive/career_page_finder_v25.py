"""
Career Page Finder v2.5 — direct from CareerPageFinderV4.

High-impact discovery changes:
1. Penalize feed/download and single-detail targets during final URL selection.
2. Detail-to-listing recovery probes (Greenhouse, Oracle CX, Zoho Portal, generic listing paths).
3. Script/anchor harvesting for hidden listing URLs (/requisitions, /job-openings, /embed/job_board).
4. Subpage + domain-path scoring focused on repeated listing structure.
"""

from __future__ import annotations

import logging
import re
from collections import defaultdict
from typing import Optional
from urllib.parse import parse_qsl, urljoin, urlparse

import httpx
from lxml import etree

from app.crawlers.career_page_finder_v4 import CareerPageFinderV4
from app.crawlers.career_page_finder_v2 import _CLIENT_HEADERS

logger = logging.getLogger(__name__)


_CAREER_HREF_PATTERN_V25 = re.compile(
    r"/(?:career|careers|jobs?|job-openings?|vacanc|opening|openings|"
    r"position|opportunit|requisition|requisitions|candidateportal|"
    r"portal\.na|jobs/search|embed/job_board|lowongan|loker|kerjaya|karir|karier)",
    re.IGNORECASE,
)

_CAREER_TEXT_PATTERN_V25 = re.compile(
    r"\b(?:career|careers|jobs?|vacanc(?:y|ies)|hiring|open\s+positions?|"
    r"job\s+openings?|current\s+vacancies|join\s+our\s+team|browse\s+jobs|"
    r"search\s+jobs|lowongan|loker|kerjaya|karir|karier)\b",
    re.IGNORECASE,
)

_REJECT_LINK_TEXT_PATTERN_V25 = re.compile(
    r"\b(?:privacy|terms|cookie|blog|news|media|investor|contact|about|"
    r"sign\s*in|sign\s*up|register|my\s+account|support|help)\b",
    re.IGNORECASE,
)

_REJECT_LINK_HREF_PATTERN_V25 = re.compile(
    r"(?:mailto:|tel:|/privacy|/terms|/cookie|/news|/blog|/investor|"
    r"/contact|/about|/login|/logout|/register|/account|wp-json|"
    r"feed|rss|downloadrssfeed|\.pdf(?:$|\?)|\.docx?(?:$|\?))",
    re.IGNORECASE,
)

_LISTING_URL_HINT_PATTERN_V25 = re.compile(
    r"/(?:jobs?|job-openings?|careers?|vacanc|opening|positions?|"
    r"jobs/search|embed/job_board|requisition|requisitions|"
    r"candidateportal|Portal\.na|PortalDetail\.na|ViewJob\.na|"
    r"lowongan|loker|kerjaya|karir|karier)",
    re.IGNORECASE,
)

_DETAIL_URL_PATTERN_V25 = re.compile(
    r"(?:/jobs?/\d+[A-Za-z0-9_-]*|/jobs?/[a-z0-9][^/?#]{5,}|"
    r"/requisition[s]?/[a-z0-9][^/?#]{2,}|/ViewJob\.na|/PortalDetail\.na|"
    r"[?&](?:jobid|job_id|requisitionid|positionid)=)",
    re.IGNORECASE,
)

_FEED_URL_PATTERN_V25 = re.compile(r"(?:rss|feed|downloadrssfeed)", re.IGNORECASE)

_ERROR_PAGE_PATTERN_V25 = re.compile(
    r"(?:\b404\b|page\s+not\s+found|something\s+went\s+wrong|error\s+occurred|"
    r"access\s+denied|forbidden|temporarily\s+unavailable|request\s+id)",
    re.IGNORECASE,
)

_LOGIN_PAGE_PATTERN_V25 = re.compile(
    r"(?:sign\s+in\s+to\s+your\s+account|candidate\s+portal\s+login|login\.aspx|mydayforce)",
    re.IGNORECASE,
)

_ROW_CLASS_PATTERN_V25 = re.compile(
    r"job|position|vacanc|opening|requisition|career|posting|listing|accordion|awsm|jobDetailRow",
    re.IGNORECASE,
)

_ORACLE_SITE_PATTERN_V25 = re.compile(r"/sites/([A-Za-z0-9_]+)/", re.IGNORECASE)
_ORACLE_SITE_NUMBER_PATTERN_V25 = re.compile(r"siteNumber\s*[:=]\s*['\"]([A-Za-z0-9_]+)", re.IGNORECASE)
_BASE_HREF_PATTERN_V25 = re.compile(r"<base[^>]+href=['\"]([^'\"]+)['\"]", re.IGNORECASE)


class CareerPageFinderV25(CareerPageFinderV4):
    """v2.5 finder with robust listing-target recovery and scoring."""

    async def find(self, domain: str, company_name: str = "") -> dict:
        disc = await super().find(domain, company_name)
        if not disc.get("url"):
            return disc

        current = dict(disc)
        current_score = self._listing_page_score_v25(current.get("url", ""), current.get("html") or "")
        current_bad = self._is_bad_target_v25(current.get("url", ""), current.get("html") or "")

        async with httpx.AsyncClient(timeout=8, follow_redirects=True, headers=_CLIENT_HEADERS) as client:
            derived = await self._probe_derived_targets_v25(client, domain, company_name, current)
            if derived:
                derived_score = self._listing_page_score_v25(derived.get("url", ""), derived.get("html") or "")
                if current_bad or derived_score >= current_score + 0.5:
                    current = derived
                    current_score = derived_score
                    current_bad = self._is_bad_target_v25(current.get("url", ""), current.get("html") or "")

            sub = await self._try_subpage_discovery_v25(client, current)
            if sub:
                sub_score = self._listing_page_score_v25(sub.get("url", ""), sub.get("html") or "")
                if current_bad or sub_score >= current_score:
                    current = sub
                    current_score = sub_score
                    current_bad = self._is_bad_target_v25(current.get("url", ""), current.get("html") or "")

            domain_probe = await self._probe_domain_paths_v25(client, domain, company_name, current)
            if domain_probe:
                probe_score = self._listing_page_score_v25(domain_probe.get("url", ""), domain_probe.get("html") or "")
                if current_bad or probe_score >= current_score:
                    current = domain_probe
                    current_score = probe_score
                    current_bad = self._is_bad_target_v25(current.get("url", ""), current.get("html") or "")

            if current_bad:
                homepage = await self._homepage_recovery_v25(client, domain)
                if homepage:
                    home_score = self._listing_page_score_v25(homepage.get("url", ""), homepage.get("html") or "")
                    if home_score >= current_score:
                        current = homepage

        return current

    async def _probe_derived_targets_v25(
        self,
        client: httpx.AsyncClient,
        domain: str,
        company_name: str,
        current: dict,
    ) -> Optional[dict]:
        current_url = current.get("url") or f"https://{domain}"
        html_body = current.get("html") or ""
        parsed = urlparse(current_url)

        candidates: list[tuple[str, str, float]] = []

        def _add(url: str, reason: str, weight: float) -> None:
            if not url:
                return
            full = url if re.match(r"^https?://", url, re.IGNORECASE) else (urljoin(current_url, url))
            if not full:
                return
            if not self._is_related_host_v25(current_url, full):
                host = (urlparse(full).hostname or "").lower()
                if "greenhouse" not in host:
                    return
            candidates.append((full, reason, weight))

        # If current URL is a detail/feed target, derive listing URLs.
        if _DETAIL_URL_PATTERN_V25.search(current_url) or _FEED_URL_PATTERN_V25.search(current_url):
            _add(f"{parsed.scheme}://{parsed.netloc}/jobs", "derived:/jobs", 3.0)
            _add(f"{parsed.scheme}://{parsed.netloc}/careers", "derived:/careers", 3.0)
            _add(f"{parsed.scheme}://{parsed.netloc}/job-openings", "derived:/job-openings", 4.0)
            _add(f"{parsed.scheme}://{parsed.netloc}/requisitions", "derived:/requisitions", 4.0)
            _add(f"{parsed.scheme}://{parsed.netloc}/recruit/Portal.na", "derived:/Portal.na", 5.0)

        # Greenhouse detail URL -> board listing URL.
        if "greenhouse" in (parsed.hostname or ""):
            parts = [p for p in parsed.path.split("/") if p]
            org = ""
            query = dict(parse_qsl(parsed.query))
            if query.get("for"):
                org = query["for"].strip()
            if not org and parts:
                if parts[0] != "embed":
                    org = parts[0]
            if org:
                _add(f"https://{parsed.netloc}/{org}", "greenhouse:root", 6.0)
                _add(f"https://{parsed.netloc}/embed/job_board?for={org}", "greenhouse:embed", 7.0)
                _add(f"https://boards.greenhouse.io/{org}", "greenhouse:boards", 5.0)

        # Oracle CX site-number probing from URL/HTML.
        if "oraclecloud.com" in (parsed.hostname or "") or "candidateexperience" in html_body.lower():
            for site_id in self._oracle_site_ids_v25(current_url, html_body)[:8]:
                _add(
                    f"{parsed.scheme}://{parsed.netloc}/hcmUI/CandidateExperience/en/sites/{site_id}/requisitions",
                    f"oracle:{site_id}",
                    7.0,
                )

        # Zoho feed target recovery.
        if "zohorecruit" in (parsed.hostname or ""):
            _add(f"{parsed.scheme}://{parsed.netloc}/recruit/Portal.na", "zoho:portal", 7.0)

        # Script URL harvesting.
        for script_url in self._extract_script_urls_v25(current_url, html_body):
            weight = 6.0 if _LISTING_URL_HINT_PATTERN_V25.search(script_url) else 3.0
            _add(script_url, "script", weight)

        # Anchor harvesting from current HTML.
        try:
            parser = etree.HTMLParser(encoding="utf-8")
            root = etree.fromstring(html_body.encode("utf-8", errors="replace"), parser)
        except Exception:
            root = None

        if root is not None:
            for a_el in root.iter("a"):
                href = (a_el.get("href") or "").strip()
                if not href or href.startswith("#") or href.startswith("javascript:"):
                    continue
                text = " ".join((a_el.text_content() or "").split())
                if self._is_rejected_link_v25(href, text):
                    continue
                full = urljoin(current_url, href)
                if not _LISTING_URL_HINT_PATTERN_V25.search(full) and not _CAREER_TEXT_PATTERN_V25.search(text):
                    continue
                base_weight = 4.5 if _LISTING_URL_HINT_PATTERN_V25.search(full) else 3.0
                if "job-openings" in full or "requisitions" in full or "Portal.na" in full:
                    base_weight += 1.5
                candidates.append((full, "anchor", base_weight))

        if not candidates:
            return None

        return await self._probe_url_candidates_v25(client, current, candidates, limit=36)

    async def _try_subpage_discovery_v25(
        self,
        client: httpx.AsyncClient,
        disc: dict,
    ) -> Optional[dict]:
        parent_url = disc.get("url")
        parent_html = disc.get("html")
        if not parent_url or not parent_html or self._is_non_html_payload_v25(parent_html):
            return None

        try:
            parser = etree.HTMLParser(encoding="utf-8")
            root = etree.fromstring(parent_html.encode("utf-8", errors="replace"), parser)
        except Exception:
            return None

        candidates: list[tuple[str, float, str]] = []
        seen: set[str] = set()

        for a_el in root.iter("a"):
            href = (a_el.get("href") or "").strip()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue

            text = " ".join((a_el.text_content() or "").split())
            if self._is_rejected_link_v25(href, text):
                continue

            full_url = urljoin(parent_url, href)
            if full_url in seen:
                continue
            seen.add(full_url)

            if not self._is_related_host_v25(parent_url, full_url):
                continue

            score = 0.0
            reason_parts: list[str] = []

            if _CAREER_HREF_PATTERN_V25.search(href):
                score += 6.0
                reason_parts.append("href")
            if _CAREER_TEXT_PATTERN_V25.search(text):
                score += 5.0
                reason_parts.append("text")
            if _LISTING_URL_HINT_PATTERN_V25.search(full_url):
                score += 3.0
                reason_parts.append("listing")
            if "job-openings" in full_url or "requisitions" in full_url or "Portal.na" in full_url:
                score += 3.0
                reason_parts.append("strong")
            if _DETAIL_URL_PATTERN_V25.search(full_url):
                score -= 7.0
                reason_parts.append("detail_penalty")
            if _FEED_URL_PATTERN_V25.search(full_url):
                score -= 8.0
                reason_parts.append("feed_penalty")

            if score > 0:
                candidates.append((full_url, score, "+".join(reason_parts) or "subpage"))

        if not candidates:
            return None

        candidates.sort(key=lambda x: x[1], reverse=True)
        best: Optional[dict] = None
        best_score = self._listing_page_score_v25(parent_url, parent_html)

        for candidate_url, weight, reason in candidates[:12]:
            try:
                resp = await client.get(candidate_url)
            except Exception:
                continue
            body = resp.text or ""
            if resp.status_code != 200 or len(body) < 200 or self._is_non_html_payload_v25(body):
                continue

            total = self._listing_page_score_v25(str(resp.url), body) + weight
            if total >= best_score:
                best_score = total
                best = {
                    "url": str(resp.url),
                    "method": disc.get("method", "") + f"+subpage_v25:{reason}",
                    "candidates": disc.get("candidates", []) + [candidate_url],
                    "html": body,
                }

        return best

    async def _probe_domain_paths_v25(
        self,
        client: httpx.AsyncClient,
        domain: str,
        company_name: str,
        current: dict,
    ) -> Optional[dict]:
        hosts = self._candidate_hosts_v25(domain, current.get("url") or f"https://{domain}")
        candidates: list[tuple[str, str, float]] = []

        broad_paths = (
            "/careers",
            "/career",
            "/jobs",
            "/jobs/search",
            "/job-openings",
            "/vacancies",
            "/openings",
            "/positions",
            "/requisitions",
            "/join-us",
            "/recruit/Portal.na",
            "/kerjaya",
            "/ms/kerjaya",
            "/lowongan",
            "/loker",
        )

        for host in hosts:
            base = f"https://{host}"
            for path in broad_paths:
                weight = 3.5 if path in {"/job-openings", "/requisitions", "/recruit/Portal.na", "/jobs/search"} else 2.5
                candidates.append((urljoin(base, path), f"domain:{path}", weight))

        if not candidates:
            return None

        return await self._probe_url_candidates_v25(client, current, candidates, limit=44)

    async def _probe_url_candidates_v25(
        self,
        client: httpx.AsyncClient,
        current: dict,
        candidates: list[tuple[str, str, float]],
        limit: int = 24,
    ) -> Optional[dict]:
        deduped: list[tuple[str, str, float]] = []
        seen: set[str] = set()
        for url, reason, weight in sorted(candidates, key=lambda x: x[2], reverse=True):
            if not url or url in seen:
                continue
            seen.add(url)
            deduped.append((url, reason, weight))

        if not deduped:
            return None

        best: Optional[dict] = None
        best_score = self._listing_page_score_v25(current.get("url", ""), current.get("html") or "")

        for candidate_url, reason, weight in deduped[:limit]:
            try:
                resp = await client.get(candidate_url)
            except Exception:
                continue

            body = resp.text or ""
            if resp.status_code != 200 or len(body) < 200 or self._is_non_html_payload_v25(body):
                continue

            listing_score = self._listing_page_score_v25(str(resp.url), body)
            total = listing_score + weight
            if total >= best_score:
                best_score = total
                best = {
                    "url": str(resp.url),
                    "method": current.get("method", "") + f"+probe_v25:{reason}",
                    "candidates": current.get("candidates", []) + [candidate_url],
                    "html": body,
                }

        return best

    async def _homepage_recovery_v25(self, client: httpx.AsyncClient, domain: str) -> Optional[dict]:
        best: Optional[dict] = None
        best_score = -10000

        for host in self._candidate_hosts_v25(domain, f"https://{domain}"):
            url = f"https://{host}"
            try:
                resp = await client.get(url)
            except Exception:
                continue

            body = resp.text or ""
            if resp.status_code != 200 or len(body) < 200 or self._is_non_html_payload_v25(body):
                continue

            disc = {
                "url": str(resp.url),
                "method": "homepage_recovery_v25",
                "candidates": [str(resp.url)],
                "html": body,
            }
            sub = await self._try_subpage_discovery_v25(client, disc)
            candidate = sub or disc
            score = self._listing_page_score_v25(candidate.get("url", ""), candidate.get("html") or "")
            if score > best_score:
                best_score = score
                best = candidate

        return best

    def _extract_script_urls_v25(self, current_url: str, html_body: str) -> list[str]:
        found: list[str] = []
        seen: set[str] = set()

        def _add(url: str) -> None:
            u = (url or "").strip()
            if not u:
                return
            if not re.match(r"^https?://", u, re.IGNORECASE):
                u = urljoin(current_url, u)
            if u in seen:
                return
            seen.add(u)
            found.append(u)

        for m in _BASE_HREF_PATTERN_V25.finditer(html_body or ""):
            base_href = m.group(1)
            _add(base_href)
            _add(urljoin(base_href, "requisitions"))

        for m in re.finditer(
            r"https?://[^\"'\s]+/(?:hcmUI/CandidateExperience/[^\"'\s]+/requisitions|"
            r"recruit/Portal\.na[^\"'\s]*|jobs/search[^\"'\s]*|job-openings/?|"
            r"embed/job_board\?[^\"'\s]+)",
            html_body or "",
            flags=re.IGNORECASE,
        ):
            _add(m.group(0))

        return found[:40]

    def _oracle_site_ids_v25(self, current_url: str, html_body: str) -> list[str]:
        seen: set[str] = set()
        out: list[str] = []

        def _add(value: str) -> None:
            v = (value or "").strip()
            if not v or v in seen:
                return
            seen.add(v)
            out.append(v)

        for m in _ORACLE_SITE_PATTERN_V25.finditer(current_url or ""):
            _add(m.group(1))
        for m in _ORACLE_SITE_PATTERN_V25.finditer(html_body or ""):
            _add(m.group(1))
        for m in _ORACLE_SITE_NUMBER_PATTERN_V25.finditer(html_body or ""):
            _add(m.group(1))
        for m in re.finditer(r"siteNumber=([A-Za-z0-9_]+)", html_body or "", re.IGNORECASE):
            _add(m.group(1))

        query = dict(parse_qsl(urlparse(current_url or "").query))
        if query.get("siteNumber"):
            _add(query.get("siteNumber", ""))

        base_ids = list(out)
        for base in base_ids:
            if re.fullmatch(r"CX(?:_\d+)?", base, flags=re.IGNORECASE):
                root = base.split("_", 1)[0]
                _add(root)
                for suffix in ("1001", "1002", "1003", "1004"):
                    _add(f"{root}_{suffix}")

        if not out:
            for fallback in ("CX", "CX_1001", "CX_1002"):
                _add(fallback)

        return out[:12]

    def _listing_page_score_v25(self, url: str, html_body: str) -> float:
        if not html_body or len(html_body) < 200:
            return -30.0
        if self._is_non_html_payload_v25(html_body):
            return -35.0

        score = 0.0
        low = html_body.lower()
        url_l = (url or "").lower()

        if _ERROR_PAGE_PATTERN_V25.search(low[:15000]):
            score -= 22
        if _LOGIN_PAGE_PATTERN_V25.search(url_l) or _LOGIN_PAGE_PATTERN_V25.search(low[:15000]):
            score -= 15
        if _FEED_URL_PATTERN_V25.search(url_l):
            score -= 24
        if _DETAIL_URL_PATTERN_V25.search(url_l):
            score -= 8

        if _LISTING_URL_HINT_PATTERN_V25.search(url):
            score += 5

        score += min(low.count("apply"), 8)
        score += min(low.count("job"), 10)
        score += min(low.count("career"), 8)

        try:
            parser = etree.HTMLParser(encoding="utf-8")
            root = etree.fromstring(html_body.encode("utf-8", errors="replace"), parser)
        except Exception:
            return score

        jobish_links = 0
        listing_links = 0
        detail_links = 0
        row_groups: dict[str, int] = defaultdict(int)

        for a_el in root.iter("a"):
            href = (a_el.get("href") or "").strip()
            if not href:
                continue
            text = " ".join((a_el.text_content() or "").split())

            if _LISTING_URL_HINT_PATTERN_V25.search(href):
                listing_links += 1
            if _DETAIL_URL_PATTERN_V25.search(href):
                detail_links += 1
            if (
                len(text) >= 5
                and (_CAREER_TEXT_PATTERN_V25.search(text) or _LISTING_URL_HINT_PATTERN_V25.search(href))
                and not _REJECT_LINK_TEXT_PATTERN_V25.search(text.lower())
            ):
                jobish_links += 1

        for el in root.iter():
            if not isinstance(el.tag, str):
                continue
            tag = el.tag.lower()
            if tag not in {"li", "div", "tr", "article", "section"}:
                continue
            cls = (el.get("class") or "").lower()
            if not cls:
                continue
            if _ROW_CLASS_PATTERN_V25.search(cls):
                key = f"{tag}:{cls.split()[0]}"
                row_groups[key] += 1

        repeated_rows = sum(v for v in row_groups.values() if v >= 3)
        score += min(jobish_links * 2.0, 24.0)
        score += min(listing_links * 2.5, 24.0)
        score += min(repeated_rows * 1.5, 24.0)

        greenhouse_jobs = len(root.xpath("//*[contains(@class,'job-post') or contains(@class,'posting')]") or [])
        if greenhouse_jobs >= 3:
            score += 8.0

        if detail_links >= 2 and listing_links <= 1:
            score -= 8.0

        return score

    def _is_bad_target_v25(self, url: str, html_body: str) -> bool:
        if not html_body or len(html_body) < 200:
            return True
        if self._is_non_html_payload_v25(html_body):
            return True

        url_l = (url or "").lower()
        low = html_body.lower()[:15000]
        score = self._listing_page_score_v25(url, html_body)

        if _ERROR_PAGE_PATTERN_V25.search(low):
            return True
        if _FEED_URL_PATTERN_V25.search(url_l):
            return True
        if _LOGIN_PAGE_PATTERN_V25.search(url_l) and score < 12:
            return True
        if _DETAIL_URL_PATTERN_V25.search(url_l) and score < 10:
            return True

        return False

    @staticmethod
    def _candidate_hosts_v25(domain: str, current_url: str) -> list[str]:
        hosts: list[str] = []
        seen: set[str] = set()

        def _add(host: str) -> None:
            h = (host or "").strip().lower()
            if not h or h in seen:
                return
            seen.add(h)
            hosts.append(h)

        _add(domain)
        _add(urlparse(current_url or "").hostname or "")

        for host in list(hosts):
            if host.startswith("www."):
                _add(host[4:])
            else:
                _add(f"www.{host}")

        return hosts[:8]

    @staticmethod
    def _is_related_host_v25(parent_url: str, child_url: str) -> bool:
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
    def _is_rejected_link_v25(href: str, text: str) -> bool:
        href_l = (href or "").lower()
        text_l = (text or "").lower()

        if _REJECT_LINK_HREF_PATTERN_V25.search(href_l):
            return True
        if _REJECT_LINK_TEXT_PATTERN_V25.search(text_l):
            return True
        return False

    @staticmethod
    def _is_non_html_payload_v25(body: Optional[str]) -> bool:
        if not body:
            return True
        sample = body[:800].lstrip()
        if sample.startswith("%PDF-"):
            return True
        sample_l = sample.lower()
        if sample_l.startswith("<?xml"):
            return True
        if (sample_l.startswith("{") or sample_l.startswith("[")) and "<html" not in sample_l[:400]:
            return True
        return False
