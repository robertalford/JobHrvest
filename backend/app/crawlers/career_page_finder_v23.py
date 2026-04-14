"""
Career Page Finder v2.3 — direct from CareerPageFinderV4 with stronger target recovery.

High-impact discovery changes:
1. Domain-safe ATS probing: keep probing the original domain host even after redirects.
2. Expanded generic/listing path probes (jobs/search, job-openings, lowongan, embed-jobs).
3. Script/base-href URL harvesting for SPA-hosted ATS pages (Oracle/Dayforce/careers shells).
4. Better bad-target rejection for login/404/namespace pages and low-signal app shells.
5. Lower-friction subpage promotion when explicit listing paths are discovered.
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


_CAREER_HREF_PATTERN_V23 = re.compile(
    r"/(?:career|careers|jobs?|vacanc|hiring|opening|openings|position|"
    r"opportunit|employment|recruit|talent|search(?:-jobs?)?|"
    r"jobs/search|job-openings|job-openings-list|embed-jobs|recruitment-campaign|"
    r"kerjaya|pekerjaan|jawatan|vacantes?|empleo|trabajo|trabalho|vaga|vagas|loker|"
    r"karir|karier|lowongan|stellen|jobsuche)",
    re.IGNORECASE,
)

_CAREER_TEXT_PATTERN_V23 = re.compile(
    r"\b(?:career|careers|jobs?|vacanc(?:y|ies)|hiring|current\s+vacancies|"
    r"open(?:\s+)?positions?|current\s+opportunities|join\s+our\s+team|"
    r"work\s+with\s+us|browse\s+jobs|job\s+search|all\s+jobs|"
    r"kerjaya|peluang\s+kerjaya|jawatan\s+kosong|"
    r"vacantes?|empleo|trabajo|trabalho|vagas?|"
    r"karir|karier|lowongan|loker|stellenangebote|jobsuche)\b",
    re.IGNORECASE,
)

_REJECT_LINK_TEXT_PATTERN_V23 = re.compile(
    r"\b(?:privacy|terms|cookie|blog|news|media|investor|contact|about|"
    r"sign\s*in|sign\s*up|register|my\s+account|support|help)\b",
    re.IGNORECASE,
)

_REJECT_LINK_HREF_PATTERN_V23 = re.compile(
    r"(?:mailto:|tel:|/privacy|/terms|/cookie|/news|/blog|/investor|"
    r"/contact|/about|/login|/logout|/register|/account|wp-json|feed|rss|\.pdf(?:$|\?)|\.docx?(?:$|\?))",
    re.IGNORECASE,
)

_LISTING_URL_HINT_PATTERN_V23 = re.compile(
    r"/(?:jobs?|careers?|vacanc|opening|openings|position|positions|"
    r"search(?:-jobs?)?|jobs/search|job-openings|embed-jobs|recruitment-campaign|"
    r"requisition|requisitions|candidateportal|portal\.na|"
    r"kerjaya|vacantes?|lowongan|loker|karir|karier)",
    re.IGNORECASE,
)

_JOB_DETAIL_URL_PATTERN_V23 = re.compile(
    r"/(?:job|jobs|requisition|requisitions)/\d+[A-Za-z0-9_-]*"
    r"|/job/view/\d+"
    r"|/jobs/\d+/"
    r"|event=jobs\.(?:checkjobdetails|viewdisplayonlyjobdetails)"
    r"|/apply/[^/]{4,}$",
    re.IGNORECASE,
)

_ERROR_PAGE_PATTERN_V23 = re.compile(
    r"(?:\b404\b|page\s+not\s+found|something\s+went\s+wrong|error\s+occurred|"
    r"access\s+denied|forbidden|temporarily\s+unavailable|request\s+id|"
    r"sorry\s+about\s+that|not\s+found\s*\|)",
    re.IGNORECASE,
)

_LOGIN_PAGE_PATTERN_V23 = re.compile(
    r"(?:mydayforce|select\s+one\s+of\s+the\s+listed\s+companies|"
    r"sign\s+in\s+to\s+your\s+account|candidate\s+portal\s+login|login\.aspx)",
    re.IGNORECASE,
)

_BASE_HREF_SITE_PATTERN_V23 = re.compile(
    r"<base[^>]+href=[\"']([^\"']+/hcmUI/CandidateExperience/[^\"']+)[\"']",
    re.IGNORECASE,
)

_CANDIDATE_PORTAL_BASE_PATTERN_V23 = re.compile(
    r"candidatePortalBaseUrl\"?\s*:\s*\"(https?://[^\"\\s]+)\"",
    re.IGNORECASE,
)

_ROW_CLASS_PATTERN_V23 = re.compile(
    r"job|position|vacanc|opening|requisition|career|posting|listing|accordion",
    re.IGNORECASE,
)

_ATS_HOST_HINT_PATTERN_V23 = re.compile(
    r"(?:dayforcehcm\.com|elmotalent\.com\.au|oraclecloud\.com)",
    re.IGNORECASE,
)


def _slugify(text: str) -> str:
    text = (text or "").strip().lower()
    text = re.sub(r"[^a-z0-9\\s-]", "", text)
    text = re.sub(r"[\\s-]+", "-", text)
    return text.strip("-")


def _slug_variants(company_name: str, current_url: str) -> list[str]:
    seen: set[str] = set()
    variants: list[str] = []

    def _add(value: str) -> None:
        value = (value or "").strip("-")
        if value and value not in seen and len(value) >= 2:
            seen.add(value)
            variants.append(value)

    if company_name:
        base = _slugify(company_name)
        _add(base)
        stripped = re.sub(
            r"\b(?:ltd|limited|pty|inc|corp|corporation|group|co|company|"
            r"holdings|services|solutions|australia|new\s+zealand)\b",
            "",
            company_name,
            flags=re.IGNORECASE,
        )
        _add(_slugify(stripped))
        words = re.findall(r"[a-z0-9]+", company_name.lower())
        if words:
            _add(_slugify(words[0]))
        if len(words) >= 2:
            _add(_slugify(" ".join(words[:2])))

    parsed = urlparse(current_url or "")
    for part in parsed.path.split("/"):
        part = part.strip().lower()
        if part and re.search(r"[a-z]", part):
            _add(_slugify(part))

    return variants


class CareerPageFinderV23(CareerPageFinderV4):
    """v2.3 finder with stronger recovery from wrong discovery targets."""

    async def find(self, domain: str, company_name: str = "") -> dict:
        disc = await super().find(domain, company_name)
        if not disc.get("url"):
            return disc

        current = dict(disc)
        current_score = self._listing_page_score(current.get("url", ""), current.get("html") or "")
        current_bad = self._is_bad_target(current.get("url", ""), current.get("html") or "")

        async with httpx.AsyncClient(
            timeout=8, follow_redirects=True, headers=_CLIENT_HEADERS,
        ) as client:
            script_candidate = await self._probe_script_and_link_targets_v23(client, current)
            if script_candidate:
                script_score = self._listing_page_score(script_candidate.get("url", ""), script_candidate.get("html") or "")
                if current_bad or script_score >= current_score:
                    current = script_candidate
                    current_score = script_score
                    current_bad = self._is_bad_target(current.get("url", ""), current.get("html") or "")

            ats_candidate = await self._probe_ats_paths_v23(client, domain, company_name, current)
            if ats_candidate:
                ats_score = self._listing_page_score(ats_candidate.get("url", ""), ats_candidate.get("html") or "")
                if current_bad or ats_score >= current_score:
                    current = ats_candidate
                    current_score = ats_score
                    current_bad = self._is_bad_target(current.get("url", ""), current.get("html") or "")

            sub_candidate = await self._try_subpage_discovery_v23(client, current)
            if sub_candidate:
                sub_score = self._listing_page_score(sub_candidate.get("url", ""), sub_candidate.get("html") or "")
                if current_bad or sub_score >= current_score:
                    current = sub_candidate
                    current_score = sub_score
                    current_bad = self._is_bad_target(current.get("url", ""), current.get("html") or "")

            if current_bad:
                homepage_candidate = await self._homepage_recovery_v23(client, domain)
                if homepage_candidate:
                    home_score = self._listing_page_score(
                        homepage_candidate.get("url", ""),
                        homepage_candidate.get("html") or "",
                    )
                    if home_score >= current_score:
                        current = homepage_candidate
                        current_score = home_score
                        current_bad = self._is_bad_target(current.get("url", ""), current.get("html") or "")

            # Final fallback: probe original domain with broad listing paths.
            if current_bad or current_score < 8:
                domain_probe = await self._probe_domain_paths_v23(client, domain, company_name, current)
                if domain_probe:
                    probe_score = self._listing_page_score(domain_probe.get("url", ""), domain_probe.get("html") or "")
                    if probe_score >= current_score:
                        current = domain_probe

        return current

    async def _try_subpage_discovery_v23(
        self,
        client: httpx.AsyncClient,
        disc: dict,
    ) -> Optional[dict]:
        parent_url = disc.get("url")
        parent_html = disc.get("html")
        if not parent_url or not parent_html or self._is_non_html_payload(parent_html):
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

            try:
                text = " ".join(a_el.text_content().split())
            except Exception:
                text = " ".join(etree.tostring(a_el, method="text", encoding="unicode").split())

            if self._is_rejected_link_v23(href, text):
                continue

            full_url = urljoin(parent_url, href)
            if full_url in seen:
                continue
            seen.add(full_url)

            if not self._is_related_host(parent_url, full_url):
                continue

            score = 0.0
            reason_parts: list[str] = []

            if _CAREER_HREF_PATTERN_V23.search(href):
                score += 6.0
                reason_parts.append("href")
            if text and _CAREER_TEXT_PATTERN_V23.search(text):
                score += 6.0
                reason_parts.append("text")
            if _LISTING_URL_HINT_PATTERN_V23.search(full_url):
                score += 3.0
                reason_parts.append("listing_path")
            if re.search(r"/jobs/search|/job-openings|/embed-jobs|/lowongan", full_url, re.IGNORECASE):
                score += 4.0
                reason_parts.append("strong_listing_path")
            if "search=" in full_url or ("?" in full_url and "job" in full_url.lower()):
                score += 2.0
                reason_parts.append("query")
            if _JOB_DETAIL_URL_PATTERN_V23.search(full_url):
                score -= 8.0
                reason_parts.append("detail_penalty")

            if score > 0:
                candidates.append((full_url, score, "+".join(reason_parts) or "candidate"))

        if not candidates:
            return None

        candidates.sort(key=lambda x: x[1], reverse=True)

        best: Optional[dict] = None
        best_score = self._listing_page_score(parent_url, parent_html)
        parent_candidates = disc.get("candidates", [])

        for candidate_url, candidate_weight, reason in candidates[:10]:
            try:
                resp = await client.get(candidate_url)
            except Exception:
                continue

            body = resp.text or ""
            if resp.status_code != 200 or len(body) < 200 or self._is_non_html_payload(body):
                continue

            total_score = self._listing_page_score(str(resp.url), body) + candidate_weight
            if total_score > best_score or (
                "strong_listing_path" in reason and total_score >= best_score - 1
            ):
                best_score = total_score
                best = {
                    "url": str(resp.url),
                    "method": disc.get("method", "") + f"+subpage_v23:{reason}",
                    "candidates": parent_candidates + [candidate_url],
                    "html": body,
                }

        return best

    async def _probe_ats_paths_v23(
        self,
        client: httpx.AsyncClient,
        domain: str,
        company_name: str,
        current: dict,
    ) -> Optional[dict]:
        current_url = current.get("url") or f"https://{domain}"
        hosts = self._candidate_hosts_v23(domain, current_url)
        html_body = current.get("html") or ""
        slugs = _slug_variants(company_name, current_url)
        if not slugs:
            slugs = []

        candidate_urls: list[tuple[str, str, float]] = []
        for host in hosts:
            base_url = f"https://{host}"

            if "dayforcehcm.com" in host:
                local_slugs = slugs or ["careers", "jobs", "candidateportal"]
                for slug in local_slugs[:8]:
                    candidate_urls.extend(
                        [
                            (urljoin(base_url, f"/CandidatePortal/en-AU/{slug}/"), f"dayforce:{slug}:en-AU", 6.0),
                            (urljoin(base_url, f"/CandidatePortal/en-US/{slug}/"), f"dayforce:{slug}:en-US", 5.5),
                            (urljoin(base_url, f"/CandidatePortal/{slug}/"), f"dayforce:{slug}:base", 5.0),
                        ]
                    )
                candidate_urls.extend(
                    [
                        (urljoin(base_url, "/CandidatePortal/en-AU/"), "dayforce:root:en-AU", 4.0),
                        (urljoin(base_url, "/CandidatePortal/en-US/"), "dayforce:root:en-US", 4.0),
                    ]
                )

            if "elmotalent.com.au" in host:
                local_slugs = slugs or ["jobs", "careers"]
                for slug in local_slugs[:8]:
                    candidate_urls.extend(
                        [
                            (urljoin(base_url, f"/careers/{slug}/jobs"), f"elmo:{slug}:jobs", 5.0),
                            (urljoin(base_url, f"/careers/{slug}"), f"elmo:{slug}:root", 4.0),
                        ]
                    )

            if "oraclecloud.com" in host:
                candidate_urls.extend(
                    [
                        (urljoin(base_url, "/hcmUI/CandidateExperience/en/sites/CX/requisitions"), "oracle:cx", 5.0),
                        (urljoin(base_url, "/hcmUI/CandidateExperience/en/sites/CX_1001/requisitions"), "oracle:cx1001", 6.0),
                        (urljoin(base_url, "/hcmUI/CandidateExperience/en/sites/CX_1002/requisitions"), "oracle:cx1002", 5.5),
                    ]
                )

            generic_paths = (
                "/careers", "/careers/", "/career", "/jobs", "/jobs/", "/jobs/search",
                "/job-openings", "/job-openings/", "/vacancies", "/career-opportunities",
                "/join-us", "/join-our-team", "/positions", "/open-positions",
                "/lowongan", "/loker", "/kerjaya", "/ms/kerjaya", "/embed-jobs",
                "/embed-jobs?pageNumber=1&pageSize=20&isActive=true&sorting=PublishDateDesc",
                "/hcmUI/CandidateExperience/en/sites/CX/requisitions",
            )
            for p in generic_paths:
                bonus = 3.0 if "jobs/search" in p or "embed-jobs" in p else 2.0
                candidate_urls.append((urljoin(base_url, p), f"path:{p}", bonus))

        for found in self._extract_script_urls_v23(current_url, html_body):
            weight = 4.5 if _LISTING_URL_HINT_PATTERN_V23.search(found) else 3.0
            candidate_urls.append((found, "script_url", weight))

        if not candidate_urls:
            return None

        return await self._probe_url_candidates_v23(
            client=client,
            current=current,
            candidates=candidate_urls,
            limit=42,
        )

    async def _probe_script_and_link_targets_v23(
        self,
        client: httpx.AsyncClient,
        current: dict,
    ) -> Optional[dict]:
        current_url = current.get("url", "")
        html_body = current.get("html") or ""
        if not current_url or not html_body:
            return None

        candidates: list[tuple[str, str, float]] = []
        for u in self._extract_script_urls_v23(current_url, html_body):
            weight = 4.5 if _LISTING_URL_HINT_PATTERN_V23.search(u) else 3.0
            candidates.append((u, "script_hint", weight))

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
                full = urljoin(current_url, href)
                if not self._is_related_host(current_url, full):
                    continue
                if not _LISTING_URL_HINT_PATTERN_V23.search(full):
                    continue
                weight = 3.5
                if re.search(r"/jobs/search|/embed-jobs|/requisitions|/lowongan", full, re.IGNORECASE):
                    weight = 4.5
                candidates.append((full, "html_link", weight))

        if not candidates:
            return None
        return await self._probe_url_candidates_v23(client, current, candidates, limit=20)

    async def _probe_domain_paths_v23(
        self,
        client: httpx.AsyncClient,
        domain: str,
        company_name: str,
        current: dict,
    ) -> Optional[dict]:
        hosts = self._candidate_hosts_v23(domain, current.get("url") or f"https://{domain}")
        slugs = _slug_variants(company_name, current.get("url") or "")
        candidates: list[tuple[str, str, float]] = []

        broad_paths = (
            "/careers", "/career", "/jobs", "/jobs/search", "/job-openings", "/vacancies",
            "/career-opportunities", "/positions", "/open-positions",
            "/join-us", "/join-our-team", "/current-opportunities",
            "/lowongan", "/loker", "/kerjaya", "/embed-jobs",
            "/embed-jobs?pageNumber=1&pageSize=20&isActive=true&sorting=PublishDateDesc",
        )
        for host in hosts:
            base = f"https://{host}"
            for p in broad_paths:
                weight = 3.0 if "jobs/search" in p or "embed-jobs" in p else 2.0
                candidates.append((urljoin(base, p), f"domain_path:{p}", weight))
            for slug in slugs[:6]:
                candidates.append((urljoin(base, f"/CandidatePortal/en-AU/{slug}/"), f"domain_dayforce:{slug}", 4.0))
                candidates.append((urljoin(base, f"/careers/{slug}"), f"domain_careers:{slug}", 3.0))

        if not candidates:
            return None
        return await self._probe_url_candidates_v23(client, current, candidates, limit=36)

    async def _probe_url_candidates_v23(
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
        best_score = self._listing_page_score(current.get("url", ""), current.get("html") or "")

        for candidate_url, reason, weight in deduped[:limit]:
            try:
                resp = await client.get(candidate_url)
            except Exception:
                continue
            body = resp.text or ""
            if resp.status_code != 200 or len(body) < 200 or self._is_non_html_payload(body):
                continue

            listing_score = self._listing_page_score(str(resp.url), body)
            total_score = listing_score + weight
            if total_score > best_score or (
                _LISTING_URL_HINT_PATTERN_V23.search(candidate_url) and total_score >= best_score - 0.5
            ):
                best_score = total_score
                best = {
                    "url": str(resp.url),
                    "method": current.get("method", "") + f"+probe_v23:{reason}",
                    "candidates": current.get("candidates", []) + [candidate_url],
                    "html": body,
                }

        return best

    def _candidate_hosts_v23(self, domain: str, current_url: str) -> list[str]:
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

        for h in list(hosts):
            if h.startswith("www."):
                _add(h[4:])
            else:
                _add(f"www.{h}")
        return hosts[:8]

    def _extract_script_urls_v23(self, current_url: str, html_body: str) -> list[str]:
        found: list[str] = []
        seen: set[str] = set()

        def _add(url: str) -> None:
            u = (url or "").strip()
            if not u:
                return
            if not re.match(r"^https?://", u, flags=re.IGNORECASE):
                u = urljoin(current_url, u)
            if u in seen:
                return
            if not self._is_related_host(current_url, u):
                return
            seen.add(u)
            found.append(u)

        for m in _BASE_HREF_SITE_PATTERN_V23.finditer(html_body or ""):
            _add(m.group(1))
            _add(urljoin(m.group(1), "requisitions"))
        for m in _CANDIDATE_PORTAL_BASE_PATTERN_V23.finditer(html_body or ""):
            _add(m.group(1))
        for m in re.finditer(
            r"https?://[^\"'\\s]+/(?:hcmUI/CandidateExperience/[^\"'\\s]+/requisitions|"
            r"CandidatePortal/[^\"'\\s]*|jobs/search[^\"'\\s]*|embed-jobs[^\"'\\s]*)",
            html_body or "",
            flags=re.IGNORECASE,
        ):
            _add(m.group(0))

        return found[:30]

    async def _homepage_recovery_v23(
        self,
        client: httpx.AsyncClient,
        domain: str,
    ) -> Optional[dict]:
        best: Optional[dict] = None
        best_score = -10_000
        for host in self._candidate_hosts_v23(domain, f"https://{domain}"):
            home_url = f"https://{host}"
            try:
                resp = await client.get(home_url)
            except Exception:
                continue

            body = resp.text or ""
            if resp.status_code != 200 or len(body) < 200 or self._is_non_html_payload(body):
                continue

            home_disc = {
                "url": str(resp.url),
                "method": "homepage_recovery_v23",
                "candidates": [str(resp.url)],
                "html": body,
            }
            sub = await self._try_subpage_discovery_v23(client, home_disc)
            candidate = sub or home_disc
            score = self._listing_page_score(candidate.get("url", ""), candidate.get("html") or "")
            if score > best_score:
                best_score = score
                best = candidate
        return best

    def _listing_page_score(self, url: str, html: str) -> int:
        if not html or len(html) < 200:
            return -20
        if self._is_non_html_payload(html):
            return -30

        score = 0
        lower = html.lower()

        if _ERROR_PAGE_PATTERN_V23.search(lower[:10000]):
            score -= 20
        if _LOGIN_PAGE_PATTERN_V23.search((url or "").lower()) or _LOGIN_PAGE_PATTERN_V23.search(lower[:15000]):
            score -= 22
        if _LISTING_URL_HINT_PATTERN_V23.search(url or ""):
            score += 4
        if _JOB_DETAIL_URL_PATTERN_V23.search(url or ""):
            score -= 6

        score += min(lower.count("apply"), 8)
        score += min(lower.count("job"), 8)
        score += min(lower.count("career"), 6)
        score += min(lower.count("lowongan"), 6)
        score += min(lower.count("loker"), 4)
        if "jobs/search" in lower:
            score += 5
        if "candidateportal" in lower and "job board" in lower:
            score += 4
        if "router-view" in lower and ("embed-jobs" in lower or "common-data-provider" in lower):
            score += 3

        if "__next_data__" in lower and '<div id="__next"></div>' in lower:
            score += 2
        if _BASE_HREF_SITE_PATTERN_V23.search(html or ""):
            score += 4

        try:
            parser = etree.HTMLParser(encoding="utf-8")
            root = etree.fromstring(html.encode("utf-8", errors="replace"), parser)
        except Exception:
            return score

        jobish_links = 0
        career_links = 0
        strong_listing_links = 0
        detail_links = 0
        row_groups: dict[str, int] = defaultdict(int)

        for a_el in root.iter("a"):
            href = (a_el.get("href") or "").strip()
            if not href:
                continue

            try:
                txt = " ".join(a_el.text_content().split())
            except Exception:
                txt = " ".join(etree.tostring(a_el, method="text", encoding="unicode").split())

            if _CAREER_HREF_PATTERN_V23.search(href) or _CAREER_TEXT_PATTERN_V23.search(txt):
                career_links += 1
            if re.search(
                r"/jobs/search|/embed-jobs|/requisitions|/candidateportal|/lowongan|/loker",
                href,
                re.IGNORECASE,
            ):
                strong_listing_links += 1

            if _JOB_DETAIL_URL_PATTERN_V23.search(href):
                detail_links += 1

            if 6 <= len(txt) <= 140 and _LISTING_URL_HINT_PATTERN_V23.search(href):
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
            if _ROW_CLASS_PATTERN_V23.search(cls):
                key = f"{tag}:{cls.split()[0]}"
                row_groups[key] += 1

        repeated_rows = sum(v for v in row_groups.values() if v >= 3)
        score += min(career_links, 12)
        score += min(jobish_links * 2, 24)
        score += min(strong_listing_links * 3, 18)
        score += min(repeated_rows * 2, 24)

        if detail_links >= 2 and jobish_links <= 1:
            score -= 8
        if career_links <= 1 and jobish_links <= 1 and strong_listing_links == 0:
            score -= 6

        return score

    def _is_bad_target(self, url: str, html: str) -> bool:
        if not html or len(html) < 200:
            return True
        if self._is_non_html_payload(html):
            return True
        lower = html.lower()[:10000]
        if _ERROR_PAGE_PATTERN_V23.search(lower):
            return True
        if _LOGIN_PAGE_PATTERN_V23.search((url or "").lower()) and self._listing_page_score(url, html) < 12:
            return True
        if _LOGIN_PAGE_PATTERN_V23.search(lower) and self._listing_page_score(url, html) < 12:
            return True
        if _JOB_DETAIL_URL_PATTERN_V23.search(url or "") and self._listing_page_score(url, html) < 8:
            return True
        return False

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
    def _is_rejected_link_v23(href: str, text: str) -> bool:
        href_l = (href or "").lower()
        text_l = (text or "").lower()

        if _REJECT_LINK_HREF_PATTERN_V23.search(href_l):
            return True
        if _REJECT_LINK_TEXT_PATTERN_V23.search(text_l):
            return True
        return False

    @staticmethod
    def _is_non_html_payload(body: Optional[str]) -> bool:
        if not body:
            return True
        sample = body[:600].lstrip()
        if sample.startswith("%PDF-"):
            return True
        sample_l = sample.lower()
        if (sample_l.startswith("{") or sample_l.startswith("[")) and "<html" not in sample_l[:300]:
            return True
        return False
