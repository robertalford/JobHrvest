"""
Career Page Finder v2.1 — direct from v1.6 finder base (CareerPageFinderV4).

High-impact discovery changes:
1. Reject obvious bad targets (404/error pages, job-detail pages, non-HTML payloads).
2. Better sub-page promotion with multilingual career/link scoring and detail URL penalties.
3. ATS path probing for high-volume hosted platforms (Dayforce, ElmoTalent, Oracle CX).
4. Homepage recovery pass when the current discovered URL is weak/wrong.
5. Broader multilingual career keyword coverage (including Malay/Spanish terms).
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


_CAREER_HREF_PATTERN_V21 = re.compile(
    r"/(?:career|careers|jobs?|vacanc|hiring|opening|openings|position|"
    r"opportunit|employment|recruit|talent|search(?:-jobs?)?|"
    r"kerjaya|pekerjaan|jawatan|vacantes?|empleo|trabajo|trabalho|vaga|vagas|"
    r"karir|karier|lowongan|stellen|jobsuche)",
    re.IGNORECASE,
)

_CAREER_TEXT_PATTERN_V21 = re.compile(
    r"\b(?:career|careers|jobs?|vacanc(?:y|ies)|hiring|current\s+vacancies|"
    r"open(?:\s+)?positions?|current\s+opportunities|join\s+our\s+team|"
    r"work\s+with\s+us|browse\s+jobs|job\s+search|all\s+jobs|"
    r"kerjaya|peluang\s+kerjaya|jawatan\s+kosong|"
    r"vacantes?|empleo|trabajo|trabalho|vagas?|"
    r"karir|karier|lowongan|stellenangebote|jobsuche)\b",
    re.IGNORECASE,
)

_REJECT_LINK_TEXT_PATTERN_V21 = re.compile(
    r"\b(?:privacy|terms|cookie|blog|news|media|investor|contact|about|"
    r"sign\s*in|sign\s*up|register|my\s+account|support|help)\b",
    re.IGNORECASE,
)

_REJECT_LINK_HREF_PATTERN_V21 = re.compile(
    r"(?:mailto:|tel:|/privacy|/terms|/cookie|/news|/blog|/investor|"
    r"/contact|/about|/login|/logout|/register|/account|wp-json|feed|rss|\.pdf(?:$|\?)|\.docx?(?:$|\?))",
    re.IGNORECASE,
)

_LISTING_URL_HINT_PATTERN_V21 = re.compile(
    r"/(?:jobs?|careers?|vacanc|opening|openings|position|positions|"
    r"search(?:-jobs?)?|requisition|requisitions|candidateportal|portal\.na|"
    r"kerjaya|vacantes?|lowongan|karir|karier)",
    re.IGNORECASE,
)

_JOB_DETAIL_URL_PATTERN_V21 = re.compile(
    r"/(?:job|jobs|requisition|requisitions)/\d+[A-Za-z0-9_-]*"
    r"|/job/view/\d+"
    r"|/jobs/\d+/"
    r"|event=jobs\.(?:checkjobdetails|viewdisplayonlyjobdetails)"
    r"|/apply/[^/]{4,}$",
    re.IGNORECASE,
)

_ERROR_PAGE_PATTERN_V21 = re.compile(
    r"(?:\b404\b|page\s+not\s+found|something\s+went\s+wrong|error\s+occurred|"
    r"access\s+denied|forbidden|temporarily\s+unavailable|request\s+id|"
    r"sorry\s+about\s+that)",
    re.IGNORECASE,
)

_ROW_CLASS_PATTERN_V21 = re.compile(
    r"job|position|vacanc|opening|requisition|career|posting|listing|accordion",
    re.IGNORECASE,
)

_ATS_HOST_HINT_PATTERN_V21 = re.compile(
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


class CareerPageFinderV21(CareerPageFinderV4):
    """v2.1 finder with stronger recovery from wrong discovery targets."""

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
            ats_candidate = await self._probe_ats_paths_v21(client, domain, company_name, current)
            if ats_candidate:
                ats_score = self._listing_page_score(ats_candidate.get("url", ""), ats_candidate.get("html") or "")
                if current_bad or ats_score > current_score + 1:
                    current = ats_candidate
                    current_score = ats_score
                    current_bad = False

            sub_candidate = await self._try_subpage_discovery_v21(client, current)
            if sub_candidate:
                sub_score = self._listing_page_score(sub_candidate.get("url", ""), sub_candidate.get("html") or "")
                if current_bad or sub_score > current_score + 1:
                    current = sub_candidate
                    current_score = sub_score
                    current_bad = False

            if current_bad:
                homepage_candidate = await self._homepage_recovery_v21(client, domain)
                if homepage_candidate:
                    home_score = self._listing_page_score(
                        homepage_candidate.get("url", ""),
                        homepage_candidate.get("html") or "",
                    )
                    if home_score > current_score:
                        current = homepage_candidate

        return current

    async def _try_subpage_discovery_v21(
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

            if self._is_rejected_link_v21(href, text):
                continue

            full_url = urljoin(parent_url, href)
            if full_url in seen:
                continue
            seen.add(full_url)

            if not self._is_related_host(parent_url, full_url):
                continue

            score = 0.0
            reason_parts: list[str] = []

            if _CAREER_HREF_PATTERN_V21.search(href):
                score += 6.0
                reason_parts.append("href")
            if text and _CAREER_TEXT_PATTERN_V21.search(text):
                score += 6.0
                reason_parts.append("text")
            if _LISTING_URL_HINT_PATTERN_V21.search(full_url):
                score += 3.0
                reason_parts.append("listing_path")
            if "search=" in full_url or ("?" in full_url and "job" in full_url.lower()):
                score += 2.0
                reason_parts.append("query")
            if _JOB_DETAIL_URL_PATTERN_V21.search(full_url):
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
            if total_score > best_score:
                best_score = total_score
                best = {
                    "url": str(resp.url),
                    "method": disc.get("method", "") + f"+subpage_v21:{reason}",
                    "candidates": parent_candidates + [candidate_url],
                    "html": body,
                }

        return best

    async def _probe_ats_paths_v21(
        self,
        client: httpx.AsyncClient,
        domain: str,
        company_name: str,
        current: dict,
    ) -> Optional[dict]:
        current_url = current.get("url") or f"https://{domain}"
        parsed = urlparse(current_url)
        host = (parsed.hostname or domain).lower()
        base_url = f"https://{host}"

        candidate_urls: list[tuple[str, str]] = []
        slugs = _slug_variants(company_name, current_url)
        if not slugs:
            slugs = []

        if "dayforcehcm.com" in host:
            if not slugs:
                slugs = ["careers", "jobs", "candidateportal"]
            for slug in slugs[:8]:
                candidate_urls.extend(
                    [
                        (urljoin(base_url, f"/CandidatePortal/en-AU/{slug}/"), f"dayforce:{slug}:en-AU"),
                        (urljoin(base_url, f"/CandidatePortal/en-US/{slug}/"), f"dayforce:{slug}:en-US"),
                        (urljoin(base_url, f"/CandidatePortal/{slug}/"), f"dayforce:{slug}:base"),
                    ]
                )

        if "elmotalent.com.au" in host:
            if not slugs:
                slugs = ["jobs", "careers"]
            for slug in slugs[:8]:
                candidate_urls.extend(
                    [
                        (urljoin(base_url, f"/careers/{slug}/jobs"), f"elmo:{slug}:jobs"),
                        (urljoin(base_url, f"/careers/{slug}"), f"elmo:{slug}:root"),
                    ]
                )

        if "oraclecloud.com" in host:
            candidate_urls.extend(
                [
                    (urljoin(base_url, "/hcmUI/CandidateExperience/en/sites/CX/requisitions"), "oracle:cx"),
                    (urljoin(base_url, "/hcmUI/CandidateExperience/en/sites/CX_1001/requisitions"), "oracle:cx1001"),
                    (urljoin(base_url, "/hcmUI/CandidateExperience/en/sites/CX_1002/requisitions"), "oracle:cx1002"),
                ]
            )
            html = current.get("html") or ""
            for found in re.findall(
                r"https?://[^\"'\\s]+/hcmUI/CandidateExperience/[^\"'\\s]+/requisitions",
                html,
                flags=re.IGNORECASE,
            ):
                candidate_urls.append((found, "oracle:from_html"))

        if not candidate_urls and _ATS_HOST_HINT_PATTERN_V21.search(host):
            for path in ("/careers", "/jobs", "/CandidatePortal", "/hcmUI/CandidateExperience/en/sites/CX/requisitions"):
                candidate_urls.append((urljoin(base_url, path), "ats:generic"))

        # Generic cross-site recovery probes.
        generic_paths = ("/careers", "/jobs", "/career", "/join-us", "/vacancies", "/kerjaya", "/ms/kerjaya")
        for p in generic_paths:
            candidate_urls.append((urljoin(base_url, p), f"path:{p}"))

        deduped: list[tuple[str, str]] = []
        seen_urls: set[str] = set()
        for u, reason in candidate_urls:
            if u not in seen_urls:
                seen_urls.add(u)
                deduped.append((u, reason))

        if not deduped:
            return None

        best: Optional[dict] = None
        best_score = self._listing_page_score(current.get("url", ""), current.get("html") or "")

        for candidate_url, reason in deduped[:24]:
            try:
                resp = await client.get(candidate_url)
            except Exception:
                continue

            body = resp.text or ""
            if resp.status_code != 200 or len(body) < 200 or self._is_non_html_payload(body):
                continue

            score = self._listing_page_score(str(resp.url), body)
            if score > best_score:
                best_score = score
                best = {
                    "url": str(resp.url),
                    "method": current.get("method", "") + f"+probe_v21:{reason}",
                    "candidates": current.get("candidates", []) + [candidate_url],
                    "html": body,
                }

        return best

    async def _homepage_recovery_v21(
        self,
        client: httpx.AsyncClient,
        domain: str,
    ) -> Optional[dict]:
        home_url = f"https://{domain}"
        try:
            resp = await client.get(home_url)
        except Exception:
            return None

        body = resp.text or ""
        if resp.status_code != 200 or len(body) < 200 or self._is_non_html_payload(body):
            return None

        home_disc = {
            "url": str(resp.url),
            "method": "homepage_recovery_v21",
            "candidates": [str(resp.url)],
            "html": body,
        }
        sub = await self._try_subpage_discovery_v21(client, home_disc)
        return sub or home_disc

    def _listing_page_score(self, url: str, html: str) -> int:
        if not html or len(html) < 200:
            return -20
        if self._is_non_html_payload(html):
            return -30

        score = 0
        lower = html.lower()

        if _ERROR_PAGE_PATTERN_V21.search(lower[:10000]):
            score -= 20
        if _LISTING_URL_HINT_PATTERN_V21.search(url or ""):
            score += 4
        if _JOB_DETAIL_URL_PATTERN_V21.search(url or ""):
            score -= 6

        score += min(lower.count("apply"), 8)
        score += min(lower.count("job"), 8)
        score += min(lower.count("career"), 6)

        if "__next_data__" in lower and '<div id="__next"></div>' in lower:
            score += 2

        try:
            parser = etree.HTMLParser(encoding="utf-8")
            root = etree.fromstring(html.encode("utf-8", errors="replace"), parser)
        except Exception:
            return score

        jobish_links = 0
        career_links = 0
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

            if _CAREER_HREF_PATTERN_V21.search(href) or _CAREER_TEXT_PATTERN_V21.search(txt):
                career_links += 1

            if _JOB_DETAIL_URL_PATTERN_V21.search(href):
                detail_links += 1

            if 6 <= len(txt) <= 140 and _LISTING_URL_HINT_PATTERN_V21.search(href):
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
            if _ROW_CLASS_PATTERN_V21.search(cls):
                key = f"{tag}:{cls.split()[0]}"
                row_groups[key] += 1

        repeated_rows = sum(v for v in row_groups.values() if v >= 3)
        score += min(career_links, 12)
        score += min(jobish_links * 2, 24)
        score += min(repeated_rows * 2, 24)

        if detail_links >= 2 and jobish_links <= 1:
            score -= 8
        if career_links <= 1 and jobish_links <= 1:
            score -= 6

        return score

    def _is_bad_target(self, url: str, html: str) -> bool:
        if not html or len(html) < 200:
            return True
        if self._is_non_html_payload(html):
            return True
        lower = html.lower()[:10000]
        if _ERROR_PAGE_PATTERN_V21.search(lower):
            return True
        if _JOB_DETAIL_URL_PATTERN_V21.search(url or "") and self._listing_page_score(url, html) < 8:
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
    def _is_rejected_link_v21(href: str, text: str) -> bool:
        href_l = (href or "").lower()
        text_l = (text or "").lower()

        if _REJECT_LINK_HREF_PATTERN_V21.search(href_l):
            return True
        if _REJECT_LINK_TEXT_PATTERN_V21.search(text_l):
            return True
        return False

    @staticmethod
    def _is_non_html_payload(body: Optional[str]) -> bool:
        if not body:
            return True
        sample = body[:600].lstrip()
        if sample.startswith("%PDF-"):
            return True
        return False
