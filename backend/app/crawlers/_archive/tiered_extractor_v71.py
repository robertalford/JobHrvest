"""
Tiered Extraction Engine v7.1 — reset to v6.9 with focused platform coverage.

Strategy:
1. Roll back v7.0's broad linked-card looseness that increased Type-1 noise.
2. Add dedicated extraction for SuccessFactors/J2W table listings with bounded
   pagination follow-up.
3. Add dedicated extraction for Homerun `job-list` state payloads.
4. Improve title precision for location/company headings and generic career labels.
5. Keep bounded, safe description cleanup for downstream readability.
"""

from __future__ import annotations

import html as html_mod
import json
import logging
import re
from typing import Any, Optional
from urllib.parse import parse_qs, urljoin, urlparse

import httpx

from app.crawlers.tiered_extractor import MAX_JOBS_PER_PAGE, _parse_html, _resolve_url, _text
from app.crawlers.tiered_extractor_v67 import _CARD_PAGINATION_HINT, _WEAK_ROLE_HINT_V67
from app.crawlers.tiered_extractor_v69 import TieredExtractorV69

logger = logging.getLogger(__name__)

_V71_NON_JOB_HEADING = re.compile(
    r"^(?:"
    r"working\s+with\s+us|"
    r"show(?:\s+\d+)?\s+more|"
    r"load(?:\s+\d+)?\s+more|"
    r"view(?:\s+\d+)?\s+more|"
    r"see(?:\s+\d+)?\s+more|"
    r"older\s+entries|newer\s+entries|"
    r"job\s+vacancies|current\s+vacancies|vacancies|"
    r"get\s+in\s+touch!?|"
    r"(?:info\s+)?peluang\s+karir|"
    r"semua\s+peluang\s+karir|"
    r"filter\s+peluang\s+karir|"
    r"careers?\s+at\s+.+"
    r")$",
    re.IGNORECASE,
)

_V71_LOCATION_CLASS_HINT = re.compile(
    r"(?:location|city|region|office|metadata|body__secondary)",
    re.IGNORECASE,
)

_V71_LOCATION_TITLE_HINT = re.compile(
    r"^(?:"
    r"[A-Z][A-Za-z.'-]{1,}(?:\s+[A-Z][A-Za-z.'-]{1,})?"
    r"(?:,\s*[A-Z][A-Za-z.' -]{1,})+|"
    r"(?:USA|US|UK|UAE|AU|NZ|SG|MY|HK|PH|ID|TH),\s*[A-Za-z][A-Za-z .'-]+"
    r")$"
)

_V71_COMPANY_SUFFIX = re.compile(
    r"\b(?:co\.?\s*,?\s*ltd\.?|pty\s+ltd\.?|inc\.?|llc\.?|gmbh|s\.a\.?|sdn\s+bhd)\b",
    re.IGNORECASE,
)

_V71_STRONG_DETAIL_PATH = re.compile(
    r"(?:"
    r"/job-detail/[^/?#]{2,}|"
    r"/jobs?/[^/?#]{3,}|"
    r"/vacancy/[^?#]*/id/[^/?#]{4,}|"
    r"[?&](?:jobid|jobadid|adid|vacancyid|ajid)=\w+"
    r")",
    re.IGNORECASE,
)

_V71_UPPERCASE_TOKEN = re.compile(r"^[A-Z][A-Z0-9+&./-]{2,20}$")

_V71_SHORT_ROLE_ALLOWLIST = {
    "sales",
    "finance",
    "marketing",
    "hr",
    "it",
    "nurse",
    "teacher",
    "chef",
    "driver",
    "cashier",
    "internship",
}

_V71_CARD_SKIP_TEXT = {
    "new",
    "apply",
    "apply now",
    "show more",
    "load more",
    "view more",
    "see more",
}

_V71_SF_TEST_TITLE = re.compile(
    r"(?:nicht\s+bewerben|no\s+aplicar|do\s+not\s+apply|prueba\s+de\s+sistema)",
    re.IGNORECASE,
)

_V71_SF_MAX_PAGES = 5
_V71_SF_TARGET_JOBS = 150


class TieredExtractorV71(TieredExtractorV69):
    """v7.1 extractor: precision reset + SuccessFactors/Homerun coverage."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        page_url = career_page.url if hasattr(career_page, "url") else str(career_page)
        working_html = html or ""

        # Dedicated SuccessFactors/J2W table extraction before generic linked-cards.
        sf_jobs = self._extract_successfactors_table_v71(working_html, page_url)
        if sf_jobs:
            sf_jobs = await self._expand_successfactors_pages_v71(working_html, page_url, sf_jobs)
            sf_jobs = self._dedupe_basic_v66(sf_jobs)
            if len(sf_jobs) >= 3 and self._passes_jobset_validation(sf_jobs, page_url):
                return self._clean_jobs_v71(sf_jobs)[:MAX_JOBS_PER_PAGE]

        # Dedicated Homerun `job-list` state parser (config-driven pages).
        homerun_jobs = self._extract_homerun_jobs_v71(working_html, page_url)
        if homerun_jobs:
            return self._clean_jobs_v71(homerun_jobs)[:MAX_JOBS_PER_PAGE]

        jobs = await super().extract(career_page, company, html)
        return self._clean_jobs_v71(jobs)[:MAX_JOBS_PER_PAGE]

    def _is_valid_title_v60(self, title: str) -> bool:
        if not super()._is_valid_title_v60(title):
            return False

        t = (title or "").strip()
        if not t:
            return False
        if _V71_NON_JOB_HEADING.match(t):
            return False
        if _V71_LOCATION_TITLE_HINT.match(t) and not self._title_has_job_signal(t):
            return False
        if _V71_COMPANY_SUFFIX.search(t) and not _WEAK_ROLE_HINT_V67.search(t):
            return False
        return True

    def _extract_linked_job_cards_v67(self, html: str, page_url: str) -> list[dict]:
        root = _parse_html(html)
        if root is None:
            return []

        anchors = root.xpath("//a[@href and not(starts-with(@href,'#')) and not(starts-with(@href,'javascript:'))]")
        if not anchors:
            return []

        jobs: list[dict] = []
        seen_urls: set[str] = set()

        for a_el in anchors[:900]:
            href = (a_el.get("href") or "").strip()
            if not href:
                continue

            source_url = _resolve_url(href, page_url) or page_url
            if source_url in seen_urls:
                continue

            lower_url = source_url.lower()
            if "/company/" in lower_url and not _V71_STRONG_DETAIL_PATH.search(lower_url):
                continue

            has_strong_job_path = self._has_strong_card_detail_url_v71(source_url, page_url)
            if not has_strong_job_path and not self._is_job_like_url(source_url):
                continue

            title = self._extract_card_title_v67(a_el)
            if not title:
                continue
            if not self._is_valid_card_title_v67(title, has_strong_job_path):
                continue

            seen_urls.add(source_url)
            context_text = " ".join((_text(a_el) or "").split())
            short_desc = context_text[:5000] if len(context_text) >= 120 and context_text.lower() != title.lower() else None

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_card_location_v67(a_el, title),
                    "description": short_desc,
                    "salary_raw": None,
                    "employment_type": None,
                    "extraction_method": "tier2_linked_cards_v67",
                    "extraction_confidence": 0.8,
                }
            )

        return self._dedupe_basic_v66(jobs)

    def _is_valid_card_title_v67(self, title: str, has_strong_job_path: bool) -> bool:
        if self._is_valid_title_v60(title):
            return True
        if not has_strong_job_path:
            return False

        t = (title or "").strip()
        if not t:
            return False
        if _V71_NON_JOB_HEADING.match(t):
            return False
        if _V71_LOCATION_TITLE_HINT.match(t):
            return False
        if not re.search(r"[A-Za-z]", t):
            return False

        words = t.split()

        # Strong-detail fallback: compact one-word role labels.
        if len(words) == 1:
            token = re.sub(r"[^A-Za-z0-9+&./-]", "", words[0])
            if not token:
                return False
            if token.lower() in _V71_SHORT_ROLE_ALLOWLIST:
                return True
            return bool(_V71_UPPERCASE_TOKEN.match(token))

        # Strong-detail fallback: long recruitment campaign titles (common on
        # government/enterprise boards) can exceed the base 14-word validator.
        if 7 <= len(words) <= 40 and re.search(r"\b(?:rekrutmen|recruitment|vacancy|hiring|program)\b", t, re.IGNORECASE):
            return True

        if len(words) > 6:
            return False
        return bool(_WEAK_ROLE_HINT_V67.search(t))

    def _extract_card_title_v67(self, a_el) -> Optional[str]:
        title_nodes = a_el.xpath(
            ".//h1|.//h2|.//h3|.//h4|"
            ".//p[contains(@class,'body--medium') or contains(@class,'sub-title') "
            "or contains(@class,'text-2xl') or contains(@class,'text-3xl') "
            "or contains(@class,'text-4xl') or contains(@class,'text-5xl') "
            "or contains(@class,'text-6xl') or contains(@class,'text-7xl')]|"
            ".//span[contains(@class,'sub-title') or contains(@class,'job-title') "
            "or contains(@class,'position-title') or contains(@class,'role-title')]|"
            ".//*[contains(@class,'job-title') or contains(@class,'position-title') "
            "or contains(@class,'role-title') or contains(@class,'jobs-title') "
            "or contains(@class,'title')]"
        )

        for node in title_nodes[:10]:
            classes = (node.get("class") or "").strip()
            if _V71_LOCATION_CLASS_HINT.search(classes):
                continue

            raw = _text(node)
            if not raw:
                continue
            raw = re.sub(r"\s+\bnew\b\s*$", "", raw, flags=re.IGNORECASE).strip()

            title = self._normalize_title(raw)
            if not title:
                continue
            if len(title) > 260:
                continue
            if len(title) > 140 and not re.search(
                r"\b(?:rekrutmen|recruitment|vacancy|hiring|program)\b",
                title,
                re.IGNORECASE,
            ):
                continue
            if _V71_NON_JOB_HEADING.match(title):
                continue
            if _V71_LOCATION_TITLE_HINT.match(title):
                continue
            return title

        # Fallback: first compact meaningful text fragment.
        for piece in a_el.itertext():
            raw_piece = " ".join((piece or "").split())
            if not raw_piece:
                continue
            if raw_piece.lower() in _V71_CARD_SKIP_TEXT:
                continue

            title = self._normalize_title(raw_piece)
            if not title:
                continue
            if len(title) > 260:
                continue
            if len(title) > 90 and not re.search(
                r"\b(?:rekrutmen|recruitment|vacancy|hiring|program)\b",
                title,
                re.IGNORECASE,
            ):
                continue
            if _V71_NON_JOB_HEADING.match(title):
                continue
            if _V71_LOCATION_TITLE_HINT.match(title):
                continue

            if self._is_valid_title_v60(title):
                return title
            if (
                len(title.split()) <= 40
                and re.search(r"\b(?:rekrutmen|recruitment|vacancy|hiring|program)\b", title, re.IGNORECASE)
            ):
                return title
            if len(title.split()) <= 5 and _WEAK_ROLE_HINT_V67.search(title):
                return title

        return None

    def _pagination_urls_v67(self, html: str, page_url: str) -> list[str]:
        root = _parse_html(html)
        if root is None:
            return []

        page_nav_links = root.xpath(
            "//nav[contains(translate(@aria-label,'PAGINATION','pagination'),'pagination') "
            "or contains(@class,'pagination') or contains(@class,'pager') "
            "or contains(@class,'nav-links')]//a[@href]"
            "|//div[contains(@class,'pagination') or contains(@class,'pager') "
            "or contains(@class,'nav-links')]//a[@href]"
            "|//div[@id='show_more_button']//a[@href]"
            "|//a[contains(@href,'show_more') and @href]"
            "|//a[@rel='next' and @href]"
        )

        candidates: list[str] = []
        page_host = (urlparse(page_url).netloc or "").lower()

        for a_el in page_nav_links:
            href = (a_el.get("href") or "").strip()
            if not href:
                continue

            full_url = urljoin(page_url, href)
            parsed = urlparse(full_url)
            if (parsed.netloc or "").lower() != page_host:
                continue
            if full_url.rstrip("/") == page_url.rstrip("/"):
                continue

            lower_url = full_url.lower()
            if not _CARD_PAGINATION_HINT.search(full_url) and "show_more" not in lower_url:
                continue
            candidates.append(full_url)

        deduped: list[str] = []
        seen: set[str] = set()
        for next_url in candidates:
            if next_url in seen:
                continue
            seen.add(next_url)
            deduped.append(next_url)
            if len(deduped) >= 3:
                break
        return deduped

    def _has_strong_card_detail_url_v71(self, source_url: str, page_url: str) -> bool:
        lower = (source_url or "").lower()
        if _V71_STRONG_DETAIL_PATH.search(lower):
            return True

        parsed = urlparse(source_url or "")
        page_host = (urlparse(page_url or "").netloc or "").lower()
        if page_host and (parsed.netloc or "").lower() != page_host:
            return False
        return bool(re.search(r"[?&](?:id|job|position|vacancy)=\w+", lower))

    @staticmethod
    def _is_successfactors_table_page_v71(page_url: str, html: str) -> bool:
        lower_html = (html or "")[:250000].lower()
        if "jobtitle-link" in lower_html and "data-row" in lower_html:
            return True
        host = (urlparse(page_url).netloc or "").lower()
        return "successfactors" in host and "j2w" in lower_html

    def _extract_successfactors_table_v71(self, html: str, page_url: str) -> list[dict]:
        if not self._is_successfactors_table_page_v71(page_url, html):
            return []

        root = _parse_html(html)
        if root is None:
            return []

        rows = root.xpath("//tr[contains(@class,'data-row') and .//a[contains(@class,'jobTitle-link') and @href]]")
        if not rows:
            return []

        jobs: list[dict] = []
        seen_urls: set[str] = set()

        for row in rows[:1200]:
            links = row.xpath(".//a[contains(@class,'jobTitle-link') and @href]")
            if not links:
                continue

            link = links[0]
            href = (link.get("href") or "").strip()
            if not href:
                continue

            source_url = _resolve_url(href, page_url) or page_url
            if source_url in seen_urls:
                continue
            if self._is_non_job_url(source_url):
                continue

            title = self._normalize_title(_text(link) or "")
            if not title:
                continue
            if _V71_SF_TEST_TITLE.search(title):
                continue
            if not self._is_valid_title_v60(title):
                continue

            location = None
            loc_nodes = row.xpath(".//*[contains(@class,'jobLocation')]")
            for node in loc_nodes[:3]:
                loc_text = " ".join((_text(node) or "").split())
                if 2 <= len(loc_text) <= 160 and loc_text.lower() != title.lower():
                    location = loc_text
                    break

            seen_urls.add(source_url)
            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "description": None,
                    "salary_raw": None,
                    "employment_type": None,
                    "extraction_method": "ats_successfactors_table_v71",
                    "extraction_confidence": 0.9,
                }
            )

        return self._dedupe_basic_v66(jobs)

    def _successfactors_pagination_urls_v71(self, html: str, page_url: str) -> list[str]:
        root = _parse_html(html)
        if root is None:
            return []

        links = root.xpath(
            "//ul[contains(@class,'pagination')]//a[contains(@href,'startrow=') and @href]"
            "|//a[contains(@class,'paginationItem') and contains(@href,'startrow=') and @href]"
        )
        if not links:
            return []

        page_host = (urlparse(page_url).netloc or "").lower()
        current_start = 0
        try:
            current_start = int((parse_qs(urlparse(page_url).query).get("startrow") or ["0"])[0])
        except Exception:
            current_start = 0

        by_start: dict[int, str] = {}
        for a_el in links:
            href = (a_el.get("href") or "").strip()
            if not href:
                continue

            full_url = urljoin(page_url, href)
            parsed = urlparse(full_url)
            if (parsed.netloc or "").lower() != page_host:
                continue

            query = parse_qs(parsed.query)
            try:
                startrow = int((query.get("startrow") or ["0"])[0])
            except Exception:
                continue
            if startrow == current_start:
                continue

            by_start[startrow] = full_url

        if not by_start:
            return []

        ordered = sorted(by_start.items(), key=lambda pair: pair[0])
        forward = [url for start, url in ordered if start > current_start]
        if len(forward) >= _V71_SF_MAX_PAGES:
            return forward[:_V71_SF_MAX_PAGES]

        fallback = [url for _, url in ordered if url not in set(forward)]
        return (forward + fallback)[:_V71_SF_MAX_PAGES]

    async def _expand_successfactors_pages_v71(
        self,
        seed_html: str,
        page_url: str,
        seed_jobs: list[dict],
    ) -> list[dict]:
        if not seed_jobs:
            return []
        if len(seed_jobs) >= _V71_SF_TARGET_JOBS:
            return self._dedupe_basic_v66(seed_jobs)

        next_urls = self._successfactors_pagination_urls_v71(seed_html, page_url)
        if not next_urls:
            return self._dedupe_basic_v66(seed_jobs)

        merged = list(seed_jobs)
        async with httpx.AsyncClient(
            timeout=4.5,
            follow_redirects=True,
            headers={"User-Agent": "Mozilla/5.0"},
        ) as client:
            for next_url in next_urls:
                if len(merged) >= _V71_SF_TARGET_JOBS:
                    break
                try:
                    resp = await client.get(next_url)
                except Exception:
                    continue
                body = resp.text or ""
                if resp.status_code != 200 or len(body) < 200:
                    continue
                merged.extend(self._extract_successfactors_table_v71(body, str(resp.url)))

        return self._dedupe_basic_v66(merged)[:_V71_SF_TARGET_JOBS]

    def _extract_homerun_jobs_v71(self, html: str, page_url: str) -> list[dict]:
        lower = (html or "")[:250000].lower()
        if "homerun" not in lower and "<job-list" not in lower:
            return []

        root = _parse_html(html)
        if root is None:
            return []

        bind_nodes = root.xpath("//job-list[@v-bind]")
        if not bind_nodes:
            return []

        for node in bind_nodes[:3]:
            payload = self._parse_homerun_bind_v71(node.get("v-bind") or "")
            if not payload:
                continue
            jobs = self._extract_homerun_payload_jobs_v71(payload, page_url)
            if jobs:
                return jobs

        return []

    def _parse_homerun_bind_v71(self, raw_bind: str) -> Optional[dict[str, Any]]:
        if not raw_bind:
            return None

        decoded = html_mod.unescape(raw_bind).strip()
        if not decoded.startswith("{"):
            return None

        try:
            parsed = json.loads(decoded)
        except Exception:
            return None
        if isinstance(parsed, dict):
            return parsed
        return None

    def _extract_homerun_payload_jobs_v71(self, payload: dict[str, Any], page_url: str) -> list[dict]:
        content = payload.get("content")
        if not isinstance(content, dict):
            return []

        vacancies = content.get("vacancies")
        if not isinstance(vacancies, list) or not vacancies:
            return []

        location_map: dict[int, str] = {}
        for loc in content.get("locations") or []:
            if isinstance(loc, dict) and isinstance(loc.get("id"), int):
                name = str(loc.get("name") or "").strip()
                if name:
                    location_map[int(loc["id"])] = name

        job_type_map: dict[int, str] = {}
        for jt in content.get("job_types") or []:
            if isinstance(jt, dict) and isinstance(jt.get("id"), int):
                name = str(jt.get("name") or "").strip()
                if name:
                    job_type_map[int(jt["id"])] = name

        jobs: list[dict] = []
        seen_urls: set[str] = set()
        for item in vacancies[:500]:
            if not isinstance(item, dict):
                continue

            title = self._normalize_title(str(item.get("title") or ""))
            if not title:
                continue
            if not self._is_valid_title_v60(title):
                continue

            source_url = _resolve_url(str(item.get("url") or ""), page_url) or page_url
            if source_url in seen_urls:
                continue
            if self._is_non_job_url(source_url):
                continue

            location_raw = None
            loc_id = item.get("location_id")
            if isinstance(loc_id, int):
                location_raw = location_map.get(loc_id)

            employment_type = None
            jt_id = item.get("job_type_id")
            if isinstance(jt_id, int):
                employment_type = job_type_map.get(jt_id)

            seen_urls.add(source_url)
            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location_raw,
                    "description": None,
                    "salary_raw": None,
                    "employment_type": employment_type,
                    "extraction_method": "ats_homerun_state_v71",
                    "extraction_confidence": 0.9,
                }
            )

        return self._dedupe_basic_v66(jobs)

    def _clean_jobs_v71(self, jobs: list[dict]) -> list[dict]:
        if not jobs:
            return []

        cleaned: list[dict] = []
        for job in jobs:
            updated = dict(job)
            updated["description"] = self._clean_description_v71(updated.get("description"))
            cleaned.append(updated)
        return cleaned

    def _clean_description_v71(self, value: Any) -> Optional[str]:
        if not isinstance(value, str):
            return value if isinstance(value, str) else None

        text = html_mod.unescape(value.replace("\xa0", " "))
        if not text.strip():
            return None

        if "<" in text and ">" in text:
            text = re.sub(r"(?is)<br\s*/?>", "\n", text)
            text = re.sub(r"(?is)</p\s*>", "\n\n", text)
            text = re.sub(r"(?is)<li\b[^>]*>", "\n- ", text)
            text = re.sub(r"(?is)</li>", "", text)
            text = re.sub(r"(?is)<[^>]+>", " ", text)

        text = re.sub(r"\r", "", text)
        text = re.sub(r"[ \t]+", " ", text)
        text = "\n".join(line.strip() for line in text.splitlines())
        text = re.sub(r"\n{3,}", "\n\n", text).strip()
        text = re.sub(r"^Back\s+to\s+jobs\s*", "", text, flags=re.IGNORECASE).strip()

        if not text:
            return None
        return text[:5000]
