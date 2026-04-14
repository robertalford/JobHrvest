"""
Tiered Extraction Engine v9.1 - linked-card account filter + EasyJobs cards.

Strategy:
1. Block account/navigation labels from linked-card extraction (e.g. "My Job Basket").
2. Add dedicated EasyJobs card extraction for boards that expose detail slugs
   without classic `/job/` URL hints, including single-job pages.
"""

from __future__ import annotations

import re
from urllib.parse import urlparse

from app.crawlers.tiered_extractor import MAX_JOBS_PER_PAGE, _parse_html, _resolve_url, _text
from app.crawlers.tiered_extractor_v90 import TieredExtractorV90

_V91_CARD_ACCOUNT_TITLE = re.compile(
    r"^(?:"
    r"my\s+job\s+basket(?:\s*\(\d+\))?|"
    r"job\s+basket(?:\s*\(\d+\))?|"
    r"my\s+interests?|"
    r"saved\s+jobs?(?:\s*\(\d+\))?|"
    r"my\s+applications?"
    r")$",
    re.IGNORECASE,
)
_V91_ACCOUNT_URL_PATH = re.compile(
    r"/(?:my[_-](?:interests?|jobs?|basket|applications?)(?:\.aspx)?|saved-jobs?|wishlist)(?:[/?#]|$)",
    re.IGNORECASE,
)
_V91_EASYJOBS_DETAIL_PATH = re.compile(r"^/[a-z0-9]+(?:-[a-z0-9]+){1,}/?$", re.IGNORECASE)


class TieredExtractorV91(TieredExtractorV90):
    """v9.1 extractor: account-label precision + EasyJobs card recovery."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        page_url = career_page.url if hasattr(career_page, "url") else str(career_page)
        working_html = html or ""

        easyjobs = self._extract_easyjobs_cards_v91(working_html, page_url)
        if self._passes_easyjobs_jobset_v91(easyjobs, page_url):
            return self._clean_jobs_v73(self._dedupe_basic_v66(easyjobs))[:MAX_JOBS_PER_PAGE]

        return await super().extract(career_page, company, working_html)

    def _is_non_job_url(self, src: str) -> bool:
        if super()._is_non_job_url(src):
            return True
        return bool(_V91_ACCOUNT_URL_PATH.search(src or ""))

    def _is_valid_card_title_v67(self, title: str, has_strong_job_path: bool) -> bool:
        normalized = self._normalize_title(title)
        if not normalized or _V91_CARD_ACCOUNT_TITLE.match(normalized):
            return False
        return super()._is_valid_card_title_v67(normalized, has_strong_job_path)

    def _extract_card_title_v67(self, a_el):
        title = super()._extract_card_title_v67(a_el)
        if not title:
            return None
        return None if _V91_CARD_ACCOUNT_TITLE.match(self._normalize_title(title)) else title

    def _extract_easyjobs_cards_v91(self, html: str, page_url: str) -> list[dict]:
        preview = (html or "")[:260000].lower()
        if "job__card" not in preview or "office__location" not in preview:
            return []
        if "app-easy-jobs" not in preview and "ej-icon" not in preview:
            return []

        root = _parse_html(html)
        if root is None:
            return []

        cards = root.xpath("//div[contains(concat(' ', normalize-space(@class), ' '), ' job__card ') and .//h3/a[@href]]")
        if not cards:
            return []

        jobs: list[dict] = []
        seen_urls: set[str] = set()

        for card in cards[:1200]:
            link_nodes = card.xpath(".//h3[1]/a[@href][1]|.//h3//a[@href][1]")
            if not link_nodes:
                continue

            link = link_nodes[0]
            href = (link.get("href") or "").strip()
            source_url = (_resolve_url(href, page_url) or "").split("#", 1)[0]
            if not source_url or source_url in seen_urls:
                continue
            if self._is_non_job_url(source_url):
                continue
            if not self._is_easyjobs_detail_url_v91(source_url, page_url):
                continue

            title = self._normalize_title(" ".join((_text(link) or "").split()))
            if not title:
                continue
            if _V91_CARD_ACCOUNT_TITLE.match(title):
                continue
            if not (
                self._is_valid_title_v60(title)
                or self._is_reasonable_structured_title_v81(title)
                or self._is_reasonable_multilingual_title_v88(title)
            ):
                continue

            apply_nodes = card.xpath(".//div[contains(@class,'job__apply')]//a[@href][1]")
            apply_url = (_resolve_url((apply_nodes[0].get("href") or "").strip(), page_url) or "") if apply_nodes else ""
            if apply_url and self._is_non_job_url(apply_url):
                apply_url = ""

            context = " ".join((_text(card) or "").split())
            description = context[:1500] if len(context) >= 40 else None
            if apply_url:
                description = f"{description} Apply now".strip() if description else "Apply now"

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_easyjobs_location_v91(card, title),
                    "description": description,
                    "salary_raw": None,
                    "employment_type": None,
                    "extraction_method": "ats_easyjobs_cards_v91",
                    "extraction_confidence": 0.9,
                }
            )
            seen_urls.add(source_url)

        return self._dedupe_basic_v66(jobs)

    def _is_easyjobs_detail_url_v91(self, source_url: str, page_url: str) -> bool:
        parsed = urlparse(source_url or "")
        if not parsed.netloc or not parsed.path:
            return False
        if parsed.path.lower().endswith("/apply") or parsed.path.lower() == "/":
            return False

        page_host = (urlparse(page_url or "").netloc or "").lower()
        source_host = (parsed.netloc or "").lower()
        if page_host and source_host != page_host:
            return False

        return bool(_V91_EASYJOBS_DETAIL_PATH.match(parsed.path))

    def _extract_easyjobs_location_v91(self, row, title: str) -> str | None:
        nodes = row.xpath(".//span[contains(@class,'office__location')][1]|.//span[contains(@class,'office-location')][1]")
        for node in nodes[:2]:
            location = " ".join((_text(node) or "").split()).strip(" ,|-")
            if not location or location.lower() == title.lower():
                continue
            if len(location) > 140:
                continue
            return location
        return None

    def _passes_easyjobs_jobset_v91(self, jobs: list[dict], page_url: str) -> bool:
        if not jobs:
            return False
        if self._passes_jobset_validation(jobs, page_url):
            return True
        if len(jobs) != 1:
            return False

        job = jobs[0]
        title = self._normalize_title(str(job.get("title") or ""))
        source_url = str(job.get("source_url") or "")
        if not title or _V91_CARD_ACCOUNT_TITLE.match(title):
            return False
        if self._is_non_job_url(source_url):
            return False
        if not self._is_easyjobs_detail_url_v91(source_url, page_url):
            return False

        return (
            self._is_valid_title_v60(title)
            or self._is_reasonable_structured_title_v81(title)
            or self._is_reasonable_multilingual_title_v88(title)
        )
