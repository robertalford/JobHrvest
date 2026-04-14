"""
Tiered Extraction Engine v7.5 — precision-safe title recovery + depth refresh.

Strategy:
1. Keep v7.4 extraction order and safety gates.
2. Recover valid specialized hyphen titles dropped by normalization.
3. Recover PageUp lead-role titles (for example "Proposals Lead").
4. Add Connx GridTable ATS extraction for unique detail URLs.
5. Re-enable bounded enrichment for large PageUp sets when descriptions are noisy.
"""

from __future__ import annotations

import asyncio
import html as html_mod
import logging
import re
from typing import Any, Optional
from urllib.parse import urljoin, urlparse

from app.crawlers.tiered_extractor import MAX_JOBS_PER_PAGE, _parse_html, _resolve_url, _text
from app.crawlers.tiered_extractor_v67 import _WEAK_ROLE_HINT_V67
from app.crawlers.tiered_extractor_v74 import (
    TieredExtractorV74,
    _V73_CARD_SKIP_TEXT,
    _V73_FAST_PATH_METHOD_PREFIXES,
    _V73_LOCATION_CLASS_HINT,
    _V73_LOCATION_TITLE_HINT,
    _V73_NAV_TITLE,
    _V73_NON_JOB_HEADING,
)

_V75_PAGEUP_ROLE_FALLBACK = re.compile(r"\b(?:lead|leading\s+hand)\b", re.IGNORECASE)
_V75_HYPHEN_META_SUFFIX = re.compile(
    r"(?:"
    r"\b(?:remote|hybrid|on[\s-]?site|full[\s-]?time|part[\s-]?time|contract|casual|temporary|"
    r"permanent|internship|posted|closing|deadline|job\s+ref|req(?:uisition)?\s*#?)\b|"
    r"\b(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\b|"
    r"\d{1,2}\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)[a-z]*\s+\d{2,4}|"
    r"\b(?:nsw|vic|qld|wa|sa|tas|nt|act|australia|new\s+zealand|singapore|malaysia)\b"
    r")",
    re.IGNORECASE,
)
_V75_ROLE_SUFFIX_HINT = re.compile(
    r"\b(?:property|infrastructure|cyber|security|liability|disputes?|operations?|technology|"
    r"compliance|finance|accounting|digital|data|platform|oracle|governance|engineering)\b",
    re.IGNORECASE,
)

_V75_FILTER_NOISE = re.compile(
    r"\bsort\s+by\b.*\bdepartments?\s*all\b.*\blocations?\s*all\b",
    re.IGNORECASE | re.DOTALL,
)
_V75_LISTING_NOISE = re.compile(
    r"\b(?:show\s+\d+\s+more|load\s+\d+\s+more|view\s+\d+\s+more|read\s+more)\b",
    re.IGNORECASE,
)

_V75_CONNX_DETAIL_PATH = re.compile(r"/job/details/[^/?#]{3,}", re.IGNORECASE)

logger = logging.getLogger(__name__)


class TieredExtractorV75(TieredExtractorV74):
    """v7.5 extractor: keeps v7.4 behavior with targeted quality/coverage fixes."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        page_url = career_page.url if hasattr(career_page, "url") else str(career_page)
        working_html = html or ""
        jobs: list[dict] = []

        sf_jobs = self._extract_successfactors_table_v73(working_html, page_url)
        if sf_jobs:
            sf_jobs = await self._expand_successfactors_pages_v73(working_html, page_url, sf_jobs)
            sf_jobs = self._dedupe_basic_v66(sf_jobs)
            if len(sf_jobs) >= 3 and self._passes_jobset_validation(sf_jobs, page_url):
                jobs = sf_jobs

        if not jobs:
            pageup_jobs = self._extract_pageup_listing_jobs_v74(working_html, page_url)
            if pageup_jobs:
                pageup_jobs = await self._expand_pageup_pages_v74(working_html, page_url, pageup_jobs)
                pageup_jobs = self._dedupe_basic_v66(pageup_jobs)
                if len(pageup_jobs) >= 3 and self._passes_jobset_validation(pageup_jobs, page_url):
                    jobs = pageup_jobs

        # Dedicated Connx GridTable ATS extraction (common on connxcareers hosts).
        if not jobs:
            connx_jobs = self._extract_connx_grid_jobs_v75(working_html, page_url)
            if connx_jobs and len(connx_jobs) >= 2 and self._passes_jobset_validation(connx_jobs, page_url):
                jobs = connx_jobs

        if not jobs:
            recruitee_jobs = self._extract_recruitee_jobs_v74(working_html, page_url)
            if recruitee_jobs:
                jobs = recruitee_jobs

        if not jobs:
            homerun_jobs = self._extract_homerun_jobs_v73(working_html, page_url)
            if homerun_jobs:
                jobs = homerun_jobs

        if not jobs:
            nuxt_rows = self._extract_nuxt_job_rows_v73(working_html, page_url)
            if nuxt_rows:
                nuxt_rows = await self._expand_nuxt_job_rows_pages_v73(working_html, page_url, nuxt_rows)
                nuxt_rows = self._drop_obvious_non_jobs_v73(nuxt_rows, page_url)
                if len(nuxt_rows) >= 3 and self._passes_jobset_validation(nuxt_rows, page_url):
                    jobs = self._dedupe(nuxt_rows, page_url)

        if not jobs:
            jobs = await super().extract(career_page, company, html)

        if len(jobs) < 3:
            json_jobs = await self._extract_jobs_json_feed_v74(page_url, working_html)
            if len(json_jobs) >= 3 and self._passes_jobset_validation(json_jobs, page_url):
                jobs = json_jobs

        jobs = self._postprocess_jobs_v73(jobs, working_html, page_url)

        if self._should_probe_nuxt_shell_v73(working_html, page_url, jobs):
            recovered = await self._probe_localized_nuxt_jobs_v73(page_url, working_html)
            if recovered:
                jobs = self._postprocess_jobs_v73(recovered, working_html, page_url)

        if self._should_enrich_fast_path_v73(jobs, page_url):
            try:
                jobs = await asyncio.wait_for(self._enrich_bounded_v64(jobs), timeout=12.0)
                jobs = self._dedupe(jobs, page_url)
            except asyncio.TimeoutError:
                logger.warning("v7.5 fast-path enrichment timeout for %s", page_url)
            except Exception:
                logger.exception("v7.5 fast-path enrichment failed for %s", page_url)

        return self._clean_jobs_v73(jobs)[:MAX_JOBS_PER_PAGE]

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
            if _V73_LOCATION_CLASS_HINT.search(classes):
                continue

            raw = _text(node)
            if not raw:
                continue
            raw = re.sub(r"\s+\bnew\b\s*$", "", raw, flags=re.IGNORECASE).strip()

            title = self._normalize_title(raw)
            title = self._restore_hyphen_specialization_v75(raw, title)
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
            if _V73_NON_JOB_HEADING.match(title):
                continue
            if _V73_LOCATION_TITLE_HINT.match(title):
                continue
            return title

        for piece in a_el.itertext():
            raw_piece = " ".join((piece or "").split())
            if not raw_piece:
                continue
            if raw_piece.lower() in _V73_CARD_SKIP_TEXT:
                continue

            title = self._normalize_title(raw_piece)
            title = self._restore_hyphen_specialization_v75(raw_piece, title)
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
            if _V73_NON_JOB_HEADING.match(title):
                continue
            if _V73_LOCATION_TITLE_HINT.match(title):
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

    def _restore_hyphen_specialization_v75(self, raw_title: str, normalized_title: str) -> str:
        raw = html_mod.unescape(" ".join(str(raw_title or "").replace("\xa0", " ").split())).strip()
        current = (normalized_title or "").strip()
        if not raw or not current or " - " not in raw:
            return current

        parts = [p.strip(" |:-") for p in raw.split(" - ") if p.strip(" |:-")]
        if len(parts) < 2:
            return current

        prefix = parts[0]
        suffix = " - ".join(parts[1:]).strip()
        if not suffix:
            return current

        # Only recover when base normalization collapsed exactly to the prefix.
        if current.lower() != prefix.lower():
            return current

        low_suffix = suffix.lower()
        if _V75_HYPHEN_META_SUFFIX.search(low_suffix):
            return current
        if "," in suffix and not _V75_ROLE_SUFFIX_HINT.search(suffix):
            return current
        if len(suffix.split()) > 7:
            return current
        if not re.search(r"[A-Za-z]", suffix):
            return current
        if not ("&" in suffix or "/" in suffix or _V75_ROLE_SUFFIX_HINT.search(suffix)):
            return current

        candidate = f"{prefix} - {suffix}".strip()
        if len(candidate.split()) > 14:
            return current
        if _V73_NON_JOB_HEADING.match(candidate) or _V73_NAV_TITLE.match(candidate):
            return current
        return candidate

    def _extract_pageup_listing_jobs_v74(self, html: str, page_url: str) -> list[dict]:
        if not self._is_pageup_listing_page_v74(page_url, html):
            return []

        root = _parse_html(html)
        if root is None:
            return []

        title_nodes = root.xpath("//h3[contains(@class,'list-title')]")
        if not title_nodes:
            return []

        jobs: list[dict] = []
        seen_urls: set[str] = set()
        for title_node in title_nodes[:1200]:
            raw_title = self._normalize_title(_text(title_node) or "")
            if not raw_title or not self._is_valid_pageup_title_v75(raw_title):
                continue

            row = (
                title_node.xpath("ancestor::div[contains(@class,'list-item row')][1]")
                or title_node.xpath("ancestor::div[contains(@class,'list-item') and contains(@class,'row')][1]")
                or title_node.xpath("ancestor::div[contains(@class,'list-item')][1]")
                or title_node.xpath("ancestor::article[1]")
                or [None]
            )[0]
            link_node = None
            if row is not None:
                link_candidates = row.xpath(".//a[@href]")
                for ln in link_candidates:
                    href = (ln.get("href") or "").strip().lower()
                    if "/job/" in href or "/listing/" in href:
                        link_node = ln
                        break
                if link_node is None and link_candidates:
                    link_node = link_candidates[0]
            if link_node is None:
                link_candidates = title_node.xpath("ancestor::a[@href][1]")
                if link_candidates:
                    link_node = link_candidates[0]
            if link_node is None:
                continue

            source_url = _resolve_url((link_node.get("href") or "").strip(), page_url) or page_url
            if source_url in seen_urls or self._is_non_job_url(source_url):
                continue

            has_strong = self._has_strong_card_detail_url_v73(source_url, page_url)
            if self._is_obvious_non_job_card_v73(raw_title, source_url, page_url, has_strong):
                continue

            location = None
            if row is not None:
                loc_nodes = row.xpath(".//span[contains(@class,'location')]|.//li[contains(@class,'location')]")
                for loc_node in loc_nodes[:4]:
                    loc_text = " ".join((_text(loc_node) or "").split()).strip(" ,|-")
                    if 2 <= len(loc_text) <= 120 and loc_text.lower() != raw_title.lower():
                        location = loc_text
                        break

            row_text = " ".join((_text(row) or "").split()) if row is not None else ""
            desc = row_text[:5000] if len(row_text) >= 80 else None

            seen_urls.add(source_url)
            jobs.append(
                {
                    "title": raw_title,
                    "source_url": source_url,
                    "location_raw": location,
                    "description": desc,
                    "salary_raw": None,
                    "employment_type": None,
                    "extraction_method": "ats_pageup_listing_v74",
                    "extraction_confidence": 0.91,
                }
            )

        return self._dedupe_basic_v66(jobs)

    def _is_valid_pageup_title_v75(self, title: str) -> bool:
        if self._is_valid_title_v60(title):
            return True
        t = (title or "").strip()
        if not t:
            return False
        if _V73_NON_JOB_HEADING.match(t) or _V73_NAV_TITLE.match(t) or _V73_LOCATION_TITLE_HINT.match(t):
            return False
        if len(t.split()) > 6:
            return False
        return bool(_V75_PAGEUP_ROLE_FALLBACK.search(t))

    def _should_enrich_fast_path_v73(self, jobs: list[dict], page_url: str) -> bool:
        if len(jobs) < 2:
            return False

        methods = {str(j.get("extraction_method") or "") for j in jobs}
        large_set = len(jobs) > 25
        large_pageup = large_set and methods and all(m.startswith("ats_pageup_listing_v74") for m in methods)
        if large_set and not large_pageup:
            return False

        host = (urlparse(page_url).netloc or "").lower()
        if host in {"example.com", "localhost", "127.0.0.1"}:
            return False

        known_fast_prefix = tuple(_V73_FAST_PATH_METHOD_PREFIXES) + ("ats_connx_grid_v75",)
        if not any(any(method.startswith(prefix) for prefix in known_fast_prefix) for method in methods):
            return False

        missing_depth = sum(
            1
            for j in jobs
            if not j.get("location_raw")
            or self._description_needs_refresh_v75(j.get("description"), str(j.get("title") or ""))
        )
        if missing_depth < max(1, int(len(jobs) * 0.4)):
            return False

        detailish = sum(
            1
            for j in jobs
            if self._is_job_like_url(str(j.get("source_url") or ""))
            or self._has_strong_card_detail_url_v73(str(j.get("source_url") or ""), page_url)
        )
        return detailish >= max(1, int(len(jobs) * 0.5))

    async def _enrich_bounded_v64(self, jobs: list[dict]) -> list[dict]:
        prepared: list[dict] = []
        for job in jobs:
            updated = dict(job)
            if self._description_needs_refresh_v75(updated.get("description"), str(updated.get("title") or "")):
                updated["description"] = None
            prepared.append(updated)

        enriched = await super()._enrich_bounded_v64(prepared)

        merged_jobs: list[dict] = []
        for original, enriched_job in zip(jobs, enriched):
            merged = dict(enriched_job)
            if self._description_is_noise_v75(merged.get("description")):
                merged["description"] = None

            if self._description_needs_refresh_v75(merged.get("description"), str(merged.get("title") or "")):
                original_desc = original.get("description")
                if isinstance(original_desc, str) and not self._description_is_noise_v75(original_desc):
                    merged["description"] = TieredExtractorV74._clean_description_v73(self, original_desc)

            merged_jobs.append(merged)

        return merged_jobs

    def _clean_description_v73(self, value: Any) -> Optional[str]:
        cleaned = TieredExtractorV74._clean_description_v73(self, value)
        if not cleaned:
            return cleaned
        if self._description_is_noise_v75(cleaned):
            return None
        return cleaned

    def _description_needs_refresh_v75(self, value: Any, title: str = "") -> bool:
        cleaned = TieredExtractorV74._clean_description_v73(self, value)
        if not cleaned:
            return True
        if self._description_is_noise_v75(cleaned):
            return True

        low = cleaned.lower()
        if len(cleaned) < 140 and _V75_LISTING_NOISE.search(low):
            return True

        norm_title = " ".join((title or "").split()).strip().lower()
        if norm_title and low.startswith(norm_title) and len(cleaned.split()) < 35:
            return True

        return False

    def _description_is_noise_v75(self, value: Any) -> bool:
        if not isinstance(value, str):
            return False
        text = " ".join(value.split())
        if not text:
            return False
        if _V75_FILTER_NOISE.search(text):
            return True
        if text.lower().startswith("sort by department location"):
            return True
        return False

    def _is_connx_listing_page_v75(self, page_url: str, html: str) -> bool:
        host = (urlparse(page_url).netloc or "").lower()
        if "connxcareers.com" in host:
            return True
        low = (html or "")[:180000].lower()
        return "gridtable--rows" in low and "class=\"name\"" in low

    def _extract_connx_grid_jobs_v75(self, html: str, page_url: str) -> list[dict]:
        if not self._is_connx_listing_page_v75(page_url, html):
            return []

        root = _parse_html(html)
        if root is None:
            return []

        rows = root.xpath(
            "//div[contains(@class,'GridTable') and contains(@class,'GridTable--rows')]/"
            "div[.//*[contains(@class,'name')]]"
        )
        if not rows:
            rows = root.xpath("//div[contains(@class,'GridTable__row') and .//*[contains(@class,'name')]]")
        if not rows:
            return []

        jobs: list[dict] = []
        seen_urls: set[str] = set()

        for row in rows[:1200]:
            title_text = " ".join((row.xpath("string(.//*[contains(@class,'name')][1])") or "").split())
            title = self._normalize_title(title_text)
            if not title or not self._is_valid_title_v60(title):
                continue

            href = (row.xpath("string((.//a[@href][1])/@href)") or "").strip()
            if not href:
                for attr in ("data-href", "data-url", "data-link"):
                    href = (row.get(attr) or "").strip()
                    if href:
                        break
            if not href:
                row_html = (row.xpath("string(.)") or "")
                m = _V75_CONNX_DETAIL_PATH.search(row_html)
                if m:
                    href = m.group(0)
            if not href:
                continue

            source_url = (_resolve_url(href, page_url) or page_url).split("#", 1)[0]
            if source_url in seen_urls:
                continue
            if source_url.rstrip("/") == page_url.rstrip("/"):
                continue
            if self._is_non_job_url(source_url):
                continue

            location = " ".join((row.xpath("string(.//*[contains(@class,'location')][1])") or "").split())
            location = location if 2 <= len(location) <= 120 else None

            employment_type = " ".join((row.xpath("string(.//*[contains(@class,'employmentType')][1])") or "").split())
            employment_type = employment_type if 2 <= len(employment_type) <= 80 else None

            desc = self._extract_row_description_v73(row, title)

            seen_urls.add(source_url)
            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "description": desc,
                    "salary_raw": None,
                    "employment_type": employment_type,
                    "extraction_method": "ats_connx_grid_v75",
                    "extraction_confidence": 0.9,
                }
            )

        return self._dedupe_basic_v66(jobs)
