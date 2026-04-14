"""
Tiered Extraction Engine v1.7 — extends v1.6 with high-impact general improvements:

1. Multi-candidate container sweep (top 25) + bucket aggregation to recover split lists
   across multiple sibling containers.
2. Global repeated-row harvesting (e.g., li.position, div.jobItem, tr.jobDetailRow)
   when the best container only captures a subset or single row.
3. Heading-section fallback for text-heavy vacancy pages where jobs are h2/h3 blocks
   with little link structure.
4. Stricter small-set validation to reduce false positives from nav/footer/marketing blocks.
5. Title normalization: strip appended location/deadline/type metadata and reject
   common e-commerce/marketing labels that are not job titles.
"""

from __future__ import annotations

import logging
import re
from collections import defaultdict
from typing import Optional
from urllib.parse import urlparse

from lxml import etree

from app.crawlers.tiered_extractor_v16 import TieredExtractorV16, _title_has_job_noun
from app.crawlers.tiered_extractor import (
    _parse_html,
    _text,
    _href,
    _resolve_url,
    _get_el_classes,
    _is_valid_title,
    _JOB_URL_PATTERN,
    _JOB_TYPE_PATTERN,
    _AU_LOCATIONS,
    MAX_JOBS_PER_PAGE,
    MIN_JOBS_FOR_SUCCESS,
)

logger = logging.getLogger(__name__)

_TITLE_HINT_PATTERN_V17 = re.compile(
    r"\b(?:job|jobs|career|careers|vacanc(?:y|ies)|opening|openings|"
    r"position|positions|role|roles|internship|intern|apprentice|"
    r"manager|engineer|developer|officer|specialist|assistant|"
    r"analyst|consultant|coordinator|executive|technician|designer|"
    r"administrator|recruit(?:er|ment)?|"
    r"akuntan|asisten|psikolog(?:i)?|fotografer|staf|pegawai|karyawan|lowongan)\b",
    re.IGNORECASE,
)

_CONTEXT_HINT_PATTERN_V17 = re.compile(
    r"\b(?:apply|deadline|closing|location|salary|compensation|"
    r"full[\s-]?time|part[\s-]?time|contract|permanent|temporary|"
    r"casual|remote|hybrid|qualifications?|requirements?)\b",
    re.IGNORECASE,
)

_REJECT_TITLE_PATTERN_V17 = re.compile(
    r"^(?:"
    r"powered\s+by|"
    r"info\s+lengkap|"
    r"current\s+vacancies|"
    r"open\s+roles?|"
    r"read\s+more|"
    r"show\s+all\s+jobs?|"
    r"show\s+advanced|"
    r"size\s*&\s*fit|"
    r"shipping\s*&\s*returns|"
    r"cart|checkout|wishlist|"
    r"saved\s+jobs?|job\s+alerts?|profile\s+details|work\s+preferences|account\s+settings|"
    r"job\s+seekers?|blogs?|"
    r"first\s+name|last\s+name|email|phone|message|"
    r"nature\s+needs\s+your\s+support|"
    r"bisa\s+kamu\s+baca\s+di\s+sini.*|"
    r"alamat\s+kantor|"
    r"main\s+menu"
    r")$",
    re.IGNORECASE,
)

# Concatenated metadata seen in job cards: location, deadline, employment type, etc.
_TRAILING_META_SPLIT_V17 = re.compile(
    r"(?:\bdeadline\s*:|\bclosing\s+date\b|\blocation\s*:|\bemployment\s+type\s*:|"
    r"\bpermanent\b|\btemporary\b|\bcontract\b|\bcasual\b|\bfull[\s-]?time\b|\bpart[\s-]?time\b)",
    re.IGNORECASE,
)

# Example: "Automation Test EngineerVijayawada, India"
_CAMEL_LOCATION_SPLIT_V17 = re.compile(
    r"^(.{3,100}?)([a-z])([A-Z][A-Za-z\.-]+(?:[,\s]+[A-Z][A-Za-z\.-]+)*)$"
)

_ROW_CLASS_STRONG_PATTERN_V17 = re.compile(
    r"job|position|vacanc|opening|posting|recruit|career|lowongan|karir|karier",
    re.IGNORECASE,
)

_CTA_TITLE_PATTERN_V17 = re.compile(
    r"^(?:become|learn|discover|protect|shop|share|follow|read|view|download|"
    r"contact|about|what\s+we|how\s+we|our\s+(?:team|culture|story|community))\b",
    re.IGNORECASE,
)

_JOB_URL_HINT_PATTERN_V17 = re.compile(
    r"/(?:job|jobs|career|careers|position|positions|vacanc|opening|openings|"
    r"role|roles|apply|recruit|search|lowongan|karir|karier|vacature|empleo|trabajo|"
    r"portal\.na|/p/)",
    re.IGNORECASE,
)

_NON_JOB_URL_PATTERN_V17 = re.compile(
    r"/(?:about|contact|news|blog|report|investor|ir|privacy|terms|cookie|"
    r"shop|store|donate|support|resource|event|story|team|culture|values)(?:/|$)",
    re.IGNORECASE,
)


class TieredExtractorV17(TieredExtractorV16):
    """v1.7 — broader coverage + stricter quality validation on Tier 2."""

    def _extract_tier2_v16(self, url: str, html: str) -> Optional[list[dict]]:
        """Override v1.6 Tier 2 with multi-strategy candidate selection."""
        root = _parse_html(html)
        if root is None:
            return None

        is_elementor = "elementor" in html[:5000].lower()
        page_apply_count = self._count_page_apply_buttons(root)

        candidates: list[tuple[str, list[dict]]] = []

        # Keep v1.6 behavior as one candidate source (for backwards compatibility).
        base_jobs = super()._extract_tier2_v16(url, html)
        if base_jobs:
            candidates.append(("v16_base", self._dedupe_jobs(base_jobs)))

        sweep_jobs = self._extract_from_candidate_sweep(root, url, is_elementor, page_apply_count)
        if sweep_jobs:
            candidates.append(("container_sweep", sweep_jobs))

        row_jobs = self._extract_from_repeated_rows(root, url)
        if row_jobs:
            candidates.append(("row_harvest", row_jobs))

        heading_jobs = self._extract_from_heading_sections(root, url)
        if heading_jobs:
            candidates.append(("heading_sections", heading_jobs))

        best = self._pick_best_jobset(candidates, url)
        if best:
            return best

        return None

    @staticmethod
    def _is_valid_title_v15(title: str) -> bool:
        """v1.7 title validation: v1.5 checks + broader non-job rejection."""
        if not TieredExtractorV16._is_valid_title_v15(title):
            return False

        t = (title or "").strip()
        if not t:
            return False

        lower = t.lower()

        # Placeholder/template noise.
        if "%header_" in lower or "%label_" in lower:
            return False

        if _REJECT_TITLE_PATTERN_V17.match(lower):
            return False

        if _CTA_TITLE_PATTERN_V17.search(lower) and not _TITLE_HINT_PATTERN_V17.search(lower):
            return False

        if len(t) > 90 and not _TITLE_HINT_PATTERN_V17.search(lower):
            return False

        words = t.split()
        if len(words) > 12:
            return False
        if len(words) > 8 and not _title_has_job_noun(t):
            return False

        # Very long titles with no spaces are usually data noise.
        if len(t) > 60 and " " not in t:
            return False

        return True

    def _extract_heuristic_job_v15(
        self, row: etree._Element, base_url: str, container_score: int
    ) -> Optional[dict]:
        """v1.7 post-processes v1.5 row extraction with title normalization."""
        job = super()._extract_heuristic_job_v15(row, base_url, container_score)
        if not job:
            return None

        normalized = self._normalize_title_v17(job.get("title", ""))
        if not self._is_valid_title_v15(normalized):
            return None

        job["title"] = normalized
        job["extraction_method"] = "tier2_heuristic_v17"

        # If the row has a better explicit link than what base extraction found, prefer it.
        if job.get("source_url") == base_url:
            for a_el in row.iter("a"):
                href = a_el.get("href")
                candidate = _resolve_url(href, base_url)
                if not candidate:
                    continue
                if _JOB_URL_PATTERN.search(candidate) or candidate != base_url:
                    job["source_url"] = candidate
                    break

        return job

    def _extract_from_candidate_sweep(
        self,
        root: etree._Element,
        url: str,
        is_elementor: bool,
        page_apply_count: int,
    ) -> Optional[list[dict]]:
        """Evaluate a wider candidate set and merge compatible containers."""
        candidates = self._score_containers_v16(root, url, is_elementor, page_apply_count)
        if not candidates:
            return None

        candidates.sort(key=lambda c: (c[1], c[2]), reverse=True)

        bucketed_jobs: dict[str, list[dict]] = defaultdict(list)

        for el, score, _child_count in candidates[:25]:
            tag = (el.tag or "").lower() if isinstance(el.tag, str) else ""
            if tag in {"a", "span", "p", "h1", "h2", "h3", "h4", "h5", "h6"}:
                continue

            jobs = self._extract_jobs_v15(el, url, score)
            if not jobs:
                continue

            jobs = self._dedupe_jobs(jobs)
            key = self._container_bucket_key(el)
            bucketed_jobs[key].extend(jobs)

        if not bucketed_jobs:
            return None

        best_jobs: Optional[list[dict]] = None
        best_score = -1.0

        for _, jobs in bucketed_jobs.items():
            deduped = self._dedupe_jobs(jobs)
            if not self._passes_jobset_validation(deduped, url):
                continue
            score = self._jobset_score(deduped, url)
            if score > best_score:
                best_score = score
                best_jobs = deduped

        return best_jobs

    def _extract_from_repeated_rows(self, root: etree._Element, url: str) -> Optional[list[dict]]:
        """Global row harvest for repeated job-like row classes across the page."""
        row_buckets: dict[str, list[etree._Element]] = defaultdict(list)

        for el in root.iter():
            if not isinstance(el.tag, str):
                continue

            tag = el.tag.lower()
            if tag not in {"li", "div", "tr", "article", "section"}:
                continue

            classes = _get_el_classes(el)
            if not classes:
                continue
            if not _ROW_CLASS_STRONG_PATTERN_V17.search(classes):
                continue

            tokens = classes.split()
            row_token = next((t for t in tokens if _ROW_CLASS_STRONG_PATTERN_V17.search(t)), tokens[0])
            key = f"{tag}:{row_token}"
            row_buckets[key].append(el)

        best_jobs: Optional[list[dict]] = None
        best_score = -1.0

        for _, rows in row_buckets.items():
            if len(rows) < 3 or len(rows) > MAX_JOBS_PER_PAGE:
                continue

            jobs: list[dict] = []
            for row in rows:
                job = self._extract_heuristic_job_v15(row, url, container_score=14)
                if job:
                    jobs.append(job)

            jobs = self._dedupe_jobs(jobs)
            if not self._passes_jobset_validation(jobs, url):
                continue

            score = self._jobset_score(jobs, url)
            if score > best_score:
                best_score = score
                best_jobs = jobs

        return best_jobs

    def _extract_from_heading_sections(self, root: etree._Element, url: str) -> Optional[list[dict]]:
        """Fallback for pages where vacancies are represented as heading sections."""
        headings = root.xpath("//main//h2 | //main//h3 | //main//h4 | //article//h2 | //article//h3 | //article//h4")
        if not headings:
            headings = root.xpath("//h2 | //h3 | //h4")

        jobs: list[dict] = []
        for h in headings:
            raw_title = _text(h)
            title = self._normalize_title_v17(raw_title)
            if not self._is_valid_title_v15(title):
                continue

            ancestor_classes = self._collect_ancestor_classes(h, depth=4)
            has_content_ancestor = bool(
                re.search(r"content|prose|article|entry|post|body|career|vacanc|job", ancestor_classes)
            )
            if not has_content_ancestor:
                continue

            parent_text = _text(h.getparent())[:700] if h.getparent() is not None else ""
            has_title_signal = self._title_has_job_signal(title)
            has_context_signal = bool(_CONTEXT_HINT_PATTERN_V17.search(parent_text))
            if not (has_title_signal or has_context_signal):
                continue

            link_href = _href(h)
            if not link_href:
                sibling_links = h.xpath("following-sibling::a[1]")
                if sibling_links:
                    link_href = sibling_links[0].get("href")

            source_url = _resolve_url(link_href, url) or url

            location = None
            loc_match = _AU_LOCATIONS.search(parent_text)
            if loc_match:
                location = loc_match.group(0)

            employment_type = None
            type_match = _JOB_TYPE_PATTERN.search(parent_text)
            if type_match:
                employment_type = type_match.group(0)

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "salary_raw": None,
                    "employment_type": employment_type,
                    "description": None,
                    "extraction_method": "tier2_heading_sections_v17",
                    "extraction_confidence": 0.62,
                }
            )

        jobs = self._dedupe_jobs(jobs)
        if not self._passes_jobset_validation(jobs, url):
            return None
        return jobs

    def _pick_best_jobset(self, jobsets: list[tuple[str, list[dict]]], page_url: str) -> Optional[list[dict]]:
        """Choose the highest-quality jobset across extraction strategies."""
        best: Optional[list[dict]] = None
        best_score = -1.0

        fallback: Optional[list[dict]] = None
        fallback_score = -1.0

        for label, jobs in jobsets:
            deduped = self._dedupe_jobs(jobs)
            if len(deduped) < MIN_JOBS_FOR_SUCCESS:
                continue

            pruned = [
                j for j in deduped
                if self._title_has_job_signal(j.get("title", "")) or self._is_job_like_url(j, page_url)
            ]
            if len(pruned) >= MIN_JOBS_FOR_SUCCESS:
                deduped = pruned

            score = self._jobset_score(deduped, page_url)

            if self._passes_jobset_validation(deduped, page_url):
                if score > best_score:
                    best_score = score
                    best = deduped
            else:
                if score > fallback_score:
                    fallback_score = score
                    fallback = deduped

            logger.debug(
                "v1.7 candidate %s: %d jobs, score=%.2f, valid=%s",
                label,
                len(deduped),
                score,
                self._passes_jobset_validation(deduped, page_url),
            )

        if best:
            return best[:MAX_JOBS_PER_PAGE]

        # Conservative fallback only when we still have clear job-like signals.
        if fallback and self._job_signal_count(fallback) >= 2:
            return fallback[:MAX_JOBS_PER_PAGE]

        return None

    def _passes_jobset_validation(self, jobs: list[dict], page_url: str) -> bool:
        """Reject low-quality sets while preserving small real-job pages."""
        if len(jobs) < MIN_JOBS_FOR_SUCCESS:
            return False

        titles = [self._normalize_title_v17(j.get("title", "")) for j in jobs]
        valid_titles = [t for t in titles if self._is_valid_title_v15(t)]
        if len(valid_titles) < MIN_JOBS_FOR_SUCCESS:
            return False

        unique_titles = len({t.lower() for t in valid_titles})
        if unique_titles < max(2, int(len(valid_titles) * 0.6)):
            return False

        reject_hits = sum(1 for t in valid_titles if _REJECT_TITLE_PATTERN_V17.match(t.lower()))
        if reject_hits >= max(1, int(len(valid_titles) * 0.4)):
            return False

        cta_hits = sum(
            1
            for t in valid_titles
            if _CTA_TITLE_PATTERN_V17.search(t) and not self._title_has_job_signal(t)
        )
        if cta_hits >= max(1, int(len(valid_titles) * 0.35)):
            return False

        job_signal_hits = self._job_signal_count(jobs)
        job_url_hits = sum(1 for j in jobs if self._is_job_like_url(j, page_url))

        if len(valid_titles) <= 3:
            return job_signal_hits >= 1 or job_url_hits >= 2

        # No-link sets are only credible when small and strongly title-driven.
        if job_url_hits == 0:
            if len(valid_titles) <= 4 and job_signal_hits >= 1:
                return True
            return len(valid_titles) <= 6 and job_signal_hits >= max(2, int(len(valid_titles) * 0.5))

        # Larger sets should contain a reasonable proportion of job-like URLs.
        if len(valid_titles) > 6 and job_url_hits < max(2, int(len(valid_titles) * 0.3)):
            return False

        return (
            job_signal_hits >= max(1, int(len(valid_titles) * 0.25))
            or job_url_hits >= max(2, int(len(valid_titles) * 0.4))
        )

    def _jobset_score(self, jobs: list[dict], page_url: str) -> float:
        """Score a jobset by volume, title quality, and URL quality."""
        if not jobs:
            return 0.0

        count = len(jobs)
        job_signals = self._job_signal_count(jobs)
        linked_hits = sum(1 for j in jobs if self._is_job_like_url(j, page_url))
        unique_urls = len({(j.get("source_url") or "").strip() for j in jobs if j.get("source_url")})

        score = count * 4.0
        score += job_signals * 2.5
        score += linked_hits * 1.5
        score += min(unique_urls, count)

        # Penalize very weak tiny sets.
        if count <= 3 and job_signals == 0 and linked_hits == 0:
            score -= 8.0

        return score

    def _job_signal_count(self, jobs: list[dict]) -> int:
        return sum(1 for j in jobs if self._title_has_job_signal(j.get("title", "")))

    @staticmethod
    def _is_job_like_url(job: dict, page_url: str) -> bool:
        src = (job.get("source_url") or "").strip()
        if not src or src == page_url:
            return False

        parsed = urlparse(src)
        path = (parsed.path or "").lower()
        query = (parsed.query or "").lower()

        if _NON_JOB_URL_PATTERN_V17.search(path):
            return False

        if _JOB_URL_HINT_PATTERN_V17.search(path):
            return True

        if "search=" in query or ("job" in query and "id=" in query):
            return True

        # Common short-posting URL pattern (/p/<slug>) on hosted job boards.
        if re.search(r"/p/[^/]{4,}", path):
            return True

        return False

    @staticmethod
    def _container_bucket_key(el: etree._Element) -> str:
        tag = (el.tag or "").lower() if isinstance(el.tag, str) else "el"
        classes = _get_el_classes(el).split()
        if not classes:
            return tag

        important = [
            c for c in classes
            if any(k in c for k in ("job", "career", "vacan", "position", "opening", "listing", "search"))
        ]
        seed = important[0] if important else classes[0]
        return f"{tag}:{seed}"

    def _title_has_job_signal(self, title: str) -> bool:
        if not title:
            return False
        if _title_has_job_noun(title):
            return True
        return bool(_TITLE_HINT_PATTERN_V17.search(title))

    def _normalize_title_v17(self, title: str) -> str:
        """Normalize extracted titles by stripping appended metadata noise."""
        if not title:
            return ""

        t = " ".join(title.replace("\u00a0", " ").split())

        # Strip placeholder artifacts early.
        t = t.replace("%HEADER_", "").replace("%LABEL_", "")
        t = " ".join(t.split())

        # If title contains explicit metadata markers, keep pre-marker segment.
        meta_match = _TRAILING_META_SPLIT_V17.search(t)
        if meta_match and meta_match.start() > 6:
            t = t[:meta_match.start()].strip(" -|:\u2013")

        # Split CamelCase title+location concatenation when trailing segment looks like location.
        camel_match = _CAMEL_LOCATION_SPLIT_V17.match(t)
        if camel_match:
            left, pivot, right = camel_match.groups()
            if 3 <= len(right) <= 60 and not self._title_has_job_signal(right):
                t = f"{left}{pivot}".strip()

        # Handles "... )Vijayawada, India" style concatenation where the pivot isn't lowercase.
        tail_loc = re.match(r"^(.{4,120})([A-Z][A-Za-z\.-]+,\s*[A-Z][A-Za-z\.-]+)$", t)
        if tail_loc:
            left, right = tail_loc.groups()
            if self._title_has_job_signal(left) and not self._title_has_job_signal(right):
                t = left.strip()

        # Strip trailing "<Country/City, City>" style location suffix.
        loc_suffix = re.match(r"^(.{4,100})\s+([A-Z][A-Za-z\.\- ]+,\s*[A-Z][A-Za-z\.\- ]+)$", t)
        if loc_suffix:
            left, right = loc_suffix.groups()
            if (
                len(right.split()) <= 4
                and self._title_has_job_signal(left)
                and not self._title_has_job_signal(right)
            ):
                t = left.strip()

        # Normalize long multi-part strings by taking the first high-signal chunk.
        parts = re.split(r"\s{2,}|\n+|\t+|\s[\-|\u2013|\u2022]\s", t)
        for part in parts:
            part = part.strip()
            if not part:
                continue
            if len(part) < 4:
                continue
            # Prefer job-like segments over metadata fragments.
            if self._title_has_job_signal(part) or _is_valid_title(part):
                t = part
                break

        # Remove trailing punctuation and compress spaces once more.
        t = " ".join(t.strip(" |:-\u2013\u2022").split())

        return t

    def _dedupe_jobs(self, jobs: list[dict]) -> list[dict]:
        deduped: list[dict] = []
        seen: set[tuple[str, str]] = set()

        for job in jobs:
            title = self._normalize_title_v17(job.get("title", ""))
            if not self._is_valid_title_v15(title):
                continue

            source_url = (job.get("source_url") or "").strip()
            key = (title.lower(), source_url.lower())
            if key in seen:
                continue

            seen.add(key)
            cloned = dict(job)
            cloned["title"] = title
            deduped.append(cloned)

        return deduped[:MAX_JOBS_PER_PAGE]

    @staticmethod
    def _collect_ancestor_classes(el: etree._Element, depth: int = 4) -> str:
        classes: list[str] = []
        node = el
        for _ in range(depth):
            node = node.getparent()
            if node is None or not isinstance(node.tag, str):
                break
            cls = (node.get("class") or "").strip()
            if cls:
                classes.append(cls.lower())
        return " ".join(classes)

    @staticmethod
    def _base_domain(hostname: str) -> str:
        host = (hostname or "").lower()
        if host.startswith("www."):
            host = host[4:]
        parts = host.split(".")
        if len(parts) >= 2:
            return ".".join(parts[-2:])
        return host

    def _same_site(self, source_url: str, page_url: str) -> bool:
        try:
            src_host = urlparse(source_url).hostname or ""
            page_host = urlparse(page_url).hostname or ""
        except Exception:
            return False
        return self._base_domain(src_host) == self._base_domain(page_host)
