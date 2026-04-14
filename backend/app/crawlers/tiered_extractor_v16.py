"""
Tiered Extraction Engine v1.6 — extends v1.5 with:

1. Apply-button container matching: if a container's child count ≈ the page-wide
   apply button count, score +20 (catches sites like Dome Cafe with 51 Apply buttons
   matching 51 job items)
2. Expanded title rejection: reject form/UI messages, navigation labels, generic
   business terms, and news-style titles
3. Post-extraction title vocabulary validation: after extracting jobs from a container,
   verify ≥30% of titles contain a job-title noun; if not, try the next container
   (catches garbage extraction like "Business Model", "united-domains")
"""

import logging
import re
from typing import Optional

from lxml import etree

from app.crawlers.tiered_extractor_v15 import (
    TieredExtractorV15,
    _JOB_TITLE_NOUNS,
    _count_title_nouns_in_text,
    _APPLY_PATTERN,
)
from app.crawlers.tiered_extractor import (
    _parse_html, _text, _href, _resolve_url, _get_el_classes,
    _child_signature, _is_valid_title, _detect_spa,
    _JOB_URL_PATTERN, _TITLE_CLASS_PATTERN, _LOCATION_CLASS_PATTERN,
    _SALARY_CLASS_PATTERN, _TYPE_CLASS_PATTERN,
    _AU_LOCATIONS, _SALARY_PATTERN, _JOB_TYPE_PATTERN,
    MAX_JOBS_PER_PAGE, MIN_JOBS_FOR_SUCCESS,
)
from app.crawlers.tiered_extractor_v13 import (
    _JOB_CLASS_PATTERN_V13, _ROW_CLASS_PATTERN_V13,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# v1.6 Fix #2: Expanded title rejection patterns
# ---------------------------------------------------------------------------

_REJECT_PATTERNS_V16 = [
    # Form/UI messages
    re.compile(r"application.*sent", re.IGNORECASE),
    re.compile(r"error\s+occurred", re.IGNORECASE),
    re.compile(r"please\s+try\s+again", re.IGNORECASE),
    re.compile(r"successfully\s+(?:submitted|applied|sent)", re.IGNORECASE),
    re.compile(r"thank\s+you\s+for", re.IGNORECASE),
    # Navigation labels
    re.compile(r"^job\s+alerts?$", re.IGNORECASE),
    re.compile(r"^manage\s+applications?$", re.IGNORECASE),
    re.compile(r"^login$", re.IGNORECASE),
    re.compile(r"^register$", re.IGNORECASE),
    re.compile(r"^sign\s+up$", re.IGNORECASE),
    re.compile(r"^sign\s+in$", re.IGNORECASE),
    re.compile(r"^log\s+in$", re.IGNORECASE),
    re.compile(r"^log\s+out$", re.IGNORECASE),
    re.compile(r"^my\s+account$", re.IGNORECASE),
    re.compile(r"^my\s+profile$", re.IGNORECASE),
    re.compile(r"^saved\s+jobs?$", re.IGNORECASE),
    # Generic business terms
    re.compile(r"^business\s+model$", re.IGNORECASE),
    re.compile(r"^management$", re.IGNORECASE),
    re.compile(r"^our\s+team$", re.IGNORECASE),
    re.compile(r"^about\s+us$", re.IGNORECASE),
    re.compile(r"^contact$", re.IGNORECASE),
    re.compile(r"^home$", re.IGNORECASE),
    re.compile(r"^company\s+overview$", re.IGNORECASE),
    re.compile(r"^our\s+(?:values|culture|mission|vision|story)$", re.IGNORECASE),
    # "View more" / "Read more" style titles
    re.compile(r"^view\s+more", re.IGNORECASE),
    re.compile(r"^read\s+more$", re.IGNORECASE),
    re.compile(r"^learn\s+more$", re.IGNORECASE),
    re.compile(r"^see\s+all$", re.IGNORECASE),
    re.compile(r"^show\s+more$", re.IGNORECASE),
    re.compile(r"^load\s+more$", re.IGNORECASE),
    # Date-prefixed titles (news articles, not jobs)
    re.compile(r"^\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{2,4}\s", re.IGNORECASE),
    re.compile(r"^\d{4}[/\-\.]\d{1,2}[/\-\.]\d{1,2}\s", re.IGNORECASE),
    re.compile(r"^(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*\s+\d{1,2}", re.IGNORECASE),
]


def _title_has_job_noun(title: str) -> bool:
    """Check if a title contains at least one word from the job title noun vocabulary."""
    if not title:
        return False
    words = set(re.findall(r"[a-z]+", title.lower()))
    return bool(words & _JOB_TITLE_NOUNS)


class TieredExtractorV16(TieredExtractorV15):
    """v1.6 — apply-button container matching, expanded title rejection, vocab validation."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        """Extract with v1.6 improvements."""
        url = career_page.url if hasattr(career_page, "url") else str(career_page)

        # v1.5 Fix #2: Smarter SPA detection
        is_spa = _detect_spa(html)
        if not is_spa:
            is_spa = self._is_js_rendered(html, url)

        if is_spa:
            rendered = await self._render_with_playwright_v13(url)
            if rendered and len(rendered) > len(html):
                logger.info("v1.6 Playwright rendered %s (%d → %d bytes)", url, len(html), len(rendered))
                html = rendered
            else:
                logger.info("v1.6 SPA detected but Playwright unavailable for %s", url)

        # Tier 1: ATS templates (v1.2 extended set)
        tier1 = self._extract_tier1_v12(url, html)
        if tier1 and len(tier1) >= MIN_JOBS_FOR_SUCCESS:
            return tier1[:MAX_JOBS_PER_PAGE]

        # Tier 2: v1.6 expanded heuristic with apply-button matching + vocab validation
        tier2 = self._extract_tier2_v16(url, html)
        if tier2 and len(tier2) >= MIN_JOBS_FOR_SUCCESS:
            # Phase 2: Pagination
            tier2 = await self._follow_pagination(url, html, tier2, career_page, company)
            # Phase 3: Detail page enrichment
            tier2 = await self._enrich_from_detail_pages(tier2)
            return tier2[:MAX_JOBS_PER_PAGE]

        # Tier 3: LLM (still deferred)
        tier3 = self._extract_tier3_llm(url, html)
        if tier3 and len(tier3) >= MIN_JOBS_FOR_SUCCESS:
            return tier3[:MAX_JOBS_PER_PAGE]

        for partial in (tier1, tier2):
            if partial:
                return partial[:MAX_JOBS_PER_PAGE]

        return []

    # ------------------------------------------------------------------
    # Tier 2 v1.6: Apply-button container matching + vocab validation
    # ------------------------------------------------------------------

    def _extract_tier2_v16(self, url: str, html: str) -> Optional[list[dict]]:
        """Tier 2 v1.6 with apply-button container matching and post-extraction
        vocabulary validation."""
        root = _parse_html(html)
        if root is None:
            return None

        is_elementor = "elementor" in html[:5000].lower()

        # Fix 1: Pre-count apply buttons on the entire page
        page_apply_count = self._count_page_apply_buttons(root)
        logger.debug("v1.6 page-wide apply button count: %d", page_apply_count)

        candidates = self._score_containers_v16(root, url, is_elementor, page_apply_count)
        if not candidates:
            return None

        # Sort by score descending, then by child count (tiebreaker)
        candidates.sort(key=lambda c: (c[1], c[2]), reverse=True)

        # Fix 6 (v1.6): Try more candidates with vocabulary validation
        for container_el, score, _child_count in candidates[:5]:
            jobs = self._extract_jobs_v16(container_el, url, score)
            if jobs and len(jobs) >= MIN_JOBS_FOR_SUCCESS:
                # Fix 3 (v1.6): Post-extraction vocabulary validation
                noun_hits = sum(1 for j in jobs if _title_has_job_noun(j["title"]))
                if noun_hits >= len(jobs) * 0.3 or len(jobs) <= 3:
                    return jobs
                else:
                    logger.info(
                        "v1.6 vocab validation failed: %d/%d titles have job nouns "
                        "(container score=%d, children=%d) — trying next container",
                        noun_hits, len(jobs), score, _child_count,
                    )
                    continue

        return None

    @staticmethod
    def _count_page_apply_buttons(root: etree._Element) -> int:
        """Count all Apply/Apply Now/Apply Here elements on the entire page."""
        count = 0
        for el in root.iter():
            if not isinstance(el.tag, str):
                continue
            tag = el.tag.lower()
            if tag in ("a", "button", "input"):
                text = _text(el)
                if _APPLY_PATTERN.match(text):
                    count += 1
        return count

    def _score_containers_v16(
        self,
        root: etree._Element,
        page_url: str,
        is_elementor: bool,
        page_apply_count: int,
    ) -> list[tuple[etree._Element, int, int]]:
        """Container scoring with all v1.5 improvements + v1.6 apply-button container matching.

        Returns list of (element, score, child_count) tuples.
        """
        # Get the base v1.5 candidates first
        candidates_v15 = self._score_containers_v15(root, page_url, is_elementor)

        # Re-score with the v1.6 apply-button container matching signal
        candidates: list[tuple[etree._Element, int, int]] = []

        for el, base_score, child_count in candidates_v15:
            score = base_score

            # ── v1.6 Fix #1: Apply-button container matching ──
            # If the number of children ≈ the page-wide apply button count,
            # this container likely holds all the job items
            if page_apply_count >= 3:
                tolerance = max(3, int(page_apply_count * 0.2))
                if abs(child_count - page_apply_count) <= tolerance:
                    score += 20
                    logger.debug(
                        "v1.6 apply-button match: container children=%d, "
                        "page apply buttons=%d (tolerance=%d) → +20",
                        child_count, page_apply_count, tolerance,
                    )

            candidates.append((el, score, child_count))

        # Also check for containers not in v1.5 candidates that match apply-button count
        # This handles edge cases where the container didn't score >= 5 in v1.5
        if page_apply_count >= 5:
            v15_els = {id(el) for el, _, _ in candidates}
            for el in root.iter():
                if id(el) in v15_els:
                    continue
                if not isinstance(el.tag, str):
                    continue
                tag = el.tag.lower()
                if tag in ("script", "style", "head", "meta", "link", "noscript", "svg"):
                    continue

                children = [c for c in el if isinstance(c.tag, str) and c.tag.lower()
                            not in ("script", "style", "br", "hr")]
                if len(children) < 3:
                    continue

                num_children = len(children)
                tolerance = max(3, int(page_apply_count * 0.2))
                if abs(num_children - page_apply_count) <= tolerance:
                    # This container matches the apply-button count — score it
                    score = 20  # Base score from apply-button match alone
                    # Add basic structural checks
                    sigs = [_child_signature(c) for c in children]
                    if sigs:
                        most_common = max(sigs.count(s) for s in set(sigs))
                        if most_common >= 3:
                            score += 5
                    candidates.append((el, score, num_children))

        return candidates

    # ------------------------------------------------------------------
    # Job extraction with v1.6 title validation
    # ------------------------------------------------------------------

    def _extract_jobs_v16(
        self, container: etree._Element, base_url: str, container_score: int
    ) -> list[dict]:
        """Extract jobs with v1.6 improvements — enhanced title validation."""
        children = [
            c for c in container
            if isinstance(c.tag, str)
            and c.tag.lower() not in ("script", "style", "br", "hr", "thead")
        ]
        if not children:
            return []

        # v1.3 anchor-as-row detection
        a_children = [c for c in children if isinstance(c.tag, str) and c.tag.lower() == "a"]
        if len(a_children) >= 3 and len(a_children) >= len(children) * 0.5:
            jobs = []
            for a_el in a_children:
                title = _text(a_el)
                if not self._is_valid_title_v16(title):
                    continue
                href = a_el.get("href")
                source_url = _resolve_url(href, base_url) or base_url

                location = None
                for sub in a_el.iter():
                    if not isinstance(sub.tag, str):
                        continue
                    cls = _get_el_classes(sub)
                    if _LOCATION_CLASS_PATTERN.search(cls):
                        loc = _text(sub)
                        if loc and 2 < len(loc) < 200 and loc != title:
                            location = loc
                            break

                jobs.append({
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "salary_raw": None,
                    "employment_type": None,
                    "description": None,
                    "extraction_method": "tier2_heuristic_v16",
                    "extraction_confidence": min(0.5 + container_score * 0.02, 0.85),
                })
                if len(jobs) >= MAX_JOBS_PER_PAGE:
                    break
            if len(jobs) >= MIN_JOBS_FOR_SUCCESS:
                return jobs

        # Standard repeating-block extraction
        sigs = [_child_signature(c) for c in children]
        sig_counts: dict[str, int] = {}
        for s in sigs:
            sig_counts[s] = sig_counts.get(s, 0) + 1

        best_sig = max(sig_counts, key=sig_counts.get) if sig_counts else ""
        rows = [c for c, s in zip(children, sigs) if s == best_sig]

        if len(rows) < MIN_JOBS_FOR_SUCCESS:
            rows = children

        jobs: list[dict] = []
        for row in rows:
            job = self._extract_heuristic_job_v16(row, base_url, container_score)
            if job:
                jobs.append(job)
            if len(jobs) >= MAX_JOBS_PER_PAGE:
                break

        # v1.3 confidence calibration
        if jobs and len(rows) > 0:
            extraction_ratio = len(jobs) / len(rows)
            if extraction_ratio < 0.3:
                for j in jobs:
                    j["extraction_confidence"] = max(0.3, j["extraction_confidence"] - 0.15)

        return jobs

    def _extract_heuristic_job_v16(
        self, row: etree._Element, base_url: str, container_score: int
    ) -> Optional[dict]:
        """Extract a single job with v1.6 title validation."""
        job = self._extract_heuristic_job(row, base_url, container_score)
        if job:
            # v1.6: Validate the title with expanded rejection
            if not self._is_valid_title_v16(job["title"]):
                return None
            job["extraction_method"] = "tier2_heuristic_v16"
        return job

    # ------------------------------------------------------------------
    # Fix #2: Expanded title validation
    # ------------------------------------------------------------------

    @staticmethod
    def _is_valid_title_v16(title: str) -> bool:
        """Enhanced title validation — includes all v1.5 checks plus expanded
        rejection patterns for form messages, navigation, business terms, and dates."""
        # Run v1.5 validation first
        if not TieredExtractorV15._is_valid_title_v15(title):
            return False

        stripped = title.strip()

        # v1.6: Check against expanded rejection patterns
        for pattern in _REJECT_PATTERNS_V16:
            if pattern.search(stripped):
                return False

        return True
