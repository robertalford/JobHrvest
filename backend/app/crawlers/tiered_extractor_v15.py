"""
Tiered Extraction Engine v1.5 — extends v1.4 with:

1. Job title vocabulary recognition (~250 common job-title nouns + signal words)
2. Smarter SPA detection: pages with >5KB HTML but no job vocabulary in body text
3. Job-title noun scoring for Tier 2 container scoring (+8/+12 bonus)
4. "Apply" button counting as container scoring signal (+10)
5. Title validation: reject phone numbers, mostly-numeric, pipe separators, single words
6. Container tiebreaker: prefer larger containers when scores are equal

Based on analysis of sites like mgcars.com where job titles ("Scaffolder", "Mechanic",
"Driver") appear without any CSS class signals — vocabulary-based scoring catches these.
"""

import logging
import re
from typing import Optional

from lxml import etree

from app.crawlers.tiered_extractor_v14 import TieredExtractorV14
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
# v1.5: Job title vocabulary
# ---------------------------------------------------------------------------

_JOB_TITLE_NOUNS = {
    "accountant", "administrator", "advisor", "analyst", "apprentice", "architect",
    "assistant", "associate", "auditor", "barista", "bookkeeper", "broker", "builder",
    "butcher", "buyer", "carpenter", "cashier", "carer", "chef", "cleaner", "clerk",
    "coach", "consultant", "coordinator", "counsellor", "custodian", "dentist",
    "designer", "developer", "director", "dispatcher", "doctor", "driver", "economist",
    "editor", "electrician", "engineer", "estimator", "executive", "fabricator",
    "facilitator", "farmer", "fitter", "foreman", "gardener", "geologist", "glazier",
    "guard", "hairdresser", "handler", "helper", "hygienist", "inspector", "instructor",
    "intern", "investigator", "janitor", "journalist", "labourer", "lawyer", "lecturer",
    "librarian", "linesman", "loader", "locksmith", "machinist", "manager", "mechanic",
    "mediator", "merchandiser", "midwife", "miner", "navigator", "negotiator", "nurse",
    "nutritionist", "officer", "operator", "optometrist", "painter", "paralegal",
    "paramedic", "pathologist", "pharmacist", "photographer", "physiotherapist", "pilot",
    "planner", "plumber", "porter", "president", "principal", "processor", "producer",
    "programmer", "receptionist", "recruiter", "registrar", "representative", "researcher",
    "scaffolder", "scientist", "secretary", "server", "solicitor", "specialist",
    "strategist", "superintendent", "supervisor", "surgeon", "surveyor", "teacher",
    "technician", "technologist", "therapist", "trader", "trainer", "treasurer", "tutor",
    "underwriter", "veterinarian", "waiter", "welder", "worker", "writer",
}

_JOB_SIGNAL_WORDS = {
    "apply", "position", "vacancy", "opening", "role", "opportunity", "career", "job",
    "employment", "hire", "hiring", "recruit", "recruitment", "resume", "salary",
    "remuneration", "compensation", "qualifications", "requirements", "full-time",
    "part-time", "contract", "casual", "permanent", "temporary", "closing", "deadline",
}

# Combined set for quick lookups
_ALL_JOB_VOCAB = _JOB_TITLE_NOUNS | _JOB_SIGNAL_WORDS

# Pattern for career/job keywords in URLs
_CAREER_URL_KEYWORDS = re.compile(
    r"career|jobs?|vacanc|hiring|recruit|employment|openings|opportunities",
    re.IGNORECASE,
)

# Title rejection patterns (v1.5 Fix #4)
_PHONE_PATTERN = re.compile(r"^[\d\s\-\+\(\)\.]{7,}$")
_MOSTLY_NUMERIC = re.compile(r"^[\d\s\-\.\,\#\:\/]{4,}$")
_PIPE_SEPARATOR = re.compile(r"\|")
_SINGLE_COMMON_WORDS = {
    "home", "menu", "search", "close", "back", "next", "previous",
    "login", "register", "contact", "about", "help", "faq", "blog",
    "news", "events", "services", "products", "team", "staff",
    "privacy", "terms", "sitemap", "subscribe", "share", "print",
    "email", "phone", "address", "map", "directions", "gallery",
    "portfolio", "resources", "support", "download", "upload",
}

# Apply button text patterns
_APPLY_PATTERN = re.compile(
    r"^\s*apply\s*(?:now|here|today|online)?\s*$",
    re.IGNORECASE,
)


def _count_job_vocab_in_text(text: str) -> int:
    """Count how many job vocabulary words appear in the given text."""
    words = set(re.findall(r"[a-z]+", text.lower()))
    return len(words & _ALL_JOB_VOCAB)


def _count_title_nouns_in_text(text: str) -> int:
    """Count how many job title nouns appear in the given text."""
    words = set(re.findall(r"[a-z]+", text.lower()))
    return len(words & _JOB_TITLE_NOUNS)


class TieredExtractorV15(TieredExtractorV14):
    """v1.5 — vocabulary scoring, smarter SPA detection, title validation, apply buttons."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        """Extract with v1.5 improvements: smarter SPA detection, vocab scoring."""
        url = career_page.url if hasattr(career_page, "url") else str(career_page)

        # v1.5 Fix #2: Smarter SPA detection
        is_spa = _detect_spa(html)
        if not is_spa:
            is_spa = self._is_js_rendered(html, url)

        if is_spa:
            rendered = await self._render_with_playwright_v13(url)
            if rendered and len(rendered) > len(html):
                logger.info("v1.5 Playwright rendered %s (%d → %d bytes)", url, len(html), len(rendered))
                html = rendered
            else:
                logger.info("v1.5 SPA detected but Playwright unavailable for %s", url)

        # Tier 1: ATS templates (v1.2 extended set)
        tier1 = self._extract_tier1_v12(url, html)
        if tier1 and len(tier1) >= MIN_JOBS_FOR_SUCCESS:
            return tier1[:MAX_JOBS_PER_PAGE]

        # Tier 2: v1.5 expanded heuristic with vocab scoring
        tier2 = self._extract_tier2_v15(url, html)
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
    # Fix #2: Smarter SPA detection
    # ------------------------------------------------------------------

    @staticmethod
    def _is_js_rendered(html: str, url: str = "") -> bool:
        """Detect JS-rendered pages that standard _detect_spa misses.

        A page with >5KB HTML but ZERO job vocabulary in the body text,
        AND a career/job keyword in the URL, is likely JS-rendered content
        that hasn't been executed yet.
        """
        if len(html) < 5000:
            return False

        # Only apply this heuristic when the URL suggests a careers page
        if url and not _CAREER_URL_KEYWORDS.search(url):
            return False

        # Extract body text
        try:
            root = _parse_html(html)
            if root is None:
                return False
            body_els = root.xpath("//body")
            if not body_els:
                return False
            body_text = etree.tostring(body_els[0], method="text", encoding="unicode").strip()
        except Exception:
            return False

        # If body text is very short relative to HTML size, it's likely JS-rendered
        if len(body_text) < 200 and len(html) > 5000:
            return True

        # Check for any job vocabulary in the body text
        title_noun_count = _count_title_nouns_in_text(body_text)
        signal_count = len(set(re.findall(r"[a-z]+", body_text.lower())) & _JOB_SIGNAL_WORDS)

        if title_noun_count == 0 and signal_count == 0:
            # >5KB HTML, career URL, but zero job words → likely JS shell
            return True

        return False

    # ------------------------------------------------------------------
    # Tier 2 v1.5: Vocabulary-enhanced scoring
    # ------------------------------------------------------------------

    def _extract_tier2_v15(self, url: str, html: str) -> Optional[list[dict]]:
        """Tier 2 v1.5 with vocabulary scoring, apply buttons, title validation."""
        root = _parse_html(html)
        if root is None:
            return None

        is_elementor = "elementor" in html[:5000].lower()

        candidates = self._score_containers_v15(root, url, is_elementor)
        if not candidates:
            return None

        # Fix #6: Sort by score descending, then by child count (tiebreaker)
        candidates.sort(key=lambda c: (c[1], c[2]), reverse=True)

        for container_el, score, _child_count in candidates[:3]:
            jobs = self._extract_jobs_v15(container_el, url, score)
            if jobs and len(jobs) >= MIN_JOBS_FOR_SUCCESS:
                return jobs

        return None

    def _score_containers_v15(
        self,
        root: etree._Element,
        page_url: str,
        is_elementor: bool,
    ) -> list[tuple[etree._Element, int, int]]:
        """Container scoring with all v1.3 improvements + v1.5 vocabulary and apply buttons.

        Returns list of (element, score, child_count) tuples.
        """
        candidates: list[tuple[etree._Element, int, int]] = []

        for el in root.iter():
            if not isinstance(el.tag, str):
                continue
            tag = el.tag.lower()
            if tag in ("script", "style", "head", "meta", "link", "noscript", "svg"):
                continue

            score = 0
            classes = _get_el_classes(el)
            el_id = (el.get("id") or "").lower()
            combined = f"{classes} {el_id}"

            # v1.3: Expanded job keyword scoring (+10)
            if _JOB_CLASS_PATTERN_V13.search(combined):
                score += 10

            children = [c for c in el if isinstance(c.tag, str) and c.tag.lower()
                        not in ("script", "style", "br", "hr")]

            if len(children) < 2:
                continue

            num_children = len(children)

            # v1.3 Change 1: <article> tag bonus (+4)
            if tag == "article":
                score += 4
            article_children = sum(1 for c in children if isinstance(c.tag, str) and c.tag.lower() == "article")
            if article_children >= 3:
                score += 6

            # Repeating structure (+5)
            if tag in ("ul", "ol", "table", "div", "section", "main", "tbody", "article", "nav"):
                if len(children) > 3:
                    sigs = [_child_signature(c) for c in children]
                    if sigs:
                        most_common = max(sigs.count(s) for s in set(sigs))
                        if most_common >= 3:
                            score += 5

            # Job URL patterns in children (+3)
            job_url_count = 0
            for child in children:
                for a_tag in child.iter("a"):
                    href = a_tag.get("href", "")
                    if _JOB_URL_PATTERN.search(href):
                        job_url_count += 1
                        break
            if job_url_count >= 3:
                score += 3

            # Structural similarity (+3)
            if len(children) > 3:
                sigs = [_child_signature(c) for c in children]
                if sigs:
                    most_common = max(sigs.count(s) for s in set(sigs))
                    if most_common > 3:
                        score += 3

            # v1.2: Link density scoring (+8)
            short_link_count = 0
            for child in children:
                for a_tag in child.iter("a"):
                    a_text = _text(a_tag)
                    if 10 <= len(a_text) <= 100 and _is_valid_title(a_text):
                        short_link_count += 1
                        break
            if short_link_count >= 5:
                score += 8
            elif short_link_count >= 3:
                score += 4

            # v1.2: Bootstrap layout (+6)
            if "row" in classes and tag == "div":
                col_children = [c for c in children if "col-" in _get_el_classes(c)]
                if len(col_children) >= 3:
                    cols_with_links = sum(
                        1 for c in col_children
                        if any(a.get("href") for a in c.iter("a"))
                    )
                    if cols_with_links >= 3:
                        score += 6

            # v1.2: Row class keyword bonus (+3)
            children_with_class = sum(
                1 for c in children
                if _ROW_CLASS_PATTERN_V13.search(_get_el_classes(c))
            )
            if children_with_class >= 3:
                score += 3

            # v1.3 Change 3: Anchor-as-row detection (+10)
            if tag in ("div", "ul", "ol", "nav", "section", "main"):
                a_children = [c for c in children
                              if isinstance(c.tag, str) and c.tag.lower() == "a"]
                if len(a_children) >= 3:
                    valid_link_children = sum(
                        1 for a in a_children
                        if _is_valid_title(_text(a)) and a.get("href")
                    )
                    if valid_link_children >= 3:
                        score += 10

            # v1.3 Change 4: Elementor site handling
            if is_elementor and score >= 5:
                career_link_count = 0
                for child in children:
                    for a_tag in child.iter("a"):
                        href = (a_tag.get("href") or "").lower()
                        if any(kw in href for kw in ("/career", "/job", "/apply", "/position", "/vacanc")):
                            career_link_count += 1
                            break
                if career_link_count >= 3:
                    score += 5

            # ── v1.5 Fix #3: Job-title noun scoring ──
            # Count children whose text contains words from _JOB_TITLE_NOUNS
            children_with_title_nouns = 0
            for child in children:
                child_text = _text(child)
                if child_text and _count_title_nouns_in_text(child_text) > 0:
                    children_with_title_nouns += 1

            if children_with_title_nouns >= 5:
                score += 12
            elif children_with_title_nouns >= 3:
                score += 8

            # ── v1.5 Fix #4: "Apply" button counting ──
            apply_button_count = 0
            for child in children:
                for sub_el in child.iter():
                    if not isinstance(sub_el.tag, str):
                        continue
                    sub_tag = sub_el.tag.lower()
                    if sub_tag in ("a", "button", "input"):
                        sub_text = _text(sub_el)
                        if _APPLY_PATTERN.match(sub_text):
                            apply_button_count += 1
                            break  # one per child
            if apply_button_count >= 3:
                score += 10

            if score >= 5:
                candidates.append((el, score, num_children))

        # Table heuristic (from v1.1)
        for table_el in root.iter("table"):
            if any(table_el is c for c, _, _ in candidates):
                continue
            tbody = table_el.find("tbody")
            target = tbody if tbody is not None else table_el
            trs = [c for c in target if isinstance(c.tag, str) and c.tag.lower() == "tr"]
            if len(trs) < 3:
                continue
            job_like_rows = 0
            for tr in trs:
                tr_text = _text(tr)
                anchors = [a for a in tr.iter("a") if a.get("href")]
                if 10 < len(tr_text) < 300 and anchors:
                    job_like_rows += 1
            if job_like_rows >= 3:
                s = 4 + min(job_like_rows, 10)
                candidates.append((target, s, len(trs)))

        return candidates

    # ------------------------------------------------------------------
    # Job extraction with v1.5 title validation
    # ------------------------------------------------------------------

    def _extract_jobs_v15(
        self, container: etree._Element, base_url: str, container_score: int
    ) -> list[dict]:
        """Extract jobs with v1.5 improvements — title validation, apply-button row boundaries."""
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
                if not self._is_valid_title_v15(title):
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
                    "extraction_method": "tier2_heuristic_v15",
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
            job = self._extract_heuristic_job_v15(row, base_url, container_score)
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

    def _extract_heuristic_job_v15(
        self, row: etree._Element, base_url: str, container_score: int
    ) -> Optional[dict]:
        """Extract a single job with v1.5 title validation."""
        job = self._extract_heuristic_job(row, base_url, container_score)
        if job:
            # v1.5: Validate the title
            if not self._is_valid_title_v15(job["title"]):
                return None
            job["extraction_method"] = "tier2_heuristic_v15"
        return job

    # ------------------------------------------------------------------
    # Fix #5: Title validation
    # ------------------------------------------------------------------

    @staticmethod
    def _is_valid_title_v15(title: str) -> bool:
        """Enhanced title validation — rejects phone numbers, numeric strings,
        pipe separators, and single common navigation words."""
        if not title or not _is_valid_title(title):
            return False

        stripped = title.strip()

        # Reject phone numbers: strings that are mostly digits with dashes/spaces/parens
        if _PHONE_PATTERN.match(stripped):
            return False

        # Reject mostly-numeric strings
        if _MOSTLY_NUMERIC.match(stripped):
            return False

        # Reject strings with pipe separators (likely breadcrumbs or nav)
        if _PIPE_SEPARATOR.search(stripped):
            return False

        # Reject single common navigation/UI words
        if stripped.lower() in _SINGLE_COMMON_WORDS:
            return False

        # Reject very short titles that are just one common word
        words = stripped.split()
        if len(words) == 1 and len(stripped) < 15:
            # Single short word — only accept if it looks like a job title noun
            if stripped.lower() not in _JOB_TITLE_NOUNS:
                return False

        return True
