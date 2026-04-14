"""
Tiered Extraction Engine — 3-tier hybrid approach for job listing extraction.

Tries tiers in order, stopping at first success:
  Tier 1: ATS template library (hardcoded per-platform selectors) — deterministic, fast
  Tier 2: Heuristic structural analysis (pattern matching from 48K wrapper data)
  Tier 3: LLM-assisted extraction (Ollama) — deferred for later wiring

Each tier returns a list of job dicts or None (meaning "no result, try next tier").
"""

import logging
import re
from typing import Optional
from urllib.parse import urljoin, urlparse

from lxml import etree

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAX_JOBS_PER_PAGE = 500
MIN_JOBS_FOR_SUCCESS = 2

# AU location keywords for heuristic matching
_AU_LOCATIONS = re.compile(
    r"\b(?:Sydney|Melbourne|Brisbane|Perth|Adelaide|Hobart|Darwin|Canberra|"
    r"Gold Coast|Newcastle|Wollongong|Geelong|Cairns|Townsville|Toowoomba|"
    r"Ballarat|Bendigo|Launceston|Mackay|Rockhampton|Bundaberg|"
    r"NSW|VIC|QLD|WA|SA|TAS|NT|ACT|"
    r"New South Wales|Victoria|Queensland|Western Australia|South Australia|"
    r"Tasmania|Northern Territory|Australian Capital Territory|"
    r"Australia|Remote)\b",
    re.IGNORECASE,
)

_SALARY_PATTERN = re.compile(
    r"\$\s?\d[\d,]*(?:\.\d{2})?(?:\s*[-–]\s*\$?\s*\d[\d,]*(?:\.\d{2})?)?"
    r"(?:\s*(?:per|p\.?a\.?|pa|/?\s*(?:year|annum|hour|hr|day|week|month)))?",
    re.IGNORECASE,
)

_JOB_TYPE_PATTERN = re.compile(
    r"\b(?:Full[\s-]?time|Part[\s-]?time|Contract|Casual|Temporary|"
    r"Permanent|Fixed[\s-]?term|Freelance|Internship)\b",
    re.IGNORECASE,
)

_JOB_URL_PATTERN = re.compile(
    r"/(?:job|jobs|career|careers|position|positions|vacancy|vacancies|opening|openings|role|roles)/",
    re.IGNORECASE,
)

_JOB_CLASS_PATTERN = re.compile(
    r"job|career|vacanc|position|listing|posting|opening",
    re.IGNORECASE,
)

_TITLE_CLASS_PATTERN = re.compile(r"title|heading|name", re.IGNORECASE)
_LOCATION_CLASS_PATTERN = re.compile(r"location|city|region|office", re.IGNORECASE)
_SALARY_CLASS_PATTERN = re.compile(r"salary|pay|remuneration|compensation", re.IGNORECASE)
_TYPE_CLASS_PATTERN = re.compile(r"type|contract|employment|tenure|commitment", re.IGNORECASE)

# ---------------------------------------------------------------------------
# ATS Templates — hardcoded selectors per platform
# ---------------------------------------------------------------------------

ATS_TEMPLATES: dict[str, dict] = {
    "lever": {
        "requires_js": False,
        "container": "div.postings-group",
        "boundary": "div.posting",
        "title": ".posting-title h5",
        "link": "a.posting-btn-submit",
        "location": ".posting-categories .sort-by-location, .location",
        "department": ".posting-categories .sort-by-team",
        "employment_type": ".posting-categories .sort-by-commitment",
    },
    "greenhouse": {
        "requires_js": False,
        "container": "table, div#content",
        "boundary": "tr.job-post, div.opening",
        "title": "td a, p.body--medium a, a.job-title",
        "link": None,  # title element IS the link
        "location": "td.location span, .location",
        "department": "td.department, .department",
    },
    "jazzhr": {
        "requires_js": False,
        "container": "ul.list-group",
        "boundary": "li.list-group-item",
        "title": "h4.list-group-item-heading a, a.list-group-item-heading",
        "link": None,  # title IS the link
        "location": ".list-group-item-text, .job-location",
    },
    "bamboohr": {
        "requires_js": True,
        "container": "main",
        "boundary_xpath": "//div[2]/main/div/div/div/div[1]/div[2]/section/div",
        "title_xpath": ".//div[1]/div/div/a",
        "link_xpath": ".//div[1]/div/div/a",
        "location_xpath": ".//div[1]/div/div[2]/span",
    },
    "workday": {
        "requires_js": True,
        "container": "ul",
        "boundary": "li.css-1q2dra3, li[class*='css-']",
        "title_xpath": ".//h3/a | .//h3",
        "link_xpath": ".//h3/a | .//a",
        "location": "dd.css-129m7dg, [data-automation-id='jobPostingLocation']",
    },
    "icims": {
        "requires_js": False,
        "container": "div.container-fluid",
        "boundary": "div.row, div.iCIMS_JobsTable div.row",
        "title": "div.title a, a.iCIMS_Anchor",
        "link": "a.iCIMS_Anchor, div.title a",
        "location": ".header .col-xs-6:nth-child(2), .location",
        "next_page_xpath": "//a[span[@title='Next page of results']]",
    },
    "smartrecruiters": {
        "requires_js": True,
        "container": "ul.jobs-list",
        "boundary_xpath": "//job[contains(@class, 'opening-job')]",
        "title": "a h4, h4",
        "link": "a",
        "location": ".job-location, .location-label",
    },
    "taleo": {
        "requires_js": True,
        "container": "tbody",
        "boundary": "tbody tr",
        "title": "th div span a, td a, .titlelink a",
        "link": "th div span a, td a",
        "location": "td:nth-child(2), .locationlink",
    },
    "livehire": {
        "requires_js": True,
        "container": "div#job-listings",
        "boundary_xpath": "//job-listing/parent::a",
        "title_xpath": ".//h3",
        "link": None,  # parent <a> IS the link
        "location_xpath": ".//span[contains(@class, 'location')]",
        "next_page": "button#show-more-button",
    },
    "applynow": {
        "requires_js": True,
        "container": "div#joblist",
        "boundary": "div.jobblock, tr",
        "title_xpath": ".//a[@class='job_title']",
        "link_xpath": ".//a[@class='job_title']",
        "location": ".location, td:nth-child(2)",
    },
    "pageup": {
        "requires_js": True,
        "container": "tbody#recent-jobs-content, div.job-list",
        "boundary": "tbody tr, div.job-item",
        "title": "td a.job-link, a.job-link, h3 a",
        "link": "td a.job-link, a.job-link, h3 a",
        "location": "td:nth-child(3), .location",
        "next_page": "a.more-link",
    },
    "jobvite": {
        "requires_js": False,
        "container": "table, tbody",
        "boundary": "tbody tr, tr.jv-job-list",
        "title": "td.jv-job-list-name a, a.jv-job-link",
        "link": "td.jv-job-list-name a, a.jv-job-link",
        "location": "td.jv-job-list-location, .location",
        "next_page_xpath": "//a[@class='jv-pagination-next']",
    },
    "teamtailor": {
        "requires_js": True,
        "container": ".jobs-list-container, div[class*='jobs']",
        "boundary": "li, .job-listing",
        "title": "a span, a",
        "link": "a",
        "location": ".location, [class*='location']",
        "next_page": "div#show_more_button a",
    },
    "ashby": {
        "requires_js": True,
        "container": "div.ashby-job-posting-brief-list",
        "boundary_xpath": ".//div/h2/following-sibling::div/a",
        "title_xpath": ".//h3",
        "link": None,  # boundary element IS an <a>
        "location": "[class*='location'], .department",
    },
}

# ATS URL detection patterns — order matters (more specific first)
_ATS_URL_PATTERNS: list[tuple[str, list[str]]] = [
    ("greenhouse", ["greenhouse.io", "boards.greenhouse"]),
    ("lever", ["lever.co", "jobs.lever"]),
    ("workday", ["workday", "myworkdayjobs"]),
    ("icims", ["icims"]),
    ("bamboohr", ["bamboohr"]),
    ("smartrecruiters", ["smartrecruiters"]),
    ("taleo", ["taleo", "careersection"]),
    ("livehire", ["livehire"]),
    ("applynow", ["applynow"]),
    ("pageup", ["pageup"]),
    ("teamtailor", ["teamtailor"]),
    ("jobvite", ["jobvite"]),
    ("ashby", ["ashbyhq"]),
    ("jazzhr", ["applytojob", "theresumator"]),
]


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def _text(el: etree._Element) -> str:
    """Safely extract text content from an lxml element."""
    if el is None:
        return ""
    try:
        txt = el.text_content()
    except Exception:
        try:
            txt = etree.tostring(el, method="text", encoding="unicode")
        except Exception:
            return ""
    return (txt or "").strip()


def _href(el: etree._Element) -> Optional[str]:
    """Extract href from an element, or from its first child <a> if the element itself has none."""
    if el is None:
        return None
    href = el.get("href")
    if href:
        return href
    # Check child <a> tags
    for child_a in el.iter("a"):
        child_href = child_a.get("href")
        if child_href:
            return child_href
    return None


def _css_multi(root: etree._Element, selector_str: str) -> list[etree._Element]:
    """Try multiple comma-separated CSS selectors, return results from whichever matches.

    Falls back gracefully if cssselect raises on any individual selector.
    """
    results: list[etree._Element] = []
    selectors = [s.strip() for s in selector_str.split(",") if s.strip()]
    for sel in selectors:
        try:
            found = root.cssselect(sel)
            if found:
                results.extend(found)
        except Exception:
            continue
    return results


def _xpath_safe(root: etree._Element, expr: str) -> list[etree._Element]:
    """Run an XPath expression, returning an empty list on any error."""
    try:
        result = root.xpath(expr)
        if isinstance(result, list):
            return [r for r in result if isinstance(r, etree._Element)]
        return []
    except Exception:
        return []


def _first_text(root: etree._Element, selector_str: Optional[str] = None,
                xpath_str: Optional[str] = None) -> str:
    """Return text from the first matching element using CSS or XPath."""
    if xpath_str:
        elems = _xpath_safe(root, xpath_str)
        if elems:
            return _text(elems[0])
    if selector_str:
        elems = _css_multi(root, selector_str)
        if elems:
            return _text(elems[0])
    return ""


def _first_href(root: etree._Element, selector_str: Optional[str] = None,
                xpath_str: Optional[str] = None) -> Optional[str]:
    """Return href from the first matching element using CSS or XPath."""
    if xpath_str:
        elems = _xpath_safe(root, xpath_str)
        if elems:
            return _href(elems[0])
    if selector_str:
        elems = _css_multi(root, selector_str)
        if elems:
            return _href(elems[0])
    return None


def _parse_html(html: str) -> Optional[etree._Element]:
    """Parse HTML string into an lxml tree root, returning None on failure."""
    try:
        parser = etree.HTMLParser(encoding="utf-8")
        tree = etree.fromstring(html.encode("utf-8", errors="replace"), parser)
        return tree
    except Exception as exc:
        logger.debug("Failed to parse HTML with lxml: %s", exc)
        return None


_BOILERPLATE = {
    "apply now", "learn more", "view all", "see all", "load more",
    "next page", "previous", "cookie", "privacy", "terms",
    "sign in", "log in", "subscribe", "follow us", "read more",
    "show more", "see more", "about us", "contact us", "our team",
    "home", "menu", "search", "close", "back", "skip to",
    "join us now", "view openings", "come work with us",
}

_EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

# SPA template syntax signals (unrendered framework templates)
_SPA_TEMPLATE_PATTERN = re.compile(
    r"\{\{.*?\}\}|\[\[.*?\]\]|v-for=|v-if=|ng-repeat|ng-bind|\*ngFor"
)


def _is_valid_title(text: str) -> bool:
    """Check if extracted text looks like a valid job title (5-200 chars, no boilerplate)."""
    if not text or len(text) < 5 or len(text) > 200:
        return False
    lower = text.lower().strip()
    # Reject boilerplate nav/UI text
    if lower in _BOILERPLATE or any(bp in lower for bp in _BOILERPLATE):
        return False
    # Reject email addresses
    if _EMAIL_PATTERN.match(text.strip()):
        return False
    # Reject SPA template syntax
    if _SPA_TEMPLATE_PATTERN.search(text):
        return False
    # Reject text that's mostly non-alphanumeric
    alnum = sum(1 for c in text if c.isalnum() or c == ' ')
    if alnum < len(text) * 0.4:
        return False
    return True


def _detect_spa(html: str) -> bool:
    """Detect if HTML is an unrendered SPA shell that needs JS rendering."""
    # Check body text length — SPA shells have minimal visible content
    try:
        from lxml import etree
        parser = etree.HTMLParser(encoding="utf-8")
        tree = etree.fromstring(html.encode("utf-8", errors="replace"), parser)
        body_els = tree.xpath("//body")
        if body_els:
            body_text = etree.tostring(body_els[0], method="text", encoding="unicode").strip()
            # Very short body text = JS shell
            if len(body_text) < 100:
                return True
    except Exception:
        pass

    # Check for SPA framework markers in first 3KB
    head = html[:3000].lower()
    spa_signals = [
        'id="root"></div>',   # React
        'id="app"></div>',    # Vue
        'ng-app',             # Angular
        'data-reactroot',
        '__nuxt',
        '<app-root>',         # Angular
    ]
    return any(s in head for s in spa_signals)


def _resolve_url(href: Optional[str], base_url: str) -> Optional[str]:
    """Resolve a potentially relative URL against a base URL."""
    if not href:
        return None
    href = href.strip()
    if not href or href.startswith("#") or href.startswith("javascript:"):
        return None
    if href.startswith("mailto:") or href.startswith("tel:"):
        return None
    return urljoin(base_url, href)


def _get_el_classes(el: etree._Element) -> str:
    """Get all class names from an element as a single space-separated string."""
    return (el.get("class") or "").lower()


def _child_signature(el: etree._Element) -> str:
    """Build a structural signature for an element based on tag and class prefix."""
    tag = el.tag or ""
    cls = (el.get("class") or "").strip()
    # Take first class token for similarity comparison
    first_cls = cls.split()[0] if cls else ""
    return f"{tag}.{first_cls}"


# ---------------------------------------------------------------------------
# TieredExtractor
# ---------------------------------------------------------------------------


class TieredExtractor:
    """3-tier hybrid extraction engine for job listings.

    Tier 1: ATS template library — deterministic, hardcoded selectors
    Tier 2: Heuristic structural analysis — pattern-based scoring
    Tier 3: LLM-assisted extraction — deferred (returns None)
    """

    async def extract(self, career_page, company, html: str) -> list[dict]:
        """Extract job listings from HTML using the tiered approach.

        Args:
            career_page: CareerPage model instance (needs .url attribute).
            company: Company model instance (for context).
            html: Raw HTML content of the career page.

        Returns:
            List of job dicts, each with: title, source_url, location_raw,
            salary_raw, employment_type, description, extraction_method,
            extraction_confidence.
        """
        url = career_page.url if hasattr(career_page, "url") else str(career_page)

        # v1.1: Detect SPA shells that need JS rendering — skip extraction
        if _detect_spa(html):
            logger.info("SPA shell detected for %s — needs JS rendering", url)
            return []

        # Tier 1: ATS template extraction
        tier1_result = self._extract_tier1_ats(url, html)
        if tier1_result and len(tier1_result) >= MIN_JOBS_FOR_SUCCESS:
            logger.info(
                "Tier 1 (ATS) extracted %d jobs from %s", len(tier1_result), url
            )
            return tier1_result[:MAX_JOBS_PER_PAGE]

        # Tier 2: Heuristic structural analysis
        tier2_result = self._extract_tier2_heuristic(url, html)
        if tier2_result and len(tier2_result) >= MIN_JOBS_FOR_SUCCESS:
            logger.info(
                "Tier 2 (heuristic) extracted %d jobs from %s",
                len(tier2_result),
                url,
            )
            return tier2_result[:MAX_JOBS_PER_PAGE]

        # Tier 3: LLM-assisted (deferred)
        tier3_result = self._extract_tier3_llm(url, html)
        if tier3_result and len(tier3_result) >= MIN_JOBS_FOR_SUCCESS:
            logger.info(
                "Tier 3 (LLM) extracted %d jobs from %s", len(tier3_result), url
            )
            return tier3_result[:MAX_JOBS_PER_PAGE]

        # All tiers failed — return whatever partial results we have
        # Prefer tier1 partial > tier2 partial > empty
        for partial in (tier1_result, tier2_result):
            if partial:
                logger.info(
                    "All tiers below threshold; returning %d partial results from %s",
                    len(partial),
                    url,
                )
                return partial[:MAX_JOBS_PER_PAGE]

        logger.info("No jobs extracted from %s across all tiers", url)
        return []

    # ------------------------------------------------------------------
    # Tier 1: ATS Template Library
    # ------------------------------------------------------------------

    def _detect_ats(self, url: str) -> Optional[str]:
        """Detect ATS platform from URL patterns."""
        url_lower = url.lower()
        for platform, patterns in _ATS_URL_PATTERNS:
            for pat in patterns:
                if pat in url_lower:
                    return platform
        return None

    def _extract_tier1_ats(self, url: str, html: str) -> Optional[list[dict]]:
        """Tier 1: Apply hardcoded ATS template if URL matches a known platform."""
        ats = self._detect_ats(url)
        if not ats or ats not in ATS_TEMPLATES:
            return None

        template = ATS_TEMPLATES[ats]

        # If the template requires JS rendering and the HTML is suspiciously short
        # or lacks expected structural elements, signal that JS rendering may be needed
        # but still attempt extraction (the HTML may already be rendered).
        if template.get("requires_js") and len(html) < 2000:
            logger.debug(
                "ATS %s requires JS but HTML is short (%d bytes); attempting anyway",
                ats,
                len(html),
            )

        root = _parse_html(html)
        if root is None:
            return None

        # Find boundary elements (individual job rows)
        boundaries = self._find_ats_boundaries(root, template)
        if not boundaries:
            logger.debug("No boundary elements found for ATS %s at %s", ats, url)
            return None

        jobs: list[dict] = []
        for boundary_el in boundaries:
            job = self._extract_ats_job(boundary_el, template, url, ats)
            if job:
                jobs.append(job)
            if len(jobs) >= MAX_JOBS_PER_PAGE:
                break

        return jobs if jobs else None

    def _find_ats_boundaries(
        self, root: etree._Element, template: dict
    ) -> list[etree._Element]:
        """Find repeating job row elements using ATS template config."""
        # Try XPath boundary first
        if "boundary_xpath" in template:
            results = _xpath_safe(root, template["boundary_xpath"])
            if results:
                return results

        # Try CSS boundary
        if "boundary" in template:
            results = _css_multi(root, template["boundary"])
            if results:
                return results

        # Fallback: find container, then get direct children
        if "container" in template:
            containers = _css_multi(root, template["container"])
            if containers:
                # Return all child elements of the first matching container
                return list(containers[0])

        return []

    def _extract_ats_job(
        self,
        el: etree._Element,
        template: dict,
        base_url: str,
        ats: str,
    ) -> Optional[dict]:
        """Extract a single job dict from a boundary element using ATS template."""
        # Extract title
        title = _first_text(
            el,
            selector_str=template.get("title"),
            xpath_str=template.get("title_xpath"),
        )
        if not _is_valid_title(title):
            return None

        # Extract link
        link_href: Optional[str] = None

        # If template says link is None, the title element or boundary IS the link
        if template.get("link") is None and template.get("link_xpath") is None:
            # Boundary or title element is the link
            link_href = _href(el)
            if not link_href:
                # Try title elements for href
                for sel_key in ("title", "title_xpath"):
                    sel = template.get(sel_key)
                    if sel:
                        if sel_key.endswith("_xpath"):
                            title_els = _xpath_safe(el, sel)
                        else:
                            title_els = _css_multi(el, sel)
                        for te in title_els:
                            h = _href(te)
                            if h:
                                link_href = h
                                break
                    if link_href:
                        break
        else:
            link_href = _first_href(
                el,
                selector_str=template.get("link"),
                xpath_str=template.get("link_xpath"),
            )

        source_url = _resolve_url(link_href, base_url)

        # Extract location
        location = _first_text(
            el,
            selector_str=template.get("location"),
            xpath_str=template.get("location_xpath"),
        )

        # Extract employment type
        employment_type = _first_text(
            el,
            selector_str=template.get("employment_type"),
            xpath_str=template.get("employment_type_xpath"),
        )

        # Clean up employment type if it doesn't look like one
        if employment_type and not _JOB_TYPE_PATTERN.search(employment_type):
            employment_type = None

        return {
            "title": title,
            "source_url": source_url or base_url,
            "location_raw": location or None,
            "salary_raw": None,  # ATS listing pages rarely show salary
            "employment_type": employment_type or None,
            "description": None,  # Listing page; detail page needed
            "extraction_method": f"tier1_ats_{ats}",
            "extraction_confidence": 0.92,
        }

    # ------------------------------------------------------------------
    # Tier 2: Heuristic Structural Analysis
    # ------------------------------------------------------------------

    def _extract_tier2_heuristic(self, url: str, html: str) -> Optional[list[dict]]:
        """Tier 2: Heuristic structural analysis using weighted scoring."""
        root = _parse_html(html)
        if root is None:
            return None

        # Step 1: Score candidate container elements
        candidates = self._score_containers(root)
        if not candidates:
            return None

        # Sort by score descending
        candidates.sort(key=lambda c: c[1], reverse=True)

        # Try the top candidates (up to 3) until one yields jobs
        for container_el, score in candidates[:3]:
            jobs = self._extract_jobs_from_container(container_el, url, score)
            if jobs and len(jobs) >= MIN_JOBS_FOR_SUCCESS:
                return jobs

        return None

    def _score_containers(
        self, root: etree._Element
    ) -> list[tuple[etree._Element, int]]:
        """Score elements as potential job listing containers."""
        candidates: list[tuple[etree._Element, int]] = []

        # Walk all elements that could be containers
        for el in root.iter():
            if not isinstance(el.tag, str):
                continue
            tag = el.tag.lower()
            if tag in ("script", "style", "head", "meta", "link", "noscript", "svg"):
                continue

            score = 0
            classes = _get_el_classes(el)
            el_id = (el.get("id") or "").lower()
            combined_attrs = f"{classes} {el_id}"

            # Score +10: class/id contains job-related keywords
            if _JOB_CLASS_PATTERN.search(combined_attrs):
                score += 10

            children = [c for c in el if isinstance(c.tag, str) and c.tag.lower()
                        not in ("script", "style", "br", "hr")]

            if len(children) < 2:
                continue  # Need at least 2 children to be a list container

            # Score +5: container tag (ul/ol/table/div/section) with >3 similar children
            if tag in ("ul", "ol", "table", "div", "section", "main", "tbody"):
                if len(children) > 3:
                    # Check structural similarity of children
                    sigs = [_child_signature(c) for c in children]
                    # Count the most common signature
                    if sigs:
                        most_common_count = max(sigs.count(s) for s in set(sigs))
                        if most_common_count >= 3:
                            score += 5

            # Score +3: children contain anchors with job-URL patterns
            job_url_children = 0
            for child in children:
                for a_tag in child.iter("a"):
                    href = a_tag.get("href", "")
                    if _JOB_URL_PATTERN.search(href):
                        job_url_children += 1
                        break
            if job_url_children >= 3:
                score += 3

            # Score +3: >3 children with similar tag+class structure
            if len(children) > 3:
                sigs = [_child_signature(c) for c in children]
                if sigs:
                    most_common_count = max(sigs.count(s) for s in set(sigs))
                    if most_common_count > 3:
                        score += 3

            if score >= 5:
                candidates.append((el, score))

        # v1.1: Table heuristic — score <table>/<tbody> elements whose rows
        # have short text + links, even without job-class CSS
        for table_el in root.iter("table"):
            if any(table_el is c for c, _ in candidates):
                continue  # already scored
            tbody = table_el.find("tbody")
            target = tbody if tbody is not None else table_el
            trs = [c for c in target if isinstance(c.tag, str) and c.tag.lower() == "tr"]
            if len(trs) < 3:
                continue
            # Check if rows look like job listings: short text + anchor links
            job_like_rows = 0
            for tr in trs:
                tr_text = _text(tr)
                anchors = [a for a in tr.iter("a") if a.get("href")]
                if 10 < len(tr_text) < 300 and anchors:
                    job_like_rows += 1
            if job_like_rows >= 3:
                score = 4 + min(job_like_rows, 10)  # 4-14 range
                candidates.append((target, score))

        return candidates

    def _extract_jobs_from_container(
        self, container: etree._Element, base_url: str, container_score: int
    ) -> list[dict]:
        """Extract job dicts from the repeating children of a scored container."""
        children = [
            c
            for c in container
            if isinstance(c.tag, str)
            and c.tag.lower() not in ("script", "style", "br", "hr", "thead")
        ]

        if not children:
            return []

        # Identify the repeating pattern — find the most common child signature
        sigs = [_child_signature(c) for c in children]
        sig_counts: dict[str, int] = {}
        for s in sigs:
            sig_counts[s] = sig_counts.get(s, 0) + 1

        # Use the most frequent signature as the "row" pattern
        best_sig = max(sig_counts, key=sig_counts.get) if sig_counts else ""
        rows = [c for c, s in zip(children, sigs) if s == best_sig]

        # If no clear repeating pattern, use all children
        if len(rows) < MIN_JOBS_FOR_SUCCESS:
            rows = children

        jobs: list[dict] = []
        for row in rows:
            job = self._extract_heuristic_job(row, base_url, container_score)
            if job:
                jobs.append(job)
            if len(jobs) >= MAX_JOBS_PER_PAGE:
                break

        return jobs

    def _extract_heuristic_job(
        self, row: etree._Element, base_url: str, container_score: int
    ) -> Optional[dict]:
        """Extract a single job from a container row using heuristic rules."""
        title = None
        link_href = None
        location = None
        salary = None
        employment_type = None

        # --- Title extraction ---
        # Priority 1: heading tags
        for heading_tag in ("h1", "h2", "h3", "h4", "h5", "h6"):
            for h_el in row.iter(heading_tag):
                candidate = _text(h_el)
                if _is_valid_title(candidate):
                    title = candidate
                    # Check if heading contains/is an anchor
                    h_href = _href(h_el)
                    if h_href:
                        link_href = h_href
                    break
            if title:
                break

        # Priority 2: anchor with text 10-200 chars
        if not title:
            for a_el in row.iter("a"):
                candidate = _text(a_el)
                if _is_valid_title(candidate) and len(candidate) >= 10:
                    title = candidate
                    link_href = a_el.get("href")
                    break

        # Priority 3: element with class containing title/heading/name
        if not title:
            for el in row.iter():
                if not isinstance(el.tag, str):
                    continue
                classes = _get_el_classes(el)
                if _TITLE_CLASS_PATTERN.search(classes):
                    candidate = _text(el)
                    if _is_valid_title(candidate):
                        title = candidate
                        link_href = _href(el)
                        break

        if not _is_valid_title(title or ""):
            return None

        # --- Link extraction (if not found with title) ---
        if not link_href:
            for a_el in row.iter("a"):
                href = a_el.get("href")
                if href and not href.startswith("#") and not href.startswith("javascript:"):
                    link_href = href
                    break

        source_url = _resolve_url(link_href, base_url)

        # --- Location extraction ---
        # Priority 1: element with location-related class
        for el in row.iter():
            if not isinstance(el.tag, str):
                continue
            classes = _get_el_classes(el)
            if _LOCATION_CLASS_PATTERN.search(classes):
                loc_text = _text(el)
                if loc_text and 2 < len(loc_text) < 200:
                    location = loc_text
                    break

        # Priority 2: text matching AU location patterns
        if not location:
            row_text = _text(row)
            match = _AU_LOCATIONS.search(row_text)
            if match:
                # Try to extract the broader location context
                # Find the element containing the match
                for el in row.iter():
                    if not isinstance(el.tag, str):
                        continue
                    el_text = _text(el)
                    if el_text and match.group(0) in el_text and len(el_text) < 200:
                        # Prefer shorter, more specific location strings
                        if not location or len(el_text) < len(location):
                            location = el_text

        # --- Salary extraction ---
        for el in row.iter():
            if not isinstance(el.tag, str):
                continue
            classes = _get_el_classes(el)
            if _SALARY_CLASS_PATTERN.search(classes):
                sal_text = _text(el)
                if sal_text and len(sal_text) < 200:
                    salary = sal_text
                    break

        if not salary:
            row_text = _text(row)
            sal_match = _SALARY_PATTERN.search(row_text)
            if sal_match:
                salary = sal_match.group(0).strip()

        # --- Employment type extraction ---
        for el in row.iter():
            if not isinstance(el.tag, str):
                continue
            classes = _get_el_classes(el)
            if _TYPE_CLASS_PATTERN.search(classes):
                type_text = _text(el)
                if type_text and _JOB_TYPE_PATTERN.search(type_text):
                    employment_type = type_text.strip()
                    break

        if not employment_type:
            row_text = _text(row)
            type_match = _JOB_TYPE_PATTERN.search(row_text)
            if type_match:
                employment_type = type_match.group(0).strip()

        # Compute confidence based on container score and data completeness
        confidence = 0.5 + min(container_score, 20) * 0.02  # 0.5 - 0.9 range
        if source_url:
            confidence += 0.02
        if location:
            confidence += 0.02
        confidence = min(confidence, 0.88)

        return {
            "title": title,
            "source_url": source_url or base_url,
            "location_raw": location or None,
            "salary_raw": salary or None,
            "employment_type": employment_type or None,
            "description": None,
            "extraction_method": "tier2_heuristic",
            "extraction_confidence": round(confidence, 2),
        }

    # ------------------------------------------------------------------
    # Tier 3: LLM-Assisted (deferred)
    # ------------------------------------------------------------------

    def _extract_tier3_llm(self, url: str, html: str) -> Optional[list[dict]]:
        """Tier 3: LLM-assisted extraction. Requires Ollama. Returns None if not available."""
        # TODO: wire in existing LLM extractor when Ollama is running
        return None
