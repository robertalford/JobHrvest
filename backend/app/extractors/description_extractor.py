"""Next-generation description extractor — 9-layer adaptive extraction pyramid.

Extraction layers (cheapest → most expensive, stops at first success):

  Layer 0  Structured data        extruct JSON-LD / Microdata / RDFa                [trust 0.98]
  Layer 1  ATS platform selectors Known CSS selectors per ATS (Greenhouse, Workday…) [trust 0.95]
  Layer 2  Learned selectors      Site-specific CSS selectors stored in SiteTemplate  [trust 0.90]
  Layer 3  Universal semantics    30+ common job-description selectors + clean <main>  [trust 0.72]
  Layer 4  Content-density DOM    Score every block by text/link/sentence density      [trust 0.65]
  Layer 4.5 ML Classifier         TF-IDF + LR block classifier — < 1ms, no GPU       [trust 0.80]
  Layer 5  Fast LLM               Small/fast model (e.g. llama3.2:3b) — handles ~80% [trust 0.82]
  Layer 6  Full LLM               Large model (e.g. llama3.1:8b) — hard cases only   [trust 0.88]
  Layer 7  Vision LLM             Playwright screenshot → multimodal LLM              [trust 0.75]

Layer 4.5 (ML classifier) sits between density heuristics and slow LLM inference.
It scores all DOM text blocks using a TF-IDF + Logistic Regression model trained on
high-quality extractions from the DB. When the model is available it handles most
remaining cases in < 1ms, dramatically reducing pressure on the LLM layers.

LLM escalation: Layer 5 fires first (fast 3b model). If result is absent or weak
(< GOOD_DESC_LEN chars), escalates to Layer 6 (full 8b model). This gives ~3-4x
LLM throughput for the common case while retaining 8b quality for hard pages.

After any LLM success → async selector learning written back to SiteTemplate.
Per-domain, per-layer success rates tracked in Redis (auto-escalation).
"""

from __future__ import annotations

import copy
import json
import logging
import re
from dataclasses import dataclass, field
from typing import Optional
from urllib.parse import urlparse

from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

MIN_DESC_LEN = 100       # Absolute minimum to count as a description
GOOD_DESC_LEN = 300      # Prefer descriptions above this length

# Per-layer confidence values (index matches layer number; 4.5 → index 5 in list below)
# Layers: structured, ats, learned, semantic, density, classifier, llm_fast, llm_full, vision
LAYER_CONFIDENCE = [0.98, 0.95, 0.90, 0.72, 0.65, 0.80, 0.82, 0.88, 0.75]
LAYER_NAMES      = ["structured", "ats", "learned", "semantic", "density",
                    "classifier", "llm_fast", "llm_full", "vision"]

# ── ATS Platform Selectors ────────────────────────────────────────────────────
# Ordered by specificity within each platform — first match wins.

ATS_SELECTORS: dict[str, list[str]] = {
    "greenhouse": [
        "[data-job-id] .job__description",
        ".job__description",
        "#app .job__description",
        "[class*='job__description']",
        ".content .section",
    ],
    "lever": [
        ".posting-description",
        ".section-wrapper .content",
        "[class*='posting-description']",
        ".content[data-field='description']",
    ],
    "workday": [
        "[data-automation-id='jobPostingDescription']",
        "[data-automation-id='richTextContainer']",
        ".wd-text",
        "[class*='css-rqgsq5']",   # Workday React class (common)
        "[class*='gwt-HTML']",
    ],
    "bamboohr": [
        "#job-description",
        ".job-description",
        "[class*='description']",
        "section.job-section",
    ],
    "ashby": [
        ".ashby-job-posting-brief-description",
        "[class*='JobPosting-descriptionWrapper']",
        "[class*='description'][class*='job']",
    ],
    "smartrecruiters": [
        ".job-sections .job-description",
        "[data-testid='job-description']",
        "[class*='job-description']",
        ".job-sections section",
    ],
    "jobvite": [
        ".jv-job-detail-description",
        ".jv-description",
        "[class*='jv-description']",
    ],
    "icims": [
        "#iCIMS_JobContent",
        "[class*='jobContent']",
        ".col-md-8 .cms-content",
        "td[class*='contentBody']",
    ],
    "taleo": [
        ".requisition-description",
        "[class*='reqDescription']",
        "td[class*='contentBody']",
    ],
    "successfactors": [
        "[class*='jd-description']",
        ".sfJobDescription",
        "[class*='jobDescription']",
    ],
}

# ATS fingerprint signals for auto-detection
_ATS_SIGNALS: dict[str, list[str]] = {
    "greenhouse":     ["boards.greenhouse.io", "greenhouse.io/", "grnhse.io", "ghippo"],
    "lever":          ["jobs.lever.co", "lever.co/"],
    "workday":        ["myworkdayjobs.com", "wd1.myworkday", "wd3.myworkday", "workday.com"],
    "bamboohr":       ["bamboohr.com/jobs", ".bamboohr.com"],
    "ashby":          ["ashbyhq.com", "jobs.ashbyhq"],
    "smartrecruiters":["smartrecruiters.com", "careers.smartrecruiters"],
    "jobvite":        ["jobs.jobvite.com", "hire.jobvite.com"],
    "icims":          ["icims.com/jobs", "careers.icims"],
    "taleo":          ["taleo.net", ".tal.net"],
    "successfactors": ["successfactors.com", "sapsf.com"],
}

# Universal CSS selectors — tried in order, highest specificity first
UNIVERSAL_SELECTORS: list[str] = [
    # Schema.org attributes
    "[itemprop='description']",
    "[itemprop='jobDescription']",
    # Data-attribute patterns (React/Vue/Angular)
    "[data-job-description]",
    "[data-testid='jobDescriptionText']",
    "[data-testid='description']",
    "[data-automation-id='description']",
    "[data-automation='description']",
    "[data-qa='description']",
    "[data-cy='description']",
    "[data-bind*='description']",
    # ARIA
    "[aria-label='Job Description']",
    "[aria-label='Description']",
    # IDs
    "#job-description", "#jobDescription", "#job-details", "#description",
    "#main-description", "#job-content", "#position-description",
    # Classes — specific patterns
    ".job-description", ".job-description__text", ".job-description-content",
    ".job-details-description", ".job-body", ".description__text",
    ".description-content", ".posting-description", ".listing-description",
    ".jd-content", ".role-description", ".vacancy-description",
    ".job-summary", ".job-overview", ".opportunity-description",
    # Platform signatures
    ".jobsearch-jobDescriptionText",   # Indeed
    ".description__text--rich",        # LinkedIn
    ".jobs-description__content",      # LinkedIn alt
    ".jobDescriptionText",             # Generic aggregator
    # Catch-all class patterns (broad, low specificity)
    "[class*='job-description']",
    "[class*='jobDescription']",
    "[class*='job-content']",
    "[class*='position-description']",
]

# Job vocabulary for content-density scoring
_JOB_TERMS = frozenset([
    "responsibilities", "requirements", "qualifications", "experience",
    "skills", "role", "position", "opportunity", "team", "company",
    "benefits", "salary", "location", "remote", "hybrid", "onsite",
    "degree", "candidate", "reporting", "collaborate", "passion",
    "ideal", "culture", "flexible", "competitive", "manage", "develop",
    "support", "lead", "join", "apply", "employment", "career",
    "permanent", "contract", "full-time", "part-time", "environment",
])

# Boilerplate text patterns that are NOT job descriptions
_BOILERPLATE = [
    r"please enable javascript",
    r"javascript is required",
    r"you need to enable javascript",
    r"cookie(s)? (notice|policy|settings|consent)",
    r"we use cookies",
    r"privacy policy",
    r"terms (of (service|use)|and conditions)",
    r"404.*not found",
    r"page not found",
    r"access denied",
    r"loading\.\.\.",
    r"^\s*(sign in|log in|register)\s*$",
]

# ── Result Dataclass ──────────────────────────────────────────────────────────

@dataclass
class DescriptionResult:
    text: str
    layer: int
    layer_name: str
    confidence: float
    selector_used: Optional[str] = None
    char_count: int = field(init=False)

    def __post_init__(self):
        self.char_count = len(self.text)


# ── Main Extractor Class ──────────────────────────────────────────────────────

class DescriptionExtractor:
    """
    Multi-layer, adaptive job description extractor.

    Usage:
        extractor = DescriptionExtractor(db=db, redis_client=redis)
        result = await extractor.extract(html, url, ats_platform="greenhouse")
        if result:
            description = result.text
            print(f"Found via {result.layer_name} with {result.confidence:.0%} confidence")
    """

    def __init__(self, db=None, redis_client=None):
        self.db = db
        self.redis = redis_client

    async def extract(
        self,
        html: str,
        url: str,
        domain: Optional[str] = None,
        ats_platform: Optional[str] = None,
        max_layer: int = 8,
    ) -> Optional[DescriptionResult]:
        """
        Run the extraction pyramid. Returns the first result that passes
        the quality gate, or None if all layers fail.

        Auto-escalation: if Redis is available and a domain has a poor track
        record for layers 3–4 (< 40% success in last 50 attempts), those
        layers are skipped and escalation goes straight to LLM.
        """
        if not html or len(html) < 200:
            return None

        domain = domain or _extract_domain(url)

        # Parse HTML once — layers that need a clean copy make their own
        soup = BeautifulSoup(html, "lxml")
        auto_ats = ats_platform or _detect_ats(soup, url)

        # Layer indices: 0-4 heuristic, 5=classifier, 6=llm_fast, 7=llm_full, 8=vision
        # Layer 4.5 (ML classifier) is represented as index 5 in the list.
        layers = [
            (0, self._layer_structured_data,   (html, url),           {}),
            (1, self._layer_ats_selectors,      (soup, auto_ats),      {}),
            (2, self._layer_learned_selector,   (soup, domain),        {}),
            (3, self._layer_semantic_selectors, (soup,),               {}),
            (4, self._layer_content_density,    (html, url),           {}),
            (5, self._layer_classifier,         (html,),               {}),
            (6, self._layer_llm_fast,           (html, url),           {}),
            (7, self._layer_llm_full,           (html, url),           {}),
            (8, self._layer_vision_screenshot,  (url,),                {}),
        ]

        for layer_idx, fn, args, kwargs in layers:
            if layer_idx > max_layer:
                break

            # Auto-escalation: skip weak layers for this domain
            if layer_idx in (3, 4) and await self._should_skip(domain, layer_idx):
                logger.debug(f"  [{LAYER_NAMES[layer_idx]}] skipping — poor track record for {domain}")
                continue

            try:
                import inspect
                if inspect.iscoroutinefunction(fn):
                    candidate = await fn(*args, **kwargs)
                else:
                    candidate = fn(*args, **kwargs)
            except Exception as e:
                logger.debug(f"  [{LAYER_NAMES[layer_idx]}] error: {e}")
                candidate = None

            if candidate and _is_good_description(candidate):
                await self._record(domain, layer_idx, success=True)
                logger.debug(
                    f"  [{LAYER_NAMES[layer_idx]}] ✓ {len(candidate)} chars for {domain}"
                )
                # After LLM success: learn selector in background (fire-and-forget)
                if layer_idx in (6, 7) and self.db:
                    import asyncio
                    asyncio.create_task(
                        self._learn_selector(html, candidate, domain)
                    )
                return DescriptionResult(
                    text=candidate[:8000],
                    layer=layer_idx,
                    layer_name=LAYER_NAMES[layer_idx],
                    confidence=LAYER_CONFIDENCE[layer_idx],
                )
            else:
                await self._record(domain, layer_idx, success=False)

        logger.debug(f"  [all layers exhausted] no description found for {url}")
        return None

    # ── Layer 0: Structured Data (JSON-LD / Microdata / RDFa) ────────────────

    async def _layer_structured_data(self, html: str, url: str) -> Optional[str]:
        """extruct: highest trust — structured data authored by the site itself."""
        try:
            import extruct
            data = extruct.extract(
                html, base_url=url,
                syntaxes=["json-ld", "microdata", "rdfa"],
                errors="ignore",
            )
        except Exception:
            return None

        for syntax in ["json-ld", "microdata", "rdfa"]:
            for item in data.get(syntax, []):
                desc = _find_schema_description(item)
                if desc:
                    cleaned = _strip_html(desc)
                    if len(cleaned) >= MIN_DESC_LEN:
                        return cleaned

        return None

    # ── Layer 1: ATS Platform Selectors ──────────────────────────────────────

    def _layer_ats_selectors(
        self, soup: BeautifulSoup, ats_platform: Optional[str]
    ) -> Optional[str]:
        """Apply known per-ATS CSS selectors — very reliable when platform matches."""
        if not ats_platform:
            return None
        for sel in ATS_SELECTORS.get(ats_platform.lower(), []):
            try:
                el = soup.select_one(sel)
                if el:
                    text = el.get_text(separator="\n", strip=True)
                    if len(text) >= MIN_DESC_LEN:
                        return text
            except Exception:
                continue
        return None

    # ── Layer 2: Learned Site-Specific Selector ───────────────────────────────

    async def _layer_learned_selector(
        self, soup: BeautifulSoup, domain: str
    ) -> Optional[str]:
        """Query SiteTemplate DB for a previously-learned CSS selector."""
        if not self.db:
            return None
        try:
            from sqlalchemy import text as sa_text
            result = await self.db.execute(sa_text("""
                SELECT st.selectors
                FROM site_templates st
                JOIN companies c ON st.company_id = c.id
                WHERE c.domain LIKE :pat
                  AND st.selectors::text LIKE '%description%'
                  AND st.is_active = true
                ORDER BY st.updated_at DESC
                LIMIT 1
            """), {"pat": f"%{domain}%"})
            row = result.fetchone()
            if not row or not row[0]:
                return None

            desc_sel = row[0].get("description")
            if not desc_sel:
                return None

            el = soup.select_one(desc_sel)
            if el:
                text = el.get_text(separator="\n", strip=True)
                if _is_good_description(text):
                    return text
                # Selector returned something but it's garbage — it may be stale;
                # don't return it, let escalation continue
                logger.debug(f"  [learned] stale selector '{desc_sel}' for {domain}")
        except Exception as e:
            logger.debug(f"  [learned] DB error: {e}")
        return None

    # ── Layer 3: Universal Semantic Selectors ─────────────────────────────────

    def _layer_semantic_selectors(self, soup: BeautifulSoup) -> Optional[str]:
        """
        Try 30+ universal job-description CSS selectors in priority order,
        then fall back to a noise-cleaned <main> block.
        """
        # Try targeted selectors first
        for sel in UNIVERSAL_SELECTORS:
            try:
                el = soup.select_one(sel)
                if el:
                    text = el.get_text(separator="\n", strip=True)
                    if len(text) >= MIN_DESC_LEN:
                        return text
            except Exception:
                continue

        # Try <main> with inner noise stripped
        main_el = (
            soup.find("main")
            or soup.find(attrs={"role": "main"})
            or soup.find(id="main-content")
            or soup.find(id="main")
        )
        if main_el:
            # Work on a string re-parse to avoid mutating original soup
            main_copy = BeautifulSoup(str(main_el), "lxml")
            for noise in main_copy(
                ["nav", "aside", "header", "footer", "script",
                 "style", "button", "form", "noscript"]
            ):
                noise.decompose()
            text = main_copy.get_text(separator="\n", strip=True)
            if len(text) >= MIN_DESC_LEN:
                return text

        return None

    # ── Layer 4: Content-Density DOM Scoring ─────────────────────────────────

    def _layer_content_density(self, html: str, url: str) -> Optional[str]:
        """
        Score every block element using a multi-signal density formula:

          score = text_len
                × text_density      (text chars / HTML chars — rewards text-rich elements)
                × (1 − link_density)  (penalises navigation blocks heavily)
                × prose_multiplier  (boosts elements with proper sentence structures)
                × vocab_multiplier  (boosts job-vocabulary-rich blocks)
                × structure_bonus   (extra credit for elements with <p> sub-elements)

        The highest-scoring candidate that passes the quality gate wins.
        """
        # Fresh parse so we can decompose without side-effects
        soup = BeautifulSoup(html, "lxml")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()

        # Locate job title for proximity scoring
        title_el = soup.find("h1") or soup.find("h2")
        title_text = title_el.get_text(strip=True).lower() if title_el else ""

        candidates: list[tuple[float, str]] = []

        for el in soup.find_all(["div", "section", "article", "td", "main"]):
            text = el.get_text(separator=" ", strip=True)
            text_len = len(text)
            if text_len < MIN_DESC_LEN:
                continue

            html_str = str(el)
            html_len = max(1, len(html_str))

            # Text density: how text-rich is this element vs. raw HTML
            text_density = min(1.0, text_len / html_len)

            # Link density: high = navigation block
            link_chars = sum(
                len(a.get_text(strip=True))
                for a in el.find_all("a", recursive=True)
            )
            link_density = min(1.0, link_chars / max(1, text_len))

            # Prose quality: how much reads like real sentences
            sentences = re.findall(r"[A-Z][^.!?]{15,}[.!?]", text)
            sentence_chars = sum(len(s) for s in sentences)
            prose_ratio = min(1.0, sentence_chars / max(1, text_len))
            prose_multiplier = 0.5 + prose_ratio * 0.5

            # Job vocabulary presence
            text_lower = text.lower()
            vocab_hits = sum(1 for t in _JOB_TERMS if t in text_lower)
            vocab_multiplier = 0.75 + min(0.25, vocab_hits / 20)

            # Structure bonus: elements with <p> children are more likely descriptions
            p_count = len(el.find_all("p", recursive=False)) + len(
                el.find_all("p", recursive=True, limit=5)
            )
            structure_bonus = 1.0 + min(0.3, p_count * 0.05)

            score = (
                text_len
                * text_density
                * (1.0 - link_density)
                * prose_multiplier
                * vocab_multiplier
                * structure_bonus
            )

            if score > 0:
                candidates.append((score, text))

        # Sort by score; validate top candidates
        candidates.sort(reverse=True, key=lambda x: x[0])
        for _, text in candidates[:8]:
            if _is_good_description(text):
                return text[:8000]

        return None

    # ── Layer 5: ML Classifier ────────────────────────────────────────────────

    def _layer_classifier(self, html: str) -> Optional[str]:
        """Layer 5: TF-IDF + Logistic Regression block classifier.

        Extracts all DOM text blocks, scores each one with the trained model,
        and returns the best-scoring block above the confidence threshold.

        Runs in < 1ms — orders of magnitude faster than LLM inference.
        Gracefully returns None if the model file hasn't been trained yet,
        allowing the pipeline to fall through to the LLM layers.
        """
        try:
            from app.ml.description_classifier import (
                DescriptionClassifier, extract_text_blocks
            )
            clf = DescriptionClassifier.get()
            if not clf.available:
                return None  # Model not trained yet — fall through

            blocks = extract_text_blocks(html, min_len=MIN_DESC_LEN)
            if not blocks:
                return None

            result = clf.best_block(blocks, threshold=0.65)
            if result is None:
                return None

            text, score = result
            logger.debug(f"  [classifier] ✓ score={score:.3f}, {len(text)} chars")
            return text[:8000]

        except Exception as e:
            logger.debug(f"  [classifier] error: {e}")
            return None

    # ── Layer 6 & 7: Tiered LLM extraction ───────────────────────────────────
    #
    # Layer 6 uses the fast/small model (OLLAMA_FAST_MODEL, e.g. llama3.2:3b).
    # If that model is not configured, Layer 6 is skipped and Layer 7 runs the
    # full model directly.
    #
    # Layer 7 uses the full model (OLLAMA_MODEL, e.g. llama3.1:8b).
    # It only fires when Layer 6 is unavailable or returns a weak result.
    #
    # Both layers use the same focused prompt — the difference is only the model.

    def _build_llm_prompt(self, html: str, url: str, max_chars: int = 8000) -> str:
        """Convert HTML to clean markdown and build the description-extraction prompt."""
        from markdownify import markdownify
        md = markdownify(
            html,
            strip=["script", "style", "nav", "header", "footer",
                   "aside", "button", "form", "noscript"],
        )
        md = re.sub(r"\n{3,}", "\n\n", md).strip()[:max_chars]
        return (
            "You are a precise data extraction engine. Extract ONLY the job description "
            "from the following job posting web page.\n\n"
            "The job description includes: role overview, responsibilities, requirements, "
            "qualifications, and anything describing what the candidate will do or needs.\n\n"
            "DO NOT include:\n"
            "- Navigation menus, page headers, footers\n"
            "- 'Apply Now' buttons, application instructions\n"
            "- Cookie notices, privacy policies\n"
            "- Generic company boilerplate unrelated to this specific role\n"
            "- Anything that appears before or after the actual job description\n\n"
            "Return ONLY the job description text as plain text with paragraph breaks. "
            "If no clear job description is visible, return an empty string and nothing else.\n\n"
            f"Page URL: {url}\n\n"
            f"Page content:\n{md}\n\n"
            "Job description:"
        )

    async def _call_ollama(self, model: str, prompt: str, timeout: int = 60) -> Optional[str]:
        """Send a generate request to Ollama and return the response text, or None on failure."""
        import httpx
        from app.core.config import settings
        try:
            async with httpx.AsyncClient(timeout=timeout) as client:
                resp = await client.post(
                    f"{settings.OLLAMA_BASE_URL}/api/generate",
                    json={"model": model, "prompt": prompt, "stream": False},
                )
                resp.raise_for_status()
                text = (resp.json().get("response") or "").strip()
            if _is_llm_refusal(text):
                return None
            return text if len(text) >= MIN_DESC_LEN else None
        except Exception as e:
            logger.debug(f"  [ollama:{model}] error: {e}")
            return None

    async def _layer_llm_fast(self, html: str, url: str) -> Optional[str]:
        """Layer 6: Fast LLM (small model). Handles the bulk of LLM extractions quickly.

        Uses OLLAMA_FAST_MODEL (e.g. llama3.2:3b). If not configured, returns None
        so the pyramid immediately falls through to Layer 7.

        A 'weak' result (< GOOD_DESC_LEN) is treated as failure here, forcing
        escalation to the full model which has better reasoning for hard cases.
        """
        from app.core.config import settings
        fast_model = getattr(settings, "OLLAMA_FAST_MODEL", None)
        if not fast_model:
            return None  # Skip — no fast model configured, fall through to Layer 6

        # Use a shorter context for the fast model to keep prefill time down (~half the time)
        prompt = self._build_llm_prompt(html, url, max_chars=4000)
        text = await self._call_ollama(fast_model, prompt, timeout=180)

        if not text:
            return None

        # If result is short/borderline, force escalation to 8b — don't accept weak output
        if len(text) < GOOD_DESC_LEN:
            logger.debug(
                f"  [llm_fast] result too short ({len(text)} chars) — escalating to full model"
            )
            return None

        return text

    async def _layer_llm_full(self, html: str, url: str) -> Optional[str]:
        """Layer 7: Full LLM (large model). Reserved for hard cases Layer 6 can't handle.

        Uses OLLAMA_MODEL (e.g. llama3.1:8b). Accepts results down to MIN_DESC_LEN
        since this is the last LLM tier before vision.
        """
        from app.core.config import settings
        prompt = self._build_llm_prompt(html, url)
        return await self._call_ollama(settings.OLLAMA_MODEL, prompt, timeout=600)

    # ── Layer 8: Playwright Screenshot + Vision LLM ───────────────────────────

    async def _layer_vision_screenshot(self, url: str) -> Optional[str]:
        """
        Last resort: take a screenshot of the rendered page and send it to a
        vision-capable LLM (e.g. LLaVA via Ollama). Extracts description from
        what the model SEES, not what the HTML says — handles obfuscated pages.
        """
        try:
            import base64
            import httpx
            from playwright.async_api import async_playwright
            from app.core.config import settings

            vision_model = getattr(settings, "OLLAMA_VISION_MODEL", None)
            if not vision_model:
                logger.debug("  [vision] OLLAMA_VISION_MODEL not configured, skipping")
                return None

            async with async_playwright() as pw:
                browser = await pw.chromium.launch(headless=True)
                try:
                    ctx = await browser.new_context(
                        viewport={"width": 1280, "height": 900},
                        user_agent=settings.CRAWL_USER_AGENT,
                    )
                    page = await ctx.new_page()
                    await page.goto(url, wait_until="domcontentloaded", timeout=30_000)
                    await page.wait_for_timeout(2500)
                    screenshot = await page.screenshot(full_page=False, type="png")
                finally:
                    await browser.close()

            img_b64 = base64.b64encode(screenshot).decode()

            async with httpx.AsyncClient(timeout=90) as client:
                resp = await client.post(
                    f"{settings.OLLAMA_BASE_URL}/api/generate",
                    json={
                        "model": vision_model,
                        "prompt": (
                            "This is a screenshot of a job posting page. "
                            "Identify and extract ONLY the job description text — "
                            "the section describing the role, responsibilities, and requirements. "
                            "Return only that text as plain text. "
                            "If no clear job description is visible, return an empty string."
                        ),
                        "images": [img_b64],
                        "stream": False,
                    },
                )
                resp.raise_for_status()
                text = (resp.json().get("response") or "").strip()

            return text if len(text) >= MIN_DESC_LEN else None

        except Exception as e:
            logger.debug(f"  [vision] error: {e}")
            return None

    # ── Adaptive Escalation (Redis counters) ─────────────────────────────────

    async def _record(self, domain: str, layer: int, success: bool) -> None:
        """Increment per-domain, per-layer attempt/success counters in Redis."""
        if not self.redis:
            return
        try:
            ka = f"desc_ext:{domain}:{layer}:attempts"
            ks = f"desc_ext:{domain}:{layer}:successes"
            self.redis.incr(ka)
            if success:
                self.redis.incr(ks)
            # Counters expire after 14 days of inactivity
            self.redis.expire(ka, 14 * 86_400)
            self.redis.expire(ks, 14 * 86_400)
        except Exception:
            pass

    async def _should_skip(self, domain: str, layer: int) -> bool:
        """
        Return True if this layer has < 35% success rate for this domain
        with at least 30 attempts — triggers auto-escalation.
        """
        if not self.redis:
            return False
        try:
            attempts = int(self.redis.get(f"desc_ext:{domain}:{layer}:attempts") or 0)
            if attempts < 30:
                return False
            successes = int(self.redis.get(f"desc_ext:{domain}:{layer}:successes") or 0)
            return (successes / attempts) < 0.35
        except Exception:
            return False

    # ── Selector Learning (post-LLM) ─────────────────────────────────────────

    async def _learn_selector(self, html: str, description: str, domain: str) -> None:
        """
        After a successful LLM extraction, find which CSS selector best encloses
        the extracted description text and write it back to SiteTemplate.
        Next visit will use Layer 2 (fast) instead of Layer 5 (LLM).
        """
        if not self.db:
            return
        try:
            selector = _find_matching_selector(html, description)
            if not selector:
                logger.debug(f"  [learn] no selector found for {domain}")
                return

            from sqlalchemy import text as sa_text
            # Find existing template for this domain
            result = await self.db.execute(sa_text("""
                SELECT st.id, st.selectors
                FROM site_templates st
                JOIN companies c ON st.company_id = c.id
                WHERE c.domain LIKE :pat
                ORDER BY st.updated_at DESC
                LIMIT 1
            """), {"pat": f"%{domain}%"})
            row = result.fetchone()

            if row:
                selectors = dict(row[1] or {})
                selectors["description"] = selector
                await self.db.execute(sa_text("""
                    UPDATE site_templates
                    SET selectors = :sel::jsonb, updated_at = NOW()
                    WHERE id = :id
                """), {"sel": json.dumps(selectors), "id": str(row[0])})
                await self.db.commit()
                logger.info(
                    f"  [learn] updated SiteTemplate for {domain} "
                    f"with selector '{selector}'"
                )
        except Exception as e:
            logger.debug(f"  [learn] error for {domain}: {e}")


# ── Module-Level Helper Functions ─────────────────────────────────────────────

def _extract_domain(url: str) -> str:
    try:
        return urlparse(url).netloc.replace("www.", "")
    except Exception:
        return url


def _detect_ats(soup: BeautifulSoup, url: str) -> Optional[str]:
    """Auto-detect ATS platform from URL and page signals."""
    page_text = (url + " " + str(soup)[:6000]).lower()
    for platform, signals in _ATS_SIGNALS.items():
        if any(sig in page_text for sig in signals):
            return platform
    return None


def _find_schema_description(item: dict) -> Optional[str]:
    """Recursively search a schema.org item dict for a JobPosting description."""
    if not isinstance(item, dict):
        return None
    item_type = item.get("@type", "")
    if isinstance(item_type, list):
        item_type = " ".join(item_type)
    if "JobPosting" in item_type:
        desc = item.get("description")
        if desc and isinstance(desc, str) and len(desc) >= MIN_DESC_LEN:
            return desc
    # Recurse into @graph
    for sub in item.get("@graph", []):
        result = _find_schema_description(sub)
        if result:
            return result
    # Recurse into ItemList
    for sub in item.get("itemListElement", []):
        result = _find_schema_description(sub)
        if result:
            return result
    return None


def _strip_html(text: str) -> str:
    """Remove HTML tags from schema.org descriptions that embed HTML."""
    if "<" in text and ">" in text:
        return BeautifulSoup(text, "lxml").get_text(separator="\n", strip=True)
    return text.strip()


def _is_good_description(text: str) -> bool:
    """
    Quality gate: does this text look like a real job description?
    Rejects: navigation menus, boilerplate pages, cookie notices,
             pure link lists, JS-disabled messages, very short fragments.
    """
    if not text:
        return False

    text = text.strip()
    if len(text) < MIN_DESC_LEN:
        return False

    # Check for boilerplate
    text_lower = text[:400].lower()
    for pattern in _BOILERPLATE:
        if re.search(pattern, text_lower):
            return False

    # Must be mostly alphabetic (not symbols/numbers)
    alpha_ratio = sum(1 for c in text if c.isalpha()) / max(1, len(text))
    if alpha_ratio < 0.45:
        return False

    # Navigation check: if > 70% of lines are very short (≤ 4 words), likely nav
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    if len(lines) >= 5:
        short = sum(1 for ln in lines if len(ln.split()) <= 3)
        if short / len(lines) > 0.70:
            return False

    return True


def _is_llm_refusal(text: str) -> bool:
    """Detect when the LLM says it couldn't find a description instead of extracting one."""
    if not text:
        return True
    lower = text.lower()[:200]
    refusal_patterns = [
        r"i (could|can)(not|'t) (find|identify|see|extract|locate)",
        r"(no|there is no|there are no) (clear |visible )?(job )?description",
        r"the page (does not|doesn't) (contain|have|show)",
        r"^(empty|n/a|none|not (available|found|provided))\.?$",
        r"i (am|'m) (unable|not able) to",
        r"unfortunately",
    ]
    return any(re.search(p, lower) for p in refusal_patterns)


def _find_matching_selector(html: str, description: str) -> Optional[str]:
    """
    Find the most specific CSS selector whose element best contains
    the LLM-extracted description text (by word-overlap ratio).
    Used to bootstrap the learned-selector layer.
    """
    if len(description.split()) < 25:
        return None  # Too short for reliable matching

    soup = BeautifulSoup(html, "lxml")
    desc_words = set(description.lower().split())
    best_selector: Optional[str] = None
    best_ratio = 0.55  # Minimum word-overlap threshold

    for el in soup.find_all(["div", "section", "article", "main", "td"]):
        el_text = el.get_text(separator=" ", strip=True)
        if len(el_text) < MIN_DESC_LEN:
            continue
        el_words = set(el_text.lower().split())
        if not el_words:
            continue
        # Jaccard-style: how many description words appear in this element?
        overlap = len(desc_words & el_words) / max(1, len(desc_words))
        if overlap > best_ratio:
            sel = _generate_selector(el)
            if sel:
                best_ratio = overlap
                best_selector = sel

    return best_selector


def _generate_selector(el) -> Optional[str]:
    """
    Generate the most specific, reusable CSS selector for a BeautifulSoup element.
    Priority: id > data-attribute > itemprop > semantic class > tag+class.
    """
    # ID is most specific
    if el.get("id"):
        return f"#{el['id']}"

    # Data attributes that semantically reference description
    for attr in el.attrs:
        if isinstance(attr, str) and attr.startswith("data-"):
            val = str(el.get(attr, "")).lower()
            if "description" in attr.lower() or "description" in val:
                return f"[{attr}]"

    # itemprop
    if el.get("itemprop"):
        return f"[itemprop='{el['itemprop']}']"

    # Semantic class names
    classes = el.get("class", [])
    for cls in classes:
        cls_l = cls.lower()
        if any(kw in cls_l for kw in [
            "description", "content", "body", "detail",
            "posting", "jd-", "job-text", "job-content"
        ]):
            return f".{cls}"

    # Fallback: tag + first class
    if classes:
        return f"{el.name}.{classes[0]}"

    return None
