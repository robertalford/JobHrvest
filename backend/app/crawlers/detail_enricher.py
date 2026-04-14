"""Detail-page enrichment — the structural fix for the field_completeness gap.

Many listing pages give only (title, link). The rich fields (description,
location, salary, employment_type) live on the per-job detail page that
Jobstream's baseline wrappers traverse via `details_page_*_paths`. Until now
the model never followed those links — which is why field_completeness sits
at ~45 while Jobstream reaches ~95.

This module adds a generic traversal layer the base TieredExtractor can call
at the end of `extract()`. It is:

  * **ATS-aware** — pulls selectors from storage/ats_templates.json via
    ats_template_loader; falls back to generic density-based extraction when
    no template is known.
  * **Budgeted** — fires only when the listing description is short, respects
    a global semaphore, per-host rate limit, and wall-clock deadline.
  * **Fail-open** — any error in enrichment leaves the original job dict
    untouched.

The enricher never overwrites a field the listing already populated; it only
fills blanks. This makes it safe to run unconditionally.
"""

from __future__ import annotations

import asyncio
import logging
import re
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Callable, Optional
from urllib.parse import urlparse

logger = logging.getLogger(__name__)


@dataclass
class EnrichmentBudget:
    """Per-extract-call budget. Defaults are conservative."""

    max_pages: int = 10
    request_timeout_s: float = 6.0
    total_deadline_s: float = 20.0
    skip_if_description_len_over: int = 400
    # Global concurrency cap across the whole enricher instance.
    global_concurrency: int = 8
    # Per-host concurrency cap. Most hosts dislike >2 concurrent requests.
    per_host_concurrency: int = 2


@dataclass
class EnrichmentReport:
    attempted: int = 0
    succeeded: int = 0
    skipped_budget: int = 0
    fetch_errors: int = 0
    fields_filled: int = 0
    per_field_fills: dict[str, int] = field(default_factory=dict)


# Tiny LRU cache — we frequently hit the same URLs across iterations within an
# A/B test. Capped so the cache never dominates memory.
class _LRU:
    def __init__(self, capacity: int = 512) -> None:
        self.capacity = capacity
        self._store: "OrderedDict[str, dict]" = OrderedDict()

    def get(self, key: str) -> Optional[dict]:
        v = self._store.get(key)
        if v is not None:
            self._store.move_to_end(key)
        return v

    def put(self, key: str, value: dict) -> None:
        if key in self._store:
            self._store.move_to_end(key)
        self._store[key] = value
        if len(self._store) > self.capacity:
            self._store.popitem(last=False)


class DetailEnricher:
    """Fetch per-job detail pages and merge richer fields into listing jobs.

    `http_fetch` is a callable (sync or async) that takes a URL and returns
    HTML text. We take it as an argument rather than importing a specific
    client so tests can pass a stub and so the module stays decoupled from the
    resilient client's retry/backoff behaviour.
    """

    def __init__(
        self,
        *,
        http_fetch: Callable[[str], "asyncio.Future[str] | str"],
        budget: Optional[EnrichmentBudget] = None,
        cache_size: int = 512,
    ) -> None:
        self.http_fetch = http_fetch
        self.budget = budget or EnrichmentBudget()
        self._cache = _LRU(cache_size)
        self._global_sem = asyncio.Semaphore(self.budget.global_concurrency)
        self._host_sems: dict[str, asyncio.Semaphore] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def enrich(
        self,
        jobs: list[dict],
        *,
        ats: Optional[str] = None,
        page_url: str = "",
    ) -> tuple[list[dict], EnrichmentReport]:
        """Enrich jobs in place and return (jobs, report).

        Only jobs with missing-or-short descriptions are candidates; the rest
        are passed through. The enriched fields never overwrite existing
        non-empty values.
        """
        report = EnrichmentReport()
        if not jobs:
            return jobs, report

        candidates = [
            (i, j) for i, j in enumerate(jobs)
            if self._needs_enrichment(j)
        ][: self.budget.max_pages]
        if not candidates:
            return jobs, report

        detail_selectors = self._load_detail_selectors(ats)
        if not detail_selectors:
            # No selectors means we can still try generic density-based
            # description extraction on the fetched HTML.
            detail_selectors = {}

        deadline = asyncio.get_running_loop().time() + self.budget.total_deadline_s

        async def _one(idx: int, job: dict) -> None:
            if asyncio.get_running_loop().time() > deadline:
                report.skipped_budget += 1
                return
            report.attempted += 1
            url = (job.get("source_url") or "").strip()
            if not url:
                return
            enriched = await self._enrich_one(url, detail_selectors)
            if enriched is None:
                report.fetch_errors += 1
                return
            filled = self._merge_fields(job, enriched)
            if filled:
                report.succeeded += 1
                report.fields_filled += sum(filled.values())
                for k, n in filled.items():
                    report.per_field_fills[k] = report.per_field_fills.get(k, 0) + n

        await asyncio.gather(*(_one(i, j) for i, j in candidates), return_exceptions=True)
        return jobs, report

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _needs_enrichment(self, job: dict) -> bool:
        """Fire only for jobs that would actually benefit from a detail-page fetch."""
        if not job.get("source_url"):
            return False
        desc = (job.get("description") or "").strip()
        if len(desc) >= self.budget.skip_if_description_len_over:
            # Listing description is already adequate.
            return False
        return True

    def _load_detail_selectors(self, ats: Optional[str]) -> dict:
        """Pull detail-page selectors out of the learned ATS template, if any."""
        if not ats:
            return {}
        try:
            from app.crawlers.ats_template_loader import default_loader
        except ImportError:
            return {}
        tpl = default_loader.lookup(ats) or {}
        return tpl.get("detail_selectors") or {}

    def _host_sem(self, url: str) -> asyncio.Semaphore:
        host = urlparse(url).netloc
        sem = self._host_sems.get(host)
        if sem is None:
            sem = asyncio.Semaphore(self.budget.per_host_concurrency)
            self._host_sems[host] = sem
        return sem

    async def _enrich_one(self, url: str, selectors: dict) -> Optional[dict]:
        cached = self._cache.get(url)
        if cached is not None:
            return cached

        async with self._global_sem, self._host_sem(url):
            try:
                html = await asyncio.wait_for(
                    self._do_fetch(url),
                    timeout=self.budget.request_timeout_s,
                )
            except (asyncio.TimeoutError, Exception) as e:  # noqa: BLE001
                logger.debug("detail_enricher: fetch failed for %s: %s", url, e)
                return None

        if not html:
            return None

        fields = _extract_detail_fields(html, url, selectors)
        self._cache.put(url, fields)
        return fields

    async def _do_fetch(self, url: str) -> str:
        """Normalise the http_fetch callable to async."""
        result = self.http_fetch(url)
        if asyncio.iscoroutine(result):
            return await result  # type: ignore[no-any-return]
        return str(result or "")

    def _merge_fields(self, job: dict, enriched: dict) -> dict[str, int]:
        """Fill blank fields on `job` from `enriched`. Returns {field: 1} for each fill."""
        filled: dict[str, int] = {}
        for key in ("description", "location_raw", "salary_raw", "employment_type"):
            existing = (job.get(key) or "").strip() if isinstance(job.get(key), str) else job.get(key)
            incoming = (enriched.get(key) or "").strip() if isinstance(enriched.get(key), str) else enriched.get(key)
            if not existing and incoming:
                job[key] = incoming
                filled[key] = 1
        if filled:
            # Mark the path that enriched this job so A/B analysis can see it
            job["enrichment_source"] = "detail_page"
        return filled


# --------------------------------------------------------------------------
# Detail-page field extraction (pure function — no HTTP, easy to unit-test)
# --------------------------------------------------------------------------

_WHITESPACE_RUN = re.compile(r"[ \t]+")
_NEWLINE_RUN = re.compile(r"\n{3,}")
_EMPLOYMENT_TERMS = re.compile(
    r"\b(Full[-\s]?[Tt]ime|Part[-\s]?[Tt]ime|Contract|Temporary|"
    r"Permanent|Casual|Internship|Graduate)\b"
)
_SALARY_PATTERN = re.compile(
    r"(?:AU?\$|USD|\$|£|€)\s?[\d,]{3,}(?:\s?(?:-|to|–)\s?(?:AU?\$|USD|\$|£|€)?\s?[\d,]{3,})?"
    r"(?:\s?(?:per|/|pa|p\.a\.|annum|year|week|day|hour|hr))?",
    re.IGNORECASE,
)


def _clean_text(text: str) -> str:
    """Normalise HTML-extracted text: collapse whitespace, trim excess newlines."""
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = _WHITESPACE_RUN.sub(" ", text)
    text = _NEWLINE_RUN.sub("\n\n", text)
    # Strip leading/trailing whitespace on every line
    text = "\n".join(line.strip() for line in text.split("\n"))
    return text.strip()


def _try_xpaths(root, selectors) -> Optional[str]:
    """Apply a selector-list (XPath or CSS) and return the first meaningful text."""
    if selectors is None:
        return None
    if isinstance(selectors, str):
        selectors = [selectors]
    for sel in selectors:
        if not sel or sel in (".", "null", "''", '""'):
            continue
        try:
            if sel.startswith("//") or sel.startswith(".//") or sel.startswith("("):
                els = root.xpath(sel)
            else:
                els = root.cssselect(sel)
        except Exception:
            continue
        for el in els:
            if el is None:
                continue
            text = getattr(el, "text_content", lambda: "")() if hasattr(el, "text_content") else str(el)
            cleaned = _clean_text(text)
            if cleaned and len(cleaned) > 5:
                return cleaned
    return None


def _extract_detail_fields(html: str, url: str, selectors: dict) -> dict:
    """Pure function: given detail-page HTML, extract fields.

    Preference order per field:
        1. ATS template selectors (when provided).
        2. Schema.org JSON-LD JobPosting blocks.
        3. Generic density-based text extraction for description.
    """
    try:
        from lxml import etree, html as lxml_html
    except ImportError:
        return {}

    try:
        root = lxml_html.fromstring(html)
    except (etree.ParserError, etree.XMLSyntaxError, ValueError):
        return {}

    fields: dict = {}

    # 1. ATS-specific selectors (highest confidence — baseline-equivalent)
    desc_sel = selectors.get("details_page_description_paths")
    loc_sel = selectors.get("details_page_location_paths")
    sal_sel = selectors.get("details_page_salary_path")
    jt_sel = selectors.get("details_page_job_type_paths")

    desc = _try_xpaths(root, desc_sel)
    if desc:
        fields["description"] = desc[:20_000]
    loc = _try_xpaths(root, loc_sel)
    if loc:
        fields["location_raw"] = loc[:200]
    sal = _try_xpaths(root, sal_sel)
    if sal:
        fields["salary_raw"] = sal[:200]
    jt = _try_xpaths(root, jt_sel)
    if jt:
        fields["employment_type"] = jt[:80]

    # 2. JSON-LD fallback — works on most ATS-backed sites
    _merge_json_ld(root, fields)

    # 3. Generic density fallback for description
    if not fields.get("description"):
        fields["description"] = _generic_description(root)

    # 4. Regex sniffers for employment_type / salary as last resort
    body_text = _clean_text(root.text_content())[:6000]
    if not fields.get("employment_type"):
        m = _EMPLOYMENT_TERMS.search(body_text)
        if m:
            fields["employment_type"] = m.group(0)
    if not fields.get("salary_raw"):
        m = _SALARY_PATTERN.search(body_text)
        if m:
            fields["salary_raw"] = m.group(0)

    return {k: v for k, v in fields.items() if v}


def _merge_json_ld(root, fields: dict) -> None:
    import json

    try:
        scripts = root.xpath("//script[@type='application/ld+json']")
    except Exception:
        return
    for script in scripts:
        try:
            payload = json.loads(script.text_content() or "")
        except (json.JSONDecodeError, AttributeError):
            continue
        for block in _iter_blocks(payload):
            if not isinstance(block, dict):
                continue
            if block.get("@type") != "JobPosting":
                continue
            if "description" not in fields and block.get("description"):
                fields["description"] = _clean_text(str(block["description"]))[:20_000]
            if "location_raw" not in fields:
                loc = block.get("jobLocation")
                loc_str = _flatten_location(loc)
                if loc_str:
                    fields["location_raw"] = loc_str[:200]
            if "employment_type" not in fields and block.get("employmentType"):
                et = block["employmentType"]
                if isinstance(et, list):
                    et = ", ".join(str(x) for x in et if x)
                fields["employment_type"] = str(et)[:80]
            if "salary_raw" not in fields:
                salary = block.get("baseSalary")
                if isinstance(salary, dict):
                    value = salary.get("value")
                    if isinstance(value, dict):
                        lo = value.get("minValue") or value.get("value")
                        hi = value.get("maxValue")
                        unit = value.get("unitText", "")
                        currency = salary.get("currency", "")
                        bits = [f"{currency} {lo}" if lo else "", f"- {hi}" if hi else "", unit]
                        joined = " ".join(b for b in bits if b).strip()
                        if joined:
                            fields["salary_raw"] = joined[:200]


def _iter_blocks(payload):
    """Flatten a JSON-LD payload into an iterable of candidate blocks."""
    if payload is None:
        return
    if isinstance(payload, list):
        for item in payload:
            yield from _iter_blocks(item)
        return
    if isinstance(payload, dict):
        if "@graph" in payload and isinstance(payload["@graph"], list):
            yield from _iter_blocks(payload["@graph"])
        yield payload


def _flatten_location(loc) -> str:
    if not loc:
        return ""
    if isinstance(loc, list):
        parts = [_flatten_location(x) for x in loc]
        return ", ".join(p for p in parts if p)
    if isinstance(loc, dict):
        addr = loc.get("address") or {}
        if isinstance(addr, dict):
            city = addr.get("addressLocality", "")
            region = addr.get("addressRegion", "")
            country = addr.get("addressCountry", "")
            return ", ".join(p for p in (city, region, country) if p) or loc.get("name", "") or ""
        return str(loc.get("name", "") or "")
    return str(loc)


def _generic_description(root) -> str:
    """Fallback: pick the densest <p>-ful container on the page."""
    try:
        containers = root.xpath("//article|//main|//section|//div")
    except Exception:
        return ""
    best: tuple[int, str] = (0, "")
    for c in containers[:40]:
        try:
            ps = c.xpath(".//p")
        except Exception:
            continue
        if len(ps) < 2:
            continue
        text = _clean_text(c.text_content())
        if len(text) < 200 or len(text) > 10000:
            continue
        score = len(ps) * 10 + min(len(text), 5000)
        if score > best[0]:
            best = (score, text)
    return best[1][:20_000]


__all__ = [
    "DetailEnricher",
    "EnrichmentBudget",
    "EnrichmentReport",
]
