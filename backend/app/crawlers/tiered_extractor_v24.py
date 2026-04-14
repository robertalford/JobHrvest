"""
Tiered Extraction Engine v2.4 — direct from v1.6 with simplified high-impact upgrades.

High-impact changes:
1. Stronger JS-shell detection + longer Playwright render (cookie click + scroll warm-up).
2. Structured/state JSON extraction with stricter job-object validation.
3. MartianLogic/MyRecruitmentPlus board probing from Next.js client metadata.
4. Multilingual job-title signals for non-English listings (e.g. lowongan/karir pages).
5. Repeated heading-card extraction for Elementor/Bootstrap tiles with strict label rejection.
"""

from __future__ import annotations

import asyncio
import html
import json
import logging
import re
from collections import defaultdict
from typing import Any, Optional
from urllib.parse import parse_qsl, urlparse

import httpx
from lxml import etree

from app.crawlers.tiered_extractor_v16 import TieredExtractorV16, _title_has_job_noun
from app.crawlers.tiered_extractor import (
    _parse_html,
    _text,
    _resolve_url,
    _get_el_classes,
    _is_valid_title,
    _AU_LOCATIONS,
    _JOB_TYPE_PATTERN,
    _SALARY_PATTERN,
    MAX_JOBS_PER_PAGE,
    MIN_JOBS_FOR_SUCCESS,
)

logger = logging.getLogger(__name__)


_TITLE_HINT_PATTERN_V24 = re.compile(
    r"\b(?:"
    r"job|jobs|career|careers|vacanc(?:y|ies)|opening|openings|position|positions|"
    r"role|roles|hiring|recruit(?:er|ment)?|intern(?:ship)?|graduate|"
    r"manager|engineer|developer|officer|specialist|assistant|analyst|consultant|"
    r"coordinator|executive|technician|designer|administrator|accountant|"
    r"supervisor|director|teacher|nurse|operator|staff|clerk|sales|marketing|model|stylist|"
    r"influencer|akuntan|asisten|psikolog(?:i)?|fotografer|videografer|desainer|"
    r"pelayan|kasir|barista|pengawas|teknisi|insinyur|manajer|staf|administrasi|"
    r"pemasaran|penjualan|keuangan|layanan\s+pelanggan|customer\s+service|"
    r"lowongan|loker|karir|karier|kerjaya|pekerjaan|jawatan|"
    r"vacantes?|empleo|empleos|trabajo|trabajos|vaga|vagas|"
    r"gerente|analista|desarrollador|ingeniero|diseñador|designer|"
    r"auxiliar|coordenador|atendente|operador|financeiro|"
    r"stellen|stellenangebote|jobsuche"
    r")\b",
    re.IGNORECASE,
)

_REJECT_TITLE_PATTERN_V24 = re.compile(
    r"^(?:"
    r"my\s+applications?|my\s+forms?|my\s+emails?|my\s+tests?|my\s+interviews?|"
    r"job\s+alerts?|saved\s+jobs?|manage\s+applications?|"
    r"start\s+new\s+application|access\s+existing\s+application|"
    r"apply\s+now|apply\s+here|read\s+more|learn\s+more|show\s+more|"
    r"entries\s+feed|comments\s+feed|rss|feed|"
    r"about\s+us|contact\s+us|privacy|terms|help|login|register|"
    r"job\s+name|closing\s+date|posted\s+date|job\s+ref|"
    r"benefits|how\s+to\s+apply|current\s+opportunities|"
    r"alamat\s+kantor|model\s+incubator|main\s+menu|header|footer|"
    r"vacantes|vacantes\s+inicio|bolsa\s+de\s+trabajo"
    r")$",
    re.IGNORECASE,
)

_GENERIC_LISTING_LABEL_PATTERN_V24 = re.compile(
    r"^(?:"
    r"jobs?|careers?|vacancies|vacantes?|open\s+positions?|job\s+openings?|"
    r"all\s+jobs?|current\s+vacancies|join\s+our\s+team|"
    r"lowongan(?:\s+kerja)?|karir|karier|loker|kerjaya|pekerjaan|jawatan"
    r")$",
    re.IGNORECASE,
)

_CATEGORY_TITLE_PATTERN_V24 = re.compile(
    r"^(?:all\s+jobs?|jobs?\s+by|browse\s+jobs?|view\s+jobs?|"
    r".{2,80}\s+jobs?|.{2,80}\s+vacancies?)$",
    re.IGNORECASE,
)

_PHONE_TITLE_PATTERN_V24 = re.compile(r"^(?:\+?\d[\d\s().-]{6,}|\d{2,4}\s?\d{3,5}\s?\d{3,5})$")

_CORPORATE_TITLE_PATTERN_V24 = re.compile(
    r"^(?:about|home|contact|consultancy|company|our\s+company|our\s+values|our\s+culture)$",
    re.IGNORECASE,
)

_COMPANY_CAREER_LABEL_PATTERN_V24 = re.compile(
    r"^[a-z0-9&.,'() -]{2,60}\s+careers?$",
    re.IGNORECASE,
)

_JOB_URL_HINT_PATTERN_V24 = re.compile(
    r"(?:/job|/jobs|/career|/careers|/position|/positions|/vacanc|"
    r"/opening|/openings|/requisition|/requisitions|/apply|"
    r"/lowongan|/loker|/karir|/karier|/kerjaya|event=jobs\.|"
    r"jobid=|job_id=|requisitionid=|candidateportal|hcmui/candidateexperience)",
    re.IGNORECASE,
)

_NON_JOB_URL_PATTERN_V24 = re.compile(
    r"(?:/privacy|/terms|/cookie|/news|/blog|/about|/contact|/investor|"
    r"/help|/login|/logout|/register|/account|/feed(?:/|$)|/rss(?:/|$)|"
    r"/wp-login(?:\.php)?|/comments/feed(?:/|$)|event=help\.|event=reg\.)",
    re.IGNORECASE,
)

_CARD_HINT_PATTERN_V24 = re.compile(
    r"card|tile|item|column|job|career|position|vacan|opening|elementor|listing",
    re.IGNORECASE,
)

_CTA_PATTERN_V24 = re.compile(
    r"(?:apply|details?|detail|read\s+more|view|see|learn\s+more|job\s+description|"
    r"info\s+lengkap|selengkapnya|lihat|lamar|daftar|pelajari)",
    re.IGNORECASE,
)

_SCRIPT_ASSIGNMENT_PATTERN_V24 = re.compile(
    r"(?:window\.[A-Za-z0-9_$.]+\s*=\s*|var\s+[A-Za-z0-9_$]+\s*=\s*)(\{.*\}|\[.*\])\s*;?",
    re.DOTALL,
)


class TieredExtractorV24(TieredExtractorV16):
    """v2.4 extractor: v1.6-first + resilient JS-shell and multilingual fallbacks."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        url = career_page.url if hasattr(career_page, "url") else str(career_page)
        working_html = html or ""

        # Improve render trigger for JS shells that hide content behind script blobs.
        if self._looks_like_js_shell_v24(working_html, url):
            rendered = await self._render_with_playwright_v13(url)
            if rendered and len(rendered) > max(500, int(len(working_html) * 0.6)):
                logger.info("v2.4 shell render %s (%d -> %d bytes)", url, len(working_html), len(rendered))
                working_html = rendered

        parent_jobs: list[dict] = []
        try:
            parent_jobs = await asyncio.wait_for(
                super().extract(career_page, company, working_html),
                timeout=24.0,
            )
        except asyncio.TimeoutError:
            logger.warning("v2.4 parent extractor timeout for %s; using local fallbacks", url)
        except Exception:
            logger.exception("v2.4 parent extractor failed for %s; using local fallbacks", url)

        parent_jobs = self._dedupe_jobs_v24(parent_jobs or [], url)
        candidates: list[tuple[str, list[dict]]] = []
        if parent_jobs:
            candidates.append(("parent_v16", parent_jobs))

        structured_jobs = self._extract_structured_jobs_v24(working_html, url)
        if structured_jobs:
            candidates.append(("structured_v24", structured_jobs))

        martian_jobs = await self._extract_martianlogic_jobs_v24(url, working_html)
        if martian_jobs:
            candidates.append(("martianlogic_v24", martian_jobs))

        root = _parse_html(working_html)
        if root is not None:
            heading_card_jobs = self._extract_repeated_heading_cards_v24(root, url)
            if heading_card_jobs:
                candidates.append(("heading_cards_v24", heading_card_jobs))

        best_label, best_jobs = self._pick_best_jobset_v24(candidates, url)
        if not best_jobs:
            return []

        # Enrich fallback sets when we have likely detail URLs.
        if (
            best_label != "parent_v16"
            and len(best_jobs) >= MIN_JOBS_FOR_SUCCESS
            and any(self._is_job_like_url_v24(j.get("source_url") or "") for j in best_jobs)
        ):
            try:
                best_jobs = await asyncio.wait_for(
                    self._enrich_from_detail_pages(best_jobs),
                    timeout=12.0,
                )
            except asyncio.TimeoutError:
                logger.warning("v2.4 enrichment timeout for %s; returning non-enriched jobs", url)
            except Exception:
                logger.exception("v2.4 enrichment failed for %s; returning non-enriched jobs", url)

        return self._dedupe_jobs_v24(best_jobs, url)[:MAX_JOBS_PER_PAGE]

    # ------------------------------------------------------------------
    # Rendering improvements
    # ------------------------------------------------------------------

    @staticmethod
    def _looks_like_js_shell_v24(html_body: str, page_url: str) -> bool:
        if not html_body:
            return False

        lower = html_body.lower()
        if len(html_body) < 1200:
            return False

        has_root_shell = bool(
            re.search(r'<div[^>]+id="(?:__next|root|app)"[^>]*>\s*</div>', lower)
            or "<app-root" in lower
            or "__next_data__" in lower
        )
        if not has_root_shell:
            return False

        root = _parse_html(html_body)
        if root is None:
            return False

        anchors = root.xpath("//body//a[@href]")
        headings = root.xpath("//body//h1 | //body//h2 | //body//h3")
        has_large_scripts = len(re.findall(r"<script", lower)) >= 6
        jobish_url = bool(
            re.search(
                r"career|jobs?|vacanc|opening|recruit|lowongan|karir|loker|candidate",
                page_url or "",
                re.IGNORECASE,
            )
        )
        visible_hint = len(anchors) + len(headings)

        # Shell-like page: many scripts, empty roots, but little visible listing structure.
        if has_large_scripts and visible_hint <= 4 and jobish_url:
            return True
        return False

    async def _render_with_playwright_v13(self, url: str) -> Optional[str]:
        """Longer render path for async job boards (cookie click + staged scroll)."""
        try:
            from playwright.async_api import async_playwright

            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                page = await browser.new_page(
                    user_agent=(
                        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
                    )
                )
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=35000)
                    await page.wait_for_timeout(1500)
                    await self._dismiss_cookie_banner_v24(page)

                    # Give async boards time to hydrate and lazily populate cards.
                    await page.evaluate("window.scrollTo(0, Math.max(400, document.body.scrollHeight * 0.5));")
                    await page.wait_for_timeout(2400)
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight);")
                    await page.wait_for_timeout(2600)
                    await page.evaluate("window.scrollTo(0, 0);")
                    await page.wait_for_timeout(1000)

                    for selector in (
                        "a[href*='job']",
                        "a[href*='career']",
                        "h2",
                        "div[class*='job']",
                        "div[class*='position']",
                    ):
                        try:
                            await page.wait_for_selector(selector, timeout=1200)
                            break
                        except Exception:
                            continue

                    return await page.content()
                except Exception as exc:
                    logger.debug("v2.4 Playwright failed for %s: %s", url, exc)
                    return None
                finally:
                    await browser.close()
        except Exception:
            return None

    async def _dismiss_cookie_banner_v24(self, page: Any) -> None:
        selectors = (
            "#accept",
            "button.accept",
            "[class*='consent'] button",
            "[id*='consent'] button",
            "button:has-text('Accept')",
            "button:has-text('Accept All')",
            "button:has-text('I Agree')",
            "button:has-text('Setuju')",
        )
        for sel in selectors:
            try:
                locator = page.locator(sel)
                if await locator.count() > 0:
                    await locator.first.click(timeout=800)
                    await page.wait_for_timeout(250)
                    return
            except Exception:
                continue

    # ------------------------------------------------------------------
    # Structured/state JSON fallbacks
    # ------------------------------------------------------------------

    def _extract_structured_jobs_v24(self, html_body: str, page_url: str) -> list[dict]:
        jobs: list[dict] = []

        # JSON-LD
        for match in re.finditer(
            r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
            html_body or "",
            re.IGNORECASE | re.DOTALL,
        ):
            raw = (match.group(1) or "").strip()
            if not raw:
                continue
            try:
                parsed = json.loads(raw)
            except Exception:
                continue
            jobs.extend(self._extract_jobs_from_json_obj_v24(parsed, page_url, "tier0_jsonld_v24"))

        # Next.js and state blobs
        script_payloads: list[str] = []
        next_match = re.search(
            r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>',
            html_body or "",
            re.IGNORECASE | re.DOTALL,
        )
        if next_match:
            script_payloads.append(next_match.group(1))

        for match in re.finditer(r"<script[^>]*>(.*?)</script>", html_body or "", re.IGNORECASE | re.DOTALL):
            body = (match.group(1) or "").strip()
            if len(body) < 40:
                continue
            lowered = body.lower()
            if "job" in lowered or "requisition" in lowered or "vacanc" in lowered:
                script_payloads.append(body)

        for payload in script_payloads[:40]:
            for parsed in self._parse_json_blobs_v24(payload):
                jobs.extend(self._extract_jobs_from_json_obj_v24(parsed, page_url, "tier0_state_v24"))

        return self._dedupe_jobs_v24(jobs, page_url)

    def _parse_json_blobs_v24(self, script_body: str) -> list[object]:
        blobs: list[object] = []
        body = (script_body or "").strip()
        if not body:
            return blobs

        if body.startswith("{") or body.startswith("["):
            try:
                blobs.append(json.loads(body))
            except Exception:
                pass

        for match in _SCRIPT_ASSIGNMENT_PATTERN_V24.finditer(body):
            raw = (match.group(1) or "").strip()
            if len(raw) < 2:
                continue
            try:
                blobs.append(json.loads(raw))
            except Exception:
                continue

        return blobs

    def _extract_jobs_from_json_obj_v24(
        self,
        data: object,
        page_url: str,
        method: str,
    ) -> list[dict]:
        jobs: list[dict] = []
        queue: list[object] = [data]
        visited = 0

        while queue and visited < 5000:
            node = queue.pop(0)
            visited += 1

            if isinstance(node, list):
                queue.extend(node[:200])
                continue
            if not isinstance(node, dict):
                continue

            queue.extend(list(node.values())[:200])
            job = self._job_from_json_dict_v24(node, page_url, method)
            if job:
                jobs.append(job)
                if len(jobs) >= MAX_JOBS_PER_PAGE:
                    break

        return jobs

    def _job_from_json_dict_v24(self, node: dict, page_url: str, method: str) -> Optional[dict]:
        title = ""
        title_key = ""
        for key in ("title", "jobTitle", "positionTitle", "requisitionTitle", "name", "jobName"):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                title = value.strip()
                title_key = key
                break

        title = self._normalize_title_v24(title)
        if not self._is_valid_title_v24(title):
            return None

        source_url = ""
        for key in (
            "url",
            "jobUrl",
            "jobURL",
            "applyUrl",
            "jobPostingUrl",
            "jobDetailUrl",
            "detailsUrl",
            "externalUrl",
            "canonicalUrl",
            "sourceUrl",
        ):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                source_url = _resolve_url(value.strip(), page_url) or ""
                break
        if not source_url:
            source_url = page_url
        source_url = self._normalize_source_url_v24(source_url, page_url)
        if self._is_non_job_url_v24(source_url):
            return None

        low_keys = {str(k).strip().lower() for k in node.keys() if isinstance(k, str)}
        key_blob = " ".join(low_keys)
        strong_key_hint = any(
            k in node
            for k in ("jobId", "jobID", "jobPostingId", "requisitionId", "positionId", "jobAdId", "employmentType")
        )
        job_key_hint = bool(re.search(r"job|position|posting|requisition|vacanc|opening", key_blob))
        jobposting_type = str(node.get("@type") or "").strip().lower() == "jobposting"
        title_hint = self._title_has_job_signal_v24(title)
        url_hint = self._is_job_like_url_v24(source_url)
        looks_label_object = low_keys.issubset({"id", "name", "label", "value", "path", "children", "parent"})
        taxonomy_hint = bool(re.search(r"department|office|filter|facet|category|taxonomy", key_blob))

        if looks_label_object and not strong_key_hint:
            return None
        if taxonomy_hint and not (strong_key_hint or jobposting_type):
            return None
        if _COMPANY_CAREER_LABEL_PATTERN_V24.match(title):
            return None
        if title_key == "name" and not (title_hint or strong_key_hint or jobposting_type):
            return None
        if not (title_hint or strong_key_hint or job_key_hint or jobposting_type):
            return None
        if not (url_hint or strong_key_hint or jobposting_type):
            return None

        location = None
        for key in ("location", "jobLocation", "city", "workLocation", "region", "addressLocality"):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                location = value.strip()[:200]
                break
            if isinstance(value, dict):
                pieces = [
                    str(value.get("addressLocality") or "").strip(),
                    str(value.get("addressRegion") or "").strip(),
                    str(value.get("addressCountry") or "").strip(),
                ]
                joined = ", ".join(p for p in pieces if p)
                if joined:
                    location = joined[:200]
                    break

        salary = None
        for key in ("salary", "compensation", "baseSalary", "payRate"):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                salary = value.strip()[:200]
                break
            if isinstance(value, dict):
                raw = json.dumps(value, ensure_ascii=False)
                sal_match = _SALARY_PATTERN.search(raw)
                if sal_match:
                    salary = sal_match.group(0).strip()
                    break

        emp_type = None
        for key in ("employmentType", "jobType", "workType"):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                emp_type = value.strip()[:120]
                break
            if isinstance(value, list):
                joined = ", ".join(str(v).strip() for v in value if str(v).strip())
                if joined:
                    emp_type = joined[:120]
                    break

        desc = None
        for key in ("description", "summary", "introduction", "previewText"):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                text = value.strip()
                if "<" in text and ">" in text:
                    parsed = _parse_html(text)
                    if parsed is not None:
                        text = _text(parsed)
                desc = text[:5000] if text else None
                break

        return {
            "title": title,
            "source_url": source_url,
            "location_raw": location,
            "salary_raw": salary,
            "employment_type": emp_type,
            "description": desc,
            "extraction_method": method,
            "extraction_confidence": 0.85,
        }

    # ------------------------------------------------------------------
    # MartianLogic / MyRecruitmentPlus fallback
    # ------------------------------------------------------------------

    async def _extract_martianlogic_jobs_v24(self, page_url: str, html_body: str) -> list[dict]:
        lower = (html_body or "").lower()
        if (
            "myrecruitmentplus" not in lower
            and "martianlogic" not in lower
            and "clientcode" not in lower
            and "__next_data__" not in lower
        ):
            return []

        context = self._extract_martian_context_v24(html_body)
        if not context.get("client_code"):
            return []

        parsed = urlparse(page_url)
        base = f"{parsed.scheme or 'https'}://{parsed.netloc}"
        client_code = context.get("client_code", "")
        endpoints = self._martian_probe_urls_v24(base, page_url, client_code)
        if not endpoints:
            return []

        jobs: list[dict] = []
        try:
            async with httpx.AsyncClient(
                timeout=8,
                follow_redirects=True,
                headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json,text/html,*/*"},
            ) as client:
                for endpoint in endpoints[:8]:
                    page_numbers = [1]
                    if "pageNumber=" in endpoint:
                        page_numbers = [1, 2, 3, 4]

                    for page_num in page_numbers:
                        probe_url = re.sub(r"pageNumber=\d+", f"pageNumber={page_num}", endpoint)
                        try:
                            resp = await client.get(probe_url)
                        except Exception:
                            break

                        if resp.status_code != 200 or not resp.text:
                            break

                        probe_jobs = self._extract_jobs_from_probe_response_v24(resp.text, str(resp.url), page_url)
                        if not probe_jobs:
                            if page_num > 1:
                                break
                            continue

                        before = len(jobs)
                        jobs.extend(probe_jobs)
                        jobs = self._dedupe_jobs_v24(jobs, page_url)

                        # Stop pagination once an extra page no longer adds jobs.
                        if page_num > 1 and len(jobs) == before:
                            break

                        if len(jobs) >= MAX_JOBS_PER_PAGE:
                            return jobs[:MAX_JOBS_PER_PAGE]
        except Exception:
            logger.exception("v2.4 MartianLogic fallback failed for %s", page_url)

        return self._dedupe_jobs_v24(jobs, page_url)

    def _extract_martian_context_v24(self, html_body: str) -> dict:
        result: dict[str, Any] = {}
        match = re.search(
            r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>',
            html_body or "",
            re.IGNORECASE | re.DOTALL,
        )
        if not match:
            return result

        try:
            data = json.loads(match.group(1))
        except Exception:
            return result

        page_props = (((data.get("props") or {}).get("pageProps") or {}) if isinstance(data, dict) else {})
        if not isinstance(page_props, dict):
            return result

        result["client_code"] = str(page_props.get("clientCode") or "").strip()
        result["recruiter_id"] = str(page_props.get("recruiterId") or "").strip()
        result["query"] = data.get("query") or {}
        return result

    def _martian_probe_urls_v24(self, base_url: str, page_url: str, client_code: str) -> list[str]:
        candidates = [
            f"{base_url}/{client_code}/",
            f"{base_url}/{client_code}/?pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
            f"{base_url}/{client_code}/embed-jobs?pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
            f"{base_url}/embed-jobs?pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc&client={client_code}",
            f"{base_url}/?client={client_code}&pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc",
        ]

        parsed = urlparse(page_url)
        query = dict(parse_qsl(parsed.query))
        if query.get("client"):
            candidates.append(
                f"{base_url}/?client={query['client']}&pageNumber=1&pageSize=50&isActive=true&sorting=PublishDateDesc"
            )

        seen: set[str] = set()
        unique: list[str] = []
        for url in candidates:
            norm = url.rstrip("/")
            if norm in seen:
                continue
            seen.add(norm)
            unique.append(url)
        return unique

    def _extract_jobs_from_probe_response_v24(self, body: str, response_url: str, page_url: str) -> list[dict]:
        jobs: list[dict] = []
        payload = (body or "").strip()
        if not payload:
            return jobs

        # JSON response
        if payload.startswith("{") or payload.startswith("["):
            try:
                parsed = json.loads(payload)
                jobs.extend(self._extract_jobs_from_json_obj_v24(parsed, response_url, "tier0_martian_api_v24"))
            except Exception:
                pass

        # HTML response fallback
        root = _parse_html(payload)
        if root is not None:
            tier2_jobs = self._extract_tier2_v16(response_url, payload) or []
            for job in tier2_jobs:
                cloned = dict(job)
                cloned["extraction_method"] = "tier2_heuristic_v24_martian"
                jobs.append(cloned)

            jobs.extend(self._extract_repeated_heading_cards_v24(root, response_url))
            jobs.extend(self._extract_structured_jobs_v24(payload, response_url))

        return self._dedupe_jobs_v24(jobs, page_url)

    # ------------------------------------------------------------------
    # Repeated heading/card extraction
    # ------------------------------------------------------------------

    def _extract_repeated_heading_cards_v24(self, root: etree._Element, page_url: str) -> list[dict]:
        groups: dict[str, list[dict]] = defaultdict(list)

        for heading in root.xpath("//h2 | //h3 | //h4"):
            title = self._normalize_title_v24(_text(heading))
            if not self._is_valid_title_v24(title):
                continue
            if _GENERIC_LISTING_LABEL_PATTERN_V24.match(title):
                continue

            card = self._find_card_ancestor_v24(heading)
            if card is None:
                continue

            card_classes = _get_el_classes(card)
            if card_classes and not _CARD_HINT_PATTERN_V24.search(card_classes):
                continue

            links = card.xpath(
                ".//a[@href and not(starts-with(@href,'#')) and not(starts-with(@href,'javascript:'))]"
            )
            href = links[0].get("href") if links else None
            source_url = self._normalize_source_url_v24(_resolve_url(href, page_url), page_url)

            if self._is_non_job_url_v24(source_url):
                source_url = self._normalize_source_url_v24(page_url, page_url)

            row_text = _text(card)[:2600]
            title_hint = self._title_has_job_signal_v24(title)
            cta_hint = bool(_CTA_PATTERN_V24.search(row_text))
            url_hint = self._is_job_like_url_v24(source_url) and (
                source_url != self._normalize_source_url_v24(page_url, page_url)
            )

            if not (title_hint or (cta_hint and url_hint)):
                continue

            sig = self._card_signature_v24(card)
            groups[sig].append(
                {
                    "title": title,
                    "source_url": source_url,
                    "row_text": row_text,
                    "title_hint": title_hint,
                    "cta_hint": cta_hint,
                    "url_hint": url_hint,
                }
            )

        jobs: list[dict] = []
        page_norm = self._normalize_source_url_v24(page_url, page_url)

        for entries in groups.values():
            if len(entries) < 3:
                continue

            unique_titles = len({e["title"].lower() for e in entries})
            unique_ratio = unique_titles / max(1, len(entries))
            title_hits = sum(1 for e in entries if e["title_hint"])
            cta_hits = sum(1 for e in entries if e["cta_hint"])
            external_hits = sum(1 for e in entries if e["source_url"] != page_norm)
            url_hits = sum(1 for e in entries if e["url_hint"])

            if unique_ratio < 0.65:
                continue
            if title_hits < max(2, int(len(entries) * 0.55)):
                continue
            if cta_hits == 0 and external_hits == 0 and url_hits == 0:
                continue

            for entry in entries:
                if not entry["title_hint"] and not entry["url_hint"]:
                    continue
                if entry["source_url"] == page_norm and not (entry["title_hint"] and entry["cta_hint"]):
                    continue
                jobs.append(
                    {
                        "title": entry["title"],
                        "source_url": entry["source_url"],
                        "location_raw": self._extract_location_from_text_v24(entry["row_text"]),
                        "salary_raw": self._extract_salary_from_text_v24(entry["row_text"]),
                        "employment_type": self._extract_type_from_text_v24(entry["row_text"]),
                        "description": entry["row_text"] if len(entry["row_text"]) > 80 else None,
                        "extraction_method": "tier2_heading_cards_v24",
                        "extraction_confidence": 0.74 if entry["title_hint"] else 0.66,
                    }
                )

        return self._dedupe_jobs_v24(jobs, page_url)

    def _find_card_ancestor_v24(self, heading_el: etree._Element) -> Optional[etree._Element]:
        node = heading_el
        depth = 0
        best: Optional[etree._Element] = None
        best_score = 0
        while node is not None and depth < 6:
            if isinstance(node.tag, str) and node.tag.lower() in {"div", "article", "li", "section"}:
                classes = _get_el_classes(node)
                score = 0
                if re.search(r"elementor-inner-column|elementor-column|card|tile|job|position|vacan|opening|listing", classes):
                    score += 3
                if re.search(r"widget-heading|widget-text-editor", classes):
                    score -= 2
                if node.xpath(".//a[@href]"):
                    score += 1
                if node.xpath(".//button"):
                    score += 1
                if _CARD_HINT_PATTERN_V24.search(classes):
                    score += 1

                if score > best_score:
                    best_score = score
                    best = node
            node = node.getparent()
            depth += 1
        return best if best_score >= 2 else None

    @staticmethod
    def _card_signature_v24(card: etree._Element) -> str:
        tag = (card.tag or "").lower() if isinstance(card.tag, str) else "div"
        tokens = []
        for token in (_get_el_classes(card) or "").split():
            if re.match(r"elementor-element-[a-f0-9]+$", token):
                continue
            if re.match(r"(?:css|sc)-[a-z0-9]{5,}$", token):
                continue
            if token.isdigit():
                continue
            tokens.append(token)
        stable = [t for t in tokens if re.search(r"elementor|column|card|tile|job|position|vacan|opening|listing", t)]
        classes = " ".join((stable or tokens)[:4])
        return f"{tag}:{classes}" if classes else tag

    @staticmethod
    def _extract_location_from_text_v24(text: str) -> Optional[str]:
        match = _AU_LOCATIONS.search(text or "")
        return match.group(0).strip() if match else None

    @staticmethod
    def _extract_salary_from_text_v24(text: str) -> Optional[str]:
        match = _SALARY_PATTERN.search(text or "")
        return match.group(0).strip() if match else None

    @staticmethod
    def _extract_type_from_text_v24(text: str) -> Optional[str]:
        match = _JOB_TYPE_PATTERN.search(text or "")
        return match.group(0).strip() if match else None

    # ------------------------------------------------------------------
    # Selection / validation / helpers
    # ------------------------------------------------------------------

    def _pick_best_jobset_v24(
        self,
        candidates: list[tuple[str, list[dict]]],
        page_url: str,
    ) -> tuple[str, list[dict]]:
        if not candidates:
            return "", []

        best_label = ""
        best_jobs: list[dict] = []
        best_score = -1.0
        parent_score = -1.0
        parent_jobs: list[dict] = []

        for label, jobs in candidates:
            deduped = self._dedupe_jobs_v24(jobs, page_url)
            if not deduped:
                continue

            score = self._jobset_score_v24(deduped, page_url)
            valid = self._passes_jobset_validation_v24(deduped, page_url)

            if label == "parent_v16":
                parent_score = score
                parent_jobs = deduped

            if valid and score > best_score:
                best_label = label
                best_jobs = deduped
                best_score = score

        if best_jobs:
            # Keep parent output unless fallback is clearly better.
            if parent_jobs and best_label != "parent_v16" and best_score < parent_score + 2.0:
                return "parent_v16", parent_jobs[:MAX_JOBS_PER_PAGE]
            return best_label, best_jobs[:MAX_JOBS_PER_PAGE]

        if parent_jobs:
            return "parent_v16", parent_jobs[:MAX_JOBS_PER_PAGE]

        largest = max(
            ((label, self._dedupe_jobs_v24(jobs, page_url)) for label, jobs in candidates),
            key=lambda item: len(item[1]),
            default=("", []),
        )
        return largest[0], largest[1][:MAX_JOBS_PER_PAGE]

    def _passes_jobset_validation_v24(self, jobs: list[dict], page_url: str) -> bool:
        if not jobs:
            return False

        titles = [self._normalize_title_v24(j.get("title", "")) for j in jobs]
        titles = [t for t in titles if self._is_valid_title_v24(t)]
        if not titles:
            return False

        unique_ratio = len(set(t.lower() for t in titles)) / max(1, len(titles))
        if len(titles) > 2 and unique_ratio < 0.6:
            return False

        reject_hits = sum(1 for t in titles if _REJECT_TITLE_PATTERN_V24.match(t.lower()))
        generic_hits = sum(1 for t in titles if _GENERIC_LISTING_LABEL_PATTERN_V24.match(t))
        nav_hits = sum(
            1
            for t in titles
            if _CATEGORY_TITLE_PATTERN_V24.match(t) or _PHONE_TITLE_PATTERN_V24.match(t) or _CORPORATE_TITLE_PATTERN_V24.match(t)
        )
        if reject_hits >= max(1, int(len(titles) * 0.25)):
            return False
        if generic_hits >= max(1, int(len(titles) * 0.2)):
            return False
        if nav_hits >= max(1, int(len(titles) * 0.2)):
            return False

        title_hits = sum(1 for t in titles if self._title_has_job_signal_v24(t))
        url_hits = sum(1 for j in jobs if self._is_job_like_url_v24(j.get("source_url") or page_url))

        if len(titles) == 1:
            return title_hits >= 1 and generic_hits == 0
        if len(titles) <= 3:
            return title_hits >= 1 and (url_hits >= 1 or title_hits >= 2)

        return title_hits >= max(2, int(len(titles) * 0.35)) or (
            title_hits >= max(2, int(len(titles) * 0.25)) and url_hits >= max(2, int(len(titles) * 0.25))
        )

    def _jobset_score_v24(self, jobs: list[dict], page_url: str) -> float:
        if not jobs:
            return 0.0

        titles = [self._normalize_title_v24(j.get("title", "")) for j in jobs]
        titles = [t for t in titles if t]
        if not titles:
            return 0.0

        count = len(titles)
        title_hits = sum(1 for t in titles if self._title_has_job_signal_v24(t))
        url_hits = sum(1 for j in jobs if self._is_job_like_url_v24(j.get("source_url") or page_url))
        reject_hits = sum(1 for t in titles if _REJECT_TITLE_PATTERN_V24.match(t.lower()))
        generic_hits = sum(1 for t in titles if _GENERIC_LISTING_LABEL_PATTERN_V24.match(t))
        nav_hits = sum(
            1
            for t in titles
            if _CATEGORY_TITLE_PATTERN_V24.match(t) or _PHONE_TITLE_PATTERN_V24.match(t) or _CORPORATE_TITLE_PATTERN_V24.match(t)
        )
        unique_titles = len(set(t.lower() for t in titles))

        score = count * 3.4
        score += title_hits * 2.0
        score += url_hits * 0.9
        score += unique_titles * 0.7
        score -= reject_hits * 3.8
        score -= generic_hits * 3.2
        score -= nav_hits * 4.2
        return score

    def _dedupe_jobs_v24(self, jobs: list[dict], page_url: str) -> list[dict]:
        deduped: list[dict] = []
        seen: set[tuple[str, str]] = set()

        for job in jobs:
            title = self._normalize_title_v24(job.get("title", ""))
            if not self._is_valid_title_v24(title):
                continue

            source_url = self._normalize_source_url_v24(job.get("source_url"), page_url)
            if self._is_non_job_url_v24(source_url):
                continue
            if _COMPANY_CAREER_LABEL_PATTERN_V24.match(title):
                continue

            key = (title.lower(), source_url.lower())
            if key in seen:
                continue
            seen.add(key)

            cloned = dict(job)
            cloned["title"] = title
            cloned["source_url"] = source_url
            deduped.append(cloned)

            if len(deduped) >= MAX_JOBS_PER_PAGE:
                break

        return deduped

    def _normalize_title_v24(self, title: str) -> str:
        if not title:
            return ""
        t = html.unescape(" ".join(title.replace("\u00a0", " ").split()))
        t = t.strip(" |:-\u2013\u2022")
        t = re.sub(r"\s+\|\s+.*$", "", t)
        t = re.sub(r"[\u200b-\u200d\ufeff]", "", t)
        t = re.sub(r"\s{2,}", " ", t)
        return t

    def _is_valid_title_v24(self, title: str) -> bool:
        if not title:
            return False

        if not TieredExtractorV16._is_valid_title_v16(title):
            if not _is_valid_title(title):
                return False

        t = title.strip()
        low = t.lower()
        if _REJECT_TITLE_PATTERN_V24.match(low):
            return False
        if _GENERIC_LISTING_LABEL_PATTERN_V24.match(t):
            return False
        if _CATEGORY_TITLE_PATTERN_V24.match(t):
            return False
        if _PHONE_TITLE_PATTERN_V24.match(t):
            return False
        if _CORPORATE_TITLE_PATTERN_V24.match(t):
            return False
        if _COMPANY_CAREER_LABEL_PATTERN_V24.match(t):
            return False
        if len(t.split()) > 14:
            return False
        return True

    def _title_has_job_signal_v24(self, title: str) -> bool:
        if not title:
            return False
        if _title_has_job_noun(title):
            return True
        return bool(_TITLE_HINT_PATTERN_V24.search(title))

    def _normalize_source_url_v24(self, src: Optional[str], page_url: str) -> str:
        source_url = (src or "").strip()
        if not source_url:
            source_url = page_url
        if "#" in source_url:
            source_url = source_url.split("#", 1)[0]
        return source_url

    def _is_job_like_url_v24(self, src: str) -> bool:
        if not src:
            return False
        if self._is_non_job_url_v24(src):
            return False
        return bool(_JOB_URL_HINT_PATTERN_V24.search(src))

    def _is_non_job_url_v24(self, src: str) -> bool:
        return bool(_NON_JOB_URL_PATTERN_V24.search((src or "").lower()))
