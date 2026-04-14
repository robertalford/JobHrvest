"""
Tiered Extraction Engine v2.1 — direct from v1.6 with conservative recall upgrades.

High-impact changes:
1. Keep v1.6 as primary path (call super.extract first), then only override when
   fallback evidence is clearly better.
2. Structured fallback: JSON-LD JobPosting and embedded JSON state parsing.
3. Global job-link harvesting with strong title/url validation to recover list pages
   and detail pages that also embed related jobs.
4. Accordion/heading extraction for Elementor and collapsed listing layouts.
5. Stronger false-positive rejection for "My Applications"/feeds/menu labels.
"""

from __future__ import annotations

import json
import logging
import re
from collections import defaultdict
from typing import Optional
from urllib.parse import urljoin, urlparse

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


_TITLE_HINT_PATTERN_V21 = re.compile(
    r"\b(?:job|jobs|career|careers|vacanc(?:y|ies)|opening|openings|"
    r"position|positions|role|roles|internship|manager|engineer|developer|"
    r"officer|specialist|assistant|analyst|consultant|coordinator|executive|"
    r"technician|designer|administrator|accountant|recruit(?:er|ment)?|"
    r"director|chef|nurse|teacher|operator|supervisor|"
    r"gerente|director|vacantes?|empleo|trabajo|lowongan|karir|karier|"
    r"kerjaya|jawatan)\b",
    re.IGNORECASE,
)

_REJECT_TITLE_PATTERN_V21 = re.compile(
    r"^(?:"
    r"my\s+applications?|my\s+forms?|my\s+emails?|my\s+tests?|my\s+interviews?|"
    r"job\s+alerts?|jobs?\s+list|job\s+search|saved\s+jobs?|manage\s+applications?|"
    r"start\s+new\s+application|access\s+existing\s+application|preview\s+application\s+form|"
    r"apply\s+now|apply\s+here|read\s+more|learn\s+more|show\s+more|"
    r"entries\s+feed|comments\s+feed|rss|feed|"
    r"about\s+us|contact\s+us|privacy|terms|help|login|register|"
    r"job\s+name|closing\s+date|posted\s+date|job\s+ref|"
    r"benefits|how\s+to\s+apply|current\s+opportunities|join\s+us(?:\s+and.*)?|"
    r"vacantes|vacantes\s+inicio|alertas?\s+de\s+vacantes?|bolsa\s+de\s+trabajo|"
    r"asesorado\s+por|"
    r"puesto\s+ciudad\s+beneficios"
    r")$",
    re.IGNORECASE,
)

_GENERIC_LISTING_LABEL_PATTERN_V21 = re.compile(
    r"^(?:jobs?|careers?|vacancies|vacantes?|job\s+openings?|open\s+positions?|"
    r"bolsa\s+de\s+trabajo|alertas?\s+de\s+vacantes?|join\s+our\s+team)$",
    re.IGNORECASE,
)

_JOB_URL_HINT_PATTERN_V21 = re.compile(
    r"(?:/job|/jobs|/career|/careers|/position|/positions|/vacanc|"
    r"/opening|/openings|/requisition|/requisitions|"
    r"event=jobs\.|jobid=|portal\.na|candidateportal|/apply|/lowongan|/karir|/kerjaya)",
    re.IGNORECASE,
)

_NON_JOB_URL_PATTERN_V21 = re.compile(
    r"(?:/privacy|/terms|/cookie|/news|/blog|/about|/contact|/investor|"
    r"/help|/login|/logout|/register|/account|/feed(?:/|$)|/rss(?:/|$)|"
    r"event=jobs\.view(?:history|myforms|myemails|mytests|myinterviews)|"
    r"event=help\.|event=reg\.)",
    re.IGNORECASE,
)

_ROW_CLASS_PATTERN_V21 = re.compile(
    r"job|position|vacanc|opening|requisition|career|posting|listing|accordion",
    re.IGNORECASE,
)

_CLASS_HINT_PATTERN_V21 = re.compile(
    r"job|position|vacanc|opening|requisition|career|listing|accordion|card|item",
    re.IGNORECASE,
)

_SCRIPT_ASSIGNMENT_PATTERN_V21 = re.compile(
    r"(?:window\.[A-Za-z0-9_$.]+\s*=\s*|var\s+[A-Za-z0-9_$]+\s*=\s*)(\{.*\}|\[.*\])\s*;?",
    re.DOTALL,
)


class TieredExtractorV21(TieredExtractorV16):
    """v2.1 extractor: v1.6-first with guarded structured/link/accordion fallbacks."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        url = career_page.url if hasattr(career_page, "url") else str(career_page)

        # Required by agent rules: run parent extractor first.
        parent_jobs = await super().extract(career_page, company, html)
        parent_jobs = self._dedupe_jobs_v21(parent_jobs or [], url)

        root = _parse_html(html)
        candidates: list[tuple[str, list[dict]]] = []
        if parent_jobs:
            candidates.append(("parent_v16", parent_jobs))

        structured_jobs = self._extract_structured_jobs_v21(html, url)
        if structured_jobs:
            candidates.append(("structured_v21", structured_jobs))

        script_jobs = self._extract_jobs_from_state_scripts_v21(html, url)
        if script_jobs:
            candidates.append(("state_script_v21", script_jobs))

        if root is not None:
            link_jobs = self._extract_from_job_links_v21(root, url)
            if link_jobs:
                candidates.append(("job_links_v21", link_jobs))

            accordion_jobs = self._extract_from_accordion_sections_v21(root, url)
            if accordion_jobs:
                candidates.append(("accordion_v21", accordion_jobs))

            heading_jobs = self._extract_from_heading_rows_v21(root, url)
            if heading_jobs:
                candidates.append(("heading_rows_v21", heading_jobs))

        best_label, best_jobs = self._pick_best_jobset_v21(candidates, url)
        if not best_jobs:
            return []

        if best_label != "parent_v16" and len(best_jobs) >= MIN_JOBS_FOR_SUCCESS:
            # Enrich fallback output when we have a credible multi-job set.
            best_jobs = await self._enrich_from_detail_pages(best_jobs)
            best_jobs = self._dedupe_jobs_v21(best_jobs, url)

        return best_jobs[:MAX_JOBS_PER_PAGE]

    # ------------------------------------------------------------------
    # Structured / state-script fallbacks
    # ------------------------------------------------------------------

    def _extract_structured_jobs_v21(self, html: str, page_url: str) -> list[dict]:
        jobs: list[dict] = []
        for match in re.finditer(
            r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>',
            html,
            re.IGNORECASE | re.DOTALL,
        ):
            raw = (match.group(1) or "").strip()
            if not raw:
                continue
            try:
                data = json.loads(raw)
            except Exception:
                continue
            jobs.extend(self._extract_jobs_from_json_obj_v21(data, page_url, "tier0_jsonld_v21"))

        return self._dedupe_jobs_v21(jobs, page_url)

    def _extract_jobs_from_state_scripts_v21(self, html: str, page_url: str) -> list[dict]:
        jobs: list[dict] = []
        script_payloads: list[str] = []

        next_data_match = re.search(
            r'<script[^>]*id="__NEXT_DATA__"[^>]*>(.*?)</script>',
            html,
            re.IGNORECASE | re.DOTALL,
        )
        if next_data_match:
            script_payloads.append(next_data_match.group(1))

        for match in re.finditer(r"<script[^>]*>(.*?)</script>", html, re.IGNORECASE | re.DOTALL):
            body = (match.group(1) or "").strip()
            if len(body) < 40:
                continue
            if "__NEXT_DATA__" in body or "dehydratedState" in body or "job" in body.lower():
                script_payloads.append(body)

        for payload in script_payloads[:40]:
            for parsed in self._parse_json_blobs_v21(payload):
                jobs.extend(self._extract_jobs_from_json_obj_v21(parsed, page_url, "tier0_state_v21"))

        return self._dedupe_jobs_v21(jobs, page_url)

    def _parse_json_blobs_v21(self, script_body: str) -> list[object]:
        results: list[object] = []
        body = (script_body or "").strip()
        if not body:
            return results

        # Direct JSON blob.
        if body.startswith("{") or body.startswith("["):
            try:
                results.append(json.loads(body))
            except Exception:
                pass

        # JS assignment wrappers.
        for m in _SCRIPT_ASSIGNMENT_PATTERN_V21.finditer(body):
            raw = (m.group(1) or "").strip()
            if len(raw) < 2:
                continue
            try:
                results.append(json.loads(raw))
            except Exception:
                continue

        return results

    def _extract_jobs_from_json_obj_v21(
        self,
        data: object,
        page_url: str,
        method: str,
    ) -> list[dict]:
        jobs: list[dict] = []
        queue = [data]

        while queue:
            node = queue.pop(0)
            if isinstance(node, list):
                queue.extend(node[:200])
                continue
            if not isinstance(node, dict):
                continue

            queue.extend(list(node.values())[:200])
            job = self._job_from_json_dict_v21(node, page_url, method)
            if job:
                jobs.append(job)
                if len(jobs) >= MAX_JOBS_PER_PAGE:
                    break

        return jobs

    def _job_from_json_dict_v21(self, node: dict, page_url: str, method: str) -> Optional[dict]:
        title = ""
        for key in ("title", "jobTitle", "positionTitle", "requisitionTitle", "name", "jobName"):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                title = value.strip()
                break

        title = self._normalize_title_v21(title)
        if not self._is_valid_title_v21(title):
            return None

        url_raw = None
        for key in (
            "url", "jobUrl", "jobURL", "applyUrl", "jobPostingUrl", "jobDetailUrl",
            "detailsUrl", "externalUrl", "canonicalUrl", "sourceUrl",
        ):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                url_raw = value.strip()
                break

        source_url = _resolve_url(url_raw, page_url) if isinstance(url_raw, str) else None
        if not source_url:
            source_url = page_url
        if self._is_non_job_url_v21(source_url):
            return None

        key_names = " ".join(node.keys()).lower()
        job_key_hint = bool(
            re.search(r"job|position|posting|requisition|vacanc|opening", key_names)
            or any(k in node for k in ("jobId", "jobID", "jobPostingId", "requisitionId", "positionId", "jobAdId"))
        )

        if not (job_key_hint or self._is_job_like_url_v21(source_url)):
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
            "extraction_confidence": 0.86,
        }

    # ------------------------------------------------------------------
    # Link/accordion/heading fallbacks
    # ------------------------------------------------------------------

    def _extract_from_job_links_v21(self, root: etree._Element, page_url: str) -> list[dict]:
        jobs: list[dict] = []

        for a_el in root.iter("a"):
            href = (a_el.get("href") or "").strip()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue

            source_url = _resolve_url(href, page_url) or page_url
            if self._is_non_job_url_v21(source_url):
                continue

            text = self._normalize_title_v21(_text(a_el) or (a_el.get("title") or ""))
            if not self._is_valid_title_v21(text):
                continue

            if _GENERIC_LISTING_LABEL_PATTERN_V21.match(text):
                continue

            parent = a_el.getparent()
            parent_text = _text(parent)[:1200] if parent is not None else ""
            combined_classes = _get_el_classes(a_el)
            if parent is not None:
                combined_classes += " " + _get_el_classes(parent)

            class_hint = bool(_CLASS_HINT_PATTERN_V21.search(combined_classes))
            url_hint = self._is_job_like_url_v21(source_url)
            title_hint = self._title_has_job_signal_v21(text)
            context_hint = bool(
                re.search(r"apply|location|department|job ref|posted|closing|employment", parent_text, re.IGNORECASE)
            )

            if not (title_hint or url_hint or (class_hint and context_hint)):
                continue

            location = None
            loc_match = _AU_LOCATIONS.search(parent_text)
            if loc_match:
                location = loc_match.group(0).strip()

            emp_type = None
            type_match = _JOB_TYPE_PATTERN.search(parent_text)
            if type_match:
                emp_type = type_match.group(0).strip()

            jobs.append(
                {
                    "title": text,
                    "source_url": source_url,
                    "location_raw": location,
                    "salary_raw": None,
                    "employment_type": emp_type,
                    "description": parent_text[:5000] if len(parent_text) > 60 else None,
                    "extraction_method": "tier2_links_v21",
                    "extraction_confidence": 0.72 if url_hint else 0.64,
                }
            )

        return self._dedupe_jobs_v21(jobs, page_url)

    def _extract_from_accordion_sections_v21(self, root: etree._Element, page_url: str) -> list[dict]:
        items = root.xpath(
            "//*[contains(@class,'accordion-item') or contains(@class,'elementor-accordion-item') or "
            "contains(@class,'accordion')]"
        )
        if not items:
            return []

        jobs: list[dict] = []
        for item in items[:200]:
            title_el = item.xpath(
                ".//*[contains(@class,'accordion-title') or contains(@class,'tab-title') or "
                "self::h1 or self::h2 or self::h3 or self::h4 or self::button]"
            )
            if not title_el:
                continue

            title = self._normalize_title_v21(_text(title_el[0]))
            if not self._is_valid_title_v21(title):
                continue

            if not self._title_has_job_signal_v21(title):
                continue

            link_el = item.xpath(".//a[@href]")
            link_href = link_el[0].get("href") if link_el else None
            source_url = _resolve_url(link_href, page_url) if link_href else page_url
            if source_url and self._is_non_job_url_v21(source_url):
                source_url = page_url

            item_text = _text(item)[:1800]

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url or page_url,
                    "location_raw": None,
                    "salary_raw": None,
                    "employment_type": None,
                    "description": item_text[:5000] if len(item_text) > 80 else None,
                    "extraction_method": "tier2_accordion_v21",
                    "extraction_confidence": 0.68,
                }
            )

        return self._dedupe_jobs_v21(jobs, page_url)

    def _extract_from_heading_rows_v21(self, root: etree._Element, page_url: str) -> list[dict]:
        containers = root.xpath("//section | //div | //article | //main")
        jobs: list[dict] = []

        for container in containers[:250]:
            headings = container.xpath(".//h2 | .//h3 | .//h4")
            if len(headings) < 2:
                continue

            classes = _get_el_classes(container)
            container_text = _text(container)[:4000]
            apply_hits = len(re.findall(r"\bapply\b", container_text, re.IGNORECASE))
            has_row_hint = bool(_ROW_CLASS_PATTERN_V21.search(classes))
            if not has_row_hint and apply_hits == 0:
                continue

            local_jobs: list[dict] = []
            for h in headings[:40]:
                title = self._normalize_title_v21(_text(h))
                if not self._is_valid_title_v21(title):
                    continue
                if not self._title_has_job_signal_v21(title):
                    continue

                link = h.xpath(".//a[@href]")
                if not link:
                    link = h.xpath("following::a[@href][1]")
                href = link[0].get("href") if link else None
                source_url = _resolve_url(href, page_url) or page_url
                if self._is_non_job_url_v21(source_url):
                    source_url = page_url

                local_jobs.append(
                    {
                        "title": title,
                        "source_url": source_url,
                        "location_raw": None,
                        "salary_raw": None,
                        "employment_type": None,
                        "description": container_text[:5000] if len(container_text) > 120 else None,
                        "extraction_method": "tier2_heading_rows_v21",
                        "extraction_confidence": 0.66,
                    }
                )

            if len(local_jobs) >= 2:
                jobs.extend(local_jobs)

        return self._dedupe_jobs_v21(jobs, page_url)

    # ------------------------------------------------------------------
    # Selection / validation / helpers
    # ------------------------------------------------------------------

    def _pick_best_jobset_v21(
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
            deduped = self._dedupe_jobs_v21(jobs, page_url)
            if not deduped:
                continue

            score = self._jobset_score_v21(deduped, page_url)
            valid = self._passes_jobset_validation_v21(deduped, page_url)

            if label == "parent_v16":
                parent_score = score
                parent_jobs = deduped

            logger.debug(
                "v2.1 candidate %s: jobs=%d score=%.2f valid=%s",
                label,
                len(deduped),
                score,
                valid,
            )

            if valid and score > best_score:
                best_label = label
                best_jobs = deduped
                best_score = score

        if best_jobs:
            # Keep parent output unless fallback is clearly better.
            if parent_jobs and best_label != "parent_v16" and best_score < parent_score + 2.0:
                return "parent_v16", parent_jobs
            return best_label, best_jobs[:MAX_JOBS_PER_PAGE]

        # If nothing passes strict validation, keep parent partial if present.
        if parent_jobs:
            return "parent_v16", parent_jobs[:MAX_JOBS_PER_PAGE]

        # Final fallback: return the largest candidate after dedupe.
        largest = max(
            ((label, self._dedupe_jobs_v21(jobs, page_url)) for label, jobs in candidates),
            key=lambda item: len(item[1]),
            default=("", []),
        )
        return largest[0], largest[1][:MAX_JOBS_PER_PAGE]

    def _passes_jobset_validation_v21(self, jobs: list[dict], page_url: str) -> bool:
        if not jobs:
            return False

        titles = [self._normalize_title_v21(j.get("title", "")) for j in jobs]
        titles = [t for t in titles if self._is_valid_title_v21(t)]
        if not titles:
            return False

        unique_ratio = len(set(t.lower() for t in titles)) / max(1, len(titles))
        if len(titles) > 2 and unique_ratio < 0.6:
            return False

        reject_hits = sum(1 for t in titles if _REJECT_TITLE_PATTERN_V21.match(t.lower()))
        if reject_hits >= max(1, int(len(titles) * 0.35)):
            return False

        title_hits = sum(1 for t in titles if self._title_has_job_signal_v21(t))
        url_hits = sum(1 for j in jobs if self._is_job_like_url_v21(j.get("source_url") or page_url))

        if len(titles) == 1:
            t = titles[0]
            src = jobs[0].get("source_url") or page_url
            return (
                (self._title_has_job_signal_v21(t) and not _GENERIC_LISTING_LABEL_PATTERN_V21.match(t))
                or self._is_job_like_url_v21(src)
            )

        if len(titles) <= 3:
            return title_hits >= 1 and (url_hits >= 1 or title_hits >= 2)

        return title_hits >= max(1, int(len(titles) * 0.2)) or url_hits >= max(2, int(len(titles) * 0.25))

    def _jobset_score_v21(self, jobs: list[dict], page_url: str) -> float:
        if not jobs:
            return 0.0

        titles = [self._normalize_title_v21(j.get("title", "")) for j in jobs]
        titles = [t for t in titles if t]
        if not titles:
            return 0.0

        count = len(titles)
        title_hits = sum(1 for t in titles if self._title_has_job_signal_v21(t))
        url_hits = sum(1 for j in jobs if self._is_job_like_url_v21(j.get("source_url") or page_url))
        reject_hits = sum(1 for t in titles if _REJECT_TITLE_PATTERN_V21.match(t.lower()))
        unique_titles = len(set(t.lower() for t in titles))

        score = count * 3.5
        score += title_hits * 2.2
        score += url_hits * 1.8
        score += unique_titles * 0.7
        score -= reject_hits * 3.5
        return score

    def _dedupe_jobs_v21(self, jobs: list[dict], page_url: str) -> list[dict]:
        deduped: list[dict] = []
        seen: set[tuple[str, str]] = set()

        for job in jobs:
            title = self._normalize_title_v21(job.get("title", ""))
            if not self._is_valid_title_v21(title):
                continue

            source_url = (job.get("source_url") or "").strip()
            if not source_url:
                source_url = page_url
            if self._is_non_job_url_v21(source_url):
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

    def _normalize_title_v21(self, title: str) -> str:
        if not title:
            return ""
        t = " ".join(title.replace("\u00a0", " ").split())
        t = t.strip(" |:-\u2013\u2022")
        t = re.sub(r"\s{2,}", " ", t)
        if " - " in t and len(t) > 40:
            parts = [p.strip() for p in t.split(" - ") if p.strip()]
            if parts and self._title_has_job_signal_v21(parts[0]):
                t = parts[0]
        return t

    def _is_valid_title_v21(self, title: str) -> bool:
        if not title:
            return False
        if not TieredExtractorV16._is_valid_title_v16(title):
            if not _is_valid_title(title):
                return False

        t = title.strip()
        low = t.lower()
        if _REJECT_TITLE_PATTERN_V21.match(low):
            return False
        if _GENERIC_LISTING_LABEL_PATTERN_V21.match(t):
            return False

        words = t.split()
        if len(words) > 12:
            return False
        return True

    def _title_has_job_signal_v21(self, title: str) -> bool:
        if not title:
            return False
        if _title_has_job_noun(title):
            return True
        return bool(_TITLE_HINT_PATTERN_V21.search(title))

    def _is_job_like_url_v21(self, src: str) -> bool:
        if not src:
            return False
        if self._is_non_job_url_v21(src):
            return False
        return bool(_JOB_URL_HINT_PATTERN_V21.search(src))

    def _is_non_job_url_v21(self, src: str) -> bool:
        return bool(_NON_JOB_URL_PATTERN_V21.search((src or "").lower()))
