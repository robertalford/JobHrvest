"""
Tiered Extraction Engine v3.7 — direct from v1.6 with state-first shell recovery.

High-impact improvements:
1. Host-diversified Martian/MyRecruitmentPlus probing plan: probe across ATS hosts
   early instead of exhausting same-host endpoints first.
2. Stronger script-chunk endpoint hint harvesting: prioritize page/client chunks and
   scan more first-party scripts before probing.
3. Next.js app-shell recovery upgrades: add _next/data probes to the fast pass for
   config-only boards with empty rendered DOM.
4. Candidate tie-break hardening: when coverage is equivalent, prefer structured
   Elementor card extraction over noisier heading-order output.
5. High-confidence JSON salvage retained for mixed payload APIs.
"""

from __future__ import annotations

import asyncio
import html as html_lib
import json
import logging
import re
from collections import defaultdict, deque
from typing import Any, Optional
from urllib.parse import parse_qsl, urlencode, urlparse

import httpx
from lxml import etree

from app.crawlers.tiered_extractor_v16 import TieredExtractorV16, _title_has_job_noun
from app.crawlers.tiered_extractor import (
    _JOB_TYPE_PATTERN,
    _SALARY_PATTERN,
    _AU_LOCATIONS,
    _get_el_classes,
    _parse_html,
    _resolve_url,
    _text,
    MAX_JOBS_PER_PAGE,
    MIN_JOBS_FOR_SUCCESS,
)

logger = logging.getLogger(__name__)


_ROLE_HINT_PATTERN_V33 = re.compile(
    r"\b(?:engineer|developer|manager|director|analyst|specialist|assistant|"
    r"consultant|coordinator|officer|administrator|accountant|technician|"
    r"designer|architect|operator|supervisor|controller|advisor|nurse|teacher|chef|driver|"
    r"recruit(?:er|ment)?|executive|intern(?:ship)?|graduate|trainee|"
    r"influencer|fotografer|videografer|akuntan|konsultan|asisten|staf|staff|"
    r"pegawai|karyawan|psycholog|psikolog(?:i)?|customer\s+service|model|sarjana|activator)\b",
    re.IGNORECASE,
)

_REJECT_TITLE_PATTERN_V33 = re.compile(
    r"^(?:"
    r"jobs?|careers?|open\s+roles?|open\s+positions?|all\s+jobs?|current\s+jobs?|"
    r"current\s+vacancies|job\s+openings?|search\s+jobs?|browse\s+jobs?|"
    r"view\s+all\s+jobs?|join\s+our\s+team|career\s+opportunities|"
    r"apply(?:\s+now|\s+here)?|read\s+more|learn\s+more|show\s+more|info\s+lengkap|"
    r"about\s+us|our\s+team|contact|privacy|terms|login|register|"
    r"job\s+description|internship\s+details|no\s+jobs?\s+found"
    r"|lowongan\s+kerja(?:\s+\w+){0,3}"
    r"|business\s+model|size\s*&\s*fit|shipping\s*&\s*returns?|cart|wishlist|"
    r"our\s+values|talent\s+stories?|get\s+started|sign\s+up\s+for\s+alerts?"
    r"track\s+order|returns?"
    r")$",
    re.IGNORECASE,
)

_NOISY_CTA_TITLE_PATTERN_V35 = re.compile(
    r"(?:^|\b)(?:our\s+values|talent\s+stories?|get\s+started|"
    r"sign\s+up\s+for\s+alerts?|alerts?\s+signup|sage\s+careers)\b",
    re.IGNORECASE,
)

_COMPANY_CAREER_LABEL_PATTERN_V35 = re.compile(
    r"^[a-z0-9&.,'() -]{2,70}\s+careers?$",
    re.IGNORECASE,
)

_GENERIC_SINGLE_TITLE_PATTERN_V33 = re.compile(
    r"^(?:internship|intern|vacancy|vacancies|positions?|roles?|jobs?)$",
    re.IGNORECASE,
)

_JOB_URL_HINT_PATTERN_V33 = re.compile(
    r"(?:/jobview/|/jobs?/[^/?#]{3,}|/career|/careers|/position|/positions|"
    r"/vacanc|/opening|/openings|/requisition|/requisitions|"
    r"/jobdetails|/p/[a-z0-9_-]{6,}|jobid=|job_id=|requisitionid=|positionid=|"
    r"ajid=|jobadid=|adid=|vacancyid=|"
    r"candidateportal|portal\.na|applicationform|embed-jobs|lowongan|karir|karier)",
    re.IGNORECASE,
)

_JOB_DETAILISH_URL_PATTERN_V33 = re.compile(
    r"(?:/jobview/[a-z0-9-]+/[0-9a-f-]{8,}|/jobs?/[a-z0-9][^/?#]{4,}|"
    r"/jobdetails(?:/|$|\?)|/p/[a-z0-9_-]{6,}|fRecruit__ApplyJob|"
    r"[?&](?:jobid|job_id|requisitionid|positionid|vacancyno|ajid|jobadid|adid|vacancyid|postid|jobcode)=[A-Za-z0-9_-]{2,})",
    re.IGNORECASE,
)

_NON_JOB_URL_PATTERN_V33 = re.compile(
    r"(?:/(?:privacy|terms|cookie|news|blog|about|contact|investor|"
    r"help|login|logout|register|account|team|culture|collections|products|cart)(?:/|$|[?#])|"
    r"fRecruit__ApplyRegister|fRecruit__ApplyExpressInterest|"
    r"(?:sign-?up|subscribe|job-alert|talent-story|our-values)(?:/|$|[?#])|"
    r"wp-json|/feed(?:/|$)|/rss(?:/|$)|"
    r"\.pdf(?:$|\?)|\.docx?(?:$|\?))",
    re.IGNORECASE,
)

_ROW_CLASS_PATTERN_V33 = re.compile(
    r"(?:job|position|vacanc|opening|requisition|career|posting|listing|accordion)",
    re.IGNORECASE,
)

_APPLY_CONTEXT_PATTERN_V33 = re.compile(
    r"(?:apply|application|apply\s+now|apply\s+here|mailto:|"
    r"job\s+description|requirements?|qualifications?|closing\s+date|"
    r"full\s*time|part\s*time|contract|permanent|temporary|remote|hybrid|"
    r"cara\s+melamar|how\s+to\s+apply|info\s+lengkap)",
    re.IGNORECASE,
)

_DESCRIPTION_CUT_PATTERN_V33 = re.compile(
    r"\b(?:how\s+to\s+apply|cara\s+melamar|application\s+process|"
    r"instructions?\s+to\s+apply|cara\s+pendaftaran|sign\s+up\s+for\s+alerts?)\b",
    re.IGNORECASE,
)

_RICH_CONTEXT_PATTERN_V35 = re.compile(
    r"(?:responsibilit|requirement|qualification|experience|salary|benefit|"
    r"full[\s-]*time|part[\s-]*time|contract|permanent|temporary|remote|hybrid|"
    r"closing\s+date|location|mailto:|apply)",
    re.IGNORECASE,
)

_LOCATION_HINT_PATTERN_V33 = re.compile(r"\b(?:location|lokasi|kota|city|office|region)\b", re.IGNORECASE)
_BOILERPLATE_BOUNDARY_PATTERN_V33 = re.compile(
    r"\b(?:apply\s+now|learn\s+more|view\s+all|see\s+all|load\s+more|next\s+page|"
    r"previous|cookie|privacy|terms|sign\s+in|log\s+in|subscribe|follow\s+us|"
    r"read\s+more|show\s+more|about\s+us|contact\s+us|our\s+team|home|menu|search|"
    r"close|back|join\s+us\s+now|view\s+openings|come\s+work\s+with\s+us)\b",
    re.IGNORECASE,
)
_TITLE_PHONE_PATTERN_V33 = re.compile(r"^[\d\s\-\+\(\)\.]{7,}$")
_TITLE_MOSTLY_NUMERIC_PATTERN_V33 = re.compile(r"^[\d\s\-\.\,\#\:\/]{4,}$")
_ACRONYM_TITLE_PATTERN_V33 = re.compile(r"^[A-Z][A-Z0-9&/\-\+]{1,9}$")
_MARTIAN_CLIENT_PATTERN_V33 = re.compile(r'"clientCode"\s*:\s*"([a-z0-9-]{3,})"', re.IGNORECASE)
_MARTIAN_RECRUITER_PATTERN_V33 = re.compile(r'"recruiterId"\s*:\s*([0-9]{2,})', re.IGNORECASE)
_MARTIAN_THEME_PATTERN_V33 = re.compile(r'"jobBoardThemeId"\s*:\s*([0-9]{2,})', re.IGNORECASE)
_MARTIAN_NAME_PATTERN_V33 = re.compile(r'"name"\s*:\s*"([^"]{2,40})"', re.IGNORECASE)
_MARTIAN_HOST_HINT_PATTERN_V33 = re.compile(
    r"https?://[A-Za-z0-9\.-]*(?:martianlogic|myrecruitmentplus)\.[^\"'\s<>]+",
    re.IGNORECASE,
)
_MARTIAN_ENDPOINT_HINT_PATTERN_V35 = re.compile(
    r"(?:https?://[A-Za-z0-9\.-]+)?/"
    r"(?:api|job-board|jobboard|embed-jobs|jobs/search|job-ads|jobads)"
    r"[A-Za-z0-9_\-/?=&.%]*",
    re.IGNORECASE,
)
_NEXT_DATA_PATTERN_V33 = re.compile(
    r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>\s*(\{.*?\})\s*</script>',
    re.IGNORECASE | re.DOTALL,
)
_ORACLE_SITE_PATTERN_V33 = re.compile(r"/sites/([A-Za-z0-9_]+)/", re.IGNORECASE)
_ORACLE_SITE_NUMBER_PATTERN_V33 = re.compile(r"siteNumber\s*[:=]\s*['\"]([A-Za-z0-9_]+)", re.IGNORECASE)
_ORACLE_API_BASE_PATTERN_V33 = re.compile(r"apiBaseUrl\s*:\s*['\"](https?://[^'\"\\s]+)", re.IGNORECASE)
_LOCATION_SUFFIX_HINT_PATTERN_V33 = re.compile(
    r"(?:remote|hybrid|onsite|metropolitan|region|india|indonesia|australia|malaysia|singapore|"
    r"philippines|thailand|vietnam|japan|korea|china|taiwan|new\s+zealand|uk|united\s+kingdom|"
    r"us|usa|united\s+states|canada|europe)",
    re.IGNORECASE,
)

_TITLE_META_TAIL_PATTERN_V35 = re.compile(
    r"(?:\bdeadline\s*:\s*[\d./-]+.*$|\bclosing\s+date\s*:\s*[\d./-]+.*$|"
    r"\bposted\s+date\s*:\s*[\d./-]+.*$|\b(?:permanent|temporary|contract|"
    r"casual|full[\s-]*time|part[\s-]*time|trainee|internship)\s+employee\b.*$)",
    re.IGNORECASE,
)

_QUERY_VARIANT_PATH_HINT_V35 = re.compile(
    r"(?:/search(?:/|$)|/jobs(?:/|$)|/vacanc|/requisition|/opening|/career)",
    re.IGNORECASE,
)
_SCRIPT_SRC_PATTERN_V37 = re.compile(
    r"<script[^>]+src=['\"]([^\"']+)['\"][^>]*>",
    re.IGNORECASE,
)
_MARTIAN_RECRUITER_ENDPOINT_JS_PATTERN_V37 = re.compile(
    r"/api/recruiter/[0-9]{2,}/(?:jobAds|jobads|job-ads|jobs)"
    r"(?:/[A-Za-z0-9_-]+)?(?:\?[A-Za-z0-9=&_%.\-]+)?",
    re.IGNORECASE,
)
_LOCATION_TAIL_STRICT_PATTERN_V37 = re.compile(
    r"(?:\b(?:remote|hybrid|onsite|on-site|work\s+from\s+home|wfh|home\s+based|"
    r"metropolitan|region|district|province|state|county|office|city|area)\b|"
    r"\b(?:us|usa|uk|uae|sg|id|au|nz|ca)\b|"
    r"\b[A-Z][a-z]+,\s*[A-Z]{2,3}\b|"
    r"\b[A-Z]{2,3}\b)$"
)


class TieredExtractorV37(TieredExtractorV16):
    """v3.7 extractor with coverage-first fallbacks and stricter validation."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        url = career_page.url if hasattr(career_page, "url") else str(career_page)
        working_html = html or ""

        candidates: list[tuple[str, list[dict]]] = []

        parent_jobs: list[dict] = []
        try:
            parent_jobs = await asyncio.wait_for(super().extract(career_page, company, working_html), timeout=24.0)
        except asyncio.TimeoutError:
            logger.warning("v3.7 parent extractor timeout for %s", url)
        except Exception:
            logger.exception("v3.7 parent extractor failed for %s", url)

        parent_jobs = self._dedupe_jobs_v37(parent_jobs or [], url)
        if parent_jobs:
            candidates.append(("parent_v16", parent_jobs))

        jsonld_jobs = self._extract_jsonld_jobs_v37(working_html, url)
        if jsonld_jobs:
            candidates.append(("jsonld_v37", jsonld_jobs))

        root = _parse_html(working_html)
        if root is not None:
            prose_jobs = self._extract_prose_heading_blocks_v37(root, url)
            if prose_jobs:
                candidates.append(("prose_blocks_v37", prose_jobs))

            elementor_jobs = self._extract_elementor_cards_v37(root, url)
            if elementor_jobs:
                candidates.append(("elementor_cards_v37", elementor_jobs))

            heading_jobs = self._extract_heading_rows_v37(root, url)
            if heading_jobs:
                candidates.append(("heading_rows_v37", heading_jobs))

            link_jobs = self._extract_job_links_v37(root, url)
            if link_jobs:
                candidates.append(("job_links_v37", link_jobs))

            grouped_rows = self._extract_repeating_row_groups_v37(root, url)
            if grouped_rows:
                candidates.append(("row_groups_v37", grouped_rows))

        martian_jobs: list[dict] = []
        try:
            martian_jobs = await asyncio.wait_for(self._extract_martian_jobs_v37(url, working_html), timeout=14.0)
        except asyncio.TimeoutError:
            logger.debug("v3.7 martian fallback timeout for %s", url)
        except Exception:
            logger.debug("v3.7 martian fallback failed for %s", url)
        if martian_jobs:
            candidates.append(("martian_api_v37", martian_jobs))

        oracle_jobs: list[dict] = []
        try:
            oracle_jobs = await asyncio.wait_for(self._extract_oracle_jobs_v37(url, working_html), timeout=12.0)
        except asyncio.TimeoutError:
            logger.debug("v3.7 oracle fallback timeout for %s", url)
        except Exception:
            logger.debug("v3.7 oracle fallback failed for %s", url)
        if oracle_jobs:
            candidates.append(("oracle_api_v37", oracle_jobs))

        if not candidates or max((len(jobs) for _label, jobs in candidates), default=0) < MIN_JOBS_FOR_SUCCESS:
            try:
                query_variant_jobs = await asyncio.wait_for(
                    self._extract_query_variant_jobs_v37(url, working_html), timeout=8.0
                )
            except asyncio.TimeoutError:
                query_variant_jobs = []
            except Exception:
                query_variant_jobs = []
            if query_variant_jobs:
                candidates.append(("query_variants_v37", query_variant_jobs))

        best_label, best_jobs = self._pick_best_jobset_v37(candidates, url)
        if not best_jobs:
            return []

        if best_label != "parent_v16" and any(self._is_job_like_url_v37(j.get("source_url") or "") for j in best_jobs):
            try:
                best_jobs = await asyncio.wait_for(self._enrich_from_detail_pages(best_jobs), timeout=18.0)
            except asyncio.TimeoutError:
                logger.warning("v3.7 enrichment timeout for %s", url)
            except Exception:
                logger.exception("v3.7 enrichment failed for %s", url)
            best_jobs = self._dedupe_jobs_v37(best_jobs, url)

        best_jobs = self._postprocess_jobs_v37(best_jobs, url)
        return best_jobs[:MAX_JOBS_PER_PAGE]

    # ------------------------------------------------------------------
    # Candidate extraction
    # ------------------------------------------------------------------

    def _extract_heading_rows_v37(self, root: etree._Element, page_url: str) -> list[dict]:
        containers = root.xpath("//section | //article | //main | //div")
        jobs: list[dict] = []

        for container in containers[:280]:
            classes = _get_el_classes(container)
            if re.search(r"\b(menu|navbar|header|footer|breadcrumb|sitemap)\b", classes):
                continue

            children = [
                c for c in container
                if isinstance(c.tag, str) and c.tag.lower() not in ("script", "style", "noscript", "svg")
            ]
            if len(children) < 2:
                continue

            row_candidates = [c for c in children if c.xpath(".//h2 | .//h3 | .//h4")]
            if len(row_candidates) < 2:
                continue
            if len(row_candidates) > 100:
                continue

            local_jobs: list[dict] = []
            evidence_hits = 0
            role_hits = 0

            for row in row_candidates[:80]:
                heading_nodes = row.xpath(".//h2 | .//h3 | .//h4")
                if not heading_nodes:
                    continue
                title = self._normalize_title_v37(_text(heading_nodes[0]))
                if not self._is_valid_title_v37(title):
                    continue

                link_nodes = heading_nodes[0].xpath(".//a[@href]")
                if not link_nodes:
                    link_nodes = row.xpath(".//a[@href][1]")
                href = link_nodes[0].get("href") if link_nodes else None
                source_url = _resolve_url(href, page_url) or page_url
                if self._is_non_job_url_v37(source_url):
                    source_url = page_url

                row_text = _text(row)[:9000]
                if len(row_text) < 160:
                    sibling = row.getnext()
                    if sibling is not None and isinstance(sibling.tag, str):
                        sibling_tag = sibling.tag.lower()
                        if sibling_tag not in {"h1", "h2", "h3", "h4", "h5"}:
                            row_text = f"{row_text} {_text(sibling)[:7000]}".strip()
                if len(row_text) < 45:
                    continue

                same_page = source_url.rstrip("/") == page_url.rstrip("/")
                apply_hint = bool(_APPLY_CONTEXT_PATTERN_V33.search(row_text))
                long_detail = len(row_text) >= 160
                job_url_hint = self._is_job_like_url_v37(source_url) or bool(_JOB_DETAILISH_URL_PATTERN_V33.search(source_url))

                if same_page and not (apply_hint or long_detail):
                    continue
                if not (job_url_hint or apply_hint or long_detail):
                    continue

                has_role = self._title_has_role_signal_v37(title)
                if not has_role and not (
                    (job_url_hint and len(title.split()) >= 2)
                    or (job_url_hint and self._is_acronym_title_v37(title))
                ):
                    continue

                location = self._extract_location_v37(row, title)
                description = self._clean_description_v37(row_text)

                local_jobs.append(
                    {
                        "title": title,
                        "source_url": source_url,
                        "location_raw": location,
                        "salary_raw": self._extract_salary_v37(row_text),
                        "employment_type": self._extract_employment_type_v37(row_text),
                        "description": description,
                        "extraction_method": "tier2_heading_rows_v37",
                        "extraction_confidence": 0.7 if (job_url_hint or apply_hint) else 0.64,
                    }
                )
                if has_role:
                    role_hits += 1
                if job_url_hint or apply_hint or long_detail:
                    evidence_hits += 1

            if len(local_jobs) < MIN_JOBS_FOR_SUCCESS:
                continue
            if role_hits < max(1, int(len(local_jobs) * 0.6)):
                continue
            if evidence_hits < max(1, int(len(local_jobs) * 0.6)):
                continue
            jobs.extend(local_jobs)

        jobs = self._dedupe_jobs_v37(jobs, page_url)
        if not self._passes_jobset_validation_v37(jobs, page_url):
            return []
        return jobs

    def _extract_prose_heading_blocks_v37(self, root: etree._Element, page_url: str) -> list[dict]:
        containers = root.xpath(
            "//*[contains(@class,'prose') or contains(@class,'entry-content') "
            "or contains(@class,'field--name-body') or contains(@class,'rich-text')]"
        )
        jobs: list[dict] = []

        for container in containers[:160]:
            headings = container.xpath(".//h2 | .//h3 | .//h4")
            if len(headings) < 2 or len(headings) > 60:
                continue

            for heading in headings:
                title = self._normalize_title_v37(_text(heading))
                if not self._is_valid_title_v37(title):
                    continue
                if not self._title_has_role_signal_v37(title):
                    continue

                block_parts = [_text(heading)]
                sibling = heading.getnext()
                steps = 0
                while sibling is not None and steps < 14:
                    if isinstance(sibling.tag, str) and sibling.tag.lower() in {"h1", "h2", "h3", "h4", "h5"}:
                        break
                    block_parts.append(_text(sibling))
                    sibling = sibling.getnext()
                    steps += 1

                block_text = " ".join(part.strip() for part in block_parts if part and part.strip())
                if len(block_text) < 180:
                    continue
                if not self._prose_block_has_job_evidence_v37(block_text):
                    continue

                jobs.append(
                    {
                        "title": title,
                        "source_url": page_url,
                        "location_raw": self._extract_location_v37(container, title),
                        "salary_raw": self._extract_salary_v37(block_text),
                        "employment_type": self._extract_employment_type_v37(block_text),
                        "description": self._clean_description_v37(block_text),
                        "extraction_method": "tier2_prose_blocks_v37",
                        "extraction_confidence": 0.73,
                    }
                )

        jobs = self._dedupe_jobs_v37(jobs, page_url)
        if not self._passes_jobset_validation_v37(jobs, page_url):
            return []
        return jobs

    def _extract_job_links_v37(self, root: etree._Element, page_url: str) -> list[dict]:
        jobs: list[dict] = []

        anchors = root.xpath("//a[@href]")
        for a_el in anchors[:6000]:
            href = (a_el.get("href") or "").strip()
            if not href or href.startswith("#") or href.startswith("javascript:"):
                continue

            source_url = _resolve_url(href, page_url)
            if not source_url or self._is_non_job_url_v37(source_url):
                continue

            heading_nodes = a_el.xpath(".//h1 | .//h2 | .//h3 | .//h4 | .//h5")
            title_raw = _text(heading_nodes[0]) if heading_nodes else (_text(a_el) or (a_el.get("title") or ""))
            title = self._normalize_title_v37(title_raw)
            if not self._is_valid_title_v37(title):
                continue

            context_el = a_el
            for _ in range(3):
                parent = context_el.getparent()
                if parent is None:
                    break
                parent_text = _text(parent)
                if len(parent_text) >= 160 or _ROW_CLASS_PATTERN_V33.search(_get_el_classes(parent)):
                    context_el = parent
                    break
                context_el = parent

            context_text = _text(context_el)[:2600]
            apply_hint = bool(_APPLY_CONTEXT_PATTERN_V33.search(context_text))
            job_url_hint = self._is_job_like_url_v37(source_url)
            strong_job_url_hint = self._is_strong_job_url_v37(source_url)
            same_page = source_url.rstrip("/") == page_url.rstrip("/")

            if same_page and not apply_hint:
                continue
            if not (job_url_hint or apply_hint):
                continue

            has_role = self._title_has_role_signal_v37(title)
            if not has_role and not (
                (strong_job_url_hint and len(title.split()) >= 2)
                or (strong_job_url_hint and self._is_acronym_title_v37(title))
            ):
                continue

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_location_v37(context_el, title),
                    "salary_raw": self._extract_salary_v37(context_text),
                    "employment_type": self._extract_employment_type_v37(context_text),
                    "description": self._clean_description_v37(context_text),
                    "extraction_method": "tier2_job_links_v37",
                    "extraction_confidence": 0.74 if job_url_hint else 0.67,
                }
            )
            if len(jobs) >= MAX_JOBS_PER_PAGE:
                break

        jobs = self._dedupe_jobs_v37(jobs, page_url)
        if not self._passes_jobset_validation_v37(jobs, page_url):
            return []
        return jobs

    def _extract_repeating_row_groups_v37(self, root: etree._Element, page_url: str) -> list[dict]:
        groups: dict[str, list[etree._Element]] = defaultdict(list)

        for el in root.iter():
            if not isinstance(el.tag, str):
                continue
            tag = el.tag.lower()
            if tag not in {"li", "div", "article", "tr", "section"}:
                continue
            classes = _get_el_classes(el)
            if not classes or not _ROW_CLASS_PATTERN_V33.search(classes):
                continue
            tokens = classes.split()
            sig = f"{tag}:{' '.join(tokens[:2])}" if tokens else tag
            groups[sig].append(el)

        jobs: list[dict] = []
        for rows in groups.values():
            if len(rows) < 3:
                continue
            for row in rows[:MAX_JOBS_PER_PAGE]:
                job = self._extract_heuristic_job_v16(row, page_url, 12)
                if not job:
                    continue

                title = self._normalize_title_v37(job.get("title") or "")
                source_url = (job.get("source_url") or page_url).strip() or page_url
                row_text = _text(row)[:3500]
                apply_hint = bool(_APPLY_CONTEXT_PATTERN_V33.search(row_text))
                has_role = self._title_has_role_signal_v37(title)
                job_url_hint = self._is_job_like_url_v37(source_url)

                if not self._is_valid_title_v37(title):
                    continue
                if self._is_non_job_url_v37(source_url):
                    continue
                if not (job_url_hint or apply_hint or len(row_text) >= 180):
                    continue
                if not has_role and not (apply_hint or (job_url_hint and self._is_acronym_title_v37(title))):
                    continue

                jobs.append(
                    {
                        "title": title,
                        "source_url": source_url,
                        "location_raw": job.get("location_raw") or self._extract_location_v37(row, title),
                        "salary_raw": job.get("salary_raw") or self._extract_salary_v37(row_text),
                        "employment_type": job.get("employment_type") or self._extract_employment_type_v37(row_text),
                        "description": self._clean_description_v37(row_text),
                        "extraction_method": "tier2_row_groups_v37",
                        "extraction_confidence": 0.72 if job_url_hint else 0.65,
                    }
                )
                if len(jobs) >= MAX_JOBS_PER_PAGE:
                    break

        jobs = self._dedupe_jobs_v37(jobs, page_url)
        if not self._passes_jobset_validation_v37(jobs, page_url):
            return []
        return jobs

    def _extract_elementor_cards_v37(self, root: etree._Element, page_url: str) -> list[dict]:
        cards = root.xpath(
            "//*[contains(@class,'elementor-column') and contains(@class,'elementor-inner-column')]"
        )
        if len(cards) < 2:
            return []

        jobs: list[dict] = []
        for card in cards[:320]:
            heading_nodes = card.xpath(".//h2[contains(@class,'elementor-heading-title')][1]")
            if not heading_nodes:
                continue
            title = self._normalize_title_v37(_text(heading_nodes[0]))
            if not self._is_valid_title_v37(title):
                continue
            if not self._title_has_role_signal_v37(title):
                continue

            source_url = page_url
            for a_el in card.xpath(".//a[@href]"):
                href = _resolve_url(a_el.get("href"), page_url)
                if not href or self._is_non_job_url_v37(href):
                    continue
                if self._is_job_like_url_v37(href):
                    source_url = href
                    break
                if source_url == page_url:
                    source_url = href

            card_text = _text(card)[:2800]
            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": self._extract_location_v37(card, title),
                    "salary_raw": self._extract_salary_v37(card_text),
                    "employment_type": self._extract_employment_type_v37(card_text),
                    "description": self._clean_description_v37(card_text),
                    "extraction_method": "tier2_elementor_cards_v37",
                    "extraction_confidence": 0.74,
                }
            )
            if len(jobs) >= MAX_JOBS_PER_PAGE:
                break

        jobs = self._dedupe_jobs_v37(jobs, page_url)
        if not self._passes_jobset_validation_v37(jobs, page_url):
            return []
        return jobs

    def _extract_jsonld_jobs_v37(self, html_body: str, page_url: str) -> list[dict]:
        jobs: list[dict] = []
        for match in re.finditer(
            r"<script[^>]+type=['\"]application/ld\+json['\"][^>]*>(.*?)</script>",
            html_body or "",
            re.IGNORECASE | re.DOTALL,
        ):
            payload = html_lib.unescape((match.group(1) or "").strip())
            if not payload:
                continue
            try:
                parsed = json.loads(payload)
            except Exception:
                continue
            jobs.extend(
                self._extract_jobs_from_json_obj_v37(
                    parsed,
                    page_url,
                    method="tier0_jsonld_v37",
                    require_job_type=True,
                )
            )

        jobs = self._dedupe_jobs_v37(jobs, page_url)
        if not self._passes_jobset_validation_v37(jobs, page_url):
            return []
        return jobs

    # ------------------------------------------------------------------
    # Config-shell fallbacks
    # ------------------------------------------------------------------

    async def _extract_martian_jobs_v37(self, page_url: str, html_body: str) -> list[dict]:
        lower = (html_body or "").lower()
        if (
            "__next_data__" not in lower
            and "myrecruitmentplus" not in lower
            and "martianlogic" not in lower
            and "clientcode" not in lower
        ):
            return []

        context = self._extract_martian_context_v37(html_body, page_url)
        client_code = (context.get("client_code") or "").strip()
        recruiter_id = (context.get("recruiter_id") or "").strip()
        if not client_code and not recruiter_id:
            return []

        # Pull API hints from first-party script chunks for config-only Next.js shells.
        extra_hints = await self._harvest_script_endpoint_hints_v37(page_url, html_body)
        if extra_hints:
            merged_hints: list[str] = []
            seen_hints: set[str] = set()
            for value in [*(context.get("endpoint_hints") or "").split("|"), *extra_hints]:
                hint = (value or "").strip()
                if not hint:
                    continue
                norm = hint.rstrip("/")
                if norm in seen_hints:
                    continue
                seen_hints.add(norm)
                merged_hints.append(hint)
            context["endpoint_hints"] = "|".join(merged_hints[:24])

        endpoints = self._martian_probe_urls_v37(page_url, context)
        if not endpoints:
            return []
        shell_like = (
            "__next_data__" in lower
            and "<div id=\"__next\"></div>" in lower
            and not re.search(r"(?:jobview|jobdetails|class=['\"][^'\"]*(?:job|vacanc|position))", lower)
        )
        probe_plan = self._martian_endpoint_plan_v37(endpoints, aggressive=shell_like)
        if not probe_plan:
            return []

        jobs: list[dict] = []
        request_count = 0
        max_requests = 12 if shell_like else 8

        try:
            async with httpx.AsyncClient(
                timeout=1.6,
                follow_redirects=True,
                headers={
                    "Accept": "application/json,text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
                    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                },
            ) as client:
                for endpoint in probe_plan:
                    for probe_url in self._martian_paged_variants_v37(endpoint)[:2]:
                        if request_count >= max_requests:
                            break
                        request_count += 1
                        try:
                            resp = await asyncio.wait_for(client.get(probe_url), timeout=1.8)
                        except Exception:
                            continue
                        if resp.status_code >= 400 or not resp.text:
                            continue

                        extracted = self._extract_jobs_from_probe_payload_v37(resp.text, str(resp.url), page_url)
                        if extracted:
                            jobs.extend(extracted)
                            if len(jobs) >= MAX_JOBS_PER_PAGE:
                                break
                    if request_count >= max_requests or len(jobs) >= MAX_JOBS_PER_PAGE:
                        break
                    if len(jobs) >= MIN_JOBS_FOR_SUCCESS and request_count >= 5:
                        break

                if request_count < max_requests and len(jobs) < MIN_JOBS_FOR_SUCCESS:
                    post_endpoints = self._martian_post_endpoints_v37(probe_plan)
                    payloads = self._martian_post_payloads_v37(context)
                    for post_url in post_endpoints:
                        for payload in payloads:
                            if request_count >= max_requests:
                                break
                            request_count += 1
                            try:
                                resp = await asyncio.wait_for(client.post(post_url, json=payload), timeout=1.8)
                            except Exception:
                                continue
                            if resp.status_code >= 400 or not resp.text:
                                continue

                            extracted = self._extract_jobs_from_probe_payload_v37(resp.text, str(resp.url), page_url)
                            if extracted:
                                jobs.extend(extracted)
                                if len(jobs) >= MAX_JOBS_PER_PAGE:
                                    break
                        if request_count >= max_requests or len(jobs) >= MAX_JOBS_PER_PAGE:
                            break
        except Exception:
            logger.debug("v3.7 martian probing failed for %s", page_url)

        jobs = self._dedupe_jobs_v37(jobs, page_url)
        if jobs and not self._passes_jobset_validation_v37(jobs, page_url):
            jobs = self._salvage_high_confidence_jobs_v37(jobs, page_url)
        if not jobs or not self._passes_jobset_validation_v37(jobs, page_url):
            return []
        return jobs

    async def _extract_oracle_jobs_v37(self, page_url: str, html_body: str) -> list[dict]:
        page_l = (page_url or "").lower()
        body_l = (html_body or "").lower()
        if "oraclecloud.com" not in page_l and "candidateexperience" not in body_l and "hcmrestapi" not in body_l:
            return []

        parsed = urlparse(page_url)
        if not parsed.netloc:
            return []

        api_base = f"{parsed.scheme or 'https'}://{parsed.netloc}"
        api_match = _ORACLE_API_BASE_PATTERN_V33.search(html_body or "")
        if api_match:
            api_base = api_match.group(1).rstrip("/")

        site_ids = self._oracle_site_ids_v37(page_url, html_body)
        if not site_ids:
            return []

        site_candidates: list[tuple[str, list[dict]]] = []
        try:
            async with httpx.AsyncClient(timeout=8, follow_redirects=True) as client:
                for site_id in site_ids[:10]:
                    site_jobs: list[dict] = []
                    for offset in range(0, 264, 24):
                        finder_variants = [
                            (
                                f"findReqs;siteNumber={site_id},"
                                "facetsList=LOCATIONS;WORK_LOCATIONS;TITLES;CATEGORIES;POSTING_DATES,"
                                f"limit=24,offset={offset}"
                            ),
                            f"findReqs;siteNumber={site_id},limit=24,offset={offset}",
                        ]

                        found_for_offset = False
                        for finder in finder_variants:
                            query = urlencode(
                                {
                                    "onlyData": "true",
                                    "expand": "requisitionList.secondaryLocations",
                                    "finder": finder,
                                }
                            )
                            api_url = f"{api_base}/hcmRestApi/resources/latest/recruitingCEJobRequisitions?{query}"
                            try:
                                resp = await client.get(api_url)
                            except Exception:
                                continue
                            if resp.status_code >= 400:
                                continue
                            try:
                                data = resp.json()
                            except Exception:
                                continue

                            batch = self._extract_oracle_items_v37(data, page_url, site_id)
                            if not batch:
                                continue
                            site_jobs.extend(batch)
                            found_for_offset = True
                            if len(batch) < 24:
                                break
                        if not found_for_offset:
                            break

                        if len(site_jobs) >= MAX_JOBS_PER_PAGE:
                            break

                    deduped = self._dedupe_jobs_v37(site_jobs, page_url)
                    if deduped:
                        site_candidates.append((site_id, deduped))
        except Exception:
            logger.debug("v3.7 oracle probing failed for %s", page_url)

        jobs: list[dict] = []
        if site_candidates:
            site_candidates.sort(
                key=lambda item: (
                    len(item[1]),
                    1 if re.search(r"_[0-9]+$", item[0]) else 0,
                    1 if item[0].upper().endswith("_1001") else 0,
                ),
                reverse=True,
            )
            jobs = site_candidates[0][1]

        jobs = self._dedupe_jobs_v37(jobs, page_url)
        if not self._passes_jobset_validation_v37(jobs, page_url):
            return []
        return jobs

    async def _extract_query_variant_jobs_v37(self, page_url: str, html_body: str) -> list[dict]:
        if not self._should_try_query_variants_v37(page_url, html_body):
            return []

        variant_urls = self._query_variant_urls_v37(page_url)
        if not variant_urls:
            return []

        jobs: list[dict] = []
        try:
            async with httpx.AsyncClient(
                timeout=6,
                follow_redirects=True,
                headers={
                    "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8",
                    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                },
            ) as client:
                for variant_url in variant_urls[:8]:
                    try:
                        resp = await client.get(variant_url)
                    except Exception:
                        continue
                    body = resp.text or ""
                    if resp.status_code >= 400 or len(body) < 200:
                        continue
                    extracted = self._extract_jobs_from_probe_payload_v37(body, str(resp.url), page_url)
                    if not extracted:
                        continue
                    jobs.extend(extracted)
                    if len(jobs) >= MAX_JOBS_PER_PAGE:
                        break
        except Exception:
            logger.debug("v3.7 query variant recovery failed for %s", page_url)

        jobs = self._dedupe_jobs_v37(jobs, page_url)
        if not self._passes_jobset_validation_v37(jobs, page_url):
            return []
        return jobs

    def _should_try_query_variants_v37(self, page_url: str, html_body: str) -> bool:
        parsed = urlparse(page_url or "")
        if not parsed.netloc or parsed.query:
            return False
        if _QUERY_VARIANT_PATH_HINT_V35.search(parsed.path or ""):
            return True
        lower = (html_body or "").lower()
        return any(
            token in lower
            for token in (
                "jobsearchbutton",
                "name=\"keywords\"",
                "show all jobs",
                "wicket:interface",
                "current vacancies",
            )
        )

    def _query_variant_urls_v37(self, page_url: str) -> list[str]:
        parsed = urlparse(page_url or "")
        if not parsed.netloc or parsed.query:
            return []

        base = page_url.rstrip("/")
        path = parsed.path or "/"
        base_candidates = [base]
        if path and not path.endswith("/"):
            base_candidates.append(f"{base}/")

        query_templates = [
            "search=",
            "search=&keywords=",
            "keywords=",
            "keyword=",
            "q=",
            "query=",
            "search=&page=1",
            "page=1",
        ]

        urls: list[str] = []
        for base_candidate in base_candidates:
            for query in query_templates:
                joiner = "&" if "?" in base_candidate else "?"
                urls.append(f"{base_candidate}{joiner}{query}")

        seen: set[str] = set()
        unique: list[str] = []
        for url in urls:
            if url in seen:
                continue
            seen.add(url)
            unique.append(url)
        return unique

    # ------------------------------------------------------------------
    # Probe parsing
    # ------------------------------------------------------------------

    def _extract_jobs_from_probe_payload_v37(self, body: str, response_url: str, page_url: str) -> list[dict]:
        payload = (body or "").strip()
        if not payload:
            return []

        jobs: list[dict] = []

        if payload.startswith("{") or payload.startswith("["):
            try:
                parsed = json.loads(payload)
                jobs.extend(self._extract_jobs_from_json_obj_v37(parsed, response_url, method="tier0_api_json_v37"))
            except Exception:
                pass

        root = _parse_html(payload)
        if root is not None:
            jobs.extend(self._extract_job_links_v37(root, response_url))
            jobs.extend(self._extract_elementor_cards_v37(root, response_url))
            jobs.extend(self._extract_heading_rows_v37(root, response_url))
            jobs.extend(self._extract_repeating_row_groups_v37(root, response_url))
            jobs.extend(self._extract_tier2_v16(response_url, payload) or [])
            for job in jobs:
                method = str(job.get("extraction_method") or "")
                if method.startswith("tier2_"):
                    job["extraction_method"] = f"{method}_probe_v37"

        jobs.extend(self._extract_jobs_from_js_literals_v37(payload, response_url, page_url))

        deduped = self._dedupe_jobs_v37(jobs, page_url)
        if deduped and not self._passes_jobset_validation_v37(deduped, page_url):
            recovered = self._salvage_high_confidence_jobs_v37(deduped, page_url)
            if recovered:
                return recovered
        return deduped

    def _extract_jobs_from_json_obj_v37(
        self,
        data: object,
        page_url: str,
        method: str,
        require_job_type: bool = False,
    ) -> list[dict]:
        jobs: list[dict] = []
        queue: deque[object] = deque([data])
        visited = 0

        while queue and visited < 7000:
            node = queue.popleft()
            visited += 1

            if isinstance(node, list):
                queue.extend(node[:240])
                continue
            if not isinstance(node, dict):
                continue

            queue.extend(list(node.values())[:240])
            job = self._job_from_json_dict_v37(node, page_url, method, require_job_type=require_job_type)
            if job:
                jobs.append(job)
                if len(jobs) >= MAX_JOBS_PER_PAGE:
                    break

        return jobs

    def _extract_jobs_from_js_literals_v37(self, payload: str, response_url: str, page_url: str) -> list[dict]:
        jobs: list[dict] = []
        if not payload:
            return jobs

        pattern = re.compile(
            r"(?:jobAdTitle|jobTitle|positionTitle|requisitionTitle|title)\s*:\s*['\"]([^\"']{4,180})['\"]"
            r".{0,320}?"
            r"(?:jobAdId|jobId|requisitionId|positionId|adId|vacancyId)\s*:\s*['\"]?([A-Za-z0-9_-]{2,40})",
            re.IGNORECASE | re.DOTALL,
        )
        for match in pattern.finditer(payload):
            title = self._normalize_title_v37(match.group(1) or "")
            if not self._is_valid_title_v37(title):
                continue
            if not self._title_has_role_signal_v37(title):
                continue

            ad_id = (match.group(2) or "").strip()
            if not ad_id:
                continue
            joiner = "&" if "?" in response_url else "?"
            source_url = f"{response_url}{joiner}jobAdId={ad_id}"
            source_url = _resolve_url(source_url, page_url) or page_url
            if self._is_non_job_url_v37(source_url):
                continue

            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": None,
                    "salary_raw": None,
                    "employment_type": None,
                    "description": None,
                    "extraction_method": "tier0_js_literal_v37",
                    "extraction_confidence": 0.78,
                }
            )
            if len(jobs) >= MAX_JOBS_PER_PAGE:
                break

        return jobs

    def _salvage_high_confidence_jobs_v37(self, jobs: list[dict], page_url: str) -> list[dict]:
        candidates = self._dedupe_jobs_v37(jobs, page_url)
        if not candidates:
            return []

        filtered = [
            job
            for job in candidates
            if self._title_has_role_signal_v37(self._normalize_title_v37(job.get("title", "")))
            and self._job_has_strong_evidence_v37(job, page_url)
        ]
        filtered = self._dedupe_jobs_v37(filtered, page_url)
        if not filtered or len(filtered) < MIN_JOBS_FOR_SUCCESS:
            return []

        if self._passes_jobset_validation_v37(filtered, page_url):
            return filtered

        role_ratio = sum(
            1
            for j in filtered
            if self._title_has_role_signal_v37(self._normalize_title_v37(j.get("title", "")))
        ) / max(1, len(filtered))
        strong_ratio = sum(1 for j in filtered if self._job_has_strong_evidence_v37(j, page_url)) / max(1, len(filtered))
        if role_ratio >= 0.8 and strong_ratio >= 0.8:
            return filtered
        return []

    def _job_from_json_dict_v37(
        self,
        node: dict[str, Any],
        page_url: str,
        method: str,
        require_job_type: bool = False,
    ) -> Optional[dict]:
        node_type_raw = node.get("@type")
        node_type = ""
        if isinstance(node_type_raw, str):
            node_type = node_type_raw.strip().lower()
        elif isinstance(node_type_raw, list):
            node_type = " ".join(str(v).lower() for v in node_type_raw)
        is_jobposting = "jobposting" in node_type

        title = ""
        for key in (
            "title",
            "jobTitle",
            "jobAdTitle",
            "advertTitle",
            "advertisementTitle",
            "positionName",
            "positionTitle",
            "requisitionTitle",
            "name",
            "jobName",
            "roleName",
            "position",
            "job_title",
            "Title",
        ):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                title = value.strip()
                break
        title = self._normalize_title_v37(title)
        if not self._is_valid_title_v37(title):
            return None

        url_raw = None
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
            "applicationFormUrl",
            "applicationUrl",
            "postingUrl",
            "jobLink",
            "ExternalURL",
            "PostingUrl",
            "advertUrl",
            "advertURL",
            "jobAdUrl",
            "jobAdURL",
            "link",
        ):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                url_raw = value.strip()
                break

        source_url = _resolve_url(url_raw, page_url) if isinstance(url_raw, str) else page_url
        source_url = source_url or page_url

        key_names = " ".join(str(k) for k in node.keys()).lower()
        job_key_hint = bool(re.search(r"job|position|posting|requisition|vacanc|opening", key_names))
        strong_id = ""
        strong_id_key = ""
        for key in (
            "jobId",
            "jobID",
            "jobPostingId",
            "requisitionId",
            "positionId",
            "jobAdId",
            "JobAdId",
            "advertisementId",
            "adId",
            "advertId",
            "referenceNumber",
            "Id",
            "id",
        ):
            value = node.get(key)
            if value in (None, ""):
                continue
            strong_id = str(value).strip()
            if strong_id:
                strong_id_key = key
                break
        strong_id_hint = bool(strong_id)
        if strong_id_hint and source_url.rstrip("/") == page_url.rstrip("/"):
            joiner = "&" if "?" in page_url else "?"
            id_param = "jobAdId" if "ad" in strong_id_key.lower() else "jobId"
            source_url = f"{page_url}{joiner}{id_param}={strong_id}"

        if require_job_type and not is_jobposting:
            return None

        has_role = self._title_has_role_signal_v37(title) or self._is_acronym_title_v37(title)
        url_hint = self._is_job_like_url_v37(source_url)
        if not has_role and not (url_hint and (job_key_hint or strong_id_hint or is_jobposting)):
            return None

        if self._is_non_job_url_v37(source_url):
            return None

        if source_url.rstrip("/") == page_url.rstrip("/") and not (strong_id_hint or is_jobposting):
            return None

        description = ""
        for key in (
            "description",
            "summary",
            "shortDescription",
            "jobDescription",
            "Description",
            "ExternalDescriptionStr",
            "advertSummary",
            "teaser",
            "jobSummary",
        ):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                description = value.strip()
                break
        description = self._clean_description_v37(description)

        location = self._extract_location_from_json_v37(node)
        salary = self._extract_salary_from_json_v37(node)
        employment_type = self._extract_employment_from_json_v37(node)

        return {
            "title": title,
            "source_url": source_url,
            "location_raw": location,
            "salary_raw": salary,
            "employment_type": employment_type,
            "description": description,
            "extraction_method": method,
            "extraction_confidence": 0.84 if (is_jobposting or url_hint) else 0.75,
        }

    # ------------------------------------------------------------------
    # Candidate arbitration / validation
    # ------------------------------------------------------------------

    def _pick_best_jobset_v37(
        self,
        candidates: list[tuple[str, list[dict]]],
        page_url: str,
    ) -> tuple[str, list[dict]]:
        if not candidates:
            return "", []

        scored: list[tuple[str, list[dict], float]] = []
        parent_jobs: list[dict] = []
        parent_score = -1e9

        for label, jobs in candidates:
            deduped = self._dedupe_jobs_v37(jobs, page_url)
            if not deduped:
                continue

            valid = self._passes_jobset_validation_v37(deduped, page_url)
            score = self._jobset_score_v37(deduped, page_url)
            logger.debug("v3.7 candidate %s: jobs=%d valid=%s score=%.2f", label, len(deduped), valid, score)

            if label == "parent_v16":
                parent_jobs = deduped
                parent_score = score

            if valid:
                scored.append((label, deduped, score))

        if not scored:
            if parent_jobs:
                return "parent_v16", parent_jobs[:MAX_JOBS_PER_PAGE]
            largest = max(
                ((label, self._dedupe_jobs_v37(jobs, page_url)) for label, jobs in candidates),
                key=lambda item: len(item[1]),
                default=("", []),
            )
            return largest[0], largest[1][:MAX_JOBS_PER_PAGE]

        best_label, best_jobs, best_score = max(scored, key=lambda item: item[2])

        for label, jobs, score in scored:
            if label == best_label:
                continue
            overlap = self._title_overlap_ratio_v37(jobs, best_jobs)
            if len(jobs) >= len(best_jobs) + 1 and overlap >= 0.62 and score >= best_score - 1.6:
                best_label, best_jobs, best_score = label, jobs, score

        for label, jobs, score in scored:
            if label == best_label:
                continue
            overlap = self._title_overlap_ratio_v37(jobs, best_jobs)
            if len(jobs) != len(best_jobs) or overlap < 0.95:
                continue
            if "elementor" in label and "heading" in best_label:
                best_label, best_jobs, best_score = label, jobs, score
                continue
            if "job_links" in label and "heading" in best_label:
                best_label, best_jobs, best_score = label, jobs, score

        for label, jobs, score in scored:
            if label == best_label:
                continue
            if abs(len(jobs) - len(best_jobs)) > 1 or score < best_score - 1.0:
                continue
            if self._jobset_quality_tuple_v37(jobs, page_url) > self._jobset_quality_tuple_v37(best_jobs, page_url):
                best_label, best_jobs, best_score = label, jobs, score

        if parent_jobs and best_label != "parent_v16":
            overlap = self._title_overlap_ratio_v37(best_jobs, parent_jobs)
            if not (len(best_jobs) >= len(parent_jobs) + 2 and overlap >= 0.55):
                if best_score < parent_score + 1.4:
                    return "parent_v16", parent_jobs[:MAX_JOBS_PER_PAGE]

        return best_label, best_jobs[:MAX_JOBS_PER_PAGE]

    def _passes_jobset_validation_v37(self, jobs: list[dict], page_url: str) -> bool:
        if not jobs:
            return False

        titles = [self._normalize_title_v37(j.get("title", "")) for j in jobs]
        titles = [t for t in titles if self._is_valid_title_v37(t)]
        if not titles:
            return False

        if len(titles) < MIN_JOBS_FOR_SUCCESS:
            return self._passes_small_high_precision_jobset_v37(jobs, page_url, titles)

        unique_ratio = len(set(t.lower() for t in titles)) / max(1, len(titles))
        if len(titles) > 3 and unique_ratio < 0.62:
            return False

        role_hits = sum(1 for t in titles if self._title_has_role_signal_v37(t))
        reject_hits = sum(1 for t in titles if _REJECT_TITLE_PATTERN_V33.match(t.lower()))
        generic_single_hits = sum(1 for t in titles if _GENERIC_SINGLE_TITLE_PATTERN_V33.match(t.lower()))
        url_hits = sum(1 for j in jobs if self._is_strong_job_url_v37(j.get("source_url") or page_url))
        detail_hits = sum(1 for j in jobs if _JOB_DETAILISH_URL_PATTERN_V33.search(j.get("source_url") or ""))
        apply_hits = sum(1 for j in jobs if _APPLY_CONTEXT_PATTERN_V33.search((j.get("description") or "")[:2200]))
        same_page_evidence_hits = sum(1 for j in jobs if self._has_same_page_evidence_v37(j, page_url))
        strong_evidence_hits = sum(1 for j in jobs if self._job_has_strong_evidence_v37(j, page_url))
        same_page_hits = sum(
            1 for j in jobs if (j.get("source_url") or page_url).rstrip("/") == page_url.rstrip("/")
        )
        metadata_tail_hits = sum(1 for t in titles if self._title_looks_metadata_heavy_v37(t))

        if reject_hits >= max(1, int(len(titles) * 0.25)):
            return False
        if generic_single_hits >= max(1, int(len(titles) * 0.2)):
            return False
        if role_hits < max(1, int(len(titles) * 0.6)):
            return False
        if strong_evidence_hits < max(1, int(len(titles) * 0.45)):
            return False

        if (
            len(titles) >= 4
            and (url_hits + detail_hits) < max(1, int(len(titles) * 0.25))
            and same_page_evidence_hits < max(1, int(len(titles) * 0.35))
            and strong_evidence_hits < max(1, int(len(titles) * 0.7))
        ):
            return False

        if same_page_hits >= max(2, int(len(titles) * 0.85)) and same_page_evidence_hits < max(
            1, int(len(titles) * 0.55)
        ):
            return False
        if same_page_hits >= max(2, int(len(titles) * 0.7)) and apply_hits == 0 and same_page_evidence_hits == 0:
            return False
        if len(titles) >= 5 and metadata_tail_hits >= max(3, int(len(titles) * 0.7)):
            return False

        return True

    def _jobset_score_v37(self, jobs: list[dict], page_url: str) -> float:
        if not jobs:
            return 0.0

        titles = [self._normalize_title_v37(j.get("title", "")) for j in jobs]
        titles = [t for t in titles if t]
        if not titles:
            return 0.0

        role_hits = sum(1 for t in titles if self._title_has_role_signal_v37(t))
        reject_hits = sum(1 for t in titles if _REJECT_TITLE_PATTERN_V33.match(t.lower()))
        generic_single_hits = sum(1 for t in titles if _GENERIC_SINGLE_TITLE_PATTERN_V33.match(t.lower()))
        url_hits = sum(1 for j in jobs if self._is_strong_job_url_v37(j.get("source_url") or page_url))
        detail_hits = sum(1 for j in jobs if _JOB_DETAILISH_URL_PATTERN_V33.search(j.get("source_url") or ""))
        apply_hits = sum(1 for j in jobs if _APPLY_CONTEXT_PATTERN_V33.search((j.get("description") or "")[:2200]))
        strong_evidence_hits = sum(1 for j in jobs if self._job_has_strong_evidence_v37(j, page_url))
        same_page_evidence_hits = sum(1 for j in jobs if self._has_same_page_evidence_v37(j, page_url))
        same_page_hits = sum(
            1 for j in jobs if (j.get("source_url") or page_url).rstrip("/") == page_url.rstrip("/")
        )
        rich_desc_hits = sum(1 for j in jobs if len((j.get("description") or "").strip()) >= 180)
        metadata_tail_hits = sum(1 for t in titles if self._title_looks_metadata_heavy_v37(t))
        off_page_detail_hits = sum(
            1
            for j in jobs
            if _JOB_DETAILISH_URL_PATTERN_V33.search(j.get("source_url") or "")
            and (j.get("source_url") or page_url).rstrip("/") != page_url.rstrip("/")
        )
        clean_title_hits = sum(1 for t in titles if not self._title_looks_metadata_heavy_v37(t) and len(t) <= 90)

        score = len(titles) * 4.2
        score += role_hits * 2.6
        score += url_hits * 1.6
        score += detail_hits * 1.4
        score += apply_hits * 1.3
        score += strong_evidence_hits * 2.0
        score += same_page_evidence_hits * 1.3
        score += rich_desc_hits * 0.6
        score += off_page_detail_hits * 1.4
        score += clean_title_hits * 0.7
        score -= reject_hits * 4.5
        score -= generic_single_hits * 3.8
        score -= metadata_tail_hits * 1.6
        if same_page_hits >= max(2, int(len(titles) * 0.8)) and same_page_evidence_hits == 0:
            score -= 8.0
        return score

    def _jobset_quality_tuple_v37(self, jobs: list[dict], page_url: str) -> tuple[float, float, float, float]:
        if not jobs:
            return (0.0, 0.0, 0.0, 0.0)
        titles = [self._normalize_title_v37(j.get("title", "")) for j in jobs]
        titles = [t for t in titles if t]
        if not titles:
            return (0.0, 0.0, 0.0, 0.0)

        detail_urls = {
            (j.get("source_url") or page_url).strip().lower()
            for j in jobs
            if _JOB_DETAILISH_URL_PATTERN_V33.search(j.get("source_url") or "")
            and (j.get("source_url") or page_url).rstrip("/") != page_url.rstrip("/")
        }
        detail_ratio = len(detail_urls) / max(1, len(jobs))
        clean_ratio = (
            sum(1 for t in titles if not self._title_looks_metadata_heavy_v37(t) and len(t) <= 90)
            / max(1, len(titles))
        )
        metadata_ratio = sum(1 for t in titles if self._title_looks_metadata_heavy_v37(t)) / max(1, len(titles))
        avg_title_len = sum(len(t) for t in titles) / max(1, len(titles))
        return (detail_ratio, clean_ratio, -metadata_ratio, -avg_title_len)

    def _passes_small_high_precision_jobset_v37(self, jobs: list[dict], page_url: str, titles: list[str]) -> bool:
        if not titles or len(titles) > 2:
            return False

        role_hits = sum(1 for t in titles if self._title_has_role_signal_v37(t))
        reject_hits = sum(1 for t in titles if _REJECT_TITLE_PATTERN_V33.match(t.lower()))
        if reject_hits > 0 or role_hits < len(titles):
            return False

        strong_hits = sum(1 for j in jobs if self._job_has_strong_evidence_v37(j, page_url))
        return strong_hits >= len(titles)

    def _prose_block_has_job_evidence_v37(self, text: str) -> bool:
        desc = (text or "").strip()
        if len(desc) < 150:
            return False
        lower = desc.lower()
        if "sorry, we couldn't find what you were looking for" in lower:
            return False

        has_apply = bool(_APPLY_CONTEXT_PATTERN_V33.search(desc[:2500])) or "mailto:" in lower or "@" in lower
        has_salary = bool(_SALARY_PATTERN.search(desc))
        has_type = bool(_JOB_TYPE_PATTERN.search(desc))
        has_rich = bool(_RICH_CONTEXT_PATTERN_V35.search(desc))
        return (has_apply and has_rich) or (has_salary and has_rich) or (has_type and has_rich)

    def _has_same_page_evidence_v37(self, job: dict, page_url: str) -> bool:
        source_url = (job.get("source_url") or page_url).strip() or page_url
        same_page = source_url.rstrip("/") == page_url.rstrip("/")
        if not same_page:
            return False

        desc = (job.get("description") or "")
        desc_l = desc.lower()
        has_apply = bool(_APPLY_CONTEXT_PATTERN_V33.search(desc[:2200])) or "mailto:" in desc_l or "@" in desc_l
        has_structured = bool(job.get("salary_raw") or job.get("employment_type") or job.get("location_raw"))
        has_rich = len(desc.strip()) >= 180 and bool(_RICH_CONTEXT_PATTERN_V35.search(desc_l))
        return has_apply or (has_structured and has_rich) or len(desc.strip()) >= 380

    def _job_has_strong_evidence_v37(self, job: dict, page_url: str) -> bool:
        source_url = (job.get("source_url") or page_url).strip() or page_url
        if self._is_non_job_url_v37(source_url):
            return False

        same_page = source_url.rstrip("/") == page_url.rstrip("/")
        title = self._normalize_title_v37(job.get("title", ""))
        if _JOB_DETAILISH_URL_PATTERN_V33.search(source_url):
            return True
        if self._is_strong_job_url_v37(source_url) and not same_page:
            return True
        if not same_page and title and self._title_has_role_signal_v37(title):
            parsed = urlparse(source_url)
            tokens = [seg for seg in (parsed.path or "").split("/") if seg]
            if tokens:
                leaf = tokens[-1].lower()
                if leaf not in {"careers", "career", "jobs", "job", "vacancies", "vacancy", "openings"}:
                    slug_words = set(re.findall(r"[a-z0-9]{3,}", re.sub(r"[^a-z0-9]+", " ", leaf)))
                    title_words = set(re.findall(r"[a-z0-9]{3,}", title.lower()))
                    if slug_words and (slug_words & title_words):
                        return True
        return self._has_same_page_evidence_v37(job, page_url)

    def _title_overlap_ratio_v37(self, a_jobs: list[dict], b_jobs: list[dict]) -> float:
        a_titles = {self._normalize_title_v37(j.get("title", "")).lower() for j in a_jobs if j.get("title")}
        b_titles = {self._normalize_title_v37(j.get("title", "")).lower() for j in b_jobs if j.get("title")}
        if not a_titles or not b_titles:
            return 0.0
        return len(a_titles & b_titles) / max(1, min(len(a_titles), len(b_titles)))

    def _dedupe_jobs_v37(self, jobs: list[dict], page_url: str) -> list[dict]:
        prelim: list[dict] = []

        for idx, raw in enumerate(jobs):
            source_url = (raw.get("source_url") or page_url).strip() or page_url
            source_url = _resolve_url(source_url, page_url) or page_url
            if "#" in source_url:
                source_url = source_url.split("#", 1)[0]
            if self._is_non_job_url_v37(source_url):
                continue

            title = self._normalize_title_v37(raw.get("title", ""))
            if not self._is_title_acceptable_for_url_v37(title, source_url):
                continue

            cloned = dict(raw)
            cloned["title"] = title
            cloned["source_url"] = source_url
            cloned["description"] = self._clean_description_v37(str(cloned.get("description") or ""))
            cloned["_order"] = idx
            prelim.append(cloned)

        if not prelim:
            return []

        by_title_url: dict[tuple[str, str], dict] = {}
        for job in prelim:
            key = (job.get("title", "").lower(), job.get("source_url", "").lower())
            current = by_title_url.get(key)
            if current is None:
                by_title_url[key] = job
                continue
            if self._title_quality_score_v37(job.get("title", "")) > self._title_quality_score_v37(current.get("title", "")):
                by_title_url[key] = job

        by_detail_url: dict[str, dict] = {}
        passthrough: list[dict] = []
        for job in by_title_url.values():
            source_url = job.get("source_url") or page_url
            is_detail_url = bool(_JOB_DETAILISH_URL_PATTERN_V33.search(source_url))
            is_same_page = source_url.rstrip("/") == page_url.rstrip("/")
            if is_detail_url and not is_same_page:
                key = source_url.lower()
                current = by_detail_url.get(key)
                if current is None:
                    by_detail_url[key] = job
                    continue
                if self._title_quality_score_v37(job.get("title", "")) > self._title_quality_score_v37(current.get("title", "")):
                    by_detail_url[key] = job
                continue
            passthrough.append(job)

        deduped = sorted(
            [*by_detail_url.values(), *passthrough],
            key=lambda item: int(item.get("_order", 0)),
        )
        for item in deduped:
            item.pop("_order", None)
        return deduped[:MAX_JOBS_PER_PAGE]

    def _postprocess_jobs_v37(self, jobs: list[dict], page_url: str) -> list[dict]:
        cleaned: list[dict] = []
        for raw in jobs:
            title = self._normalize_title_v37(raw.get("title", ""))
            source_url = _resolve_url(raw.get("source_url"), page_url) or page_url
            if not self._is_valid_title_v37(title):
                continue
            if self._is_non_job_url_v37(source_url):
                continue

            cloned = dict(raw)
            cloned["title"] = title
            cloned["source_url"] = source_url
            cloned["description"] = self._clean_description_v37(str(cloned.get("description") or ""))
            cleaned.append(cloned)

        return self._dedupe_jobs_v37(cleaned, page_url)

    # ------------------------------------------------------------------
    # Normalization / validation helpers
    # ------------------------------------------------------------------

    def _normalize_title_v37(self, title: str) -> str:
        raw = html_lib.unescape((title or "").strip())
        t = raw.replace("|", " - ")
        t = re.sub(r"\s+", " ", t)
        t = t.strip(" \t\r\n-–|:;,.")
        t = re.sub(r"\s*(?:just\s+posted!?|posted\s+today!?|new)\s*$", "", t, flags=re.IGNORECASE)
        t = re.sub(
            r"\s*(?:read\s+more|learn\s+more|apply\s+now|apply\s+here|info\s+lengkap)\s*$",
            "",
            t,
            flags=re.IGNORECASE,
        )
        t = _TITLE_META_TAIL_PATTERN_V35.sub("", t).strip()
        t = re.sub(r"\b(?:deadline|closing\s+date|posted\s+date)\b.*$", "", t, flags=re.IGNORECASE).strip()
        t = self._strip_location_suffix_v37(t, raw)
        t = self._strip_comma_location_tail_v37(t)
        t = _TITLE_META_TAIL_PATTERN_V35.sub("", t).strip()
        return t.strip(" \t\r\n-–|:;,.")

    def _strip_comma_location_tail_v37(self, text: str) -> str:
        value = (text or "").strip()
        if "," not in value:
            return value
        head, tail = value.rsplit(",", 1)
        head = head.strip()
        tail = tail.strip()
        if not head or not tail or not self._title_has_role_signal_v37(head):
            return value

        location_tail = bool(
            _LOCATION_SUFFIX_HINT_PATTERN_V33.search(tail)
            or re.search(r"\b(?:city|region|state|county|province|district)\b", tail, re.IGNORECASE)
        )
        head_words = head.split()
        last_word = head_words[-1] if head_words else ""
        last_word_is_location = bool(_LOCATION_SUFFIX_HINT_PATTERN_V33.search(last_word))

        if not (location_tail or last_word_is_location):
            return value
        if last_word_is_location and len(head_words) > 2:
            candidate = " ".join(head_words[:-1]).strip()
        else:
            candidate = head
        if candidate and self._title_has_role_signal_v37(candidate):
            return candidate
        return value

    def _title_looks_metadata_heavy_v37(self, title: str) -> bool:
        t = " ".join((title or "").split())
        if not t:
            return False
        lower = t.lower()
        if _TITLE_META_TAIL_PATTERN_V35.search(t):
            return True
        if re.search(r"\b\d{1,2}[./-]\d{1,2}[./-]\d{2,4}\b", t) and re.search(
            r"\b(?:deadline|closing|posted|permanent|temporary|trainee|internship|contract)\b",
            lower,
        ):
            return True
        if len(t) > 95 and re.search(r"\b(?:deadline|closing|posted|permanent|temporary|trainee|internship)\b", lower):
            return True
        return False

    def _title_quality_score_v37(self, title: str) -> float:
        t = self._normalize_title_v37(title)
        if not t:
            return -10.0
        score = 0.0
        score += 2.0 if self._title_has_role_signal_v37(t) else 0.0
        score += 1.0 if self._is_valid_title_v37(t) else 0.0
        score -= 2.0 if self._title_looks_metadata_heavy_v37(t) else 0.0
        score -= max(0.0, (len(t) - 80) / 60.0)
        return score

    def _is_valid_title_v37(self, title: str) -> bool:
        if not title:
            return False
        t = title.strip()
        if len(t) < 4 or len(t) > 200:
            return False
        if len(t) < 5 and not self._is_acronym_title_v37(t):
            return False
        if _TITLE_PHONE_PATTERN_V33.match(t) or _TITLE_MOSTLY_NUMERIC_PATTERN_V33.match(t):
            return False
        if "@" in t and re.match(r"^[^@\s]+@[^@\s]+\.[^@\s]+$", t):
            return False
        if not re.search(r"[A-Za-z]", t):
            return False
        lower = t.lower()
        if _BOILERPLATE_BOUNDARY_PATTERN_V33.search(lower):
            # Keep role-like titles that contain legitimate words around boundary phrases.
            if not self._title_has_role_signal_v37(t):
                return False
        if _NOISY_CTA_TITLE_PATTERN_V35.search(lower):
            return False
        if _COMPANY_CAREER_LABEL_PATTERN_V35.match(lower) and not self._title_has_role_signal_v37(t):
            return False
        if _REJECT_TITLE_PATTERN_V33.match(t):
            return False
        if _GENERIC_SINGLE_TITLE_PATTERN_V33.match(t):
            return False
        return True

    def _title_has_role_signal_v37(self, title: str) -> bool:
        if not title:
            return False
        return _title_has_job_noun(title) or bool(_ROLE_HINT_PATTERN_V33.search(title)) or self._is_acronym_title_v37(title)

    def _is_title_acceptable_for_url_v37(self, title: str, source_url: str) -> bool:
        if self._is_valid_title_v37(title):
            return True
        return self._is_acronym_title_v37(title) and self._is_job_like_url_v37(source_url)

    @staticmethod
    def _is_acronym_title_v37(title: str) -> bool:
        t = (title or "").strip()
        if not _ACRONYM_TITLE_PATTERN_V33.match(t):
            return False
        return t.lower() not in {"faq", "home", "menu", "login", "logout"}

    def _strip_location_suffix_v37(self, normalized: str, raw: str) -> str:
        text = (normalized or "").strip()
        if not text:
            return text

        # Split common role-to-location joins without breaking mixed-case role words.
        before_role_split = text
        text = re.sub(
            r"\b("
            r"Engineer|Manager|Developer|Analyst|Executive|Scientist|Technician|Officer|"
            r"Administrator|Coordinator|Specialist|Designer|Architect|Consultant|Accountant|"
            r"Intern(?:ship)?|Director|President|Owner|Recruiter|Supervisor|Operator|Nurse|"
            r"Teacher|Chef|Driver|Lead"
            r")(?=[A-Z])",
            r"\1 ",
            text,
        )
        had_role_location_split = text != before_role_split

        had_or_segment = False
        if re.search(r"\s+OR\s+", text, re.IGNORECASE):
            before_or = re.split(r"\s+OR\s+", text, maxsplit=1, flags=re.IGNORECASE)[0].strip()
            if before_or and self._title_has_role_signal_v37(before_or):
                text = re.sub(r"([a-z])([A-Z][a-z]{2,})$", r"\1 \2", before_or)
                had_or_segment = True

        words = text.split()
        if len(words) < 2 or not self._title_has_role_signal_v37(text):
            return text

        if had_or_segment:
            tail = words[-1]
            if not self._title_has_role_signal_v37(tail) and len(tail) >= 3:
                candidate = " ".join(words[:-1]).strip()
                if candidate and self._title_has_role_signal_v37(candidate):
                    return candidate

        # If we split a role-word/location join, drop trailing location token.
        if had_role_location_split:
            tail = words[-1]
            if not self._title_has_role_signal_v37(tail) and len(tail) >= 3:
                candidate = " ".join(words[:-1]).strip()
                if candidate and self._title_has_role_signal_v37(candidate):
                    return candidate

        for cut in range(len(words) - 1, 0, -1):
            head = " ".join(words[:cut]).strip()
            tail = " ".join(words[cut:]).strip(" ,")
            if not head or not tail:
                continue
            if not self._title_has_role_signal_v37(head):
                continue
            if self._title_has_role_signal_v37(tail):
                continue
            if len(tail.split()) > 5:
                continue
            if re.search(r"\b(project|programme|program)\b", tail, re.IGNORECASE):
                continue
            if self._looks_like_location_tail_v37(tail):
                return head
        return text

    def _looks_like_location_tail_v37(self, tail: str) -> bool:
        value = (tail or "").strip()
        if not value:
            return False
        if _LOCATION_SUFFIX_HINT_PATTERN_V33.search(value):
            return True
        if _LOCATION_TAIL_STRICT_PATTERN_V37.search(value):
            return True
        # Preserve specialization tails like "Fashion, Skincare & Kosmetik".
        if "," in value:
            if re.search(r"\b[A-Z][a-z]+,\s*[A-Z]{2,3}\b", value):
                return True
            if re.search(
                r"\b(?:india|indonesia|australia|malaysia|singapore|canada|usa|uk|philippines)\b",
                value,
                re.IGNORECASE,
            ):
                return True
            return False
        return bool(re.search(r"\s+OR\s+", value, re.IGNORECASE))

    def _is_job_like_url_v37(self, url: str) -> bool:
        value = (url or "").strip()
        if not value:
            return False
        if _NON_JOB_URL_PATTERN_V33.search(value):
            return False
        return bool(_JOB_URL_HINT_PATTERN_V33.search(value) or _JOB_DETAILISH_URL_PATTERN_V33.search(value))

    def _is_strong_job_url_v37(self, url: str) -> bool:
        value = (url or "").strip()
        if not value or self._is_non_job_url_v37(value):
            return False
        if _JOB_DETAILISH_URL_PATTERN_V33.search(value):
            return True
        return bool(
            re.search(r"/(?:jobs?|job-openings?|openings?|vacanc(?:y|ies)|requisitions?)(?:/|$|[?#])", value, re.IGNORECASE)
            or re.search(r"[?&](?:jobid|job_id|requisitionid|positionid|vacancyid|jobadid|adid|ajid)=", value, re.IGNORECASE)
        )

    def _is_non_job_url_v37(self, url: str) -> bool:
        value = (url or "").strip().lower()
        if not value:
            return False
        if value.startswith("mailto:") or value.startswith("tel:"):
            return True
        if "applyregister" in value or "applyexpressinterest" in value:
            return True
        if _NON_JOB_URL_PATTERN_V33.search(value):
            return True
        return False

    def _clean_description_v37(self, description: str) -> Optional[str]:
        text = " ".join((description or "").split())
        if not text:
            return None
        cut_match = _DESCRIPTION_CUT_PATTERN_V33.search(text)
        if cut_match:
            text = text[: cut_match.start()].strip()
        if len(text) < 45:
            return None
        return text[:5000]

    def _extract_location_v37(self, row: etree._Element, title: str) -> Optional[str]:
        for el in row.iter():
            if not isinstance(el.tag, str):
                continue
            cls = _get_el_classes(el)
            if "location" in cls or "map-marker" in cls or _LOCATION_HINT_PATTERN_V33.search(cls):
                loc = " ".join(_text(el).split())
                if loc and loc != title and 2 < len(loc) < 160:
                    return loc

        row_text = _text(row)
        match = _AU_LOCATIONS.search(row_text) if row_text else None
        if match:
            return match.group(0)[:120]
        return None

    @staticmethod
    def _extract_salary_v37(text: str) -> Optional[str]:
        if not text:
            return None
        match = _SALARY_PATTERN.search(text)
        if match:
            return match.group(0).strip()[:120]
        return None

    @staticmethod
    def _extract_employment_type_v37(text: str) -> Optional[str]:
        if not text:
            return None
        match = _JOB_TYPE_PATTERN.search(text)
        if match:
            return match.group(0).strip()[:80]
        return None

    def _extract_location_from_json_v37(self, node: dict[str, Any]) -> Optional[str]:
        for key in ("location", "jobLocation", "city", "workLocation", "region", "addressLocality"):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()[:180]
            if isinstance(value, dict):
                parts = [
                    str(value.get("addressLocality") or "").strip(),
                    str(value.get("addressRegion") or "").strip(),
                    str(value.get("addressCountry") or "").strip(),
                ]
                joined = ", ".join(p for p in parts if p)
                if joined:
                    return joined[:180]
            if isinstance(value, list):
                loc_parts: list[str] = []
                for entry in value[:5]:
                    if isinstance(entry, str) and entry.strip():
                        loc_parts.append(entry.strip())
                    elif isinstance(entry, dict):
                        city = str(entry.get("addressLocality") or entry.get("city") or "").strip()
                        region = str(entry.get("addressRegion") or entry.get("state") or "").strip()
                        country = str(entry.get("addressCountry") or "").strip()
                        joined = ", ".join(p for p in (city, region, country) if p)
                        if joined:
                            loc_parts.append(joined)
                if loc_parts:
                    return " | ".join(dict.fromkeys(loc_parts))[:180]
        return None

    def _extract_salary_from_json_v37(self, node: dict[str, Any]) -> Optional[str]:
        for key in ("salary", "salaryText", "salaryRange", "compensation", "pay"):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()[:180]
            if isinstance(value, dict):
                minimum = str(value.get("minValue") or value.get("minimum") or "").strip()
                maximum = str(value.get("maxValue") or value.get("maximum") or "").strip()
                currency = str(value.get("currency") or value.get("currencyCode") or "").strip()
                joined = " - ".join(v for v in (minimum, maximum) if v)
                if joined:
                    return f"{currency} {joined}".strip()[:180]
        return None

    def _extract_employment_from_json_v37(self, node: dict[str, Any]) -> Optional[str]:
        for key in ("employmentType", "jobType", "workType", "timeType"):
            value = node.get(key)
            if isinstance(value, str) and value.strip():
                found = _JOB_TYPE_PATTERN.search(value)
                if found:
                    return found.group(0).strip()[:80]
                return value.strip()[:80]
        return None

    # ------------------------------------------------------------------
    # Martian context/probing helpers
    # ------------------------------------------------------------------

    async def _harvest_script_endpoint_hints_v37(self, page_url: str, html_body: str) -> list[str]:
        script_urls = self._script_src_urls_v37(page_url, html_body)
        if not script_urls:
            return []

        hints: list[str] = []
        seen: set[str] = set()
        try:
            async with httpx.AsyncClient(
                timeout=1.5,
                follow_redirects=True,
                headers={
                    "Accept": "application/javascript,text/javascript,text/plain;q=0.9,*/*;q=0.8",
                    "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
                },
            ) as client:
                for script_url in script_urls[:5]:
                    try:
                        resp = await asyncio.wait_for(client.get(script_url), timeout=1.6)
                    except Exception:
                        continue
                    payload = resp.text or ""
                    if resp.status_code >= 400 or len(payload) < 120:
                        continue

                    for match in _MARTIAN_ENDPOINT_HINT_PATTERN_V35.finditer(payload):
                        candidate = (match.group(0) or "").strip().strip("\"' ")
                        if not candidate:
                            continue
                        if not candidate.startswith("http"):
                            candidate = _resolve_url(candidate, script_url) or ""
                        if not candidate:
                            continue
                        norm = candidate.rstrip("/")
                        if norm in seen:
                            continue
                        seen.add(norm)
                        hints.append(candidate)

                    for match in _MARTIAN_RECRUITER_ENDPOINT_JS_PATTERN_V37.finditer(payload):
                        candidate = _resolve_url(match.group(0), script_url) or ""
                        if not candidate:
                            continue
                        norm = candidate.rstrip("/")
                        if norm in seen:
                            continue
                        seen.add(norm)
                        hints.append(candidate)
                    if len(hints) >= 20:
                        break
        except Exception:
            return []

        return hints[:36]

    def _script_src_urls_v37(self, page_url: str, html_body: str) -> list[str]:
        ranked: list[tuple[int, str]] = []
        seen: set[str] = set()
        page_host = urlparse(page_url or "").netloc.lower()

        for match in _SCRIPT_SRC_PATTERN_V37.finditer(html_body or ""):
            src = (match.group(1) or "").strip()
            if not src:
                continue
            resolved = _resolve_url(src, page_url) or ""
            if not resolved:
                continue
            parsed = urlparse(resolved)
            host = parsed.netloc.lower()
            if not host:
                continue
            if (
                page_host
                and host != page_host
                and "martianlogic" not in host
                and "myrecruitmentplus" not in host
            ):
                continue
            norm = resolved.split("#", 1)[0]
            if norm in seen:
                continue
            seen.add(norm)
            path_l = (parsed.path or "").lower()
            score = 0
            if "martianlogic" in host or "myrecruitmentplus" in host:
                score += 6
            if "/pages/" in path_l or "%5b" in path_l or "[client]" in path_l:
                score += 8
            if "chunk" in path_l:
                score += 3
            if "main" in path_l or "_app" in path_l:
                score += 2
            if "webpack" in path_l or "polyfills" in path_l:
                score -= 5
            ranked.append((score, norm))

        ranked.sort(key=lambda item: item[0], reverse=True)
        return [url for _, url in ranked[:10]]

    def _extract_martian_context_v37(self, html_body: str, page_url: str) -> dict[str, str]:
        result = {
            "client_code": "",
            "recruiter_id": "",
            "job_board_theme_id": "",
            "board_name": "",
            "build_id": "",
            "next_page": "",
            "next_query": "",
            "host_hints": "",
            "endpoint_hints": "",
        }

        next_data_match = _NEXT_DATA_PATTERN_V33.search(html_body or "")
        if next_data_match:
            try:
                parsed = json.loads(next_data_match.group(1))
                if isinstance(parsed, dict):
                    page_props = parsed.get("props", {}).get("pageProps", {})
                    if isinstance(page_props, dict):
                        result["client_code"] = str(page_props.get("clientCode") or "").strip()
                        result["recruiter_id"] = str(page_props.get("recruiterId") or "").strip()
                        result["job_board_theme_id"] = str(page_props.get("jobBoardThemeId") or "").strip()
                        result["board_name"] = str(page_props.get("name") or "").strip()
                    result["build_id"] = str(parsed.get("buildId") or "").strip()
                    result["next_page"] = str(parsed.get("page") or "").strip()
                    query = parsed.get("query")
                    if isinstance(query, dict):
                        result["next_query"] = urlencode(
                            {str(k): str(v) for k, v in query.items() if isinstance(v, (str, int, float))}
                        )
                        if not result["client_code"]:
                            result["client_code"] = str(query.get("client") or query.get("clientCode") or "").strip()
                        if not result["recruiter_id"]:
                            result["recruiter_id"] = str(query.get("recruiterId") or "").strip()
            except Exception:
                pass

        if not result["client_code"]:
            m = _MARTIAN_CLIENT_PATTERN_V33.search(html_body or "")
            if m:
                result["client_code"] = m.group(1).strip()
        if not result["recruiter_id"]:
            m = _MARTIAN_RECRUITER_PATTERN_V33.search(html_body or "")
            if m:
                result["recruiter_id"] = m.group(1).strip()
        if not result["job_board_theme_id"]:
            m = _MARTIAN_THEME_PATTERN_V33.search(html_body or "")
            if m:
                result["job_board_theme_id"] = m.group(1).strip()
        if not result["board_name"]:
            m = _MARTIAN_NAME_PATTERN_V33.search(html_body or "")
            if m:
                result["board_name"] = m.group(1).strip()

        if not result["client_code"]:
            path_parts = [p for p in (urlparse(page_url).path or "").split("/") if p]
            if path_parts:
                candidate = re.sub(r"[^a-z0-9-]", "", path_parts[-1].lower())
                if len(candidate) >= 3:
                    result["client_code"] = candidate

        host_hints: list[str] = []
        seen_hosts: set[str] = set()
        for match in _MARTIAN_HOST_HINT_PATTERN_V33.finditer(html_body or ""):
            parsed = urlparse(match.group(0))
            if not parsed.netloc:
                continue
            host = f"{parsed.scheme or 'https'}://{parsed.netloc}".rstrip("/")
            if host in seen_hosts:
                continue
            seen_hosts.add(host)
            host_hints.append(host)
        if host_hints:
            result["host_hints"] = "|".join(host_hints[:6])

        endpoint_hints: list[str] = []
        seen_endpoints: set[str] = set()
        for match in _MARTIAN_ENDPOINT_HINT_PATTERN_V35.finditer(html_body or ""):
            candidate = match.group(0).strip().strip("\"' ")
            if not candidate:
                continue
            if not candidate.startswith("http"):
                candidate = _resolve_url(candidate, page_url) or ""
            if not candidate:
                continue
            normalized = candidate.rstrip("/")
            if normalized in seen_endpoints:
                continue
            seen_endpoints.add(normalized)
            endpoint_hints.append(candidate)
        if endpoint_hints:
            result["endpoint_hints"] = "|".join(endpoint_hints[:12])

        return result

    def _martian_probe_urls_v37(self, page_url: str, context: dict[str, str]) -> list[str]:
        parsed = urlparse(page_url or "")
        base_host = f"{parsed.scheme or 'https'}://{parsed.netloc}" if parsed.netloc else ""
        if not base_host:
            return []

        client_code = (context.get("client_code") or "").strip()
        recruiter_id = (context.get("recruiter_id") or "").strip()
        theme_id = (context.get("job_board_theme_id") or "").strip()
        board_name = (context.get("board_name") or "").strip()
        host_hints = [h for h in (context.get("host_hints") or "").split("|") if h]

        ats_hosts = [
            "https://web.martianlogic.com",
            "https://form.myrecruitmentplus.com",
            "https://jobs.myrecruitmentplus.com",
            "https://jobs.martianlogic.com",
        ]
        base_host_l = urlparse(base_host).netloc.lower()
        base_is_ats = "martianlogic" in base_host_l or "myrecruitmentplus" in base_host_l
        hosts: list[str] = [*ats_hosts, *host_hints, base_host] if not base_is_ats else [base_host, *ats_hosts, *host_hints]
        if client_code:
            hosts.extend(
                [
                    f"https://{client_code}.myrecruitmentplus.com",
                    f"https://{client_code}.martianlogic.com",
                ]
            )

        host_seen: set[str] = set()
        deduped_hosts: list[str] = []
        for host in hosts:
            norm_host = host.rstrip("/")
            if not norm_host or norm_host in host_seen:
                continue
            host_seen.add(norm_host)
            deduped_hosts.append(norm_host)
        hosts = deduped_hosts[:9]

        query_templates = [
            "pageNumber=1&pageSize=50&isActive=true",
            "offset=0&limit=50",
            "page=1&pageSize=50",
            "page=1&perPage=50",
        ]
        if client_code:
            query_templates.extend(
                [
                    f"client={client_code}&page=1&pageSize=50",
                    f"clientCode={client_code}&page=1&pageSize=50",
                    f"client={client_code}&pageNumber=1&pageSize=50&isActive=true",
                    f"clientCode={client_code}&pageNumber=1&pageSize=50&isActive=true",
                ]
            )
        if recruiter_id:
            query_templates.extend(
                [
                    f"recruiterId={recruiter_id}&page=1&pageSize=50",
                    f"recruiterId={recruiter_id}&pageNumber=1&pageSize=50&isActive=true",
                ]
            )
            if client_code:
                query_templates.extend(
                    [
                        f"client={client_code}&recruiterId={recruiter_id}&page=1&pageSize=50",
                        f"clientCode={client_code}&recruiterId={recruiter_id}&page=1&pageSize=50",
                        f"client={client_code}&recruiterId={recruiter_id}&pageNumber=1&pageSize=50&isActive=true",
                        f"clientCode={client_code}&recruiterId={recruiter_id}&pageNumber=1&pageSize=50&isActive=true",
                    ]
                )
        if theme_id and client_code:
            query_templates.extend(
                [
                    f"client={client_code}&jobBoardThemeId={theme_id}&pageNumber=1&pageSize=50&isActive=true",
                    f"clientCode={client_code}&jobBoardThemeId={theme_id}&pageNumber=1&pageSize=50&isActive=true",
                ]
            )
        if board_name and client_code:
            encoded_name = board_name.replace(" ", "%20")
            query_templates.extend(
                [
                    f"client={client_code}&name={encoded_name}&pageNumber=1&pageSize=50&isActive=true",
                    f"clientCode={client_code}&name={encoded_name}&pageNumber=1&pageSize=50&isActive=true",
                ]
            )

        search_paths = [
            "/api/jobs/search",
            "/api/job-search",
            "/api/jobads/search",
            "/api/job-ads/search",
            "/api/jobAds/search",
            "/api/search/jobs",
            "/job-board/api/jobs/search",
            "/job-board/api/jobads/search",
            "/job-board/api/jobAds/search",
            "/jobs/search",
            "/embed-jobs",
        ]
        plain_paths = [
            "/api/jobs",
            "/api/jobads",
            "/api/job-ads",
            "/api/jobAds",
            "/job-board/api/jobs",
            "/job-board/api/jobads",
            "/job-board/api/jobAds",
            "/jobs",
            "/jobads",
            "/job-ads",
        ]
        if client_code:
            search_paths.extend(
                [
                    f"/{client_code}/search",
                    f"/{client_code}/jobads",
                    f"/{client_code}/job-ads",
                    f"/{client_code}/jobAds",
                    f"/{client_code}/jobs/search",
                    f"/{client_code}/embed-jobs",
                ]
            )
            plain_paths.extend(
                [
                    f"/{client_code}",
                    f"/{client_code}/jobs",
                    f"/{client_code}/job-board",
                    f"/{client_code}/jobboard",
                ]
            )

        endpoint_hints = [h for h in (context.get("endpoint_hints") or "").split("|") if h]

        candidates: list[str] = []
        next_data_candidates: list[str] = []
        for host in hosts:
            if recruiter_id:
                for recruiter_path in (
                    f"/api/recruiter/{recruiter_id}/jobs",
                    f"/api/recruiter/{recruiter_id}/jobs/search",
                    f"/api/recruiter/{recruiter_id}/jobads",
                    f"/api/recruiter/{recruiter_id}/job-ads",
                    f"/api/recruiter/{recruiter_id}/jobAds",
                ):
                    base = f"{host}{recruiter_path}"
                    candidates.append(base)
                    candidates.append(f"{base}?pageNumber=1&pageSize=50")
                    candidates.append(f"{base}?page=1&pageSize=50")
                    if client_code:
                        candidates.append(f"{base}?clientCode={client_code}&page=1&pageSize=50")
                        candidates.append(f"{base}?client={client_code}&page=1&pageSize=50")

            for path in search_paths:
                base = f"{host}{path.rstrip('/')}"
                candidates.append(base)
                for query in query_templates:
                    candidates.append(f"{base}?{query}")
            for path in plain_paths:
                base = f"{host}{path.rstrip('/')}"
                candidates.append(base)
                if client_code:
                    candidates.append(f"{base}?clientCode={client_code}&page=1&pageSize=50")
                    candidates.append(f"{base}?client={client_code}&page=1&pageSize=50")

            next_data_for_host = self._next_data_candidate_urls_v37(host, page_url, context, client_code)
            candidates.extend(next_data_for_host)
            next_data_candidates.extend(next_data_for_host[:2])
        candidates.extend(endpoint_hints)

        page_query = dict(parse_qsl(parsed.query))
        if page_query.get("jobAdId"):
            if client_code:
                candidates.append(
                    f"{base_host}/?client={client_code}&jobAdId={page_query['jobAdId']}&pageNumber=1&pageSize=50"
                )
            candidates.append(
                f"{base_host}/?jobAdId={page_query['jobAdId']}&pageNumber=1&pageSize=50"
            )

        seen: set[str] = set()
        unique: list[str] = []
        for url in candidates:
            if not url:
                continue
            norm = url.rstrip("/")
            if norm in seen:
                continue
            seen.add(norm)
            unique.append(url)

        def _priority(candidate_url: str) -> int:
            score = self._martian_endpoint_priority_v37(candidate_url)
            host = urlparse(candidate_url).netloc.lower()
            if (client_code or recruiter_id) and host == base_host_l and not base_is_ats:
                score -= 4
            return score

        unique.sort(key=_priority, reverse=True)
        selected = unique[:280]
        if next_data_candidates:
            selected_set = {u.rstrip("/") for u in selected}
            for candidate in next_data_candidates[:12]:
                norm = candidate.rstrip("/")
                if not norm or norm in selected_set:
                    continue
                selected.append(candidate)
                selected_set.add(norm)
        return selected[:300]

    @staticmethod
    def _martian_endpoint_priority_v37(candidate_url: str) -> int:
        low = candidate_url.lower()
        host = urlparse(candidate_url).netloc.lower()
        score = 0
        if "martianlogic" in host or "myrecruitmentplus" in host:
            score += 8
        if "/api/recruiter/" in low:
            score += 12
        elif "/api/" in low:
            score += 8
        if "/_next/data/" in low:
            score += 8
        if "search" in low:
            score += 6
        if "jobads" in low or "job-ads" in low:
            score += 5
        elif "/jobs" in low:
            score += 3
        if "clientcode=" in low or "client=" in low:
            score += 3
        if "recruiterid=" in low:
            score += 3
        if "pagenumber=1" in low or "page=1" in low or "offset=0" in low:
            score += 2
        return score

    def _martian_endpoint_plan_v37(self, endpoints: list[str], aggressive: bool = False) -> list[str]:
        if not endpoints:
            return []

        max_total = 42 if aggressive else 28
        max_per_host = 10 if aggressive else 7

        buckets: dict[str, list[str]] = defaultdict(list)
        for endpoint in endpoints:
            host = urlparse(endpoint).netloc.lower() or "_"
            buckets[host].append(endpoint)

        ordered_hosts = sorted(
            buckets.keys(),
            key=lambda host: (
                0 if ("martianlogic" in host or "myrecruitmentplus" in host) else 1,
                host,
            ),
        )

        trimmed: dict[str, list[str]] = {}
        for host in ordered_hosts:
            ranked = sorted(
                buckets[host],
                key=self._martian_endpoint_priority_v37,
                reverse=True,
            )
            keep = ranked[:max_per_host]
            if keep and not any("/_next/data/" in endpoint.lower() for endpoint in keep):
                next_data_endpoint = next((endpoint for endpoint in ranked if "/_next/data/" in endpoint.lower()), "")
                if next_data_endpoint:
                    if len(keep) >= max_per_host:
                        keep = [*keep[:-1], next_data_endpoint]
                    else:
                        keep.append(next_data_endpoint)
            trimmed[host] = keep

        plan: list[str] = []
        while len(plan) < max_total:
            added = False
            for host in ordered_hosts:
                if not trimmed[host]:
                    continue
                plan.append(trimmed[host].pop(0))
                added = True
                if len(plan) >= max_total:
                    break
            if not added:
                break

        return plan

    def _next_data_candidate_urls_v37(
        self,
        host: str,
        page_url: str,
        context: dict[str, str],
        client_code: str,
    ) -> list[str]:
        build_id = (context.get("build_id") or "").strip()
        if not build_id:
            return []

        parsed = urlparse(page_url)
        query_pairs = dict(parse_qsl(parsed.query))
        next_query = (context.get("next_query") or "").strip()
        if next_query:
            for key, value in parse_qsl(next_query):
                query_pairs[key] = value

        if client_code:
            query_pairs.setdefault("client", client_code)
        encoded_query = urlencode(query_pairs)

        path = parsed.path or "/"
        norm_path = "/" + "/".join(seg for seg in path.split("/") if seg)
        if not norm_path:
            norm_path = "/"

        candidates: list[str] = [f"{host}/_next/data/{build_id}/index.json"]
        if norm_path != "/":
            candidates.append(f"{host}/_next/data/{build_id}{norm_path.rstrip('/')}.json")
            candidates.append(f"{host}/_next/data/{build_id}{norm_path.rstrip('/')}/index.json")

        dynamic_keys = [
            client_code,
            str(query_pairs.get("clientCode") or ""),
            str(query_pairs.get("slug") or ""),
            str(query_pairs.get("tenant") or ""),
            str(query_pairs.get("company") or ""),
        ]
        page_value = str(context.get("next_page") or "").strip()
        if page_value:
            dynamic_keys.append(page_value.replace("[", "").replace("]", "").strip("/"))
        dynamic_keys.extend([seg for seg in norm_path.split("/") if seg and not seg.startswith("[")])

        seen_keys: set[str] = set()
        for key in dynamic_keys:
            cleaned = (key or "").strip().strip("/")
            if not cleaned or cleaned in seen_keys:
                continue
            seen_keys.add(cleaned)
            candidates.append(f"{host}/_next/data/{build_id}/{cleaned}.json")
            candidates.append(f"{host}/_next/data/{build_id}/{cleaned}/index.json")
            if encoded_query:
                candidates.append(f"{host}/_next/data/{build_id}/{cleaned}.json?{encoded_query}")

        if encoded_query:
            candidates = [f"{url}?{encoded_query}" if "?" not in url else url for url in candidates]
        return candidates

    @staticmethod
    def _martian_paged_variants_v37(endpoint: str) -> list[str]:
        variants = [endpoint]
        if "pageNumber=1" in endpoint:
            variants.append(endpoint.replace("pageNumber=1", "pageNumber=2"))
            variants.append(endpoint.replace("pageNumber=1", "pageNumber=3"))
        if "page=1" in endpoint:
            variants.append(endpoint.replace("page=1", "page=2"))
            variants.append(endpoint.replace("page=1", "page=3"))
        if "offset=0" in endpoint:
            variants.append(endpoint.replace("offset=0", "offset=50"))
            variants.append(endpoint.replace("offset=0", "offset=100"))
        seen: set[str] = set()
        unique: list[str] = []
        for value in variants:
            if value in seen:
                continue
            seen.add(value)
            unique.append(value)
        return unique[:3]

    @staticmethod
    def _martian_post_endpoints_v37(endpoints: list[str]) -> list[str]:
        postable: list[str] = []
        seen: set[str] = set()
        for endpoint in endpoints:
            low = endpoint.lower()
            if "/api/" not in low:
                continue
            if "search" not in low and "jobads" not in low and "job-ads" not in low and "/jobs" not in low:
                continue
            base = endpoint.split("?", 1)[0].rstrip("/")
            if base in seen:
                continue
            seen.add(base)
            postable.append(base)
        return postable[:20]

    @staticmethod
    def _martian_post_payloads_v37(context: dict[str, str]) -> list[dict[str, Any]]:
        client_code = (context.get("client_code") or "").strip()
        recruiter_id = (context.get("recruiter_id") or "").strip()
        theme_id = (context.get("job_board_theme_id") or "").strip()
        board_name = (context.get("board_name") or "").strip()

        payloads: list[dict[str, Any]] = []
        if client_code:
            payloads.extend(
                [
                    {"client": client_code, "pageNumber": 1, "pageSize": 50, "isActive": True},
                    {"clientCode": client_code, "pageNumber": 1, "pageSize": 50, "isActive": True},
                    {"client": client_code, "offset": 0, "limit": 50},
                    {"clientCode": client_code, "offset": 0, "limit": 50},
                ]
            )
        else:
            payloads.append({"pageNumber": 1, "pageSize": 50, "isActive": True})

        if recruiter_id:
            payloads.append(
                {
                    "recruiterId": recruiter_id,
                    "pageNumber": 1,
                    "pageSize": 50,
                    "isActive": True,
                }
            )
            if client_code:
                payloads.append(
                    {
                        "client": client_code,
                        "recruiterId": recruiter_id,
                        "pageNumber": 1,
                        "pageSize": 50,
                        "isActive": True,
                    }
                )
        if theme_id:
            theme_payload = {
                "jobBoardThemeId": theme_id,
                "pageNumber": 1,
                "pageSize": 50,
                "isActive": True,
            }
            if client_code:
                theme_payload["clientCode"] = client_code
            payloads.append(theme_payload)

        if board_name and client_code:
            payloads.append(
                {"client": client_code, "name": board_name, "pageNumber": 1, "pageSize": 50, "isActive": True}
            )
        return payloads

    # ------------------------------------------------------------------
    # Oracle helpers
    # ------------------------------------------------------------------

    def _oracle_site_ids_v37(self, page_url: str, html_body: str) -> list[str]:
        seen: set[str] = set()
        ordered: list[str] = []

        def _add(value: str) -> None:
            site = (value or "").strip()
            if not site or site in seen:
                return
            seen.add(site)
            ordered.append(site)

        for match in _ORACLE_SITE_PATTERN_V33.finditer(page_url or ""):
            _add(match.group(1))
        for match in _ORACLE_SITE_PATTERN_V33.finditer(html_body or ""):
            _add(match.group(1))
        for match in _ORACLE_SITE_NUMBER_PATTERN_V33.finditer(html_body or ""):
            _add(match.group(1))
        for match in re.finditer(r"siteNumber=([A-Za-z0-9_]+)", html_body or "", re.IGNORECASE):
            _add(match.group(1))

        query_pairs = dict(parse_qsl(urlparse(page_url or "").query))
        if query_pairs.get("siteNumber"):
            _add(query_pairs.get("siteNumber", ""))

        base_ids = list(ordered)
        for site_id in base_ids:
            if re.fullmatch(r"CX(?:_\d+)?", site_id, flags=re.IGNORECASE):
                root = site_id.split("_", 1)[0]
                _add(root)
                for suffix in ("1001", "1002", "1003"):
                    _add(f"{root}_{suffix}")

        if not ordered:
            for fallback in ("CX", "CX_1001", "CX_1002"):
                _add(fallback)
        ordered.sort(
            key=lambda site: (
                0 if re.search(r"_[0-9]+$", site) else 1,
                0 if site.upper().endswith("_1001") else 1,
                site.lower(),
            )
        )
        return ordered[:12]

    def _extract_oracle_items_v37(self, data: Any, page_url: str, site_id: str) -> list[dict]:
        rows: list[dict[str, Any]] = []

        def _add_row(row: Any) -> None:
            if isinstance(row, dict):
                rows.append(row)

        if isinstance(data, dict):
            top_reqs = data.get("requisitionList")
            if isinstance(top_reqs, list):
                for row in top_reqs:
                    _add_row(row)
            elif isinstance(top_reqs, dict):
                for key in ("items", "requisitionList", "list"):
                    nested = top_reqs.get(key)
                    if isinstance(nested, list):
                        for row in nested:
                            _add_row(row)

            items = data.get("items")
            if isinstance(items, list):
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    if any(k in item for k in ("Title", "title", "JobTitle", "jobTitle", "Id", "id")):
                        _add_row(item)

                    req_list = item.get("requisitionList")
                    if isinstance(req_list, list):
                        for row in req_list:
                            _add_row(row)
                    elif isinstance(req_list, dict):
                        for key in ("items", "requisitionList", "list"):
                            nested = req_list.get(key)
                            if isinstance(nested, list):
                                for row in nested:
                                    _add_row(row)

        jobs: list[dict] = []
        parsed = urlparse(page_url)
        host_base = f"{parsed.scheme or 'https'}://{parsed.netloc}" if parsed.netloc else ""

        for row in rows:
            title = self._normalize_title_v37(
                str(
                    row.get("Title")
                    or row.get("title")
                    or row.get("JobTitle")
                    or row.get("jobTitle")
                    or row.get("requisitionTitle")
                    or ""
                )
            )
            if not self._is_valid_title_v37(title):
                continue
            if not (self._title_has_role_signal_v37(title) or self._is_acronym_title_v37(title)):
                continue

            req_id = str(
                row.get("Id")
                or row.get("id")
                or row.get("RequisitionId")
                or row.get("requisitionId")
                or row.get("jobId")
                or ""
            ).strip()

            source_url = ""
            for key in ("ExternalURL", "externalUrl", "PostingUrl", "postingUrl", "jobUrl", "url"):
                value = row.get(key)
                if isinstance(value, str) and value.strip():
                    source_url = value.strip()
                    break
            source_url = _resolve_url(source_url, page_url) or page_url
            if req_id and source_url.rstrip("/") == page_url.rstrip("/") and host_base:
                source_url = f"{host_base}/hcmUI/CandidateExperience/en/sites/{site_id}/job/{req_id}"
            if self._is_non_job_url_v37(source_url):
                continue

            location = self._extract_location_from_json_v37(row)
            if not location:
                primary = str(row.get("PrimaryLocation") or "").strip()
                country = str(row.get("PrimaryLocationCountry") or "").strip()
                joined = ", ".join(p for p in (primary, country) if p)
                location = joined or None

            description = self._clean_description_v37(
                str(
                    row.get("Description")
                    or row.get("description")
                    or row.get("ShortDescription")
                    or row.get("ExternalDescriptionStr")
                    or ""
                )
            )
            jobs.append(
                {
                    "title": title,
                    "source_url": source_url,
                    "location_raw": location,
                    "salary_raw": self._extract_salary_from_json_v37(row),
                    "employment_type": self._extract_employment_from_json_v37(row),
                    "description": description,
                    "extraction_method": "tier0_oracle_api_v37",
                    "extraction_confidence": 0.9,
                }
            )
            if len(jobs) >= MAX_JOBS_PER_PAGE:
                break

        return jobs
