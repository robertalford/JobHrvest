"""Tiered Extraction Engine v7.0 — listing-context enrichment + safe merge."""
from __future__ import annotations
import re
from lxml import etree
from app.crawlers.tiered_extractor import MAX_JOBS_PER_PAGE, _parse_html, _resolve_url, _text
from app.crawlers.tiered_extractor_v16 import TieredExtractorV16
from app.crawlers.tiered_extractor_v69 import TieredExtractorV69

_CTA_TAIL = re.compile(
    r"\b(?:view\s+details|apply\s+now|apply\s+here|learn\s+more|read\s+more|sponsored)\b",
    re.IGNORECASE,
)
_DATE_ONLY = re.compile(r"^\d{1,2}[/-]\d{1,2}[/-]\d{2,4}$")
_WEAK_ROLE_HINT = re.compile(
    r"\b(?:manager|engineer|developer|analyst|coordinator|specialist|assistant|"
    r"technician|operator|consultant|advisor|officer|executive|administrator|"
    r"supervisor|sales|attendant|teacher|designer|janitor)\b",
    re.IGNORECASE,
)
_NON_JOB_TITLE = re.compile(
    r"^(?:job\s+vacancies|current\s+vacancies|search\s+jobs|view\s+details|"
    r"join\s+our\s+team|working\s+with\s+us)$",
    re.IGNORECASE,
)
_SHOW_MORE_TITLE = re.compile(r"^show\s+\d+\s+more$", re.IGNORECASE)

class TieredExtractorV70(TieredExtractorV16):
    """v7.0 extractor: keep v6.9 precision and fill missing listing fields."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        page_url = career_page.url if hasattr(career_page, "url") else str(career_page)
        v69 = self._delegate_v69()
        jobs = await v69.extract(career_page, company, html or "")
        if not jobs:
            return []
        methods = {str(j.get("extraction_method") or "") for j in jobs}
        if methods == {"tier2_linked_cards_v67"}:
            jobs = self._merge_with_broader_tier2_v70(jobs, page_url, html or "", v69)
        jobs = self._enrich_from_listing_context_v70(jobs, page_url, html or "", v69)
        jobs = self._drop_obvious_non_jobs_v70(jobs)
        return jobs[:MAX_JOBS_PER_PAGE]

    @staticmethod
    def _delegate_v69() -> TieredExtractorV69:
        return TieredExtractorV69()

    def _merge_with_broader_tier2_v70(
        self, jobs: list[dict], page_url: str, html: str, v69: TieredExtractorV69
    ) -> list[dict]:
        tier2 = self._extract_tier2_v16(page_url, html) or []
        if len(tier2) < len(jobs) + 2:
            return jobs
        tier2 = [j for j in tier2 if self._is_merge_candidate_v70(j, v69)]
        if len(tier2) < len(jobs) + 2 or not v69._passes_jobset_validation(tier2, page_url):
            return jobs
        out = list(jobs)
        seen = {self._norm_url_v70(j.get("source_url") or "") for j in jobs}
        for cand in tier2:
            url = self._norm_url_v70(cand.get("source_url") or "")
            if not url or url in seen:
                continue
            out.append(cand)
            seen.add(url)
        return v69._dedupe_basic_v66(out)

    def _is_merge_candidate_v70(self, job: dict, v69: TieredExtractorV69) -> bool:
        title = str(job.get("title") or "").strip()
        source_url = str(job.get("source_url") or "")
        if v69._is_valid_title_v60(title):
            return True
        if not v69._is_job_like_url(source_url):
            return False
        if not title or len(title) > 80 or len(title.split()) > 5:
            return False
        if _NON_JOB_TITLE.match(title):
            return False
        return bool(_WEAK_ROLE_HINT.search(title))

    def _enrich_from_listing_context_v70(
        self, jobs: list[dict], page_url: str, html: str, v69: TieredExtractorV69
    ) -> list[dict]:
        root = _parse_html(html)
        if root is None:
            return jobs

        contexts = self._build_listing_context_map_v70(root, page_url, v69)
        if not contexts:
            return jobs

        out: list[dict] = []
        for job in jobs:
            item = dict(job)
            ctx = contexts.get(self._norm_url_v70(item.get("source_url") or ""))
            if not ctx:
                out.append(item)
                continue

            if not (item.get("location_raw") or "").strip() and ctx.get("location"):
                item["location_raw"] = ctx["location"]

            curr = (item.get("description") or "").strip()
            cand = str(ctx.get("description") or "").strip()
            if cand and len(cand) > len(curr):
                item["description"] = cand
            out.append(item)
        return out

    def _build_listing_context_map_v70(
        self, root: etree._Element, page_url: str, v69: TieredExtractorV69
    ) -> dict[str, dict[str, str]]:
        contexts: dict[str, dict[str, str]] = {}
        anchors = root.xpath("//a[@href and not(starts-with(@href,'#')) and not(starts-with(@href,'javascript:'))]")
        for a_el in anchors[:1000]:
            source_url = _resolve_url((a_el.get("href") or "").strip(), page_url) or ""
            if not source_url or source_url.rstrip("/") == page_url.rstrip("/"):
                continue

            card = self._card_container_v70(a_el)
            card_text = self._clean_v70(_text(card) or "")
            if len(card_text) < 20 or len(card_text) > 1400:
                continue

            title = v69._normalize_title((v69._extract_card_title_v67(a_el) or _text(a_el) or ""))
            if not title or not v69._is_valid_title_v60(title):
                continue

            description = self._desc_from_card_v70(card_text, title)
            location = self._location_from_card_v70(card, card_text, title)
            if not description and not location:
                continue

            rec = contexts.setdefault(self._norm_url_v70(source_url), {"description": "", "location": ""})
            if description and len(description) > len(rec.get("description") or ""):
                rec["description"] = description
            if location and not rec.get("location"):
                rec["location"] = location
        return contexts

    def _card_container_v70(self, node: etree._Element) -> etree._Element:
        best, cur = node, node
        for _ in range(5):
            parent = cur.getparent()
            if parent is None or not isinstance(parent.tag, str):
                break
            txt = self._clean_v70(_text(parent) or "")
            links = len(parent.xpath(".//a[@href]"))
            if 20 <= len(txt) <= 1400 and links <= 8:
                best = parent
            if len(txt) > 3000 or links > 20:
                break
            cur = parent
        return best

    def _location_from_card_v70(self, container: etree._Element, card_text: str, title: str) -> str | None:
        loc_nodes = container.xpath(
            ".//*[contains(@class,'location') or contains(@class,'city') or contains(@class,'region')"
            " or contains(@class,'office') or contains(@class,'country') or contains(@class,'state')]"
        )
        for node in loc_nodes[:4]:
            txt = self._clean_v70(_text(node) or "")
            if self._is_reasonable_location_v70(txt, title):
                return txt

        m = re.search(r"\s-\s([^|·•]{2,80})(?:\||$)", card_text)
        if m:
            cand = self._clean_v70(m.group(1))
            if self._is_reasonable_location_v70(cand, title):
                return cand
        return None

    def _desc_from_card_v70(self, card_text: str, title: str) -> str | None:
        text = card_text
        if title:
            i = text.lower().find(title.lower())
            if i >= 0:
                text = (text[:i] + text[i + len(title):]).strip()

        text = self._clean_v70(_CTA_TAIL.sub(" ", text)).strip(" -|·•")
        if len(text) < 20 or _DATE_ONLY.match(text):
            return None
        return text[:900]

    def _is_reasonable_location_v70(self, value: str, title: str) -> bool:
        txt = self._clean_v70(value)
        if not txt or len(txt) < 2 or len(txt) > 100:
            return False
        if txt.lower() == (title or "").strip().lower() or _DATE_ONLY.match(txt):
            return False
        if not any(ch.isalpha() for ch in txt):
            return False
        if re.search(r"(?:\$|salary|apply|view\s+details|full\s*time|part\s*time)", txt, re.IGNORECASE):
            return False
        return True

    @staticmethod
    def _clean_v70(value: str) -> str:
        return re.sub(r"\s+", " ", (value or "")).strip()

    @staticmethod
    def _norm_url_v70(value: str) -> str:
        return str(value or "").strip().rstrip("/")

    def _drop_obvious_non_jobs_v70(self, jobs: list[dict]) -> list[dict]:
        out: list[dict] = []
        for job in jobs:
            title = self._clean_v70(str(job.get("title") or ""))
            if not title:
                continue
            title_l = title.lower()
            source_url = self._norm_url_v70(job.get("source_url") or "")

            if _NON_JOB_TITLE.match(title) or _SHOW_MORE_TITLE.match(title):
                continue
            if "/show_more" in source_url and title_l.startswith("show "):
                continue

            item = dict(job)
            item["title"] = title
            out.append(item)
        return out
