"""Failure-mode analysis using the local Ollama instance.

After every experiment, the orchestrator collects the holdout cases the
challenger got wrong and asks the local LLM to surface common patterns and
suggest implementable feature improvements. Output is structured JSON so it
can drive automated next-iteration challenger generation.

Two flavours are offered:
  1. classifier-style `analyze_failures` (the original) for FailureCase objects
     coming from the holdout evaluator.
  2. `build_next_iteration_brief` for the A/B test flow in
     backend/scripts/auto_improve.py, which works from the richer
     analyse_results() dict (baseline vs model vs Jobstream wrappers).

We deliberately use Ollama (already running per CLAUDE.md) instead of the
Claude API — it keeps the loop offline-capable and free per iteration.
"""

from __future__ import annotations

import difflib
import html as html_lib
import json
import logging
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import httpx

# `app.core.config.settings` is only needed for the LLM call in
# `analyze_failures` (Ollama URL/model). The other public helpers in this
# module (`build_next_iteration_brief`, `format_brief_for_prompt`,
# `cluster_failures_by_ats`) are pure-Python and are imported by the
# host-side auto_improve daemon, which often does not have the API
# container's full dep set (notably `pydantic_settings`). Defer the import
# into `analyze_failures` so the host-side code path doesn't break on
# `ModuleNotFoundError: No module named 'pydantic_settings'`.

logger = logging.getLogger(__name__)


FAILURE_ANALYSIS_PROMPT = """\
You are helping improve a machine learning model that classifies whether a web \
page is a careers/jobs page, and that extracts job listings from such pages.

Below are {n} cases the model got WRONG on the GOLD holdout set. Each case shows:
- the URL
- the page title
- the model's prediction and confidence
- the ground truth
- a few key features extracted from the page

Your task: identify common patterns in these failures and propose specific, \
implementable improvements.

Respond ONLY with valid JSON in this exact shape:
{{
  "patterns": [
    "<one-sentence pattern observed across multiple failures>",
    ...
  ],
  "missing_features": [
    "<a feature the model clearly isn't using that would help>",
    ...
  ],
  "suggested_features": [
    {{
      "name": "<snake_case feature name>",
      "description": "<one sentence>",
      "python_pseudocode": "<a few lines of pseudocode using BeautifulSoup or regex>"
    }},
    ...
  ],
  "edge_cases": [
    "<categories of pages the model will keep struggling with>",
    ...
  ]
}}

---
FAILED CASES:
{cases}
---
"""


@dataclass
class FailureCase:
    url: str
    title: str
    predicted_label: int
    predicted_confidence: float
    true_label: int
    features: dict

    @property
    def failure_type(self) -> str:
        if self.predicted_label == 1 and self.true_label == 0:
            return "false_positive"
        if self.predicted_label == 0 and self.true_label == 1:
            return "false_negative"
        return "correct"


def _format_cases(cases: list[FailureCase], max_cases: int = 30) -> str:
    """Render up to `max_cases` failures as a readable text block."""
    chunks = []
    for case in cases[:max_cases]:
        nonzero_features = {k: v for k, v in case.features.items() if v not in (0, 0.0, None, "", False)}
        chunks.append(
            f"URL: {case.url}\n"
            f"Title: {case.title}\n"
            f"Type: {case.failure_type}\n"
            f"Confidence: {case.predicted_confidence:.3f}\n"
            f"Key features: {json.dumps(nonzero_features, default=str)[:600]}"
        )
    return "\n\n".join(chunks)


async def analyze_failures(
    cases: list[FailureCase],
    *,
    model: Optional[str] = None,
    timeout_s: int = 120,
) -> dict:
    """Send a batch of failure cases to the local LLM and parse its analysis.

    Returns a dict with keys: patterns, missing_features, suggested_features,
    edge_cases. On any error returns an empty-skeleton dict — the orchestrator
    treats "no analysis produced" as a non-fatal soft failure.
    """
    if not cases:
        return _empty_analysis()

    prompt = FAILURE_ANALYSIS_PROMPT.format(
        n=min(len(cases), 30),
        cases=_format_cases(cases),
    )

    from app.core.config import settings  # deferred — see module-level note

    target_model = model or settings.OLLAMA_MODEL

    try:
        async with httpx.AsyncClient(timeout=timeout_s) as client:
            resp = await client.post(
                f"{settings.OLLAMA_BASE_URL}/api/generate",
                json={
                    "model": target_model,
                    "prompt": prompt,
                    "format": "json",
                    "stream": False,
                    "options": {"temperature": 0.2, "num_predict": 2000},
                },
            )
            resp.raise_for_status()
            payload = resp.json()
            raw = payload.get("response", "")
    except (httpx.HTTPError, json.JSONDecodeError) as e:
        logger.warning("failure_analysis: Ollama call failed: %s", e)
        return _empty_analysis()

    parsed = _safe_parse_json(raw)
    if not parsed:
        logger.warning("failure_analysis: LLM returned non-JSON output")
        return _empty_analysis()

    # Coerce missing keys to empty lists for predictable downstream use
    return {
        "patterns": parsed.get("patterns") or [],
        "missing_features": parsed.get("missing_features") or [],
        "suggested_features": parsed.get("suggested_features") or [],
        "edge_cases": parsed.get("edge_cases") or [],
    }


def _safe_parse_json(text: str) -> Optional[dict]:
    """Parse JSON tolerant of fenced code blocks and trailing prose."""
    if not text:
        return None
    cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip(), flags=re.MULTILINE)
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        # Try to extract the first {...} block
        match = re.search(r"\{.*\}", cleaned, re.DOTALL)
        if not match:
            return None
        try:
            return json.loads(match.group(0))
        except json.JSONDecodeError:
            return None


def _empty_analysis() -> dict:
    return {"patterns": [], "missing_features": [], "suggested_features": [], "edge_cases": []}


# ─── A/B-test flavour: next-iteration brief from analyse_results() dict ─────
#
# The daemon loop (backend/scripts/auto_improve.py) produces an analysis dict
# shaped like:
#   {
#     "failures":   [{domain, ats?, baseline_jobs, model_jobs, baseline_full_wrapper, ...}, ...],
#     "gaps":       [ ... same shape ...],
#     "spot_checks":[ ... ],
#     "fail_count", "gap_count", "success_count", "volume_ratio",
#     "total_baseline_jobs", "total_model_jobs", "match_breakdown", ...
#   }
# Codex already reads this, but only as free-form text. `build_next_iteration_brief`
# condenses it into a constrained schema naming THE axis + THE ATS cluster with
# the highest expected composite-score delta.

# Axes we report. Keep in sync with _composite_score_standalone in ml_models.py.
_AXES = ("field_completeness", "quality_extraction", "volume_accuracy", "discovery")

# When grouping failures by ATS we key off `ats_platform` on the entry;
# callers are expected to populate it via tiered_extractor.ats_fingerprinter.
_UNKNOWN_ATS = "unknown"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _noise_score(text: str) -> float:
    """Estimate description noise from whitespace/entity density."""
    if not text:
        return 0.0
    raw = text or ""
    entities = len(re.findall(r"&[a-zA-Z#0-9]+;", raw))
    whitespace = sum(1 for ch in raw if ch in " \t\n\r")
    newline_penalty = len(re.findall(r"\n{2,}", raw)) * 0.1
    return round(min(1.0, ((entities + whitespace) / max(1, len(raw))) + newline_penalty), 4)


def _landmark_excerpt(html: str, xpath: str, max_chars: int = 400) -> str:
    if not html or not xpath:
        return ""
    try:
        from lxml import html as lxml_html
    except ImportError:
        return ""
    try:
        root = lxml_html.fromstring(html)
        matches = root.xpath(xpath)
    except Exception:  # noqa: BLE001
        return ""
    for match in matches:
        text = match.text_content() if hasattr(match, "text_content") else str(match)
        cleaned = re.sub(r"\s+", " ", html_lib.unescape(text or "")).strip()
        if cleaned:
            return cleaned[:max_chars]
    return ""


def _axes_scorecard(analysis: dict) -> dict[str, float]:
    """Derive a rough 4-axis scorecard from the analysis dict.

    This isn't the authoritative composite (that lives in ml_models.py) — it
    just gives Codex a directional read on which axis is hurting most.
    """
    fail = int(analysis.get("fail_count", 0) or 0)
    gap = int(analysis.get("gap_count", 0) or 0)
    succ = int(analysis.get("success_count", 0) or 0)
    total = max(1, fail + gap + succ)

    # Discovery ≈ sites where the model found *any* page (no discovery failure)
    discovery_denom = total
    discovery_hits = total - sum(
        1 for f in analysis.get("failures", []) if not f.get("model_url_found")
    )
    discovery = 100.0 * discovery_hits / max(1, discovery_denom)

    # Quality_extraction ≈ sites with non-empty model output minus Type-1 warnings
    quality_hits = succ + gap  # any jobs at all
    quality = 100.0 * quality_hits / max(1, total)

    # Volume accuracy from total_* counters
    vol_ratio = float(analysis.get("volume_ratio", 1.0) or 0.0)
    if vol_ratio <= 0:
        volume = 0.0
    elif vol_ratio <= 1.0:
        volume = 100.0 * vol_ratio
    else:
        # Symmetric penalty once ratio > 1.5
        overshoot = max(0.0, vol_ratio - 1.5)
        volume = max(0.0, 100.0 - 100.0 * overshoot)

    # Field completeness — best effort from per-site `fields` dicts
    field_vals = []
    for section in ("failures", "gaps", "spot_checks"):
        for e in analysis.get(section, []) or []:
            mf = e.get("model_fields") or {}
            if not mf:
                continue
            present = sum(1 for v in mf.values() if v)
            field_vals.append(100.0 * present / 6.0)
    field_completeness = (sum(field_vals) / len(field_vals)) if field_vals else 0.0

    return {
        "discovery": round(discovery, 1),
        "quality_extraction": round(quality, 1),
        "volume_accuracy": round(volume, 1),
        "field_completeness": round(field_completeness, 1),
    }


def _derive_ats(entry: dict) -> str:
    """Pull an ATS label out of whatever fields happen to be populated.

    auto_improve.py doesn't directly fingerprint the ATS today; we peek at the
    wrapper, the URL, and the model's tier label. If nothing matches we bucket
    as "unknown" rather than inventing one.
    """
    ats = (entry.get("ats_platform") or "").strip().lower()
    if ats:
        return ats
    wrapper = entry.get("baseline_full_wrapper") or {}
    url = (entry.get("model_url_found") or entry.get("test_url") or "").lower()
    tier = (entry.get("model_tier") or "").lower()
    hay = " ".join([json.dumps(wrapper)[:500].lower(), url, tier])
    for needle, label in (
        ("greenhouse", "greenhouse"),
        ("lever.co", "lever"),
        ("ashby", "ashby"),
        ("workday", "workday"),
        ("myworkdayjobs", "workday"),
        ("bamboohr", "bamboohr"),
        ("smartrecruiters", "smartrecruiters"),
        ("icims", "icims"),
        ("taleo", "taleo"),
        ("successfactors", "successfactors"),
        ("jobvite", "jobvite"),
        ("breezy", "breezyhr"),
        ("rippling", "rippling"),
        ("oracle", "oracle_cx"),
        ("salesforce", "salesforce"),
        ("martianlogic", "martianlogic"),
    ):
        if needle in hay:
            return label
    return _UNKNOWN_ATS


def cluster_failures_by_ats(entries: list[dict]) -> dict[str, list[dict]]:
    """Group failure/gap entries by detected ATS platform.

    Returns a dict keyed by ATS label. Each entry gets an `_ats` side-field
    populated in place so downstream code can re-use the label.
    """
    clusters: dict[str, list[dict]] = defaultdict(list)
    for e in entries or []:
        label = _derive_ats(e)
        e["_ats"] = label
        clusters[label].append(e)
    # Biggest clusters first
    return dict(sorted(clusters.items(), key=lambda kv: -len(kv[1])))


def _wrapper_selector_hint(entry: dict) -> dict:
    """Pull the 3 highest-signal keys from baseline_full_wrapper.

    Matches ``auto_improve._compact_baseline_selectors`` — prompt + brief MUST
    describe selectors the same way, and both pay tokens for every key inlined.
    The full wrapper remains on disk in the per-iteration context dir for
    Codex to open when it actually needs the rest.
    """
    w = entry.get("baseline_full_wrapper") or {}
    if not isinstance(w, dict):
        return {}
    keys = ("boundary", "title", "details_page_description_paths")
    out: dict = {}
    for k in keys:
        if k in w and w[k]:
            v = w[k]
            if isinstance(v, str) and len(v) > 160:
                v = v[:157] + "..."
            out[k] = v
    return out


def _score_ats_cluster(entries: list[dict]) -> float:
    """Estimate composite-point delta recoverable by fixing this ATS cluster."""
    baseline = sum(int(e.get("baseline_jobs", 0) or 0) for e in entries)
    model = sum(int(e.get("model_jobs", 0) or 0) for e in entries)
    missing = max(0, baseline - model)
    # Rough heuristic: each missing job is worth ~0.05 composite points up to a
    # cluster cap of 20 points. This is directional guidance, not a promise.
    return round(min(20.0, missing * 0.05), 1)


def build_next_iteration_brief(
    analysis: dict,
    *,
    max_ats_clusters: int = 4,
    max_entries_per_cluster: int = 3,
) -> dict:
    """Build a compact, actionable brief for the next Codex iteration.

    Output schema (stable — tweak with care, Codex reads it):
        {
          "axes_scorecard": {<axis>: <0..100>},
          "top_axis_to_fix": "<axis>",
          "ats_clusters": [
            {
              "ats": "<label>",
              "sites_failing": int,
              "expected_delta": float,
              "baseline_selector_hint": { ... wrapper keys ... },
              "example_domains": [...]
            }, ...
          ],
          "detail_page_candidates": [
            {"domain": ..., "baseline_desc_paths": [...], "model_desc_len": int}
          ],
          "simplification_candidates": ["tier v1.6 under-used", ...]
        }
    """
    scorecard = _axes_scorecard(analysis)
    # Pick the axis furthest from 100 as the primary target.
    top_axis = min(_AXES, key=lambda a: scorecard.get(a, 100.0))

    improvement_targets = (analysis.get("failures") or []) + (analysis.get("gaps") or [])
    clusters = cluster_failures_by_ats(improvement_targets)

    ats_clusters = []
    for label, entries in list(clusters.items())[:max_ats_clusters]:
        if label == _UNKNOWN_ATS and len(ats_clusters) >= 1:
            # Keep at most one unknown-ATS bucket — not a useful fix target.
            continue
        sample = entries[:max_entries_per_cluster]
        # Take the wrapper hint from the first entry that actually has one.
        hint: dict = {}
        for e in sample:
            h = _wrapper_selector_hint(e)
            if h:
                hint = h
                break
        ats_clusters.append({
            "ats": label,
            "sites_failing": len(entries),
            "expected_delta": _score_ats_cluster(entries),
            "baseline_selector_hint": hint,
            "example_domains": [e.get("domain", "?") for e in sample],
        })

    # Detail-page candidates: sites where baseline has details_page_description_paths
    # but the model's description is short/empty.
    detail_candidates = []
    for e in improvement_targets:
        w = e.get("baseline_full_wrapper") or {}
        paths = w.get("details_page_description_paths") if isinstance(w, dict) else None
        if not paths:
            continue
        model_desc = (e.get("model_sample_desc") or "").strip()
        if len(model_desc) >= 400:
            continue  # already has a decent description
        detail_candidates.append({
            "domain": e.get("domain", "?"),
            "baseline_desc_paths": paths if isinstance(paths, list) else [paths],
            "model_desc_len": len(model_desc),
        })
        if len(detail_candidates) >= 6:
            break

    # Simplification candidates — lightweight heuristics, pattern-based.
    simplification = []
    tier_breakdown = analysis.get("tier_breakdown") or {}
    if tier_breakdown:
        parent_hits = tier_breakdown.get("parent_v16", 0) or tier_breakdown.get("v16", 0)
        total_tier = sum(v for v in tier_breakdown.values() if isinstance(v, int))
        if total_tier and parent_hits / max(1, total_tier) < 0.1:
            simplification.append(
                "parent v1.6 tier hit-rate <10% — recent fallback tiers may be over-firing"
            )
    if analysis.get("fail_count", 0) and analysis.get("gap_count", 0) == 0:
        simplification.append("all misses are hard failures (no gaps) — bias may be too conservative")
    if analysis.get("gap_count", 0) > 3 * max(1, analysis.get("fail_count", 1)):
        simplification.append(
            "gaps >> failures — listing extraction largely works; invest in detail-page enrichment, not more tiers"
        )

    return {
        "axes_scorecard": scorecard,
        "top_axis_to_fix": top_axis,
        "ats_clusters": ats_clusters,
        "detail_page_candidates": detail_candidates,
        "simplification_candidates": simplification,
    }


def format_brief_for_prompt(brief: dict) -> str:
    """Render a next-iteration brief as a compact Markdown block."""
    if not brief:
        return ""
    scorecard = brief.get("axes_scorecard") or {}
    top = brief.get("top_axis_to_fix") or "?"

    lines = ["## NEXT-ITERATION BRIEF (READ FIRST)", ""]
    lines.append(f"**Top axis to fix:** `{top}` (lowest scorecard value)")
    lines.append("")
    lines.append("| Axis | Score |")
    lines.append("|------|-------|")
    for axis in _AXES:
        lines.append(f"| {axis} | {scorecard.get(axis, 'n/a')} |")
    lines.append("")

    clusters = brief.get("ats_clusters") or []
    if clusters:
        lines.append("### ATS clusters (highest-impact first)")
        lines.append("")
        for c in clusters:
            lines.append(
                f"- **{c.get('ats','?')}** — {c.get('sites_failing',0)} sites, "
                f"~{c.get('expected_delta',0)} composite pts recoverable"
            )
            hint = c.get("baseline_selector_hint") or {}
            if hint:
                hint_json = json.dumps(hint, ensure_ascii=False)
                if len(hint_json) > 900:
                    hint_json = hint_json[:900] + "…"
                lines.append(f"  - baseline wrapper hint: `{hint_json}`")
            examples = c.get("example_domains") or []
            if examples:
                lines.append(f"  - examples: {', '.join(examples)}")
        lines.append("")

    details = brief.get("detail_page_candidates") or []
    if details:
        lines.append("### Detail-page enrichment candidates")
        lines.append(
            "Sites where Jobstream follows per-job URLs to fetch richer "
            "description/location. The model's current description is <400 chars; "
            "enriching the detail page is the highest-ROI field_completeness fix."
        )
        lines.append("")
        for d in details:
            paths = d.get("baseline_desc_paths") or []
            paths_s = ", ".join(p for p in paths if isinstance(p, str))[:300]
            lines.append(
                f"- {d.get('domain','?')}: baseline paths=[{paths_s}], "
                f"model desc len={d.get('model_desc_len',0)}"
            )
        lines.append("")

    simp = brief.get("simplification_candidates") or []
    if simp:
        lines.append("### Simplification signals")
        for s in simp:
            lines.append(f"- {s}")
        lines.append("")

    lines.append(
        "**Action:** design a SINGLE targeted change that moves `"
        f"{top}` (the weakest axis). Prefer a fix to the top ATS cluster or "
        "generic detail-page enrichment over narrow pattern patches."
    )
    lines.append("")
    return "\n".join(lines)


def build_site_diff_package(entry: dict, html_file: str | None = None, detail_html_file: str | None = None) -> dict:
    baseline_jobs = _coerce_jobs(entry.get("baseline_extracted_jobs"))
    champion_jobs = _coerce_jobs(entry.get("model_extracted_jobs"))
    wrapper = entry.get("baseline_full_wrapper") if isinstance(entry.get("baseline_full_wrapper"), dict) else {}
    listing_html = _read_text_if_exists(html_file)
    detail_html = _read_text_if_exists(detail_html_file)

    champion_summary = _job_output_summary(champion_jobs, entry.get("model_tier") or "")
    baseline_summary = _job_output_summary(baseline_jobs, "jobstream_wrapper")

    diff = {
        "missing_jobs": _titles_minus(baseline_jobs, champion_jobs),
        "extra_jobs": _titles_minus(champion_jobs, baseline_jobs),
        "field_gap_per_job": _field_gap_per_job(baseline_jobs, champion_jobs),
        "description_noise_delta": round(
            champion_summary["desc_noise_score"] - baseline_summary["desc_noise_score"],
            4,
        ),
        "volume_ratio": round(len(champion_jobs) / max(1, len(baseline_jobs)), 4),
    }
    return {
        "url": entry.get("test_url") or entry.get("model_url_found") or "",
        "ats": _derive_ats(entry),
        "champion_output": champion_summary,
        "baseline_output": baseline_summary,
        "diff": diff,
        "html_landmark_excerpts": {
            "listing_page_tier_hint": _landmark_excerpt(listing_html, wrapper.get("boundary") or "//main"),
            "detail_page_tier_hint": _landmark_excerpt(detail_html, (wrapper.get("details_page_description_paths") or ["//article"])[0] if wrapper.get("details_page_description_paths") else "//article"),
            "baseline_wrapper_boundary": _landmark_excerpt(listing_html, wrapper.get("boundary") or "//main"),
            "baseline_wrapper_title": _landmark_excerpt(listing_html, wrapper.get("title") or "//title") or _landmark_excerpt(listing_html, "//main"),
            "baseline_wrapper_detail_desc_paths": [
                _landmark_excerpt(detail_html, xpath)
                for xpath in (wrapper.get("details_page_description_paths") or [])[:3]
                if xpath
            ],
        },
    }


def write_postmortem(prev_version: str, outcome: dict) -> Optional[dict]:
    parent_version = outcome.get("parent_version") or _previous_version(prev_version)
    if not parent_version:
        return None
    child_file = _version_file(prev_version)
    parent_file = _version_file(parent_version)
    if not child_file.exists() or not parent_file.exists():
        return None

    diff_text = "".join(
        difflib.unified_diff(
            parent_file.read_text().splitlines(True),
            child_file.read_text().splitlines(True),
            fromfile=parent_file.name,
            tofile=child_file.name,
            n=3,
        )
    )[:6000]
    analysis = _ollama_postmortem(diff_text, outcome)
    if not analysis:
        return None
    likely = (analysis.get("likely_cause") or "").strip()
    if not _references_exist_in_diff(diff_text, likely):
        logger.warning("postmortem dropped for %s: likely_cause did not match diff", prev_version)
        return None

    from app.ml.champion_challenger import memory_store

    return memory_store.append_rejection(
        prev_version,
        gate_failures=outcome.get("gate_failures") or [],
        axes_delta=outcome.get("axes_delta") or {},
        symptom=(analysis.get("symptom") or "")[:240],
        likely_cause=likely[:240],
        rule_for_future=(analysis.get("rule_for_future") or "")[:240],
        fixture_score=outcome.get("fixture_score"),
        composite=outcome.get("composite"),
        path=outcome.get("memory_path"),
    )


def _coerce_jobs(jobs: object) -> list[dict]:
    return [job for job in (jobs or []) if isinstance(job, dict)]


def _job_output_summary(jobs: list[dict], tier_used: str) -> dict:
    descriptions = [(job.get("description") or "") for job in jobs]
    desc_sample = next((desc for desc in descriptions if desc.strip()), "")
    fields_present = {
        field: sum(1 for job in jobs if (job.get(field) or "").strip())
        for field in ("title", "source_url", "location_raw", "salary_raw", "employment_type", "description")
    }
    avg_desc_len = round(sum(len(desc.strip()) for desc in descriptions) / max(1, len(descriptions)), 1) if jobs else 0
    return {
        "tier_used": tier_used,
        "titles": [job.get("title") or "" for job in jobs[:6]],
        "jobs_count": len(jobs),
        "fields_present_per_job": fields_present,
        "avg_desc_len": avg_desc_len,
        "desc_noise_score": _noise_score(desc_sample),
        "sample_description_chunk": (desc_sample or "")[:180],
    }


def _titles_minus(left: list[dict], right: list[dict]) -> list[str]:
    right_titles = {(job.get("title") or "").strip().lower() for job in right}
    missing = []
    for job in left:
        title = (job.get("title") or "").strip()
        if title and title.lower() not in right_titles:
            missing.append(title)
    return missing[:6]


def _field_gap_per_job(baseline_jobs: list[dict], champion_jobs: list[dict]) -> dict[str, int]:
    gaps: dict[str, int] = {}
    for field in ("title", "source_url", "location_raw", "salary_raw", "employment_type", "description"):
        baseline_present = sum(1 for job in baseline_jobs if (job.get(field) or "").strip())
        champion_present = sum(1 for job in champion_jobs if (job.get(field) or "").strip())
        gaps[field] = max(0, baseline_present - champion_present)
    return gaps


def _read_text_if_exists(path: str | None) -> str:
    if not path:
        return ""
    p = Path(path)
    if not p.exists():
        return ""
    return p.read_text(encoding="utf-8", errors="replace")


def _version_file(version: str) -> Path:
    num = re.sub(r"[^\d]", "", version)
    return _repo_root() / "backend" / "app" / "crawlers" / f"tiered_extractor_v{num}.py"


def _previous_version(version: str) -> str | None:
    digits = re.sub(r"[^\d]", "", version)
    if not digits:
        return None
    numeric = int(digits)
    if numeric <= 16:
        return None
    return f"v{numeric - 1}"


def _ollama_postmortem(diff_text: str, outcome: dict) -> Optional[dict]:
    prompt = (
        "Return strict JSON with keys symptom, likely_cause, rule_for_future.\n"
        f"Outcome: {json.dumps(outcome, default=str)[:1000]}\n"
        f"Diff:\n{diff_text[:6000]}"
    )
    try:
        from app.core.config import settings
    except Exception:  # noqa: BLE001
        return None
    try:
        with httpx.Client(timeout=60) as client:
            resp = client.post(
                f"{settings.OLLAMA_BASE_URL}/api/generate",
                json={
                    "model": settings.OLLAMA_MODEL,
                    "prompt": prompt,
                    "format": "json",
                    "stream": False,
                    "options": {"temperature": 0.2, "num_predict": 400},
                },
            )
            resp.raise_for_status()
            payload = resp.json()
    except Exception as exc:  # noqa: BLE001
        logger.warning("postmortem ollama call failed: %s", exc)
        return None
    parsed = _safe_parse_json(payload.get("response", ""))
    if not parsed:
        return None
    return {
        "symptom": parsed.get("symptom") or "",
        "likely_cause": parsed.get("likely_cause") or "",
        "rule_for_future": parsed.get("rule_for_future") or "",
    }


def _references_exist_in_diff(diff_text: str, likely_cause: str) -> bool:
    if not likely_cause:
        return False
    line_match = re.search(r"line\s+(\d+)", likely_cause, re.IGNORECASE)
    if line_match:
        return f"@@ -{line_match.group(1)}" in diff_text or f"+{line_match.group(1)}" in diff_text
    fn_matches = re.findall(r"([A-Za-z_][A-Za-z0-9_]*)\s*\(", likely_cause)
    if fn_matches:
        return any(name in diff_text for name in fn_matches)
    return any(token in diff_text for token in re.findall(r"[A-Za-z_][A-Za-z0-9_]{3,}", likely_cause)[:3])
