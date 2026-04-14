#!/usr/bin/env python3
"""
Automated Model Improvement Loop

After a test completes on the /models page, this script:
1. Fetches the test results via API
2. Analyses failures (model_worse, model_failed) with HTML context
3. Generates a detailed prompt for Codex (GPT 5.3)
4. Runs `codex exec --full-auto` to implement improvements
5. Triggers a new 50-site test on the new model

Usage:
    python scripts/auto_improve.py                    # Run once
    python scripts/auto_improve.py --loop             # Continuous loop
    python scripts/auto_improve.py --model-id <UUID>  # Specific model
"""

import argparse
import json
import os
import re
import subprocess
import sys
import time
from datetime import datetime
from urllib.parse import urlparse

import httpx

# ── Config ──
API_BASE = os.environ.get("JH_API_URL", "http://localhost:8001/api/v1")
USERNAME = os.environ.get("JH_USERNAME", "r.m.l.alford@gmail.com")
PASSWORD = os.environ.get("JH_PASSWORD", "Uu00dyandben!")
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CODEX_MODEL = os.environ.get("CODEX_MODEL", "gpt-5.3-codex")
SAMPLE_SIZE = 40  # 20 regression (from prev run) + 20 new sites
MAX_FAILURES_IN_PROMPT = 4  # one representative per top ATS cluster (brief clusters do the ranking)
MAX_GAPS_IN_PROMPT = 4       # same — post-2026-04-14 compression target
MAX_HTML_SNIPPET = 1500  # Max chars of HTML per failure
MAX_DETAIL_HTML_SNIPPET = 3000  # detail pages are denser and deserve more budget
PROMPT_MAX_BYTES = 16 * 1024  # 16 KB ceiling — down from 28 KB (dynamic head + compact brief = more signal, fewer tokens)
# Multi-candidate generation: how many parallel Codex runs per cycle. Each
# runs with a distinct FOCUS DIRECTIVE (see DEFAULT_FOCUS_DIRECTIVES); the best
# by fixture composite wins. Default 3 post-2026-04-14 reset because we now
# have the fixture harness + silver labels to differentiate candidates.
AUTO_IMPROVE_CANDIDATES_N = int(os.environ.get("AUTO_IMPROVE_CANDIDATES_N", "3"))

# Focus directives — one per axis. Position matters: index 0 is the default
# for single-candidate mode, so keep it pointed at the weakest axis of the
# current champion. v6.9's axes at reset: discovery 100 · quality 100 ·
# volume 96.2 · field-completeness 45.3 → field-completeness is the dragging
# axis, so that is candidate 0. Quality/volume candidates exist mostly to
# catch regressions on the strong axes.
DEFAULT_FOCUS_DIRECTIVES = (
    "Axis: FIELD_COMPLETENESS. The dragging axis — jobs found but missing "
    "location/description/salary/employment_type that the baseline extracts. "
    "Prefer general detail-page enrichment (extend the existing enricher) "
    "over per-site selectors. Goal: +10 points on this axis without "
    "regressing the other three.",
    "Axis: QUALITY_EXTRACTION. Target the biggest ATS cluster whose jobs are "
    "being misclassified (nav/heading/CMS artefacts slipping through, or real "
    "jobs being filtered out). Fix at the platform level — one handler, 3+ "
    "sites improved. Goal: keep quality ≥ current champion's score (no Type 1 "
    "regressions) while recovering missed jobs.",
    "Axis: VOLUME_ACCURACY. Target clusters where volume_ratio is <0.9 or "
    ">1.5 (over-extraction). Prefer pagination, structured-data cascading, or "
    "API-level fixes over DOM hacks. Goal: tighten volume ratio toward 1.0 "
    "without crossing the 1.5 over-extraction guard.",
)


def _weakest_axis(axes: dict) -> str:
    """Return the axis with the lowest score (our target axis). Defaults to
    field_completeness when axes are missing — that's the known weak spot
    post-2026-04-14 reset."""
    if not axes:
        return "field_completeness"
    return min(axes, key=lambda k: axes.get(k, 100))


def _truncate_bytes(text: str, limit: int) -> str:
    encoded = text.encode("utf-8")
    if len(encoded) <= limit:
        return text
    return encoded[: max(0, limit - 3)].decode("utf-8", errors="ignore") + "..."


def _load_previous_fixture_report(current_name: str) -> str:
    report_dir = os.path.join(os.path.dirname(PROJECT_DIR), "storage", "auto_improve_fixture_reports")
    if not os.path.isdir(report_dir):
        return "_no fixture report found_"
    preferred = os.path.join(report_dir, f"{current_name.replace('.', '')}.json")
    candidates = [preferred] if os.path.exists(preferred) else []
    if not candidates:
        candidates = sorted(
            (os.path.join(report_dir, name) for name in os.listdir(report_dir) if name.endswith(".json")),
            key=os.path.getmtime,
            reverse=True,
        )
    if not candidates:
        return "_no fixture report found_"
    try:
        with open(candidates[0]) as fh:
            payload = json.load(fh)
        challenger = payload.get("challenger") or {}
        return (
            f"fixtures={challenger.get('fixtures', '?')}, "
            f"passed={challenger.get('fixtures_passed', '?')}, "
            f"composite={challenger.get('composite', '?')}, "
            f"axes={json.dumps(challenger.get('axes') or {})}"
        )
    except Exception:
        return "_fixture report unreadable_"


def _build_current_champion_head(current_model: dict, token: str | None = None) -> str:
    """Queries the live champion + recent memory and returns a compact Markdown
    block that leads the prompt. This is the ONE place Codex learns what the
    current state of the world is; everything else in the prompt is diff against it.
    """
    # Champion composite + axes come from the last completed test run's summary
    tr = current_model.get("latest_test_run") or {}
    summary = (tr.get("results_detail") or {}).get("summary") or {}
    champ = summary.get("champion_composite") or {}

    axes = {k: champ.get(k, 0) for k in
            ("discovery", "quality_extraction", "volume_accuracy", "field_completeness")}
    composite = champ.get("composite", 0)

    # Resolve live champion name via API if possible — falls back to whatever
    # the test run labelled as champion.
    champion_name = "unknown"
    try:
        if token:
            models = api_get("/ml-models/?page=1&page_size=20", token).get("items", [])
            for m in models:
                if (m.get("status") or "").lower() in ("live", "champion"):
                    champion_name = m["name"]
                    break
    except Exception:
        pass
    if champion_name == "unknown":
        champion_name = current_model.get("name", "?")

    weak_axis = _weakest_axis(axes)
    gap_to_90 = max(0.0, 90.0 - float(composite or 0))

    # Pull recent changes + bans from the memory store (v2 schema). If the
    # module isn't importable (running outside backend sys.path) we quietly
    # fall through to a minimal block — losing these signals is degradation,
    # not a failure mode.
    recent_changes = "_memory_store unavailable_"
    recent_rejections = "_memory_store unavailable_"
    banned = "_memory_store unavailable_"
    try:
        sys.path.insert(0, os.path.join(os.path.dirname(PROJECT_DIR), "backend"))
        from app.ml.champion_challenger import memory_store  # type: ignore
        mem = memory_store.load()
        recent_changes = memory_store.render_recent_changes_for_prompt(mem, max_items=3)
        recent_rejections = memory_store.render_recent_rejections_for_prompt(mem, max_items=3)
        banned = memory_store.render_banned_for_prompt(mem)
    except Exception as e:
        print(f"[auto_improve] memory_store unavailable: {e}")

    return "\n".join([
        _truncate_bytes(
            f"## CURRENT CHAMPION\n\n"
            f"Champion: **{champion_name}**   Composite: **{composite}**\n"
            f"Axes: discovery {axes['discovery']} · quality {axes['quality_extraction']} · volume {axes['volume_accuracy']} · field-completeness {axes['field_completeness']}\n"
            f"Target axis this run: **{weak_axis}**   Gap to 90: **{gap_to_90:.1f}**\n",
            400,
        ),
        _truncate_bytes(f"## LAST 3 REJECTION POST-MORTEMS\n\n{recent_rejections}\n", 1200),
        _truncate_bytes(f"## RECENT PLATFORM CHANGES\n\n{recent_changes}\n", 1200),
        _truncate_bytes(f"## BANNED APPROACHES\n\n{banned}\n", 400),
        _truncate_bytes(
            "## FOCUS DIRECTIVE\n\n"
            "Do not regress axes already at or above 95. Prefer overriding `_extract_raw` "
            "or narrower extractor helpers instead of replacing `extract()` unless the "
            "change is explicitly about enrichment finalization.\n",
            600,
        ),
        "---\n",
    ])


def get_token() -> str:
    r = httpx.post(f"{API_BASE}/auth/login", data={"username": USERNAME, "password": PASSWORD})
    r.raise_for_status()
    return r.json()["access_token"]


def api_get(path: str, token: str):
    r = httpx.get(f"{API_BASE}{path}", headers={"Authorization": f"Bearer {token}"}, timeout=30)
    r.raise_for_status()
    return r.json()


def api_post(path: str, token: str, data: dict = None):
    r = httpx.post(f"{API_BASE}{path}", headers={"Authorization": f"Bearer {token}"}, json=data or {}, timeout=300)
    r.raise_for_status()
    return r.json()


def get_latest_model(token: str) -> dict:
    """Get the most recently created model."""
    models = api_get("/ml-models/?page=1&page_size=1", token)
    if not models["items"]:
        raise RuntimeError("No models found")
    return models["items"][0]


def get_latest_test_run(token: str, model_id: str) -> dict:
    """Get the most recent completed test run for a model."""
    runs = api_get(f"/ml-models/{model_id}/test-runs?page=1&page_size=1", token)
    if not runs["items"]:
        return None
    run = runs["items"][0]
    if run["status"] != "completed":
        return None
    return run


def wait_for_test(token: str, model_id: str, timeout: int = 600) -> dict:
    """Wait for a running test to complete."""
    start = time.time()
    while time.time() - start < timeout:
        run = api_get(f"/ml-models/{model_id}/test-runs?page=1&page_size=1", token)
        if run["items"] and run["items"][0]["status"] == "completed":
            return run["items"][0]
        time.sleep(10)
    raise TimeoutError("Test did not complete within timeout")


def fetch_failure_html(url: str) -> str:
    """Fetch full HTML from a URL. Written to files, not inlined in prompt."""
    try:
        r = httpx.get(url, follow_redirects=True, timeout=10,
                      headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"})
        return r.text
    except Exception as e:
        return f"<!-- FETCH FAILED: {e} -->"


def _detail_page_url_for(entry: dict) -> str | None:
    """Pick a detail-page URL to fetch alongside the listing HTML.

    Prefers the first job's `source_url` from the *baseline* (Jobstream)
    extraction — that's the detail URL we know the baseline uses to pull
    richer description/location/salary. Falls back to the first model-extracted
    job if baseline data is absent.
    """
    # baseline_full_wrapper may carry details paths even if we can't see the jobs
    wrapper = entry.get("baseline_full_wrapper") or {}
    if not isinstance(wrapper, dict):
        wrapper = {}
    has_detail_paths = bool(
        wrapper.get("details_page_description_paths")
        or wrapper.get("details_page_location_paths")
    )

    for key in ("baseline_extracted_jobs", "model_extracted_jobs"):
        jobs = entry.get(key) or []
        if not jobs:
            continue
        url = (jobs[0] or {}).get("source_url") if isinstance(jobs[0], dict) else None
        if url:
            return url if has_detail_paths or key == "baseline_extracted_jobs" else None
    return None


def _compact_baseline_selectors(entry: dict) -> dict:
    """Return ONLY the three highest-signal selector keys from the baseline wrapper.

    Inlining the full wrapper was a token drain — Codex rarely needs every field,
    and the full JSON already sits on disk at ``wrapper_file``. We keep:
      - ``boundary``: container selector, the critical upstream choice.
      - ``title``: proves the wrapper is actually locating job titles.
      - ``details_page_description_paths``: the detail-page hint that drives
        `field_completeness`, which is v6.9's dragging axis.
    Any other key can be fetched by reading ``wrapper_file`` on disk.
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


def analyse_results(run: dict, context_dir: str) -> dict:
    """Extract failure details, write context files (HTML + wrappers) for Codex."""
    import random

    rd = run.get("results_detail", {})
    sites = rd.get("sites", [])
    summary = rd.get("summary", {})

    # Create context directory for HTML and wrapper files
    os.makedirs(context_dir, exist_ok=True)

    failures = []       # model_worse, model_failed — hard failures
    gaps = []           # partial — model found jobs but fewer/lower quality than baseline
    successes = []      # model_equal_or_better, model_only
    for s in sites:
        baseline = s.get("baseline", {})
        model = s.get("model", {})
        # Sample descriptions for quality comparison (first job from each)
        baseline_descs = [j.get("description", "")[:300] for j in baseline.get("extracted_jobs", [])[:1]]
        model_descs = [j.get("description", "")[:300] for j in model.get("extracted_jobs", [])[:1]]

        entry = {
            "company": s["company"],
            "domain": s.get("domain", ""),
            "test_url": s["url"],
            "match": s["match"],
            "baseline_jobs": baseline.get("jobs", 0),
            "baseline_titles": baseline.get("sample_titles", [])[:3],
            "baseline_selectors": baseline.get("selectors_used", {}),
            "baseline_full_wrapper": baseline.get("full_wrapper", {}),
            "baseline_quality": baseline.get("quality_score", 0),
            "baseline_fields": baseline.get("fields", {}),
            "baseline_sample_desc": baseline_descs[0] if baseline_descs else "",
            "baseline_extracted_jobs": baseline.get("extracted_jobs", [])[:3],
            "model_jobs": model.get("jobs", 0),
            "model_jobs_quality": model.get("jobs_quality", model.get("jobs", 0)),
            "model_titles": model.get("sample_titles", [])[:3],
            "model_tier": model.get("tier_used"),
            "model_url_found": model.get("url_found"),
            "model_discovery": model.get("discovery_method"),
            "model_error": model.get("error"),
            "model_quality": model.get("quality_score", 0),
            "model_fields": model.get("fields", {}),
            "model_sample_desc": model_descs[0] if model_descs else "",
            "model_extracted_jobs": model.get("extracted_jobs", [])[:3],
            "ats_platform": (s.get("ats_platform") or baseline.get("ats_platform")
                             or model.get("ats_platform")),
        }
        # Compute volume ratio — how much of the baseline's jobs did the model capture?
        bj = entry["baseline_jobs"]
        mj = entry["model_jobs_quality"]
        entry["volume_ratio"] = round(mj / bj, 2) if bj > 0 else (1.0 if mj > 0 else 0.0)

        if s["match"] in ("model_worse", "model_failed"):
            failures.append(entry)
        elif s["match"] == "partial":
            gaps.append(entry)
        elif s["match"] in ("model_equal_or_better", "model_only"):
            # Even "equal or better" sites might have quality/field gaps vs baseline
            if bj > 0 and (entry["volume_ratio"] < 0.90 or entry.get("model_quality", 100) < entry.get("baseline_quality", 0) - 10):
                gaps.append(entry)
            else:
                successes.append(entry)

    # Write context files for failures AND gaps (both are improvement opportunities)
    improvement_targets = failures + gaps  # failures first (highest priority), then gaps
    for i, f in enumerate(improvement_targets[:MAX_FAILURES_IN_PROMPT]):
        kind = "failure" if f in failures else "gap"
        slug = re.sub(r'[^a-z0-9]+', '_', f["domain"].lower())[:30]

        # Fetch and save full HTML (not truncated)
        url = f["model_url_found"] or f["test_url"]
        html = fetch_failure_html(url)
        html_file = os.path.join(context_dir, f"{kind}_{i+1}_{slug}.html")
        with open(html_file, "w", encoding="utf-8") as fh:
            fh.write(html)
        f["html_file"] = html_file

        # Also fetch the TEST URL html (what the baseline used) if different
        if f["model_url_found"] and f["model_url_found"] != f["test_url"]:
            baseline_html = fetch_failure_html(f["test_url"])
            baseline_html_file = os.path.join(context_dir, f"{kind}_{i+1}_{slug}_baseline.html")
            with open(baseline_html_file, "w", encoding="utf-8") as fh:
                fh.write(baseline_html)
            f["baseline_html_file"] = baseline_html_file

        # Save full wrapper config as JSON
        if f.get("baseline_full_wrapper"):
            wrapper_file = os.path.join(context_dir, f"{kind}_{i+1}_{slug}_wrapper.json")
            with open(wrapper_file, "w") as fh:
                json.dump(f["baseline_full_wrapper"], fh, indent=2)
            f["wrapper_file"] = wrapper_file

        # NEW: fetch the first detail page when baseline uses details_page_*
        # selectors. The detail page is where description/location/salary live
        # on most ATS-backed sites — showing Codex the exact page it would need
        # to traverse is the single biggest unlock for field_completeness.
        detail_url = _detail_page_url_for(f)
        if detail_url:
            detail_html = fetch_failure_html(detail_url)
            detail_file = os.path.join(context_dir, f"{kind}_{i+1}_{slug}_detail.html")
            with open(detail_file, "w", encoding="utf-8") as fh:
                # Cap at MAX_DETAIL_HTML_SNIPPET — Codex doesn't need the footer
                fh.write(detail_html[:MAX_DETAIL_HTML_SNIPPET * 8]
                         if detail_html else "<!-- detail fetch returned empty -->")
            f["detail_html_file"] = detail_file
            f["detail_url"] = detail_url

        try:
            sys.path.insert(0, os.path.join(os.path.dirname(PROJECT_DIR), "backend"))
            from app.ml.champion_challenger.failure_analysis import build_site_diff_package  # type: ignore

            diff_package = build_site_diff_package(
                f,
                html_file=f.get("html_file"),
                detail_html_file=f.get("detail_html_file"),
            )
            diff_file = os.path.join(context_dir, f"diff_{kind}_{i+1}_{slug}.json")
            with open(diff_file, "w", encoding="utf-8") as fh:
                json.dump(diff_package, fh, ensure_ascii=False, separators=(",", ":"))
            f["diff_package_file"] = diff_file
            f["diff_package"] = diff_package
        except Exception as e:  # noqa: BLE001
            print(f"[auto_improve] could not build site diff package for {slug}: {e}")

    # Spot-check successes
    spot_checks = random.sample(successes, min(3, len(successes)))
    for i, s in enumerate(spot_checks):
        slug = re.sub(r'[^a-z0-9]+', '_', s["domain"].lower())[:30]
        url = s["model_url_found"] or s["test_url"]
        html = fetch_failure_html(url)
        html_file = os.path.join(context_dir, f"spotcheck_{i+1}_{slug}.html")
        with open(html_file, "w", encoding="utf-8") as fh:
            fh.write(html)
        s["html_file"] = html_file

    # Compute aggregate gap metrics
    all_sites_with_baseline = [s for s in sites if s.get("baseline", {}).get("jobs", 0) > 0]
    total_baseline_jobs = sum(s["baseline"]["jobs"] for s in all_sites_with_baseline)
    total_model_jobs = sum(
        s["model"].get("jobs_quality", s["model"].get("jobs", 0))
        for s in all_sites_with_baseline
    )
    volume_ratio = total_model_jobs / max(1, total_baseline_jobs)

    return {
        "summary": summary,
        "total_sites": summary.get("total_sites", len(sites)),
        "accuracy": summary.get("accuracy", 0),
        "match_breakdown": summary.get("match_breakdown", {}),
        "tier_breakdown": summary.get("tier_breakdown", {}),
        "failures": failures[:MAX_FAILURES_IN_PROMPT],
        "gaps": gaps[:MAX_GAPS_IN_PROMPT],
        "spot_checks": spot_checks,
        "success_count": len(successes),
        "fail_count": len(failures),
        "gap_count": len(gaps),
        "improvement_count": len(failures) + len(gaps),
        "volume_ratio": round(volume_ratio, 3),
        "total_baseline_jobs": total_baseline_jobs,
        "total_model_jobs": total_model_jobs,
        "context_dir": context_dir,
    }


def determine_next_version(current_name: str) -> str:
    """Determine the next version by checking what files already exist."""
    # Find the highest existing extractor file version
    import glob
    existing = glob.glob(os.path.join(PROJECT_DIR, "app", "crawlers", "tiered_extractor_v*.py"))
    max_file_ver = 16  # v1.6 is our base
    for f in existing:
        basename = os.path.basename(f).replace("tiered_extractor_v", "").replace(".py", "")
        try:
            ver = int(basename)
            if ver > max_file_ver:
                max_file_ver = ver
        except ValueError:
            pass

    next_file_ver = max_file_ver + 1
    if 600 <= next_file_ver < 700:
        return f"v6.{next_file_ver - 600}"
    if 100 <= next_file_ver < 200:
        return f"v10.{next_file_ver - 100}"
    return f"v{next_file_ver // 10}.{next_file_ver % 10}"


def _regression_alert(current_accuracy: float, best_accuracy: float) -> str:
    if current_accuracy >= best_accuracy:
        return ""
    pct_current = f"{current_accuracy:.0%}"
    pct_best = f"{best_accuracy:.0%}"
    return (
        f"## ⚠️ REGRESSION ALERT\n"
        f"Previous version scored {pct_current}, down from best of {pct_best}.\n"
        f"The approach FAILED. Read memory to see why. Try a COMPLETELY DIFFERENT strategy.\n"
        f"DO NOT add more complexity. Simplify. The v1.6 base (66%) works — build on it carefully."
    )


def build_prompt(
    analysis: dict,
    current_model: dict,
    next_version: str,
    best_accuracy: float = 0.66,
    focus_directive: str | None = None,
    token: str | None = None,
    parent_source: str | None = None,
    ancestors: list[dict] | None = None,
) -> str:
    """Build the improvement prompt for Codex.

    Reads the base prompt from new-prompt.md (the canonical auto-improve instructions)
    and appends:
      1. A next-iteration brief (from failure_analysis.build_next_iteration_brief)
         that names the weakest axis + highest-impact ATS cluster + detail-page
         candidates. Prepended to the prompt so Codex reads it first.
      2. Per-failure baseline wrapper selectors inlined (compact JSON).
      3. Detail-page HTML references when the baseline traverses detail pages.
      4. Optional focus_directive (used by multi-candidate generation).

    Enforces PROMPT_MAX_BYTES as a soft ceiling — context files stay, only the
    narrative sections are trimmed if we overshoot.
    """
    current_name = current_model["name"]
    current_desc = current_model.get("description", "")

    context_dir = analysis.get("context_dir", "/tmp")
    best_accuracy_pct = f"{best_accuracy:.0%}"
    regression_alert = _regression_alert(analysis['accuracy'], best_accuracy)

    # Read the canonical prompt from new-prompt.md
    prompt_file = os.path.join(os.path.dirname(PROJECT_DIR), "new-prompt.md")
    try:
        with open(prompt_file) as f:
            base_prompt = f.read()
    except FileNotFoundError:
        base_prompt = "# Auto-Improve Agent\n\nRead agent-instructions.md and storage/auto_improve_memory.json for full context.\n"

    # Build the next-iteration brief + pull top-3 past plays. Both are imported
    # locally so we don't hard-depend on the backend package when auto_improve
    # is run outside the container (unit tests, dry runs).
    brief_section = ""
    plays_section = ""
    query_summary = ""
    try:
        sys.path.insert(0, os.path.join(os.path.dirname(PROJECT_DIR), "backend"))
        from app.ml.champion_challenger.failure_analysis import (  # type: ignore
            build_next_iteration_brief,
            format_brief_for_prompt,
        )
        brief = build_next_iteration_brief(analysis)
        brief_section = format_brief_for_prompt(brief)
        # Build a query summary from the brief's headline axis + ATS list;
        # this is what we semantically match against past plays.
        axis = brief.get("top_axis_to_fix") or ""
        ats_list = ", ".join(c.get("ats", "") for c in brief.get("ats_clusters") or [])
        query_summary = f"Target axis {axis}. ATS clusters: {ats_list}"
    except Exception as e:  # noqa: BLE001 — brief is advisory
        print(f"[auto_improve] could not build next-iteration brief: {e}")

    try:
        from app.ml.champion_challenger.play_library import (  # type: ignore
            default_library, format_plays_for_prompt,
        )
        top_plays = default_library.retrieve(query_summary or current_name, k=3)
        plays_section = format_plays_for_prompt(top_plays)
        if not top_plays:
            # Surface an empty library prominently — silent failure here is what
            # caused Codex to run without exemplars for weeks. Pre-reset, the
            # library held v6.0→v6.9 plays; the backfill_play_library.py script
            # repopulates it from promoted test runs.
            print(
                "[auto_improve] play library is empty — run "
                "`docker exec jobharvest-api python -m scripts.backfill_play_library` "
                "to seed it from historical promotions"
            )
    except Exception as e:  # noqa: BLE001 — library is advisory but should be loud
        print(f"[auto_improve] could not retrieve play library: {e}")
        import traceback; traceback.print_exc()

    focus_section = ""
    if focus_directive:
        focus_section = f"""
## FOCUS DIRECTIVE (this candidate only)

{focus_directive}

Other candidates in this cycle may pursue different focuses — pick the most
impactful single change under this directive and resist scope creep.
"""

    # Build dynamic failure details.
    #
    # UNIVERSALITY-FIRST REDESIGN (2026-04-14): surfacing named per-site failures
    # was training Codex to write narrow fixes ("fix example.com"). The prompt
    # now leads with PATTERN CARDS — an anonymised summary of every cluster
    # with ≥3 failing sites — and only drills down to named per-site examples
    # for small clusters (1–2 sites) where platform generalisation isn't
    # possible yet.
    #
    # The goal: a fix must help a pattern, not a domain. When Codex writes a
    # change, the promotion gate (ml_tasks._aggregate) blocks it if any ATS
    # cluster regresses by ≥2 points OR any ever-passed site is lost OR an
    # oscillating site is newly failing. The prompt must therefore frame each
    # failure as evidence of a pattern, not as a URL to fix.
    try:
        sys.path.insert(0, os.path.join(os.path.dirname(PROJECT_DIR), "backend"))
        from app.ml.champion_challenger.failure_analysis import (  # type: ignore
            cluster_failures_by_ats,
        )
    except Exception:  # noqa: BLE001
        def cluster_failures_by_ats(entries):  # type: ignore
            buckets: dict[str, list[dict]] = {}
            for e in entries or []:
                buckets.setdefault(
                    (e.get("ats_platform") or e.get("_ats") or "unknown").lower(),
                    [],
                ).append(e)
            return dict(sorted(buckets.items(), key=lambda kv: -len(kv[1])))

    improvement_targets = list(analysis.get("failures") or []) + list(analysis.get("gaps") or [])
    clusters = cluster_failures_by_ats(improvement_targets)

    # Pattern cards for every cluster with ≥3 failing sites. These are the
    # only high-signal buckets; small clusters get per-site drill-down below.
    CLUSTER_MIN_FOR_CARD = 3
    cards_text = ""
    card_num = 0
    for ats_label, entries in clusters.items():
        if len(entries) < CLUSTER_MIN_FOR_CARD:
            continue
        card_num += 1
        # Shared baseline selector hint: the first entry with a wrapper wins.
        hint = {}
        for e in entries:
            h = _compact_baseline_selectors(e)
            if h:
                hint = h
                break
        hint_json = json.dumps(hint, ensure_ascii=False) if hint else "{}"
        if len(hint_json) > 900:
            hint_json = hint_json[:897] + "..."

        # Aggregate volume signal for the cluster — shows whether the cluster
        # is dominated by hard failures (model=0) vs partials.
        total_b = sum(int(e.get("baseline_jobs") or 0) for e in entries)
        total_m = sum(int(e.get("model_jobs_quality") or e.get("model_jobs") or 0) for e in entries)
        vr = (total_m / total_b) if total_b else 0.0
        hard_fails = sum(1 for e in entries if (e.get("model_jobs") or 0) == 0)

        # Example HTML files — use up to 2, anonymised by slug. Codex gets
        # filesystem paths, not company names or URLs.
        example_files = []
        for e in entries[:2]:
            for key in ("html_file", "baseline_html_file", "detail_html_file"):
                fp = e.get(key)
                if fp:
                    example_files.append(f"  - {os.path.basename(fp)}  ({key})")
                    break
        examples_block = "\n".join(example_files) if example_files else "  (no HTML captured)"
        rep_pkg = entries[0].get("diff_package") if entries and entries[0].get("diff_package") else {}
        rep_diff = _truncate_bytes(json.dumps(rep_pkg, ensure_ascii=False, separators=(",", ":")), 800) if rep_pkg else "{}"

        cards_text += f"""
--- Pattern Card {card_num}: ATS = {ats_label} ({len(entries)} sites) ---
Aggregate: baseline={total_b} jobs, model={total_m} jobs ({vr:.0%}), hard-failures={hard_fails}
Shared baseline wrapper hint: {hint_json}
Representative diff package: {rep_diff}
Sample HTML files (anonymised, in {context_dir}):
{examples_block}

Your fix for this pattern must help ≥{CLUSTER_MIN_FOR_CARD} of these {len(entries)} sites
without regressing any OTHER cluster (promotion gate blocks cluster drops >2 pts).
"""

    # Single/pair clusters — show per-site because cluster generalisation isn't
    # possible yet. Capped at MAX_FAILURES_IN_PROMPT total entries so the prompt
    # stays within its byte budget.
    small_cluster_entries: list[dict] = []
    for ats_label, entries in clusters.items():
        if len(entries) < CLUSTER_MIN_FOR_CARD:
            small_cluster_entries.extend(entries)
    small_cluster_entries = small_cluster_entries[:MAX_FAILURES_IN_PROMPT]

    failures_text = ""
    for i, f in enumerate(small_cluster_entries):
        hint = _compact_baseline_selectors(f)
        hint_json = json.dumps(hint, ensure_ascii=False) if hint else "{}"
        if len(hint_json) > 900:
            hint_json = hint_json[:897] + "..."
        detail_ref = ""
        if f.get("detail_html_file"):
            detail_ref = (
                f"\n  Detail page (baseline traverses): {f.get('detail_url','?')}"
                f"\n  Detail HTML saved to:            {f.get('detail_html_file')}"
            )
        failures_text += f"""
--- Long-tail {i+1}: ATS={f.get('_ats', f.get('ats_platform','?'))} ---
Match: {f['match']}  |  Baseline: {f['baseline_jobs']} jobs  |  Model: {f['model_jobs']} jobs ({f.get('volume_ratio', 0):.0%})
Baseline wrapper hint: {hint_json}
Tier: {f['model_tier']}  |  Discovery: {f['model_discovery']} → {f['model_url_found']}
Error: {f['model_error']}{detail_ref}
Context files (READ THESE for full analysis):
  HTML (model's discovered page): {f.get('html_file', 'N/A')}
  HTML (baseline's test URL):     {f.get('baseline_html_file', 'same as above')}
  Full wrapper config (JSON):     {f.get('wrapper_file', 'N/A')}
  Diff package (JSON):            {f.get('diff_package_file', 'N/A')}
  Company: {f['company']}  |  Domain: {f['domain']}  (use for reference only)
"""

    # Build gap details (partial matches — model found some jobs but not all).
    # These are kept in aggregate form — the cluster cards above already list
    # gaps alongside failures, and repeating them inflates prompt size.
    gaps_text = ""
    # Preserved for reference / byte-budget compatibility with downstream regex:
    for i, g in enumerate(list(analysis.get("gaps", []))[:0]):  # intentionally empty
        desc_comparison = ""
        b_desc = g.get("baseline_sample_desc", "").strip()
        m_desc = g.get("model_sample_desc", "").strip()
        if b_desc or m_desc:
            desc_comparison = f"""
  Description quality comparison (first job):
    Baseline: {repr(b_desc[:200]) if b_desc else '(empty)'}
    Model:    {repr(m_desc[:200]) if m_desc else '(empty)'}"""
        hint = _compact_baseline_selectors(g)
        hint_json = json.dumps(hint, ensure_ascii=False) if hint else "{}"
        if len(hint_json) > 900:
            hint_json = hint_json[:897] + "..."
        detail_ref = ""
        if g.get("detail_html_file"):
            detail_ref = (
                f"\n  Detail page (baseline traverses): {g.get('detail_url','?')}"
                f"\n  Detail HTML saved to:            {g.get('detail_html_file')}"
            )

        gaps_text += f"""
--- Gap {i+1}: {g['match']} (volume ratio: {g.get('volume_ratio', 0):.0%}) ---
Company: {g['company']}
Domain: {g['domain']}  |  ATS: {g.get('_ats', g.get('ats_platform','?'))}
Test URL: {g['test_url']}
Baseline: {g['baseline_jobs']} jobs | Quality: {g.get('baseline_quality', '?')}% | Titles: {g['baseline_titles']}
  Baseline wrapper hint (COMPACT): {hint_json}
Model: {g['model_jobs']} jobs ({g.get('volume_ratio', 0):.0%} of baseline) | Quality: {g.get('model_quality', '?')}% | Tier: {g['model_tier']}
  Titles: {g['model_titles']}
  Discovery: {g['model_discovery']} → {g['model_url_found']}{desc_comparison}{detail_ref}
Context files:
  HTML: {g.get('html_file', 'N/A')}
  Baseline HTML: {g.get('baseline_html_file', 'same as above')}
  Wrapper JSON: {g.get('wrapper_file', 'N/A')}
"""

    spot_check_text = ""
    for i, s in enumerate(analysis.get("spot_checks", [])):
        desc_cmp = ""
        b_d = s.get("baseline_sample_desc", "").strip()
        m_d = s.get("model_sample_desc", "").strip()
        if b_d or m_d:
            desc_cmp = f"""
  Description quality comparison (first job):
    Baseline: {repr(b_d[:200]) if b_d else '(empty)'}
    Model:    {repr(m_d[:200]) if m_d else '(empty)'}"""

        spot_check_text += f"""
--- Spot-check {i+1}: {s['match']} ---
Company: {s['company']}
Domain: {s['domain']}
Test URL: {s['test_url']}
Baseline: {s['baseline_jobs']} jobs | Titles: {s['baseline_titles']}
Model: {s['model_jobs']} jobs | Tier: {s['model_tier']} | Titles: {s['model_titles']}
  Discovery: {s['model_discovery']} → {s['model_url_found']}{desc_cmp}
  HTML file: {s.get('html_file', 'N/A')}
"""

    vol_ratio = analysis.get('volume_ratio', 0)
    vol_pct = f"{vol_ratio:.0%}"
    baseline_jobs = analysis.get('total_baseline_jobs', 0)
    model_jobs = analysis.get('total_model_jobs', 0)

    # ── Build self-improvement section: review previous Codex run logs ──
    self_improvement_section = ""
    log_dir = os.path.join(os.path.dirname(PROJECT_DIR), "storage", "auto_improve_logs")
    # Find the most recent Codex log (for the model that was just tested)
    prev_model_id = current_model.get("id", "")
    prev_log_file = os.path.join(log_dir, f"{prev_model_id}.log") if prev_model_id else ""
    prev_log_content = ""
    if prev_log_file and os.path.exists(prev_log_file):
        try:
            with open(prev_log_file) as f:
                raw = f.read()
            # Extract only errors, warnings, timeouts, and key decisions (last 3000 chars)
            important_lines = []
            for line in raw.split("\n"):
                ll = line.lower()
                if any(kw in ll for kw in ["error", "fail", "timeout", "traceback", "exception",
                                            "warning", "❌", "⚠️", "stuck", "retry",
                                            "could not", "import error", "syntax error"]):
                    important_lines.append(line.strip())
            if important_lines:
                prev_log_content = "\n".join(important_lines[-40:])  # Last 40 important lines
        except Exception:
            pass

    if prev_log_content:
        self_improvement_section = f"""
---

## STEP 0: Self-Improvement — Review Previous Auto-Improve Run

**BEFORE implementing model improvements, review these issues from the last Codex run.**
If any of these indicate bugs in YOUR code, broken imports, syntax errors, or process failures,
FIX THEM FIRST. A model improvement is worthless if the extractor can't even import cleanly.

### Errors/Warnings from Previous Run

```
{prev_log_content}
```

**Action required:**
1. Scan for import errors, syntax errors, or broken code that YOU introduced
2. Check for timeouts or failures that suggest your approach was too complex
3. If you see patterns of the same error recurring, address the root cause
4. Fix these issues FIRST, then proceed to model improvements below

"""

    # ── Build multi-run trend analysis (last 5 iterations) ──
    trend_section = ""
    memory_file = os.path.join(os.path.dirname(PROJECT_DIR), "storage", "auto_improve_memory.json")
    try:
        with open(memory_file) as f:
            mem = json.load(f)
        iterations = mem.get("iterations", [])
        if len(iterations) >= 3:
            recent = iterations[-5:] if len(iterations) >= 5 else iterations
            trend_lines = []
            for it in recent:
                name = it.get("model", "?")
                acc = it.get("accuracy")
                bl_acc = it.get("baseline_accuracy", "?")
                acc_str = f"{acc:.0%}" if acc is not None else "?"
                bl_str = f"{bl_acc:.0%}" if isinstance(bl_acc, (int, float)) else str(bl_acc)
                trend_lines.append(f"  {name}: accuracy={acc_str}, baseline_accuracy={bl_str}")

            # Calculate trajectory
            accs = [it["accuracy"] for it in recent if it.get("accuracy") is not None]
            if len(accs) >= 2:
                first_half = sum(accs[:len(accs)//2]) / max(1, len(accs)//2)
                second_half = sum(accs[len(accs)//2:]) / max(1, len(accs) - len(accs)//2)
                delta = second_half - first_half
                if delta > 0.05:
                    trajectory = "IMPROVING — good momentum, keep the current strategy direction"
                elif delta > -0.02:
                    trajectory = "PLATEAU — small/no gains. You need a DIFFERENT approach, not more of the same"
                else:
                    trajectory = "REGRESSING — recent changes are making things worse. REVERT to simpler approach"

                avg_gain = (accs[-1] - accs[0]) / max(1, len(accs) - 1) if len(accs) > 1 else 0
                pace_note = ""
                if abs(avg_gain) < 0.02:
                    pace_note = (
                        "\n\n**PACE WARNING:** Average gain per iteration is < 2%. "
                        "Stop making small incremental tweaks. Instead:\n"
                        "- Identify the BIGGEST category of failures (e.g. all ATS X sites fail)\n"
                        "- Fix that ONE category completely — that's worth 10-20% in one shot\n"
                        "- Don't spread effort across many small fixes\n"
                    )
            else:
                trajectory = "INSUFFICIENT DATA"
                pace_note = ""

            trend_section = f"""
---

## Multi-Run Trend Analysis (Last {len(recent)} Iterations)

| Run | Accuracy |
|-----|----------|
{"".join(f"| {it.get('model','?')} | {it['accuracy']:.0%} |{chr(10)}" for it in recent if it.get('accuracy') is not None)}

**Trajectory:** {trajectory}
**Best ever:** {best_accuracy_pct}
**Current:** {analysis['accuracy']:.0%}
{pace_note}

**Strategic guidance:**
- If accuracy is plateauing, the current approach has hit its ceiling. You need architectural changes, not parameter tuning.
- If accuracy is regressing, your last change broke something. Read the memory to understand what worked before.
- Prioritize fixes by IMPACT: one fix that handles 5 failing sites > five fixes that each handle 1 site.
- Before implementing, estimate: "How many of the {analysis['fail_count']} failures will this fix address?" If < 3, find a bigger lever.

"""
    except Exception:
        pass

    # Dynamic head leads EVERY prompt — it's the one compact block that pins
    # Codex to the current champion + target axis + banned approaches. Below it
    # come the per-iteration brief, exemplars, and the canonical base prompt.
    head_section = _build_current_champion_head(current_model, token=token)
    fixture_report = _load_previous_fixture_report(current_name)
    diff_mode = ""
    if parent_source:
        ancestor_lines = "\n".join(
            f"- {a.get('version_tag')}: {a.get('summary') or a.get('gate_verdict') or 'ancestor reference'}"
            for a in (ancestors or [])[:3]
        )
        diff_mode = _truncate_bytes(
            "## DIFF-GROUNDED MUTATION MODE\n\n"
            "Return only SEARCH/REPLACE blocks in this exact format:\n"
            "<<<<<<< SEARCH\n<exact parent lines>\n=======\n<replacement>\n>>>>>>> REPLACE\n\n"
            f"Ancestors:\n{ancestor_lines or '- none'}\n\n"
            f"Parent source:\n```python\n{parent_source}\n```\n",
            3500,
        )

    prompt = f"""{head_section}{_truncate_bytes(brief_section, 2000)}
{_truncate_bytes(plays_section, 1500)}
{_truncate_bytes(focus_section, 600)}
{diff_mode}
---

{_truncate_bytes(base_prompt, 6000)}
{self_improvement_section}
{_truncate_bytes(trend_section, 400)}
---

## THIS ITERATION — Dynamic Test Results

### Current State

Model: {current_name}
Description: {current_desc}
Next version to create: **{next_version}**
Best historical accuracy: {best_accuracy_pct}
Match breakdown: {json.dumps(analysis['match_breakdown'])}
Tier breakdown: {json.dumps(analysis['tier_breakdown'])}

### Baseline Gap Summary

**You are NOT done until you match or exceed the Jobstream baseline on ALL dimensions.**

| Metric | Baseline | Model | Gap |
|--------|----------|-------|-----|
| Sites with jobs | {analysis['total_sites']} | {analysis['success_count'] + analysis.get('gap_count', 0)} extracted, {analysis['fail_count']} failed | {analysis['fail_count']} hard failures |
| Total jobs | {baseline_jobs} | {model_jobs} | **{vol_pct} of baseline** ({baseline_jobs - model_jobs} jobs missing) |
| Volume gaps (partial) | — | — | **{analysis.get('gap_count', 0)} sites** where model found fewer jobs than baseline |

Even if the model finds SOME jobs on every site, there is still work to do if:
- Volume ratio < 100% (missing jobs that baseline can extract)
- Quality < baseline quality (missing fields, bad titles)
- Any "partial" matches exist (model extracts fewer jobs than baseline)

### Pattern Cards — clusters with ≥3 failing/partial sites (fix THESE first)

Every card below is a pattern across multiple sites. The promotion gate **will
block** any change that regresses an existing passing cluster by more than
2 composite points, or that loses a site some earlier version had already
passed (ever-passed gate), or that fails a site currently flagged as
oscillating. Optimise for the pattern, not the site.

{_truncate_bytes(cards_text if cards_text.strip() else "No failing clusters of ≥3 sites — the remaining long-tail is below.", 2500)}

### Long-tail — single/paired failures (analyse for root cause, not selectors)

These sites each represent a platform with only 1–2 failing instances today.
They are NOT promotion-blocking on their own (cluster gate requires ≥3 sites)
but they're a seedbed for tomorrow's clusters. Look for SHARED root causes.

{_truncate_bytes(failures_text if failures_text.strip() else "No long-tail failures this run.", 1500)}

### Volume/Quality Gaps

Partial matches are folded into the pattern cards above — extracted alongside
hard failures since both represent the same underlying pattern gap. Per-site
drill-down for gaps that don't fit a ≥3-site cluster:

{gaps_text if gaps_text.strip() else "No un-clustered gaps."}

### Spot-Check Successes

{spot_check_text if spot_check_text.strip() else "No successes to spot-check."}

### Previous Fixture Report

{_truncate_bytes(fixture_report, 800)}

### Context Files (MUST READ)

Full HTML and wrapper configs are saved in: {context_dir}

File patterns:
- `failure_N_domain.html` / `gap_N_domain.html` — page HTML
- `*_baseline.html` — baseline's version of the page (if different URL)
- `*_wrapper.json` — full selector config the baseline uses

### Testing & Validation

**BEFORE declaring done, validate your changes with pytest (takes seconds, catches bugs early):**

```bash
cd {os.path.dirname(PROJECT_DIR)}
python -m pytest backend/tests/test_extractor_smoke.py -v --tb=short
```

You can also run quick manual extraction tests against the context HTML files:
```bash
python3 -c "
import sys; sys.path.insert(0, 'backend')
from app.crawlers.tiered_extractor_v{{VER}} import TieredExtractorV{{VER}}
ext = TieredExtractorV{{VER}}()
with open('PATH_TO_CONTEXT_HTML') as f: html = f.read()
class P:
    url='https://example.com/careers'; requires_js_rendering=False
class C:
    name='Test'; ats_platform=None
import asyncio
jobs = asyncio.run(ext.extract(P(), C(), html))
for j in jobs[:3]: print(j.get('title','?'), '|', j.get('location_raw',''), '|', len(j.get('description','')), 'chars')
"
```

**Do NOT deploy broken code. Fix test failures first.**

### Sandbox Rules

- **DO NOT use Playwright, Docker, curl, or API calls.** They won't work in the sandbox.
- **DO use pytest** and direct Python scripts to test your extractor against context HTML files.
- Use the pre-fetched HTML and wrapper JSON files in the context directory for analysis.
- Deployment (Docker rebuild, model creation, test trigger) is handled AUTOMATICALLY after you finish.

{regression_alert}
"""
    # Soft ceiling — we never want prompts so large they blow past Codex's
    # useful-context window. Trim the spot-checks first (lowest signal), then
    # gaps, then failures, keeping the brief + base prompt intact.
    if len(prompt.encode("utf-8")) > PROMPT_MAX_BYTES:
        for pattern in (
            r"(?s)### Spot-Check Successes.*?(?=###|\Z)",
            r"(?s)### Volume/Quality Gaps.*?(?=###|\Z)",
        ):
            prompt = re.sub(pattern, "", prompt, count=1)
            if len(prompt.encode("utf-8")) <= PROMPT_MAX_BYTES:
                break
    return prompt


def run_codex(prompt: str, working_dir: str, model_id: str = ""):
    """Run codex exec with the improvement prompt, streaming output to a log file."""
    print(f"\n{'='*60}")
    print(f"Running Codex ({CODEX_MODEL}) with improvement prompt...")
    print(f"{'='*60}\n")

    # Log file in shared storage so the API can serve it
    log_dir = os.path.join(os.path.dirname(PROJECT_DIR), "storage", "auto_improve_logs")
    os.makedirs(log_dir, exist_ok=True)
    log_file = os.path.join(log_dir, f"{model_id or 'latest'}.log")

    # Clear previous log
    with open(log_file, "w") as f:
        f.write(f"[{datetime.now().isoformat()}] Starting Codex ({CODEX_MODEL})...\n")

    # Write prompt to a temp file — passing it as CLI arg hits OS limits
    prompt_file = os.path.join(log_dir, f"{model_id or 'latest'}_prompt.md")
    with open(prompt_file, "w") as f:
        f.write(prompt)

    cmd = [
        "codex", "exec",
        "--full-auto",
        "--json",  # JSONL events — gives us reasoning, tool calls, file writes
        "-m", CODEX_MODEL,
        "-C", working_dir,
        f"Read and follow the instructions in {prompt_file}",
    ]

    def _unwrap_bash(cmd: str) -> str:
        """Extract the inner command from /bin/bash -lc '...' or /bin/bash -lc \"...\" wrappers."""
        if "bash -lc" not in cmd:
            return cmd
        try:
            idx = cmd.index("bash -lc") + 8
            rest = cmd[idx:].lstrip()
            if rest.startswith("'"):
                return rest[1:].rsplit("'", 1)[0]
            elif rest.startswith('"'):
                return rest[1:].rsplit('"', 1)[0]
            return rest
        except Exception:
            return cmd

    def _format_event(raw_line: str) -> str:
        """Parse a JSONL event from codex into a human-readable log line."""
        try:
            evt = json.loads(raw_line)
        except Exception:
            stripped = raw_line.strip()
            return stripped if stripped else ""

        evt_type = evt.get("type", "")
        ts = datetime.now().strftime("%H:%M:%S")

        # Agent message — the model's thinking/explanation
        if evt_type == "item.completed":
            item = evt.get("item", {})
            item_type = item.get("type", "")

            if item_type == "agent_message":
                text = item.get("text", "")
                if text:
                    # Show first 3 lines of the message
                    lines = text.strip().split("\n")
                    preview = "\n  ".join(lines[:3])
                    if len(lines) > 3:
                        preview += f"\n  ... ({len(lines)} total lines)"
                    return f"[{ts}] 🤖 {preview[:400]}"

            elif item_type == "command_execution":
                inner = _unwrap_bash(item.get("command", ""))
                return f"[{ts}] $ {inner[:250]}"

            elif item_type == "file_write":
                path = item.get("path", "?")
                return f"[{ts}] 📝 Writing: {path}"

            elif item_type == "file_edit":
                path = item.get("path", "?")
                return f"[{ts}] ✏️ Editing: {path}"

            return ""

        elif evt_type == "item.started":
            item = evt.get("item", {})
            item_type = item.get("type", "")
            if item_type == "command_execution":
                inner = _unwrap_bash(item.get("command", ""))
                return f"[{ts}] ⏳ {inner[:200]}"
            return ""

        elif evt_type == "turn.started":
            return f"[{ts}] 🔄 New turn started"

        elif evt_type == "turn.completed":
            return f"[{ts}] ✅ Turn completed"

        elif evt_type == "thread.started":
            return f"[{ts}] 🚀 Codex session started"

        elif evt_type == "thread.completed":
            return f"[{ts}] 🏁 Codex session completed"

        elif evt_type == "error":
            return f"[{ts}] ❌ ERROR: {evt.get('message', str(evt)[:200])}"

        # Skip noisy event types
        if evt_type in ("item.started",):
            return ""

        return ""

    try:
        proc = subprocess.Popen(
            cmd,
            cwd=working_dir,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )

        # Per-candidate timeout: 30 min default. Was 20 min and that killed
        # Codex right at the end of productive runs (e.g. 2026-04-14, where
        # Codex had finished writing v70 but timed out during its final
        # self-review summary, leaving an unwired but valid extractor).
        # Overridable via CODEX_TIMEOUT_SEC env var; lower it to 1200 (20 min)
        # if you re-enable multi-candidate parallel mode and want tighter
        # peer-blocking semantics.
        _timeout = int(os.environ.get("CODEX_TIMEOUT_SEC", "1800"))
        deadline = time.time() + _timeout

        with open(log_file, "a") as f:
            while proc.poll() is None:
                if time.time() > deadline:
                    msg = f"[{datetime.now().isoformat()}] TIMEOUT — killing Codex after {_timeout//60} min"
                    f.write(msg + "\n")
                    print(msg)
                    proc.kill()
                    break
                try:
                    line = proc.stdout.readline()
                    if line:
                        formatted = _format_event(line)
                        if formatted:
                            f.write(formatted + "\n")
                            f.flush()
                            print(formatted)
                except Exception:
                    break

            # Read remaining
            try:
                for line in proc.stdout:
                    formatted = _format_event(line)
                    if formatted:
                        f.write(formatted + "\n")
                        f.flush()
                        print(formatted)
            except Exception:
                pass

            # Also capture stderr
            try:
                stderr = proc.stderr.read()
                if stderr and stderr.strip():
                    f.write(f"[STDERR] {stderr[:500]}\n")
                    f.flush()
            except Exception:
                pass

        with open(log_file, "a") as f:
            f.write(f"\n[{datetime.now().isoformat()}] Codex exited with code {proc.returncode}\n")

        if proc.returncode != 0:
            print(f"Codex exited with code {proc.returncode}")
        return proc.returncode

    except Exception as e:
        print(f"Codex execution error: {e}")
        with open(log_file, "a") as f:
            f.write(f"\n[{datetime.now().isoformat()}] ERROR: {e}\n")
        return 1


def _run_single_codex_candidate(
    prompt: str,
    working_dir: str,
    model_id: str,
    candidate_idx: int,
    target_version_file_ver: int,
) -> tuple[int, str | None]:
    """Run one Codex invocation for multi-candidate mode.

    The candidate writes to `tiered_extractor_v{VER}_cand{i}.py` and class
    `TieredExtractorV{VER}Cand{i}` to avoid collisions. Returns
    (exit_code, extractor_path_if_created).
    """
    cand_suffix = f"_cand{candidate_idx}"
    target_file = os.path.join(
        PROJECT_DIR, "app", "crawlers",
        f"tiered_extractor_v{target_version_file_ver}{cand_suffix}.py",
    )
    target_class = f"TieredExtractorV{target_version_file_ver}Cand{candidate_idx}"

    # Append a file-rename directive to the prompt so Codex writes to the
    # per-candidate filename.
    candidate_tail = f"""

---

## CANDIDATE {candidate_idx} FILE NAMING (IMPORTANT)

This run is part of a multi-candidate batch. Do NOT write the new extractor
to `tiered_extractor_v{target_version_file_ver}.py`. Instead write it to
`backend/app/crawlers/tiered_extractor_v{target_version_file_ver}{cand_suffix}.py`
with class name `{target_class}`. The best candidate across the batch will be
promoted to the canonical filename after fixture evaluation.
"""
    exit_code = run_codex(
        prompt + candidate_tail,
        working_dir,
        f"{model_id}_cand{candidate_idx}",
    )
    return exit_code, target_file if os.path.exists(target_file) else None


def _score_candidate_via_fixtures(version_tag: str) -> float | None:
    """Run the fixture harness on a candidate and return its composite score."""
    report_path = os.path.join(
        os.path.dirname(PROJECT_DIR),
        "storage", "auto_improve_fixture_reports",
        f"{version_tag}.json",
    )
    cmd = [
        "python3", "-m", "scripts.verify_challenger",
        "--version", version_tag,
        "--json",
    ]
    try:
        res = subprocess.run(
            cmd, cwd=PROJECT_DIR, capture_output=True, text=True, timeout=90,
        )
    except subprocess.TimeoutExpired:
        return None
    if res.returncode == 2:
        # No fixtures — caller decides what to do
        return None
    try:
        payload = json.loads(res.stdout)
        return float(payload.get("challenger", {}).get("composite") or 0.0)
    except (json.JSONDecodeError, TypeError):
        # Fallback: parse the written report file
        if os.path.exists(report_path):
            try:
                with open(report_path) as fh:
                    payload = json.load(fh)
                return float(payload.get("challenger", {}).get("composite") or 0.0)
            except Exception:
                return None
        return None


def generate_candidates(
    prompt_base: str,
    working_dir: str,
    model_id: str,
    next_version: str,
    n: int,
) -> str | None:
    """Run N Codex candidates, pick the best by fixture composite, promote it.

    Returns the canonical extractor file path on success, or None if all
    candidates fail to produce a working extractor.
    """
    if n <= 1:
        # Single-candidate mode: run the normal flow against canonical file
        run_codex(prompt_base, working_dir, model_id)
        file_ver = int(next_version.replace("v", "").replace(".", ""))
        canonical = os.path.join(
            PROJECT_DIR, "app", "crawlers", f"tiered_extractor_v{file_ver}.py",
        )
        return canonical if os.path.exists(canonical) else None

    file_ver = int(next_version.replace("v", "").replace(".", ""))
    canonical = os.path.join(
        PROJECT_DIR, "app", "crawlers", f"tiered_extractor_v{file_ver}.py",
    )

    # Generate directives for N candidates (cycle through defaults; user can
    # supply more via AUTO_IMPROVE_EXTRA_DIRECTIVES if needed later).
    directives = list(DEFAULT_FOCUS_DIRECTIVES)
    while len(directives) < n:
        directives += list(DEFAULT_FOCUS_DIRECTIVES)
    directives = directives[:n]

    candidates: list[dict] = []
    for i, directive in enumerate(directives, start=1):
        print(f"[auto_improve] candidate {i}/{n} — {directive[:80]}")
        # Re-compose the prompt with this candidate's directive. We use the
        # simple textual injection pattern here rather than threading a param
        # down through build_prompt() for each candidate.
        prompt = prompt_base.replace(
            "## FOCUS DIRECTIVE (this candidate only)",
            f"## FOCUS DIRECTIVE (candidate {i}/{n})\n\n{directive}\n\n"
            f"## FOCUS DIRECTIVE (this candidate only)",
            1,
        )
        exit_code, path = _run_single_codex_candidate(
            prompt, working_dir, model_id, i, file_ver,
        )
        candidates.append({
            "idx": i,
            "directive": directive,
            "exit_code": exit_code,
            "path": path,
        })

    # Score each candidate via the fixture harness
    for c in candidates:
        if not c["path"]:
            c["composite"] = None
            continue
        tag = f"v{file_ver}cand{c['idx']}"
        c["composite"] = _score_candidate_via_fixtures(tag)

    # Archive everything for post-hoc analysis
    from datetime import datetime as _dt
    archive = os.path.join(
        os.path.dirname(PROJECT_DIR), "storage",
        "auto_improve_candidates",
        _dt.now().strftime("%Y%m%d_%H%M%S_") + next_version,
    )
    os.makedirs(archive, exist_ok=True)
    for c in candidates:
        if c["path"] and os.path.exists(c["path"]):
            import shutil
            shutil.copy2(c["path"], os.path.join(archive, os.path.basename(c["path"])))
    with open(os.path.join(archive, "summary.json"), "w") as fh:
        json.dump(candidates, fh, indent=2, default=str)

    # Winner: highest composite; break ties by smallest file (simplicity bias)
    best = None
    for c in candidates:
        if not c["path"] or c["composite"] is None:
            continue
        size = os.path.getsize(c["path"]) if os.path.exists(c["path"]) else 10**9
        score_key = (c["composite"], -size)
        if best is None or score_key > best[0]:
            best = (score_key, c)

    if best is None:
        print("[auto_improve] all candidates failed — no canonical file produced")
        return None

    _, winner = best
    # Promote winner to canonical filename + class name
    import shutil
    winner_path: str = winner["path"]
    with open(winner_path) as fh:
        body = fh.read()
    body = body.replace(
        f"TieredExtractorV{file_ver}Cand{winner['idx']}",
        f"TieredExtractorV{file_ver}",
    ).replace(
        f"_cand{winner['idx']}",
        "",
    )
    with open(canonical, "w") as fh:
        fh.write(body)
    print(
        f"[auto_improve] promoted candidate {winner['idx']} → {canonical} "
        f"(composite={winner['composite']})"
    )
    # Remove the per-candidate files now that the winner is canonical
    for c in candidates:
        if c["path"] and os.path.exists(c["path"]):
            try:
                os.remove(c["path"])
            except OSError:
                pass
    return canonical


def run_iteration(token: str, model_id: str = None):
    """Run one iteration of the improvement loop."""
    # Get the latest model
    if model_id:
        model = api_get(f"/ml-models/{model_id}", token)
    else:
        model = get_latest_model(token)

    print(f"\nModel: {model['name']} [{model['status']}]")

    # Get the latest test run
    run = get_latest_test_run(token, model["id"])
    if not run:
        print("No completed test run found. Trigger a test first.")
        return False

    # Determine next version first (needed for context dir)
    next_version = determine_next_version(model["name"])
    print(f"Next version: {next_version}")

    # Analyse results — write HTML + wrapper files to context dir
    context_dir = os.path.join(os.path.dirname(PROJECT_DIR), "storage", "auto_improve_context", next_version.replace(".", "_"))
    analysis = analyse_results(run, context_dir)
    print(f"Results: {analysis['accuracy']:.0%} success ({analysis['success_count']}/{analysis['total_sites']})")
    print(f"Failures: {analysis['fail_count']} (model_worse + model_failed)")
    print(f"Context files written to: {context_dir}")

    # Record for convergence tracking
    baseline_accuracy = 0
    sites = run.get("results_detail", {}).get("sites", [])
    if sites:
        baseline_extracted = sum(1 for s in sites if s.get("baseline", {}).get("jobs", 0) > 0)
        baseline_accuracy = baseline_extracted / max(1, len(sites))
    record_iteration(model["name"], analysis["accuracy"], baseline_accuracy)

    # Update memory file with this iteration's results (for the PREVIOUS version)
    update_memory_with_results(model["name"], analysis["accuracy"])

    improvement_count = analysis.get("improvement_count", analysis["fail_count"])
    vol_ratio = analysis.get("volume_ratio", 1.0)
    if improvement_count == 0 and vol_ratio >= 0.95:
        print(f"No improvement opportunities — {vol_ratio:.0%} volume ratio. Done!")
        return False
    print(f"Improvement targets: {analysis['fail_count']} failures + {analysis.get('gap_count', 0)} gaps, volume ratio {vol_ratio:.0%}")

    # Regression gate: if this model is worse than the best, Codex needs to know
    best_accuracy = 0
    try:
        with open(os.path.join(os.path.dirname(PROJECT_DIR), "storage", "auto_improve_memory.json")) as f:
            mem = json.load(f)
        for it in mem.get("iterations", []):
            acc = it.get("accuracy")
            if acc is not None and acc > best_accuracy:
                best_accuracy = acc
    except Exception:
        pass

    if analysis["accuracy"] < best_accuracy:
        print(f"⚠️ REGRESSION: {analysis['accuracy']:.0%} < best {best_accuracy:.0%}")
        print(f"  Previous approach made things worse. Codex must try a DIFFERENT strategy.")

    # Build prompt
    prompt = build_prompt(analysis, model, next_version, best_accuracy)

    # Write prompt to file for reference
    prompt_file = os.path.join(PROJECT_DIR, f"auto_improve_prompt_{next_version}.md")
    with open(prompt_file, "w") as f:
        f.write(prompt)
    print(f"Prompt saved to: {prompt_file}")

    # Run Codex — single-candidate (N=1) is the classic flow. N>1 spawns N
    # parallel candidates, each with a distinct FOCUS DIRECTIVE, and picks the
    # best by fixture composite before proceeding.
    n_candidates = AUTO_IMPROVE_CANDIDATES_N
    if n_candidates > 1:
        canonical = generate_candidates(
            prompt, os.path.dirname(PROJECT_DIR), model["id"], next_version, n_candidates,
        )
        exit_code = 0 if canonical else 1
    else:
        exit_code = run_codex(prompt, os.path.dirname(PROJECT_DIR), model["id"])

    if exit_code != 0:
        print(f"Codex exited with code {exit_code}. Checking if it created files anyway...")

    # ── Post-Codex deployment (handles what Codex can't do from sandbox) ──
    print(f"\nRunning post-Codex deployment...")

    # Check if Codex created the extractor file
    file_ver = int(next_version.replace("v", "").replace(".", ""))
    extractor_file = os.path.join(PROJECT_DIR, "app", "crawlers", f"tiered_extractor_v{file_ver}.py")
    if not os.path.exists(extractor_file):
        print(f"❌ Codex did not create {extractor_file}. Skipping deployment.")
        return True

    print(f"✅ Found extractor file: {extractor_file}")

    # Rebuild Docker API
    print("Rebuilding API container...")
    rebuild = subprocess.run(
        ["docker", "compose", "-f", "docker-compose.server.yml", "up", "-d", "--build", "api"],
        cwd=os.path.dirname(PROJECT_DIR),
        capture_output=True, text=True, timeout=120,
    )
    if rebuild.returncode != 0:
        print(f"⚠️ Docker rebuild issue: {rebuild.stderr[:200]}")

    subprocess.run(["docker", "restart", "jobharvest-api"],
                    capture_output=True, timeout=30)
    time.sleep(10)
    print("API rebuilt and restarted")

    # Verify the extractor imports
    verify = subprocess.run(
        ["docker", "exec", "jobharvest-api", "python3", "-c",
         f"from app.crawlers.tiered_extractor_v{file_ver} import TieredExtractorV{file_ver}; print('OK')"],
        capture_output=True, text=True, timeout=15,
    )
    if "OK" not in (verify.stdout or ""):
        print(f"❌ Import failed: {verify.stderr[:200]}")
        return True

    print(f"✅ TieredExtractorV{file_ver} imports OK")

    # Create model via API (if Codex didn't manage to)
    token = get_token()
    models = api_get("/ml-models/?page=1&page_size=5", token)
    model_exists = any(next_version in m["name"] for m in models["items"])

    if not model_exists:
        print(f"Creating model {next_version}...")
        try:
            api_post("/ml-models/", token, {
                "name": next_version,
                "model_type": "tiered_extractor",
                "description": f"{next_version}: Codex auto-improve iteration. See memory for details.",
            })
            print(f"✅ Model created")
        except Exception as e:
            print(f"❌ Model creation failed: {e}")
            return True

    # Trigger test
    token = get_token()
    new_model = get_latest_model(token)
    if next_version not in new_model["name"]:
        print(f"⚠️ Latest model is {new_model['name']}, expected {next_version}")
        return True

    print(f"Triggering test for {new_model['name']}...")
    try:
        api_post(f"/ml-models/{new_model['id']}/test-runs/execute", token, {
            "sample_size": SAMPLE_SIZE,
            "auto_improve": True,
            "use_fixed_set": True,
            "include_exploration": False,
        })
        print(f"✅ Test triggered")
    except Exception as e:
        print(f"❌ Test trigger failed: {e}")
        return True

    print(f"\nWaiting for test to complete...")
    try:
        new_run = wait_for_test(token, new_model["id"], timeout=600)
        context_dir_new = os.path.join(os.path.dirname(PROJECT_DIR), "storage", "auto_improve_context", next_version.replace(".", "_"))
        new_analysis = analyse_results(new_run, context_dir_new)
        print(f"\n{'='*60}")
        print(f"NEW MODEL: {new_model['name']}")
        print(f"Results: {new_analysis['accuracy']:.0%} success ({new_analysis['success_count']}/{new_analysis['total_sites']})")
        print(f"Improvement: {analysis['accuracy']:.0%} → {new_analysis['accuracy']:.0%}")
        print(f"{'='*60}")
        return True
    except TimeoutError:
        print("Test timed out waiting for completion")
        return False


def check_convergence() -> bool:
    """Check if the model has converged (no improvement in last 5 iterations).

    Reads accuracy from the last 5 test runs across all models.
    If the best accuracy hasn't improved by >2% in 5 iterations, we've converged.
    """
    history_file = os.path.join(os.path.dirname(PROJECT_DIR), "storage", "auto_improve_history.json")
    if not os.path.exists(history_file):
        return False

    try:
        with open(history_file) as f:
            history = json.load(f)
    except Exception:
        return False

    # Need at least 5 entries from DIFFERENT model versions
    seen_models = []
    for h in history:
        m = h.get("model", "")
        if m not in seen_models:
            seen_models.append(m)
    if len(seen_models) < 5:
        return False

    # Take last 5 DISTINCT model entries
    last_5 = []
    seen = set()
    for h in reversed(history):
        m = h.get("model", "")
        if m not in seen:
            seen.add(m)
            last_5.append(h)
        if len(last_5) >= 5:
            break
    last_5.reverse()

    accuracies = [h.get("accuracy", 0) for h in last_5]
    best = max(accuracies)
    worst = min(accuracies)

    # Check if all last 5 are outperforming baseline
    all_beat_baseline = all(h.get("accuracy", 0) > h.get("baseline_accuracy", 0) for h in last_5)

    # Check if improvement is < 2% across last 5 distinct versions
    stable = (best - worst) < 0.02

    if all_beat_baseline and stable:
        print(f"\n{'='*60}")
        print(f"CONVERGENCE DETECTED")
        print(f"Last 5 accuracies: {[f'{a:.1%}' for a in accuracies]}")
        print(f"Range: {worst:.1%} - {best:.1%} (< 2% variation)")
        print(f"All beat baseline: {all_beat_baseline}")
        print(f"Stopping auto-improve loop.")
        print(f"{'='*60}")
        return True

    return False


def update_memory_with_results(model_name: str, accuracy: float):
    """No-op under the v2 memory schema (2026-04-14 reset).

    The old schema tracked one `iterations[]` entry per Codex run and back-filled
    `accuracy` after the A/B test completed. The v2 schema instead splits that
    responsibility: `memory_store.append_promotion` records promoted challengers
    (called from the daemon after the promotion gate passes), and
    `append_rejection` records candidates that fell short. Accuracy-alone isn't
    meaningful without the axis breakdown, so recording it per-model is a
    footgun we've deliberately removed.
    """
    return


def record_iteration(model_name: str, accuracy: float, baseline_accuracy: float = 0):
    """Record an iteration result for convergence tracking."""
    history_file = os.path.join(os.path.dirname(PROJECT_DIR), "storage", "auto_improve_history.json")
    try:
        with open(history_file) as f:
            history = json.load(f)
    except Exception:
        history = []

    # Only add if this model isn't already in history (prevent duplicates from retries)
    if not any(h.get("model") == model_name for h in history):
        history.append({
            "model": model_name,
            "accuracy": accuracy,
            "baseline_accuracy": baseline_accuracy,
            "timestamp": datetime.now().isoformat(),
        })

    with open(history_file, "w") as f:
        json.dump(history, f, indent=2)


def watch_triggers():
    """Watch for trigger files from the API and process them."""
    trigger_dir = os.path.join(os.path.dirname(PROJECT_DIR), "storage", "auto_improve_triggers")
    os.makedirs(trigger_dir, exist_ok=True)
    print(f"Watching for triggers in: {trigger_dir}")

    while True:
        for f in os.listdir(trigger_dir):
            if not f.endswith(".trigger"):
                continue
            filepath = os.path.join(trigger_dir, f)
            try:
                with open(filepath) as fh:
                    trigger = json.load(fh)
                model_id = trigger["model_id"]
                print(f"\nTrigger found: {trigger['model_name']} ({model_id})")
                os.remove(filepath)  # Remove trigger before processing

                # Check convergence before running
                if check_convergence():
                    print("Converged — skipping auto-improve. Task complete.")
                    continue

                try:
                    token = get_token()
                    run_iteration(token, model_id)
                except Exception as e:
                    print(f"Iteration error (will retry on next trigger): {e}")
                    import traceback
                    traceback.print_exc()
            except Exception as e:
                print(f"Error processing trigger {f}: {e}")
                import traceback
                traceback.print_exc()
                try:
                    os.remove(filepath)
                except Exception:
                    pass
        time.sleep(5)


def main():
    parser = argparse.ArgumentParser(description="Automated model improvement loop")
    parser.add_argument("--loop", action="store_true", help="Run continuously (iterate on latest model)")
    parser.add_argument("--watch", action="store_true", help="Watch for trigger files from the API")
    parser.add_argument("--max-iterations", type=int, default=5, help="Max iterations in loop mode")
    parser.add_argument("--model-id", type=str, help="Specific model ID to improve")
    args = parser.parse_args()

    if args.watch:
        watch_triggers()
        return

    token = get_token()
    print(f"Authenticated with API")

    if args.loop:
        for i in range(args.max_iterations):
            print(f"\n{'#'*60}")
            print(f"ITERATION {i+1}/{args.max_iterations}")
            print(f"{'#'*60}")
            success = run_iteration(token, args.model_id)
            if not success:
                print("Stopping loop — no further improvements possible")
                break
            token = get_token()
    else:
        run_iteration(token, args.model_id)


if __name__ == "__main__":
    main()
