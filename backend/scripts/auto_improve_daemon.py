#!/usr/bin/env python3
"""
Auto-Improve Daemon — single robust process that handles the ENTIRE improvement loop.

Replaces the separate supervisor + watcher + manual trigger approach with one daemon
that never dies, auto-recovers from every failure, and provides health status.

Run: nohup python3 -B -u backend/scripts/auto_improve_daemon.py &

Health check: curl http://localhost:8001/api/v1/ml-models/auto-improve/health
"""

import json
import os
import re
import signal
import subprocess
import sys
import time
from datetime import datetime

import httpx

# ── Config ──
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ROOT_DIR = os.path.dirname(PROJECT_DIR)
API_BASE = os.environ.get("JH_API_URL", "http://localhost:8001/api/v1")
USERNAME = os.environ.get("JH_USERNAME", "r.m.l.alford@gmail.com")
PASSWORD = os.environ.get("JH_PASSWORD", "Uu00dyandben!")
CODEX_MODEL = os.environ.get("CODEX_MODEL", "gpt-5.3-codex")
CODEX_TIMEOUT = 2700  # 45 min
POLL_INTERVAL = 30  # seconds between checks
STATUS_FILE = os.path.join(ROOT_DIR, "storage", "auto_improve_status.json")
LOG_DIR = os.path.join(ROOT_DIR, "storage", "auto_improve_logs")


def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    _write_status(msg)


def _write_status(msg: str):
    """Write daemon status for health check API."""
    try:
        os.makedirs(os.path.dirname(STATUS_FILE), exist_ok=True)
        status = {
            "last_activity": datetime.now().isoformat(),
            "message": msg[:200],
            "pid": os.getpid(),
            "alive": True,
        }
        with open(STATUS_FILE, "w") as f:
            json.dump(status, f)
    except Exception:
        pass


def get_token() -> str:
    try:
        r = httpx.post(f"{API_BASE}/auth/login",
                       data={"username": USERNAME, "password": PASSWORD}, timeout=10)
        return r.json().get("access_token", "")
    except Exception:
        return ""


def api_get(path: str, token: str):
    r = httpx.get(f"{API_BASE}{path}", headers={"Authorization": f"Bearer {token}"}, timeout=15)
    r.raise_for_status()
    return r.json()


def api_post(path: str, token: str, data: dict = None):
    r = httpx.post(f"{API_BASE}{path}", headers={"Authorization": f"Bearer {token}"},
                   json=data or {}, timeout=300)
    r.raise_for_status()
    return r.json()


def get_models(token: str) -> list:
    return api_get("/ml-models/?page=1&page_size=20", token).get("items", [])


def _is_v10_model(name: str) -> bool:
    """Check if a model is a v10+ (LLM-based) model."""
    ver = _extract_version(name)
    return ver is not None and ver >= 100


def find_model_needing_improvement(token: str) -> dict | None:
    """Find a model that needs improvement.

    Two separate tracks:
    - v10+ (LLM): always improve v10.0 in-place (iterate on prompt/code, re-test same model)
    - v8.x (heuristic): improve from best model, create new versions

    v10+ is prioritized.
    """
    models = get_models(token)

    # ── Track 1: Check for v10+ model needing improvement ──
    for model in models:
        if not _is_v10_model(model.get("name", "")):
            continue
        tr = model.get("latest_test_run")
        if not tr or tr.get("status") != "completed":
            continue
        config = tr.get("test_config") or {}
        if not config.get("auto_improve"):
            continue
        # v10 uses a marker file to prevent re-processing the same test run
        marker = os.path.join(ROOT_DIR, "storage", f"v10_improved_{tr['id']}")
        if os.path.exists(marker):
            continue  # Already improved from this test run
        return model

    # ── Track 2: v8.x heuristic models (original logic) ──
    latest_completed = None
    for model in models:
        if model.get("model_type") != "tiered_extractor":
            continue
        if _is_v10_model(model.get("name", "")):
            continue  # Skip v10+ models
        tr = model.get("latest_test_run")
        if not tr or tr.get("status") != "completed":
            continue
        config = tr.get("test_config") or {}
        if not config.get("auto_improve"):
            continue
        model_ver = _extract_version(model["name"])
        if model_ver is None:
            continue
        next_file_ver = model_ver + 1
        next_file = os.path.join(PROJECT_DIR, "app", "crawlers", f"tiered_extractor_v{next_file_ver}.py")
        skip_file = next_file + ".skip"
        if os.path.exists(next_file) or os.path.exists(skip_file):
            continue
        latest_completed = model
        break

    if not latest_completed:
        return None

    # Rollback logic for v8.x only
    best_model = None
    best_score = 0
    for model in models:
        if model.get("model_type") != "tiered_extractor":
            continue
        if _is_v10_model(model.get("name", "")):
            continue
        tr = model.get("latest_test_run")
        if not tr or tr.get("status") != "completed":
            continue
        rd = tr.get("results_detail") or {}
        summary = rd.get("summary") or {}
        ch_score = (summary.get("challenger_composite") or {}).get("composite", 0)
        if ch_score and ch_score > best_score:
            best_score = ch_score
            best_model = model

    latest_tr = latest_completed.get("latest_test_run") or {}
    latest_rd = latest_tr.get("results_detail") or {}
    latest_summary = latest_rd.get("summary") or {}
    latest_score = (latest_summary.get("challenger_composite") or {}).get("composite", 0)

    if best_model and best_score > 0 and latest_score < best_score * 0.95:
        log(f"⚠️ {latest_completed['name']} regressed ({latest_score:.1f} < {best_score:.1f}). "
            f"Rolling back to {best_model['name']} as improvement base.")
        return best_model

    return latest_completed


STUCK_TEST_TIMEOUT = 3600  # 60 min — tests on 50+ sites need time


def _backfill_composites(test_run_id: str):
    """Compute composite scores AND run promotion logic for a force-completed test run.

    Runs inside the API container where SQLAlchemy and the scoring logic live.
    """
    script = f"""
import asyncio, json
from sqlalchemy import select
from sqlalchemy.orm.attributes import flag_modified
from app.db.base import AsyncSessionLocal
from app.models.ml_model import MLModel, MLModelTestRun

async def score():
    async with AsyncSessionLocal() as db:
        run = await db.get(MLModelTestRun, '{test_run_id}')
        if not run or not run.results_detail: return
        sites = run.results_detail.get('sites', [])
        summary = run.results_detail.get('summary', {{}})
        if not sites: return
        def _s(r, pk):
            t=len(r)
            if t==0: return {{'composite':0,'discovery':0,'quality_extraction':0,'field_completeness':0,'volume_accuracy':0}}
            d=sum(1 for s in r if (s.get(pk) or {{}}).get('url_found') and not ((s.get(pk) or {{}}).get('error') or '').startswith('Could not'))/t*100
            qe=sum(1 for s in r if (s.get(pk) or {{}}).get('jobs_quality',(s.get(pk) or {{}}).get('jobs',0))>0)
            qw=sum(1 for s in r if (s.get(pk) or {{}}).get('quality_warning'))
            q=max(0,(qe-qw)/max(1,t)*100)
            tj=sum((s.get(pk) or {{}}).get('jobs_quality',(s.get(pk) or {{}}).get('jobs',0)) for s in r)
            tf=sum(sum(v for k,v in ((s.get(pk) or {{}}).get('fields') or {{}}).items() if not k.startswith('_')) for s in r)
            fc=tf/max(1,tj*6)*100
            bt=sum((s.get('baseline') or {{}}).get('jobs',0) for s in r)
            mt=sum((s.get(pk) or {{}}).get('jobs_quality',(s.get(pk) or {{}}).get('jobs',0)) for s in r)
            ratio=mt/bt if bt>0 else 0
            va=(max(0,min(100,100-max(0,(ratio-1.5)*100))) if ratio>=1.0 else max(0,100*ratio)) if bt>0 else (50 if mt>0 else 0)
            return {{'composite':round(.2*d+.3*q+.25*fc+.25*va,1),'discovery':round(d,1),'quality_extraction':round(q,1),'field_completeness':round(fc,1),'volume_accuracy':round(va,1)}}

        # Compute scores if not already done
        if not summary.get('challenger_composite'):
            summary['challenger_composite'] = _s(sites, 'model')
            summary['champion_composite'] = _s(sites, 'champion')
            flag_modified(run, 'results_detail')

        ch_score = summary['challenger_composite']['composite']
        champ_score = summary['champion_composite']['composite']
        print(f"Scored: challenger={{ch_score}}, champion={{champ_score}}")

        # --- Promotion logic (same as Celery task) ---
        model = await db.get(MLModel, run.model_id)
        if not model:
            await db.commit()
            return

        reg_acc = summary.get('accuracy', 0)
        if isinstance(reg_acc, (int, float)) and reg_acc < 1:
            pass  # already a fraction
        elif isinstance(reg_acc, (int, float)):
            reg_acc = reg_acc / 100  # convert from percentage

        should_promote = (
            ch_score > 0
            and reg_acc >= 0.60
            and ch_score > champ_score
        )

        if should_promote:
            old_live = list(await db.scalars(
                select(MLModel).where(
                    MLModel.model_type == model.model_type,
                    MLModel.status == 'live',
                )
            ))
            for old in old_live:
                old.status = 'tested'
            model.status = 'live'
            print(f"PROMOTED {{model.name}} to live ({{ch_score}} > {{champ_score}})")
        else:
            if model.status == 'new':
                model.status = 'tested'
            print(f"Not promoted {{model.name}} (challenger={{ch_score}} vs champion={{champ_score}}, reg_acc={{reg_acc:.0%}})")

        await db.commit()

asyncio.run(score())
"""
    try:
        result = subprocess.run(
            ["docker", "exec", "jobharvest-api", "python3", "-c", script],
            capture_output=True, text=True, timeout=15,
        )
        if result.stdout.strip():
            log(f"  {result.stdout.strip()}")
        if result.returncode != 0 and result.stderr.strip():
            log(f"  Score error: {result.stderr[:200]}")
    except Exception as e:
        log(f"  Score backfill failed: {e}")

def is_test_running(token: str) -> bool:
    """Check if any test is currently running. Auto-completes stuck tests."""
    models = get_models(token)
    for m in models:
        tr = m.get("latest_test_run")
        if tr and tr.get("status") == "running":
            # Check if it's stuck
            started = tr.get("started_at")
            if started:
                try:
                    from datetime import datetime as _dt
                    started_dt = _dt.fromisoformat(started.replace("Z", "+00:00"))
                    elapsed = (_dt.now(started_dt.tzinfo) - started_dt).total_seconds()
                    if elapsed > STUCK_TEST_TIMEOUT:
                        log(f"⚠️ Test for {m['name']} stuck ({int(elapsed)}s). Marking complete.")
                        try:
                            # Mark test as completed
                            subprocess.run([
                                "docker", "exec", "jobharvest-postgres", "psql",
                                "-U", "jobharvest", "-d", "jobharvest", "-c",
                                f"UPDATE ml_model_test_runs SET status='completed', "
                                f"completed_at=NOW(), "
                                f"error_message='Auto-completed: stuck after {int(elapsed)}s' "
                                f"WHERE id='{tr['id']}' AND status='running';"
                            ], capture_output=True, timeout=10)
                            subprocess.run([
                                "docker", "exec", "jobharvest-postgres", "psql",
                                "-U", "jobharvest", "-d", "jobharvest", "-c",
                                f"UPDATE ml_models SET status='tested' "
                                f"WHERE id='{m['id']}' AND status='new';"
                            ], capture_output=True, timeout=10)
                            # Compute composite scores (the test endpoint would normally do this)
                            _backfill_composites(tr['id'])
                            log(f"✅ Marked {m['name']} test as completed + scored")
                            return False  # No longer running
                        except Exception as e:
                            log(f"❌ Failed to fix stuck test: {e}")
                except Exception:
                    pass
            return True
    return False


def _current_champion_tag(token: str) -> str | None:
    """Return the live champion's normalised version tag (e.g. 'v91') or None.

    Used by the fixture harness to compare challenger vs champion offline.
    Falls back to the highest-numbered extractor file on disk if the API
    doesn't expose a live model.
    """
    try:
        models = get_models(token)
        for m in models:
            status = (m.get("status") or "").lower()
            name = m.get("name") or ""
            if status in ("live", "champion") and name.startswith("v"):
                return name.replace(".", "")
    except Exception:
        pass
    # Fallback: pick the highest-numbered file (excluding the one we just wrote
    # if the caller mutates PROJECT_DIR-level state; we leave that to the caller)
    import glob
    paths = glob.glob(os.path.join(PROJECT_DIR, "app", "crawlers", "tiered_extractor_v*.py"))
    best = 0
    for p in paths:
        m = re.search(r"tiered_extractor_v(\d+)\.py", p)
        if m:
            best = max(best, int(m.group(1)))
    return f"v{best}" if best else None


def _extract_version(name: str) -> int | None:
    """Extract file version number from model name. 'v1.9' → 19, 'v2.0' → 20."""
    match = re.search(r"v(\d+)\.(\d+)", name)
    if match:
        return int(match.group(1)) * 10 + int(match.group(2))
    return None


def _create_improvement_run(token: str, data: dict) -> dict | None:
    """Create a CodexImprovementRun record via API."""
    try:
        return api_post("/ml-models/improvement-runs", token, data)
    except Exception as e:
        log(f"⚠️ Failed to create improvement run record: {e}")
        return None


def _update_improvement_run(token: str, run_id: str, data: dict):
    """Update a CodexImprovementRun record via API."""
    try:
        r = httpx.patch(f"{API_BASE}/ml-models/improvement-runs/{run_id}",
                        headers={"Authorization": f"Bearer {token}"},
                        json=data, timeout=15)
        r.raise_for_status()
    except Exception as e:
        log(f"⚠️ Failed to update improvement run {run_id}: {e}")


def _build_improvement_description(analysis: dict, model_id: str, model_name: str, next_version: str) -> str:
    """Build a rich 1-3 sentence description of what the improvement run did.

    Combines analysis data (which sites failed, what gaps existed) with
    the Codex log (what specific changes were implemented).
    """
    # Part 1: What was the problem (from analysis)
    failures = analysis.get("failures", [])
    gaps = analysis.get("gaps", [])
    fail_companies = [f.get("company", "?") for f in failures[:4]]
    gap_companies = [g.get("company", "?") for g in gaps[:4]]
    accuracy = analysis.get("accuracy", 0)
    vol_ratio = analysis.get("volume_ratio", 1.0)

    problem_parts = []
    if fail_companies:
        problem_parts.append(f"Targeted {len(failures)} failing site{'s' if len(failures) != 1 else ''} ({', '.join(fail_companies)})")
    if gap_companies:
        problem_parts.append(f"{len(gaps)} gap{'s' if len(gaps) != 1 else ''} ({', '.join(gap_companies)})")
    if vol_ratio < 0.95 and not problem_parts:
        problem_parts.append(f"volume at {vol_ratio:.0%} of baseline")

    problem_line = ". ".join(problem_parts) + "." if problem_parts else ""

    # Part 2: What was done (from Codex log — extract the "implementing" summary)
    codex_summary = ""
    log_file = os.path.join(LOG_DIR, f"{model_id}.log")
    if os.path.exists(log_file):
        try:
            with open(log_file) as f:
                lines = f.readlines()
            # Find the best "I'm now implementing..." or "I'm applying..." line
            implement_lines = []
            for line in lines:
                stripped = line.strip()
                if "🤖" in stripped:
                    lower = stripped.lower()
                    # Look for lines that describe what was built/changed
                    if any(kw in lower for kw in [
                        "implementing", "i'm applying", "i'm adding",
                        "applied a focused", "update:", "robust ",
                        "dedicated ", "improved ", "added ",
                        "fix set", "patch that", "change set",
                    ]):
                        # Extract just the content after the timestamp and emoji
                        content = stripped
                        if "🤖" in content:
                            content = content.split("🤖", 1)[1].strip()
                        # Remove leading "I've" / "I'm" boilerplate
                        for prefix in ["I've ", "I'm now ", "I'm applying ", "I'm "]:
                            if content.startswith(prefix):
                                content = content[len(prefix):]
                                break
                        implement_lines.append(content)

            if implement_lines:
                # Use the most detailed implementation line (usually the last one before tests)
                best = max(implement_lines, key=len)
                # Truncate to ~250 chars
                if len(best) > 250:
                    best = best[:247] + "..."
                codex_summary = best
        except Exception:
            pass

    # Part 3: Compose final description
    parts = []
    if problem_line:
        parts.append(problem_line)
    if codex_summary:
        parts.append(codex_summary)
    elif problem_parts:
        parts.append(f"Codex auto-improved {model_name} → {next_version}.")

    # Add accuracy context
    parts.append(f"Previous accuracy: {accuracy:.0%}, volume: {vol_ratio:.0%}.")

    return " ".join(parts)


def _determine_test_winner(model: dict) -> str | None:
    """Determine winner from the model's latest test run composite scores."""
    tr = model.get("latest_test_run")
    if not tr:
        return None
    rd = tr.get("results_detail") or {}
    summary = rd.get("summary") or {}
    champ_score = (summary.get("champion_composite") or {}).get("composite")
    chall_score = (summary.get("challenger_composite") or {}).get("composite")
    if champ_score is not None and chall_score is not None:
        if champ_score > chall_score:
            return "champion"
        elif chall_score > champ_score:
            return "challenger"
        else:
            return "tie"
    return None


def run_v10_improvement(model: dict, token: str):
    """Run one v10 improvement iteration: self-review → Codex (full autonomy) → rebuild → re-test.

    Unlike v8.x iterations which create new Python files, v10 improves in-place:
    - Codex can modify the extraction prompt, the extractor code, the LLM worker, or anything else
    - Codex reviews its own previous run logs to understand what to improve
    - The same v10.0 model is re-tested after changes
    """
    model_name = model["name"]
    model_id = model["id"]
    tr = model.get("latest_test_run") or {}
    test_run_id = tr.get("id")

    log(f"🔄 Starting v10 improvement for {model_name}")

    # Create improvement run record
    imp_run = _create_improvement_run(token, {
        "source_model_id": model_id,
        "test_run_id": test_run_id,
        "source_model_name": model_name,
        "test_winner": _determine_test_winner(model),
        "status": "analysing",
    })
    imp_run_id = imp_run["id"] if imp_run else None

    # Import auto_improve for analysis
    sys.path.insert(0, PROJECT_DIR)
    import importlib.util
    spec = importlib.util.spec_from_file_location("auto_improve",
        os.path.join(PROJECT_DIR, "scripts", "auto_improve.py"))
    ai = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(ai)

    # Get test run results
    runs = api_get(f"/ml-models/{model_id}/test-runs?page=1&page_size=1", token)
    if not runs["items"]:
        log("❌ No test run found")
        if imp_run_id:
            _update_improvement_run(token, imp_run_id, {"status": "failed", "error_message": "No test run found"})
        return

    run = runs["items"][0]

    # Analyse results (reuse existing analysis for failure/gap context files)
    context_dir = os.path.join(ROOT_DIR, "storage", "auto_improve_context", "v10_latest")
    analysis = ai.analyse_results(run, context_dir)
    log(f"📊 v10 Results: {analysis['accuracy']:.0%} ({analysis['success_count']}/{analysis['total_sites']})")
    log(f"📊 Failures: {analysis['fail_count']}, Gaps: {analysis.get('gap_count', 0)}, Volume: {analysis.get('volume_ratio', 0):.0%}")

    if imp_run_id:
        _update_improvement_run(token, imp_run_id, {"status": "running_codex"})

    # ── Build v10 self-improvement prompt ──
    # Read the base v10 auto-improve instructions
    v10_prompt_file = os.path.join(ROOT_DIR, "storage", "v10_auto_improve_prompt.md")
    try:
        with open(v10_prompt_file) as f:
            base_prompt = f.read()
    except FileNotFoundError:
        base_prompt = "Improve the v10 extraction system. Read storage/v10_extraction_prompt.md and modify it."

    # Read the previous Codex log for self-review
    prev_log_content = ""
    prev_log_file = os.path.join(LOG_DIR, f"{model_id}.log")
    if os.path.exists(prev_log_file):
        try:
            with open(prev_log_file) as f:
                raw = f.readlines()
            # Extract key lines: errors, decisions, file changes
            important = []
            for line in raw:
                ll = line.lower().strip()
                if any(kw in ll for kw in ["error", "fail", "❌", "⚠️", "🤖", "📝", "✏️",
                                            "timeout", "implemented", "created", "modified"]):
                    important.append(line.strip())
            if important:
                prev_log_content = "\n".join(important[-50:])  # Last 50 important lines
        except Exception:
            pass

    # Build failure/gap details (same format as v8.x)
    failures_text = ""
    for i, f in enumerate(analysis.get("failures", [])[:10]):
        failures_text += f"""
--- Failure {i+1}: {f['match']} ---
Company: {f['company']} | Domain: {f['domain']}
Test URL: {f['test_url']}
Baseline: {f['baseline_jobs']} jobs | Titles: {f['baseline_titles'][:3]}
Model: {f['model_jobs']} jobs | Error: {f['model_error']}
Context HTML: {f.get('html_file', 'N/A')}
Wrapper JSON: {f.get('wrapper_file', 'N/A')}
"""

    gaps_text = ""
    for i, g in enumerate(analysis.get("gaps", [])[:10]):
        gaps_text += f"""
--- Gap {i+1}: {g['match']} (volume: {g.get('volume_ratio', 0):.0%}) ---
Company: {g['company']} | Domain: {g['domain']}
Baseline: {g['baseline_jobs']} jobs | Model: {g['model_jobs']} jobs
Context HTML: {g.get('html_file', 'N/A')}
"""

    # Build the full Codex prompt
    prompt = f"""{base_prompt}

---

## STEP 1: Self-Review — What Happened Last Time

{"### Previous Run Log" + chr(10) + "```" + chr(10) + prev_log_content + chr(10) + "```" if prev_log_content else "No previous run log available (first iteration)."}

## STEP 2: Current Test Results

Model: {model_name}
Accuracy: {analysis['accuracy']:.0%} ({analysis['success_count']}/{analysis['total_sites']} sites passed)
Volume ratio: {analysis.get('volume_ratio', 0):.0%} of baseline
Match breakdown: {json.dumps(analysis['match_breakdown'])}

### Failures ({analysis['fail_count']} sites)
{failures_text if failures_text.strip() else "No hard failures."}

### Gaps ({analysis.get('gap_count', 0)} sites)
{gaps_text if gaps_text.strip() else "No gaps."}

### Context Files
Full HTML and wrapper configs are in: {context_dir}
File patterns: `failure_N_domain.html`, `gap_N_domain.html`, `*_wrapper.json`

## STEP 3: Your Mission

You have FULL AUTONOMY. You may modify ANY files to improve the v10 extraction system:

- `storage/v10_extraction_prompt.md` — the prompt sent to the LLM for each site
- `backend/app/crawlers/tiered_extractor_v100.py` — the extractor code
- `backend/scripts/v10_llm_worker.py` — the host-side Codex worker
- Create new utility scripts, tools, or helpers
- Add heuristics, pre-processing, or post-processing logic

The goal: increase the number of sites where v10 successfully extracts jobs that match
the baseline (Jobstream wrapper). Currently at {analysis['accuracy']:.0%} — every percentage
point matters.

### Key principles:
1. **Analyse failures deeply** — read the context HTML files to understand WHY extraction failed
2. **Fix the biggest categories first** — if 10 sites fail because the LLM can't parse tables, fix table extraction
3. **Test your changes** — use the context HTML files to verify your improvements work
4. **Be bold** — small incremental tweaks got v8.x stuck at 46%. Try fundamentally different approaches.
5. **The LLM worker runs codex exec on the host** — it has access to the full filesystem via /storage/v10_queue/

### Sandbox rules:
- DO NOT use Playwright, Docker, curl, or API calls (sandbox restrictions)
- DO use Python scripts to test extraction against context HTML files
- DO modify the extraction prompt and/or extractor code
"""

    # Save and run Codex
    os.makedirs(LOG_DIR, exist_ok=True)
    prompt_file = os.path.join(LOG_DIR, f"{model_id}_prompt.md")
    with open(prompt_file, "w") as f:
        f.write(prompt)

    log(f"🤖 Running Codex ({CODEX_MODEL}) for v10 improvement...")
    exit_code = ai.run_codex(prompt, ROOT_DIR, model_id)
    log(f"Codex exited with code {exit_code}")

    if exit_code != 0:
        log(f"❌ Codex failed with exit code {exit_code} — skipping deployment")
        if imp_run_id:
            _update_improvement_run(token, imp_run_id, {
                "status": "failed",
                "error_message": f"Codex failed with exit code {exit_code}",
            })
        raise RuntimeError(f"Codex failed with exit code {exit_code}")

    # Mark this test run as processed (prevent re-processing)
    marker = os.path.join(ROOT_DIR, "storage", f"v10_improved_{test_run_id}")
    with open(marker, "w") as f:
        f.write(f"Improved at {time.strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Rebuild API + workers to pick up any code changes
    log("🔄 Rebuilding containers to pick up v10 changes...")
    try:
        subprocess.run(
            ["docker", "compose", "-f", "docker-compose.server.yml", "up", "-d", "--build",
             "api", "celery-worker", "celery-worker-2", "celery-worker-ml", "celery-worker-ml-test"],
            cwd=ROOT_DIR, capture_output=True, timeout=180)
        time.sleep(10)
        log("✅ Containers rebuilt")
    except Exception as e:
        log(f"⚠️ Rebuild issue: {e}")

    # Restart the v10 LLM worker (it runs on the host)
    try:
        subprocess.run(["pkill", "-f", "v10_llm_worker"], capture_output=True, timeout=5)
        time.sleep(2)
        subprocess.Popen(
            ["python3", "-B", "-u", os.path.join(PROJECT_DIR, "scripts", "v10_llm_worker.py")],
            stdout=open("/tmp/v10_llm_worker.log", "w"),
            stderr=subprocess.STDOUT,
            cwd=ROOT_DIR,
        )
        log("✅ v10 LLM worker restarted")
    except Exception as e:
        log(f"⚠️ LLM worker restart issue: {e}")

    # Re-trigger test on the same v10.0 model
    token = get_token()
    try:
        api_post(f"/ml-models/{model_id}/test-runs/execute", token, {
            "sample_size": 50,
            "auto_improve": True,
            "use_fixed_set": True,
            "include_exploration": False,
        })
        log(f"✅ Test re-triggered for {model_name}")
    except Exception as e:
        log(f"❌ Test trigger failed: {e}")

    # Build description
    description = _build_improvement_description(analysis, model_id, model_name, "v10.0 (improved)")

    if imp_run_id:
        _update_improvement_run(token, imp_run_id, {
            "status": "completed",
            "output_model_id": model_id,  # Same model — improved in-place
            "output_model_name": f"{model_name} (iter {len(os.listdir(os.path.join(ROOT_DIR, 'storage'))) if False else '?'})",
            "description": description,
        })

    # Wait for the test to complete before returning — prevents the main loop
    # from immediately starting another Codex run while the test is still running
    log(f"⏳ Waiting for test to complete before next iteration...")
    test_wait_start = time.time()
    test_wait_timeout = 600  # 10 min max wait
    while time.time() - test_wait_start < test_wait_timeout:
        time.sleep(POLL_INTERVAL)
        try:
            token = get_token()
            models = get_models(token)
            for m in models:
                if m["id"] == model_id:
                    tr = m.get("latest_test_run")
                    if tr and tr.get("status") == "completed":
                        # Report test results
                        rd = tr.get("results_detail") or {}
                        summary = rd.get("summary") or {}
                        ch = summary.get("challenger_composite") or {}
                        score = ch.get("composite", 0)
                        sites = ch.get("sites_matched", 0)
                        total = ch.get("total_sites", 0)
                        log(f"📊 Test completed: {score:.1f} composite, {sites}/{total} sites matched")
                        return
                    elif tr and tr.get("status") == "running":
                        elapsed = int(time.time() - test_wait_start)
                        log(f"⏳ Test still running ({elapsed}s elapsed)...")
                    break
        except Exception as e:
            log(f"⚠️ Error checking test status: {e}")
    log(f"⚠️ Test wait timed out after {test_wait_timeout}s")


def run_improvement(model: dict, token: str):
    """Run one improvement iteration: analyse → Codex → deploy → test."""

    model_name = model["name"]
    model_id = model["id"]
    model_ver = _extract_version(model_name)
    next_file_ver = model_ver + 1
    next_version = f"v{next_file_ver // 10}.{next_file_ver % 10}"

    log(f"🔄 Starting improvement: {model_name} → {next_version}")

    # Determine test winner and test run ID for the improvement run record
    test_run_id = None
    tr = model.get("latest_test_run")
    if tr:
        test_run_id = tr.get("id")
    test_winner = _determine_test_winner(model)

    # Create improvement run record at the start
    imp_run = _create_improvement_run(token, {
        "source_model_id": model_id,
        "test_run_id": test_run_id,
        "source_model_name": model_name,
        "test_winner": test_winner,
        "status": "analysing",
    })
    imp_run_id = imp_run["id"] if imp_run else None

    # Import the auto_improve module for analysis and prompt building
    sys.path.insert(0, PROJECT_DIR)
    import importlib.util
    spec = importlib.util.spec_from_file_location("auto_improve",
        os.path.join(PROJECT_DIR, "scripts", "auto_improve.py"))
    ai = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(ai)

    # Get test run
    runs = api_get(f"/ml-models/{model_id}/test-runs?page=1&page_size=1", token)
    if not runs["items"]:
        log("❌ No test run found")
        if imp_run_id:
            _update_improvement_run(token, imp_run_id, {
                "status": "failed", "error_message": "No test run found"})
        return

    run = runs["items"][0]

    # Analyse results
    context_dir = os.path.join(ROOT_DIR, "storage", "auto_improve_context",
                               next_version.replace(".", "_"))
    analysis = ai.analyse_results(run, context_dir)
    log(f"📊 Results: {analysis['accuracy']:.0%} ({analysis['success_count']}/{analysis['total_sites']})")

    # Record iteration
    best_accuracy = 0.66
    try:
        with open(os.path.join(ROOT_DIR, "storage", "auto_improve_memory.json")) as f:
            mem = json.load(f)
        for it in mem.get("iterations", []):
            acc = it.get("accuracy")
            if acc is not None and acc > best_accuracy:
                best_accuracy = acc
    except Exception:
        pass

    ai.record_iteration(model_name, analysis["accuracy"],
                        analysis.get("baseline_accuracy", 0))
    ai.update_memory_with_results(model_name, analysis["accuracy"])

    improvement_count = analysis.get("improvement_count", analysis["fail_count"])
    vol_ratio = analysis.get("volume_ratio", 1.0)
    if improvement_count == 0 and vol_ratio >= 0.95:
        log(f"✅ No improvement opportunities — {analysis['accuracy']:.0%} accuracy, {vol_ratio:.0%} volume ratio")
        # Touch a marker so the daemon doesn't re-process this model
        marker = os.path.join(PROJECT_DIR, "app", "crawlers", f"tiered_extractor_v{next_file_ver}.py.skip")
        with open(marker, "w") as f:
            f.write(f"# Skipped — {model_name} had 0 failures + 0 gaps at {vol_ratio:.0%} volume\n")
        if imp_run_id:
            _update_improvement_run(token, imp_run_id, {
                "status": "skipped",
                "description": f"No improvement needed — {analysis['accuracy']:.0%} accuracy, {vol_ratio:.0%} volume ratio",
            })
        return
    log(f"📊 Improvement targets: {analysis['fail_count']} failures + {analysis.get('gap_count', 0)} gaps, volume ratio {vol_ratio:.0%}")

    # Update status to running_codex
    if imp_run_id:
        _update_improvement_run(token, imp_run_id, {"status": "running_codex"})

    # Build prompt
    prompt = ai.build_prompt(analysis, model, next_version, best_accuracy)

    # Save prompt
    os.makedirs(LOG_DIR, exist_ok=True)
    prompt_file = os.path.join(LOG_DIR, f"{model_id}_prompt.md")
    with open(prompt_file, "w") as f:
        f.write(prompt)

    # Run Codex
    log(f"🤖 Running Codex ({CODEX_MODEL}) for {next_version}...")
    exit_code = ai.run_codex(prompt, ROOT_DIR, model_id)
    log(f"Codex exited with code {exit_code}")

    if exit_code != 0:
        log(f"❌ Codex failed with exit code {exit_code} — skipping deployment")
        if imp_run_id:
            _update_improvement_run(token, imp_run_id, {
                "status": "failed",
                "error_message": f"Codex failed with exit code {exit_code}",
            })
        raise RuntimeError(f"Codex failed with exit code {exit_code}")

    # Post-Codex deployment
    extractor_file = os.path.join(PROJECT_DIR, "app", "crawlers",
                                   f"tiered_extractor_v{next_file_ver}.py")
    if not os.path.exists(extractor_file):
        log(f"❌ Codex did not create {extractor_file}")
        if imp_run_id:
            _update_improvement_run(token, imp_run_id, {
                "status": "failed",
                "error_message": f"Codex did not create extractor file (exit code {exit_code})",
            })
        return

    log(f"✅ Extractor file created. Deploying...")

    # Update status to deploying
    if imp_run_id:
        _update_improvement_run(token, imp_run_id, {"status": "deploying"})

    # Docker rebuild
    try:
        subprocess.run(
            ["docker", "compose", "-f", "docker-compose.server.yml", "up", "-d", "--build", "api"],
            cwd=ROOT_DIR, capture_output=True, timeout=120)
        subprocess.run(["docker", "restart", "jobharvest-api"],
                       capture_output=True, timeout=30)
        time.sleep(10)
        log("✅ API rebuilt")
    except Exception as e:
        log(f"⚠️ Docker rebuild issue: {e}")

    # Verify import
    verify = subprocess.run(
        ["docker", "exec", "jobharvest-api", "python3", "-c",
         f"from app.crawlers.tiered_extractor_v{next_file_ver} import TieredExtractorV{next_file_ver}; print('OK')"],
        capture_output=True, text=True, timeout=15)
    if "OK" not in (verify.stdout or ""):
        log(f"❌ Import failed: {verify.stderr[:200]}")
        if imp_run_id:
            _update_improvement_run(token, imp_run_id, {
                "status": "failed",
                "error_message": f"Import failed: {verify.stderr[:200]}",
            })
        return

    # Fixture smoke verification — runs in seconds vs the full A/B (10-30min).
    # Gated by FIXTURE_VERIFY_ENABLED so we can disable quickly if fixtures
    # drift or the harness produces noise. A significant regression aborts
    # the cycle before we spend A/B budget.
    if os.environ.get("FIXTURE_VERIFY_ENABLED", "1") == "1":
        champion_tag = _current_champion_tag(token)
        log(f"🧪 Running fixture harness: challenger={next_version} champion={champion_tag or 'none'}")
        cmd = [
            "docker", "exec", "jobharvest-api", "python3", "-m",
            "scripts.verify_challenger",
            "--version", next_version.replace(".", ""),
            "--tolerance", os.environ.get("FIXTURE_TOLERANCE", "2.0"),
        ]
        if champion_tag:
            cmd.extend(["--champion", champion_tag])
        fixture_res = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if fixture_res.returncode == 1:
            # Challenger regressed beyond tolerance — skip the full A/B
            log(f"❌ Fixture smoke failed — skipping full A/B test")
            log(f"   stdout tail: {(fixture_res.stdout or '')[-500:]}")
            if imp_run_id:
                _update_improvement_run(token, imp_run_id, {
                    "status": "failed",
                    "error_message": "Fixture smoke regressed beyond tolerance",
                })
            return
        elif fixture_res.returncode == 2:
            log("⚠️ Fixture harness could not run (no fixtures yet?) — proceeding to A/B")
        else:
            log("✅ Fixture smoke passed — proceeding to A/B")

    # Create model
    token = get_token()
    try:
        api_post("/ml-models/", token, {
            "name": next_version,
            "model_type": "tiered_extractor",
            "description": f"{next_version}: Codex auto-improve. See memory for details.",
        })
        log(f"✅ Model {next_version} created")
    except Exception as e:
        log(f"⚠️ Model creation: {e}")

    # Trigger test
    token = get_token()
    models = get_models(token)
    new_model = next((m for m in models if next_version in m["name"]), None)
    if not new_model:
        log(f"❌ Can't find model {next_version}")
        if imp_run_id:
            _update_improvement_run(token, imp_run_id, {
                "status": "failed",
                "error_message": f"Could not find created model {next_version}",
            })
        return

    try:
        api_post(f"/ml-models/{new_model['id']}/test-runs/execute", token, {
            "sample_size": 50,
            "auto_improve": True,
            "use_fixed_set": True,
            "include_exploration": False,
        })
        log(f"✅ Test triggered for {next_version}")
    except Exception as e:
        log(f"❌ Test trigger failed: {e}")

    # Build rich description from analysis + Codex log
    description = _build_improvement_description(analysis, model_id, model_name, next_version)

    # Mark improvement run as completed
    if imp_run_id:
        _update_improvement_run(token, imp_run_id, {
            "status": "completed",
            "output_model_id": new_model["id"],
            "output_model_name": next_version,
            "description": description,
        })


def main():
    log("🚀 Auto-improve daemon started")
    log(f"  Codex model: {CODEX_MODEL}")
    log(f"  Codex timeout: {CODEX_TIMEOUT}s")
    log(f"  Poll interval: {POLL_INTERVAL}s")

    while True:
        try:
            token = get_token()
            if not token:
                log("⚠️ Auth failed — retrying in 60s")
                time.sleep(60)
                continue

            # Check if a test is currently running — wait for it
            if is_test_running(token):
                _write_status("Waiting for test to complete...")
                time.sleep(POLL_INTERVAL)
                continue

            # Find a model that needs improvement
            model = find_model_needing_improvement(token)
            if not model:
                _write_status("Idle — no models need improvement")
                time.sleep(POLL_INTERVAL)
                continue

            # Run the improvement (route v10+ to its own function)
            try:
                if _is_v10_model(model.get("name", "")):
                    run_v10_improvement(model, token)
                else:
                    run_improvement(model, token)
            except Exception as e:
                log(f"❌ Improvement failed: {e}")
                import traceback
                traceback.print_exc()
                # Wait before retrying to avoid tight error loops
                time.sleep(120)

        except KeyboardInterrupt:
            log("Shutting down")
            break
        except Exception as e:
            log(f"❌ Daemon error: {e}")
            time.sleep(60)


if __name__ == "__main__":
    main()
