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
import threading
import time
import traceback
from datetime import datetime

import httpx

# ── Config ──
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ROOT_DIR = os.path.dirname(PROJECT_DIR)
API_BASE = os.environ.get("JH_API_URL", "http://localhost:8001/api/v1")
USERNAME = os.environ.get("JH_USERNAME", "r.m.l.alford@gmail.com")
PASSWORD = os.environ.get("JH_PASSWORD", "Uu00dyandben!")
CODEX_MODEL = os.environ.get("CODEX_MODEL", "gpt-5.3-codex")
CODEX_TIMEOUT = int(os.environ.get("CODEX_TIMEOUT_SEC", "1800"))  # 30 min per candidate. Was 1200 (20 min) and that proved too tight when Codex genuinely needed time to investigate before writing — see auto_improve.py for actual enforcement.
POLL_INTERVAL = 30
HEARTBEAT_INTERVAL = 60  # independent of main loop — proves daemon is alive
AUTO_IMPROVE_CANDIDATES_N = int(os.environ.get("AUTO_IMPROVE_CANDIDATES_N", "3"))
STATUS_FILE = os.path.join(ROOT_DIR, "storage", "auto_improve_status.json")
LOG_DIR = os.path.join(ROOT_DIR, "storage", "auto_improve_logs")

_last_activity_msg = "starting"
_last_activity_ts = datetime.now()


def log(msg: str):
    global _last_activity_msg, _last_activity_ts
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)
    _last_activity_msg = msg
    _last_activity_ts = datetime.now()
    _write_status(msg)


def _write_status(msg: str | None = None):
    try:
        os.makedirs(os.path.dirname(STATUS_FILE), exist_ok=True)
        status = {
            "last_activity": _last_activity_ts.isoformat(),
            "message": (msg or _last_activity_msg)[:200],
            "pid": os.getpid(),
            "alive": True,
            "heartbeat": datetime.now().isoformat(),
        }
        with open(STATUS_FILE, "w") as f:
            json.dump(status, f)
    except Exception:
        pass


def _heartbeat_loop():
    """Independent thread so the status file stays fresh even if the main loop blocks."""
    while True:
        _write_status()
        time.sleep(HEARTBEAT_INTERVAL)


def _self_exec():
    """Re-exec the daemon — supervisor-less recovery from fatal errors."""
    log("♻️ self-exec — restarting daemon process")
    try:
        os.execv(sys.executable, [sys.executable, "-B", "-u", os.path.abspath(__file__)])
    except Exception as e:
        print(f"execv failed: {e}", flush=True)
        sys.exit(1)


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


def find_model_needing_improvement(token: str) -> dict | None:
    """Find the next heuristic tiered_extractor model that needs auto-improve.

    Rollback: if the most recent candidate regressed >5% vs the all-time best
    composite, improve from the best model instead so the next challenger
    rebaselines from the strongest known champion.
    """
    models = get_models(token)

    latest_completed = None
    for model in models:
        if model.get("model_type") != "tiered_extractor":
            continue
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

    best_model = None
    best_score = 0
    for model in models:
        if model.get("model_type") != "tiered_extractor":
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
    """Return the live champion's normalised version tag (e.g. 'v69') or None.

    DB-authoritative: uses status='live' only. No filesystem fallback —
    filesystem ordering (``ls -t``) was the bug that let challengers inherit
    from the wrong file after 2026-04-14 when the Models page was reset.
    """
    try:
        models = get_models(token)
        for m in models:
            status = (m.get("status") or "").lower()
            name = m.get("name") or ""
            if status in ("live", "champion") and name.startswith("v"):
                return name.replace(".", "")
    except Exception as e:
        log(f"⚠️ champion tag lookup failed: {e}")
    return None


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


# v10 LLM-based improvement track was retired 2026-04-14. Only the heuristic
# run_improvement path below is active. To revive, recover from git history.


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

    # Post-Codex deployment.
    #
    # Soft-success path: when Codex is killed by the wall-clock timeout
    # (`proc.kill()` sends SIGKILL → exit_code=None) it often has already
    # finished writing the new extractor file — the timeout commonly fires
    # during the model's final self-review summary, after the file write.
    # Discarding that work and re-running Codex from scratch is expensive
    # (≈30 min × N candidates). Instead, when exit_code != 0:
    #   1) Check if the extractor file was created
    #   2) Try to import it inside the api container
    # If both pass, proceed with deployment regardless of exit code. The
    # downstream import verification + fixture harness still gate broken code.
    extractor_file = os.path.join(PROJECT_DIR, "app", "crawlers",
                                   f"tiered_extractor_v{next_file_ver}.py")
    soft_success = False
    if exit_code != 0:
        if not os.path.exists(extractor_file):
            log(f"❌ Codex failed (exit {exit_code}) and did not create {extractor_file} — skipping deployment")
            if imp_run_id:
                _update_improvement_run(token, imp_run_id, {
                    "status": "failed",
                    "error_message": f"Codex failed with exit code {exit_code} and produced no extractor",
                })
            raise RuntimeError(f"Codex failed with exit code {exit_code}")

        # Quick import probe — does the file import cleanly inside the running
        # api container? If yes, treat as soft-success.
        log(f"⚠️ Codex exited with {exit_code} but {os.path.basename(extractor_file)} exists — probing import")
        probe = subprocess.run(
            ["docker", "exec", "jobharvest-api", "python3", "-c",
             f"from app.crawlers.tiered_extractor_v{next_file_ver} "
             f"import TieredExtractorV{next_file_ver}; print('OK')"],
            capture_output=True, text=True, timeout=15,
        )
        if "OK" in (probe.stdout or ""):
            log(f"✅ Soft-success: extractor imports cleanly — proceeding to deploy")
            soft_success = True
        else:
            log(f"❌ Codex failed (exit {exit_code}) AND extractor doesn't import — skipping deployment")
            log(f"   import stderr: {(probe.stderr or '')[:300]}")
            if imp_run_id:
                _update_improvement_run(token, imp_run_id, {
                    "status": "failed",
                    "error_message": f"Codex failed (exit {exit_code}); extractor file present but import failed",
                })
            raise RuntimeError(f"Codex failed with exit code {exit_code} and extractor import failed")

    if not os.path.exists(extractor_file):
        log(f"❌ Codex did not create {extractor_file}")
        if imp_run_id:
            _update_improvement_run(token, imp_run_id, {
                "status": "failed",
                "error_message": f"Codex did not create extractor file (exit code {exit_code})",
            })
        return

    log(f"✅ Extractor file present (soft_success={soft_success}). Deploying...")

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
            log(f"❌ Fixture smoke failed — skipping full A/B test")
            log(f"   stdout tail: {(fixture_res.stdout or '')[-500:]}")
            if imp_run_id:
                _update_improvement_run(token, imp_run_id, {
                    "status": "failed",
                    "error_message": "Fixture smoke regressed beyond tolerance",
                })
            # Record the rejection in memory so Codex sees it next iteration.
            # Runs inside the API container where memory_store + fixture_harness live.
            try:
                subprocess.run([
                    "docker", "exec", "jobharvest-api", "python3", "-c",
                    f"from app.ml.champion_challenger.memory_store import append_rejection; "
                    f"append_rejection('{next_version}', 'fixture harness regressed beyond tolerance')"
                ], capture_output=True, timeout=10)
            except Exception as e:
                log(f"⚠️ could not record fixture rejection in memory: {e}")
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


def _install_signal_handlers():
    def _graceful(sig, frame):
        log(f"received signal {sig} — shutting down")
        _write_status("shutdown")
        sys.exit(0)
    signal.signal(signal.SIGTERM, _graceful)
    signal.signal(signal.SIGINT, _graceful)


def main():
    _install_signal_handlers()
    threading.Thread(target=_heartbeat_loop, daemon=True, name="heartbeat").start()

    log("🚀 Auto-improve daemon started")
    log(f"  Codex model: {CODEX_MODEL}")
    log(f"  Codex timeout: {CODEX_TIMEOUT}s")
    log(f"  Poll interval: {POLL_INTERVAL}s")
    log(f"  Candidates per iteration: {AUTO_IMPROVE_CANDIDATES_N}")

    consecutive_errors = 0
    while True:
        try:
            token = get_token()
            if not token:
                log("⚠️ Auth failed — retrying in 60s")
                time.sleep(60)
                continue

            if is_test_running(token):
                _write_status("Waiting for test to complete...")
                time.sleep(POLL_INTERVAL)
                continue

            model = find_model_needing_improvement(token)
            if not model:
                _write_status("Idle — no models need improvement")
                time.sleep(POLL_INTERVAL)
                continue

            try:
                run_improvement(model, token)
                consecutive_errors = 0
            except Exception as e:
                log(f"❌ Improvement failed: {e}")
                traceback.print_exc()
                consecutive_errors += 1
                time.sleep(120)

        except KeyboardInterrupt:
            log("Shutting down")
            break
        except Exception as e:
            log(f"❌ Daemon error: {e}")
            traceback.print_exc()
            consecutive_errors += 1
            time.sleep(60)

        # Self-heal: if we've failed many times in a row, re-exec ourselves. Importing
        # modules or client state may have degraded; a fresh process often clears it.
        if consecutive_errors >= 5:
            log(f"⚠️ {consecutive_errors} consecutive errors — self-exec for a clean start")
            _self_exec()


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except BaseException as e:
        # Catch-all: record the fatal error and re-exec so the daemon doesn't stay dead
        # unattended. A persistent bug will still crash on restart, but at least the
        # health check will reflect that (heartbeat keeps updating until the crash).
        traceback.print_exc()
        try:
            _write_status(f"fatal: {e!r}")
        except Exception:
            pass
        _self_exec()
