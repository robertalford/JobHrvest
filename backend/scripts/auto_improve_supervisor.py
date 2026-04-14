#!/usr/bin/env python3
"""
Auto-Improve Supervisor — keeps the improvement loop alive.

Monitors and auto-recovers:
1. Watcher process (picks up triggers from completed tests)
2. Stuck test runs (marks them complete after timeout)
3. Orphaned Codex processes (kills after 15 min)
4. Missing triggers (writes trigger if test completed with auto_improve but no trigger exists)

Run this as a persistent background process:
    nohup python3 backend/scripts/auto_improve_supervisor.py &
"""

import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime

import httpx

# Config
PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ROOT_DIR = os.path.dirname(PROJECT_DIR)
API_BASE = os.environ.get("JH_API_URL", "http://localhost:8001/api/v1")
USERNAME = os.environ.get("JH_USERNAME", "r.m.l.alford@gmail.com")
PASSWORD = os.environ.get("JH_PASSWORD", "Uu00dyandben!")
CHECK_INTERVAL = 30  # seconds between health checks
STUCK_TEST_TIMEOUT = 300  # 5 min — mark running tests as completed
CODEX_MAX_RUNTIME = 2700  # 45 min — kill codex if running too long
WATCHER_SCRIPT = os.path.join(PROJECT_DIR, "scripts", "auto_improve.py")
LOG_FILE = "/tmp/auto_improve_supervisor.log"


def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line, flush=True)
    try:
        with open(LOG_FILE, "a") as f:
            f.write(line + "\n")
    except Exception:
        pass


def get_token() -> str:
    try:
        r = httpx.post(f"{API_BASE}/auth/login", data={"username": USERNAME, "password": PASSWORD}, timeout=10)
        r.raise_for_status()
        return r.json()["access_token"]
    except Exception as e:
        log(f"Auth failed: {e}")
        return ""


def is_process_running(name: str) -> list[int]:
    """Find PIDs of processes matching name."""
    try:
        result = subprocess.run(
            ["pgrep", "-f", name], capture_output=True, text=True
        )
        pids = [int(p) for p in result.stdout.strip().split("\n") if p.strip()]
        # Exclude our own PID
        own_pid = os.getpid()
        return [p for p in pids if p != own_pid]
    except Exception:
        return []


def ensure_watcher_running():
    """Make sure the auto_improve watcher process is running."""
    pids = is_process_running("auto_improve.py --watch")
    if pids:
        return  # Already running

    log("⚠️ Watcher not running — starting it")
    env = os.environ.copy()
    env["PYTHONUNBUFFERED"] = "1"
    subprocess.Popen(
        [sys.executable, WATCHER_SCRIPT, "--watch"],
        cwd=ROOT_DIR,
        env=env,
        stdout=open("/tmp/auto_improve_watcher.log", "a"),
        stderr=subprocess.STDOUT,
    )
    log("✅ Watcher started")


def check_stuck_tests():
    """Find test runs stuck in 'running' state and mark them completed."""
    token = get_token()
    if not token:
        return

    try:
        models = httpx.get(
            f"{API_BASE}/ml-models/?page=1&page_size=20",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        ).json()

        for model in models.get("items", []):
            tr = model.get("latest_test_run")
            if not tr or tr.get("status") != "running":
                continue

            started = tr.get("started_at")
            if not started:
                continue

            # Parse started time
            try:
                started_dt = datetime.fromisoformat(started.replace("Z", "+00:00"))
                elapsed = (datetime.now(started_dt.tzinfo) - started_dt).total_seconds()
            except Exception:
                continue

            if elapsed > STUCK_TEST_TIMEOUT:
                progress = (tr.get("results_detail") or {}).get("progress", {})
                done = progress.get("done", "?")
                total = progress.get("total", "?")
                log(f"⚠️ Stuck test for {model['name']}: {done}/{total} after {elapsed:.0f}s — marking complete")

                # Mark as completed via direct DB update
                try:
                    subprocess.run([
                        "docker", "exec", "jobharvest-postgres", "psql",
                        "-U", "jobharvest", "-d", "jobharvest", "-c",
                        f"UPDATE ml_model_test_runs SET status = 'completed', "
                        f"completed_at = NOW(), "
                        f"error_message = 'Auto-recovered: stuck after {elapsed:.0f}s ({done}/{total})' "
                        f"WHERE id = '{tr['id']}' AND status = 'running';"
                    ], capture_output=True, timeout=10)

                    subprocess.run([
                        "docker", "exec", "jobharvest-postgres", "psql",
                        "-U", "jobharvest", "-d", "jobharvest", "-c",
                        f"UPDATE ml_models SET status = 'tested' "
                        f"WHERE id = '{model['id']}' AND status = 'new';"
                    ], capture_output=True, timeout=10)

                    log(f"✅ Marked {model['name']} test as completed")
                except Exception as e:
                    log(f"❌ Failed to fix stuck test: {e}")

    except Exception as e:
        log(f"Error checking tests: {e}")


def check_orphaned_codex():
    """Kill Codex processes running longer than CODEX_MAX_RUNTIME."""
    pids = is_process_running("codex exec")
    for pid in pids:
        try:
            # Get process elapsed time
            result = subprocess.run(
                ["ps", "-p", str(pid), "-o", "etimes="],
                capture_output=True, text=True,
            )
            elapsed = int(result.stdout.strip())
            if elapsed > CODEX_MAX_RUNTIME:
                log(f"⚠️ Codex PID {pid} running for {elapsed}s — killing")
                os.kill(pid, signal.SIGKILL)
                log(f"✅ Killed orphaned Codex PID {pid}")
        except Exception:
            pass


def check_missing_triggers():
    """If a test completed with auto_improve=true but no trigger file exists, write one."""
    token = get_token()
    if not token:
        return

    trigger_dir = os.path.join(ROOT_DIR, "storage", "auto_improve_triggers")
    os.makedirs(trigger_dir, exist_ok=True)

    # Check if there are any pending triggers already
    existing = [f for f in os.listdir(trigger_dir) if f.endswith(".trigger")]
    if existing:
        return  # Don't add more triggers if one is pending

    # Check if any codex or auto_improve is currently running
    if is_process_running("codex exec") or is_process_running("auto_improve.py --model-id"):
        return  # Something is already running

    try:
        models = httpx.get(
            f"{API_BASE}/ml-models/?page=1&page_size=10",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        ).json()

        for model in models.get("items", []):
            tr = model.get("latest_test_run")
            if not tr:
                continue
            if tr.get("status") != "completed":
                continue

            config = tr.get("test_config") or {}
            if not config.get("auto_improve"):
                continue

            # Check if this model has already been processed (a newer model exists)
            # Simple heuristic: if this is the newest model with a completed test, trigger it
            if model.get("status") in ("tested", "live"):
                # Check if a trigger was already processed recently
                log_dir = os.path.join(ROOT_DIR, "storage", "auto_improve_logs")
                log_file = os.path.join(log_dir, f"{model['id']}.log")
                if os.path.exists(log_file):
                    # Check if log is recent (within last 30 min)
                    mtime = os.path.getmtime(log_file)
                    if time.time() - mtime < 1800:
                        continue  # Already processed recently

                trigger_file = os.path.join(trigger_dir, f"{model['id']}.trigger")
                with open(trigger_file, "w") as f:
                    json.dump({
                        "model_id": model["id"],
                        "model_name": model["name"],
                        "triggered_at": datetime.now().isoformat(),
                        "auto_improve": True,
                        "source": "supervisor_recovery",
                    }, f)
                log(f"📝 Wrote recovery trigger for {model['name']}")
                break  # Only one trigger at a time

    except Exception as e:
        log(f"Error checking triggers: {e}")


def main():
    log("🚀 Auto-improve supervisor started")
    log(f"  Check interval: {CHECK_INTERVAL}s")
    log(f"  Stuck test timeout: {STUCK_TEST_TIMEOUT}s")
    log(f"  Codex max runtime: {CODEX_MAX_RUNTIME}s")

    while True:
        try:
            ensure_watcher_running()
            check_stuck_tests()
            check_orphaned_codex()
            check_missing_triggers()
        except Exception as e:
            log(f"❌ Supervisor error: {e}")

        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    main()
