#!/usr/bin/env python3
"""Launch and supervise multiple host-side company enrichment worker processes."""

from __future__ import annotations

from pathlib import Path
import math
import os
import signal
import subprocess
import sys
import time


PROJECT_ROOT = Path(__file__).resolve().parents[2]
WORKER_SCRIPT = PROJECT_ROOT / "backend" / "scripts" / "company_enrichment_worker.py"


def _split_slots(total_slots: int, process_count: int) -> list[int]:
    process_count = max(process_count, 1)
    total_slots = max(total_slots, 1)
    base = total_slots // process_count
    remainder = total_slots % process_count
    slots = []
    for idx in range(process_count):
        value = base + (1 if idx < remainder else 0)
        slots.append(max(value, 1))
    return slots


def main() -> int:
    process_count = int(os.environ.get("COMPANY_ENRICHMENT_WORKER_PROCESSES", "1"))
    total_codex_slots = int(os.environ.get("COMPANY_ENRICHMENT_GLOBAL_MAX_CONCURRENCY", "3"))
    total_fetch_slots = int(os.environ.get("COMPANY_ENRICHMENT_FETCH_MAX_CONCURRENCY", str(max(total_codex_slots, 1) * 2)))
    total_inflight = int(os.environ.get("COMPANY_ENRICHMENT_MAX_INFLIGHT_ROWS", str(max(total_codex_slots, total_fetch_slots))))

    codex_slots = _split_slots(total_codex_slots, process_count)
    fetch_slots = _split_slots(total_fetch_slots, process_count)
    inflight_slots = _split_slots(total_inflight, process_count)

    children: list[subprocess.Popen[bytes]] = []
    stopping = False

    def _shutdown(*_args: object) -> None:
        nonlocal stopping
        stopping = True
        for child in children:
            if child.poll() is None:
                child.terminate()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    for idx in range(process_count):
        env = {
            **os.environ,
            "COMPANY_ENRICHMENT_GLOBAL_MAX_CONCURRENCY": str(codex_slots[idx]),
            "COMPANY_ENRICHMENT_FETCH_MAX_CONCURRENCY": str(fetch_slots[idx]),
            "COMPANY_ENRICHMENT_MAX_INFLIGHT_ROWS": str(max(inflight_slots[idx], codex_slots[idx])),
            "COMPANY_ENRICHMENT_WORKER_MANAGER_INDEX": str(idx + 1),
        }
        child = subprocess.Popen(
            [sys.executable, "-B", "-u", str(WORKER_SCRIPT)],
            cwd=str(PROJECT_ROOT),
            env=env,
        )
        children.append(child)

    try:
        while not stopping:
            alive = [child for child in children if child.poll() is None]
            if not alive:
                return 1
            time.sleep(2)
    finally:
        for child in children:
            if child.poll() is None:
                child.terminate()
        deadline = time.time() + 10
        for child in children:
            remaining = max(0, deadline - time.time())
            try:
                child.wait(timeout=remaining)
            except subprocess.TimeoutExpired:
                child.kill()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
