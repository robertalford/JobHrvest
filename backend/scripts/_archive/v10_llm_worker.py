#!/usr/bin/env python3
"""
V10 LLM Worker — Host-side process that runs Codex CLI for the v10 extractor.

Monitors /storage/v10_queue/ for .prompt files, runs each through Codex,
and writes the result to .result files.

Run: nohup python3 -B -u backend/scripts/v10_llm_worker.py &
"""

import glob
import json
import os
import subprocess
import sys
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

QUEUE_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "..", "storage", "v10_queue")
CODEX_MODEL = os.environ.get("V10_CODEX_MODEL", "gpt-5.3-codex")
CODEX_TIMEOUT = int(os.environ.get("V10_CODEX_TIMEOUT", "40"))
MAX_WORKERS = int(os.environ.get("V10_MAX_WORKERS", "2"))
POLL_INTERVAL = 0.25  # seconds


def log(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def process_request(prompt_file: str):
    """Process a single .prompt file through Codex and write .result."""
    req_id = os.path.basename(prompt_file).replace(".prompt", "")
    result_file = prompt_file.replace(".prompt", ".result")

    try:
        with open(prompt_file) as f:
            prompt = f.read()

        # Run Codex with JSONL events and parse assistant text directly.
        cmd = [
            "codex", "exec",
            "--json",
            "-m", CODEX_MODEL,
            prompt,
        ]

        log(f"🔧 {req_id}: running codex exec -m {CODEX_MODEL}")
        result = subprocess.run(
            cmd,
            capture_output=True, text=True,
            timeout=CODEX_TIMEOUT,
            cwd=os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
            env={**os.environ},
        )
        log(f"🔧 {req_id}: codex returned code={result.returncode}, stdout={len(result.stdout)}b, stderr={len(result.stderr)}b")
        if result.returncode != 0:
            log(f"🔧 {req_id}: stderr: {result.stderr[:300]}")

        output = _extract_text_from_jsonl(result.stdout)
        if not output:
            output = result.stdout or result.stderr or ""

        json_output = _extract_json(output)

        with open(result_file, "w") as f:
            f.write(json_output)

        log(f"✅ {req_id}: processed ({len(json_output)} bytes)")

    except subprocess.TimeoutExpired:
        log(f"⏰ {req_id}: timeout ({CODEX_TIMEOUT}s)")
        with open(result_file, "w") as f:
            f.write('{"jobs": [], "error": "timeout"}')
    except Exception as e:
        log(f"❌ {req_id}: {e}")
        with open(result_file, "w") as f:
            f.write(f'{{"jobs": [], "error": "{str(e)[:100]}"}}')


def _extract_json(output: str) -> str:
    """Extract JSON from Codex output, handling various formats."""
    import re

    if not output:
        return '{"jobs": []}'

    # Try direct parse
    try:
        json.loads(output)
        return output
    except json.JSONDecodeError:
        pass

    # Try code block extraction
    match = re.search(r'```(?:json)?\s*\n?(.*?)\n?```', output, re.DOTALL)
    if match:
        try:
            json.loads(match.group(1))
            return match.group(1)
        except json.JSONDecodeError:
            pass

    # Try finding JSON object with "jobs"
    for m in re.finditer(r'\{[^{}]*"jobs"\s*:\s*\[.*?\].*?\}', output, re.DOTALL):
        try:
            json.loads(m.group())
            return m.group()
        except json.JSONDecodeError:
            continue

    # Last resort — return the raw output and let the extractor try to parse it
    return output


def _extract_text_from_jsonl(stdout: str) -> str:
    """Extract the latest assistant text payload from codex --json events."""
    if not stdout:
        return ""

    latest_text = ""
    for line in stdout.strip().split("\n"):
        line = line.strip()
        if not line:
            continue
        try:
            evt = json.loads(line)
        except Exception:
            continue

        # Most messages arrive as completed items with content blocks.
        if evt.get("type") == "item.completed":
            item = evt.get("item", {})
            for content in item.get("content", []):
                text = content.get("text", "")
                if text:
                    latest_text = text
        # Fallback for alternative event shapes.
        elif evt.get("type") == "response.completed":
            text = evt.get("response", {}).get("output_text", "")
            if text:
                latest_text = text

    return latest_text


def main():
    os.makedirs(QUEUE_DIR, exist_ok=True)
    log(f"🚀 V10 LLM Worker started (model={CODEX_MODEL}, workers={MAX_WORKERS}, timeout={CODEX_TIMEOUT}s)")
    log(f"   Queue dir: {QUEUE_DIR}")

    executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
    active_futures = {}

    while True:
        try:
            # Find pending .prompt files (no matching .result yet)
            prompts = glob.glob(os.path.join(QUEUE_DIR, "*.prompt"))
            for pf in prompts:
                req_id = os.path.basename(pf).replace(".prompt", "")
                result_file = pf.replace(".prompt", ".result")

                # Skip if already being processed or already done
                if req_id in active_futures or os.path.exists(result_file):
                    continue

                # Submit to thread pool
                future = executor.submit(process_request, pf)
                active_futures[req_id] = future
                log(f"📋 {req_id}: queued")

            # Clean up completed futures
            done = [rid for rid, f in active_futures.items() if f.done()]
            for rid in done:
                try:
                    active_futures[rid].result()  # Raise any exceptions
                except Exception as e:
                    log(f"❌ {rid}: thread error: {e}")
                del active_futures[rid]

            time.sleep(POLL_INTERVAL)

        except KeyboardInterrupt:
            log("Shutting down")
            executor.shutdown(wait=False)
            break
        except Exception as e:
            log(f"❌ Worker error: {e}")
            time.sleep(5)


if __name__ == "__main__":
    main()
