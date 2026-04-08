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
CODEX_TIMEOUT = int(os.environ.get("V10_CODEX_TIMEOUT", "60"))
MAX_WORKERS = int(os.environ.get("V10_MAX_WORKERS", "8"))
POLL_INTERVAL = 0.5  # seconds


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

        # Write prompt to a temp file for Codex
        tmp_prompt = f"/tmp/v10_{req_id}.md"
        with open(tmp_prompt, "w") as f:
            f.write(prompt)

        # Run Codex with --json to get JSONL events
        cmd = [
            "codex", "exec",
            "--full-auto",
            "--json",
            "-m", CODEX_MODEL,
            f"Read {tmp_prompt} and follow the instructions exactly. "
            f"Write the JSON result to {tmp_prompt.replace('.md', '.out')}",
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

        # Try to read the output file Codex was asked to create
        out_file = tmp_prompt.replace('.md', '.out')
        output = ""
        if os.path.exists(out_file):
            with open(out_file) as f:
                output = f.read().strip()
            log(f"🔧 {req_id}: read output file ({len(output)} bytes)")
            try:
                os.unlink(out_file)
            except Exception:
                pass
        else:
            log(f"🔧 {req_id}: no output file at {out_file}")

        # If no output file, try to extract from JSONL events (agent messages)
        if not output and result.stdout:
            for line in result.stdout.strip().split("\n"):
                try:
                    evt = json.loads(line)
                    if evt.get("type") == "item.completed":
                        item = evt.get("item", {})
                        for content in item.get("content", []):
                            text = content.get("text", "")
                            if '"jobs"' in text:
                                output = text
                                break
                except Exception:
                    continue

        json_output = _extract_json(output)

        with open(result_file, "w") as f:
            f.write(json_output)

        log(f"✅ {req_id}: processed ({len(json_output)} bytes)")

        # Cleanup temp
        try:
            os.unlink(tmp_prompt)
        except Exception:
            pass

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
