"""
V10.0 — LLM-based extractor using Codex CLI on the host.

Writes HTML to shared /storage volume, signals the host-side extraction
helper via a file-based queue, and reads back JSON results.

The extraction prompt is stored in storage/v10_extraction_prompt.md.
"""

import asyncio
import json
import logging
import os
import re
import hashlib
import time
import uuid

logger = logging.getLogger(__name__)

PROMPT_FILE = "/storage/v10_extraction_prompt.md"
QUEUE_DIR = "/storage/v10_queue"
RESULT_TIMEOUT = 90  # seconds to wait for LLM result


def _load_prompt() -> str:
    """Load the extraction prompt from file."""
    if os.path.exists(PROMPT_FILE):
        with open(PROMPT_FILE) as f:
            return f.read()
    return "Extract all job listings from the HTML. Return JSON with a 'jobs' array."


def _truncate_html(html: str, max_chars: int = 60000) -> str:
    """Truncate HTML to fit within LLM context."""
    cleaned = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
    cleaned = re.sub(r'<style[^>]*>.*?</style>', '', cleaned, flags=re.DOTALL | re.IGNORECASE)
    cleaned = re.sub(r'<!--.*?-->', '', cleaned, flags=re.DOTALL)
    cleaned = re.sub(r'\s+', ' ', cleaned)
    cleaned = re.sub(r'\s+data-[a-z-]+="[^"]*"', '', cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r'<svg[^>]*>.*?</svg>', '', cleaned, flags=re.DOTALL | re.IGNORECASE)

    if len(cleaned) <= max_chars:
        return cleaned
    half = max_chars // 2
    return cleaned[:half] + "\n<!-- truncated -->\n" + cleaned[-half:]


class TieredExtractorV100:
    """LLM-based extractor using host-side Codex CLI via shared storage queue."""

    async def extract(self, career_page, company, html: str) -> list[dict]:
        url = career_page.url if career_page else ""
        company_name = company.name if company else ""

        prompt = _load_prompt()
        truncated_html = _truncate_html(html)

        # Build full prompt
        full_prompt = f"""{prompt}

## Context
- Company: {company_name}
- Page URL: {url}

## HTML Content

{truncated_html}

Now extract all jobs from this HTML. Return ONLY the JSON object, no other text."""

        # Write request to queue (host-side worker picks it up)
        os.makedirs(QUEUE_DIR, exist_ok=True)
        req_id = str(uuid.uuid4())[:8]
        req_file = os.path.join(QUEUE_DIR, f"{req_id}.prompt")
        result_file = os.path.join(QUEUE_DIR, f"{req_id}.result")

        with open(req_file, "w") as f:
            f.write(full_prompt)

        # Wait for result
        try:
            result = await asyncio.get_event_loop().run_in_executor(
                None, lambda: self._wait_for_result(result_file, req_id, url)
            )
            return result
        finally:
            # Cleanup
            for f in [req_file, result_file]:
                try:
                    os.unlink(f)
                except Exception:
                    pass

    def _wait_for_result(self, result_file: str, req_id: str, page_url: str) -> list[dict]:
        """Wait for the host-side worker to produce a result."""
        start = time.time()
        while time.time() - start < RESULT_TIMEOUT:
            if os.path.exists(result_file):
                try:
                    with open(result_file) as f:
                        output = f.read()
                    return self._parse_response(output, page_url)
                except Exception as e:
                    logger.error("v10: failed to read result for %s: %s", page_url, e)
                    return []
            time.sleep(1)

        logger.warning("v10: timeout waiting for LLM result (%ds) for %s", RESULT_TIMEOUT, page_url)
        return []

    def _parse_response(self, output: str, page_url: str) -> list[dict]:
        if not output:
            return []

        try:
            data = json.loads(output)
            return self._normalize_jobs(data, page_url)
        except json.JSONDecodeError:
            pass

        json_match = re.search(r'```(?:json)?\s*\n?(.*?)\n?```', output, re.DOTALL)
        if json_match:
            try:
                data = json.loads(json_match.group(1))
                return self._normalize_jobs(data, page_url)
            except json.JSONDecodeError:
                pass

        for match in re.finditer(r'\{[^{}]*"jobs"\s*:\s*\[.*?\].*?\}', output, re.DOTALL):
            try:
                data = json.loads(match.group())
                return self._normalize_jobs(data, page_url)
            except json.JSONDecodeError:
                continue

        logger.warning("v10: could not parse response for %s", page_url)
        return []

    def _normalize_jobs(self, data: dict, page_url: str) -> list[dict]:
        from urllib.parse import urljoin

        if not isinstance(data, dict):
            return []

        jobs = data.get("jobs", [])
        if not isinstance(jobs, list):
            return []

        result = []
        for j in jobs:
            if not isinstance(j, dict):
                continue
            title = (j.get("title") or "").strip()
            if not title or len(title) < 3:
                continue

            source_url = (j.get("source_url") or j.get("url") or "").strip()
            if source_url and not source_url.startswith("http"):
                source_url = urljoin(page_url, source_url)

            result.append({
                "title": title,
                "source_url": source_url or page_url,
                "location_raw": j.get("location_raw") or j.get("location") or None,
                "salary_raw": j.get("salary_raw") or j.get("salary") or None,
                "employment_type": j.get("employment_type") or j.get("type") or None,
                "description": j.get("description") or None,
                "extraction_method": "v10_llm",
                "extraction_confidence": 0.85,
            })

        # Save wrapper
        wrapper = data.get("wrapper")
        if wrapper and isinstance(wrapper, dict):
            try:
                wrapper_dir = "/storage/v10_wrappers"
                os.makedirs(wrapper_dir, exist_ok=True)
                domain_hash = hashlib.md5(page_url.encode()).hexdigest()[:12]
                with open(os.path.join(wrapper_dir, f"{domain_hash}.json"), "w") as f:
                    json.dump({"url": page_url, "wrapper": wrapper, "job_count": len(result)}, f, indent=2)
            except Exception:
                pass

        logger.info("v10 extracted %d jobs from %s", len(result), page_url)
        return result
