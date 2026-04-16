"""Host-worker Codex client for company enrichment rows."""

from __future__ import annotations

from datetime import datetime, timezone
from glob import glob
import json
import os
import re
from typing import Any
from urllib.parse import urlparse

from pydantic import BaseModel, Field, ValidationError

from app.core.config import settings


class CompanyEnrichmentResult(BaseModel):
    job_page_url: str = Field(description="URL for the official job listing or careers page, or 'not found'")
    job_count: str = Field(description="Exact number, approximate count, or 'not found'")
    comment: str = Field(description="Short explanation of the selected result or why it was not found")


BLOCKED_RESULT_DOMAINS = (
    "jobstreet.com",
    "seek.com",
    "seek.com.au",
    "jora.com",
    "jobsdb.com",
    "linkedin.com",
    "indeed.com",
)


def is_blocked_job_board_url(url: str | None) -> bool:
    if not url:
        return False
    normalized = url.strip()
    if not normalized or normalized.lower() == "not found":
        return False
    try:
        hostname = (urlparse(normalized).hostname or "").strip(".").lower()
    except Exception:
        return False
    if not hostname:
        return False
    return any(hostname == domain or hostname.endswith(f".{domain}") for domain in BLOCKED_RESULT_DOMAINS)


PRIMARY_PROMPT = """You are a specialist at finding company careers and job listing pages.

For the company "{company}" in "{country}", do ALL of the following, in order:
1. Identify the company's official website domain.
2. On that domain or clearly associated subdomains (e.g. jobs.*, careers.*, workwithus.*, or ATS hosts), find the official job listing or careers page.
   Prefer URLs whose path contains one of: /careers, /career-opportunities, /jobs, /vacancies, /join-us, /work-with-us.
3. If no dedicated job-listing page exists but there is a general careers page, return that.
4. If absolutely nothing exists, return "not found".

Only consider official company or ATS-hosted pages that clearly belong to this company.
Never return a URL on these blocked job-board domains or any of their subdomains:
- jobstreet.com
- seek.com
- seek.com.au
- jora.com
- jobsdb.com
- linkedin.com
- indeed.com
If your best candidate is on one of those blocked domains, treat it as not found and continue looking for an official company or ATS-hosted page instead.

Respond ONLY with JSON:
{{
  "job_page_url": "URL or 'not found'",
  "job_count": "exact number like '5', or 'approx N', or 'not found'",
  "comment": "brief note <=200 chars explaining your choice or why not found"
}}
"""


SECOND_PASS_PROMPT = """In a previous attempt you could not confidently find a job listing page.

Try again for the company "{company}" in "{country}" by searching the web more broadly.

- Consider search patterns like: "{company} careers", "{company} jobs", "{company} vacancies".
- Consider ATS or hosted career platforms that are clearly the official posting channel for this company.
- Prefer URLs which list multiple open positions or a structured job list.
- Never return a URL on these blocked job-board domains or any of their subdomains: jobstreet.com, seek.com, seek.com.au, jora.com, jobsdb.com, linkedin.com, indeed.com.

If you find an official job listing or careers page (even if on a third-party ATS), return that URL.
If no such page exists, keep "not found".

Respond ONLY with JSON:
{{
  "job_page_url": "URL or 'not found'",
  "job_count": "exact number like '5', or 'approx N', or 'not found'",
  "comment": "brief note <=200 chars explaining your choice or why not found"
}}
"""


def extract_json_object(output: str) -> str:
    """Extract the first valid JSON object from raw Codex output."""
    if not output:
        return ""

    text = output.strip()
    try:
        json.loads(text)
        return text
    except json.JSONDecodeError:
        pass

    fenced = re.search(r"```(?:json)?\s*\n?(.*?)\n?```", text, re.DOTALL)
    if fenced:
        candidate = fenced.group(1).strip()
        try:
            json.loads(candidate)
            return candidate
        except json.JSONDecodeError:
            pass

    depth = 0
    start = None
    for idx, char in enumerate(text):
        if char == "{":
            if depth == 0:
                start = idx
            depth += 1
        elif char == "}":
            if depth > 0:
                depth -= 1
                if depth == 0 and start is not None:
                    candidate = text[start:idx + 1]
                    try:
                        json.loads(candidate)
                        return candidate
                    except json.JSONDecodeError:
                        start = None
                        continue
    return ""


class CompanyEnrichmentCodexClient:
    def __init__(self) -> None:
        self.model = settings.CODEX_MODEL
        self.status_file = settings.COMPANY_ENRICHMENT_WORKER_STATUS_FILE
        self.stale_after_seconds = settings.COMPANY_ENRICHMENT_WORKER_STALE_AFTER_SEC

    def get_worker_health(self) -> dict[str, Any]:
        candidate_files: list[str] = []
        if os.path.exists(self.status_file):
            candidate_files.append(self.status_file)
        candidate_files.extend(sorted(glob(f"{self.status_file}.*.json")))
        if not candidate_files:
            return {"alive": False, "message": "Host enrichment worker is not running", "last_heartbeat": None}
        try:
            workers: list[dict[str, Any]] = []
            latest_heartbeat_raw: str | None = None
            latest_heartbeat_dt: datetime | None = None
            alive_workers = 0
            active_workers = 0
            global_max_concurrency = 0
            per_run_max_concurrency = 0
            latest_message: str | None = None
            for path in candidate_files:
                with open(path, encoding="utf-8") as f:
                    data = json.load(f)
                heartbeat_raw = data.get("last_heartbeat")
                if not heartbeat_raw:
                    continue
                heartbeat = datetime.fromisoformat(heartbeat_raw)
                age = (datetime.now(timezone.utc) - heartbeat).total_seconds()
                is_alive = age <= self.stale_after_seconds
                if is_alive:
                    alive_workers += 1
                    active_workers += int(data.get("active_workers", 0) or 0)
                    global_max_concurrency += int(data.get("global_max_concurrency", 0) or 0)
                    per_run_max_concurrency = max(per_run_max_concurrency, int(data.get("per_run_max_concurrency", 0) or 0))
                if latest_heartbeat_dt is None or heartbeat > latest_heartbeat_dt:
                    latest_heartbeat_dt = heartbeat
                    latest_heartbeat_raw = heartbeat_raw
                    latest_message = data.get("message")
                workers.append({
                    "pid": data.get("pid"),
                    "worker_id": data.get("worker_id"),
                    "active_workers": data.get("active_workers", 0),
                    "global_max_concurrency": data.get("global_max_concurrency"),
                    "per_run_max_concurrency": data.get("per_run_max_concurrency"),
                    "last_heartbeat": heartbeat_raw,
                    "age_seconds": round(age, 1),
                    "alive": is_alive,
                })

            if latest_heartbeat_dt is None or latest_heartbeat_raw is None:
                return {"alive": False, "message": "Worker heartbeat missing", "last_heartbeat": None}

            age = (datetime.now(timezone.utc) - latest_heartbeat_dt).total_seconds()
            alive = alive_workers > 0
            return {
                "alive": alive,
                "message": latest_message or ("Worker healthy" if alive else "Worker heartbeat is stale"),
                "last_heartbeat": latest_heartbeat_raw,
                "age_seconds": round(age, 1),
                "pid": workers[0].get("pid") if len(workers) == 1 else None,
                "active_workers": active_workers,
                "global_max_concurrency": global_max_concurrency,
                "per_run_max_concurrency": per_run_max_concurrency,
                "worker_processes": alive_workers,
                "workers": workers,
            }
        except Exception as exc:
            return {"alive": False, "message": f"Worker status unreadable: {exc}", "last_heartbeat": None}

    def assert_worker_available(self) -> None:
        status = self.get_worker_health()
        if not status.get("alive"):
            raise RuntimeError(str(status.get("message") or "Host enrichment worker is unavailable"))

    def _parse_result(self, content: str) -> CompanyEnrichmentResult | None:
        if not content:
            return None
        try:
            data = json.loads(extract_json_object(content) or content)
            parsed = CompanyEnrichmentResult.model_validate(data)
            if is_blocked_job_board_url(parsed.job_page_url):
                return CompanyEnrichmentResult(
                    job_page_url="not found",
                    job_count="not found",
                    comment="Blocked job-board domain returned; no official careers page confirmed.",
                )
            return parsed
        except (json.JSONDecodeError, ValidationError):
            return None
