#!/usr/bin/env python3
"""Host-side worker for concurrent Company Enrichment Codex requests."""

from __future__ import annotations

from concurrent.futures import Future, ThreadPoolExecutor
from datetime import datetime, timezone
import html
import json
import os
from pathlib import Path
import re
import subprocess
import threading
import time
from typing import Any
from urllib.parse import urlparse
import uuid

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session, sessionmaker


PROJECT_ROOT = Path(__file__).resolve().parents[2]
def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        os.environ.setdefault(key.strip(), value.strip())


_load_env_file(PROJECT_ROOT / ".env")


STORAGE_DIR = PROJECT_ROOT / "storage" / "company_enrichment"
OUTPUT_DIR = STORAGE_DIR / "outputs"
STATUS_FILE_BASE = Path(os.environ.get("COMPANY_ENRICHMENT_WORKER_STATUS_FILE", str(STORAGE_DIR / "worker_status.json")))
STATUS_FILE = STATUS_FILE_BASE.parent / f"{STATUS_FILE_BASE.name}.{os.getpid()}.json"
CODEX_MODEL = os.environ.get("CODEX_MODEL", "gpt-5.3-codex")
CODEX_TIMEOUT = int(os.environ.get("COMPANY_ENRICHMENT_CODEX_TIMEOUT_SEC", "120"))
PAGE_FETCH_TIMEOUT = int(os.environ.get("COMPANY_ENRICHMENT_PAGE_FETCH_TIMEOUT_SEC", "20"))
GLOBAL_MAX_WORKERS = int(os.environ.get("COMPANY_ENRICHMENT_GLOBAL_MAX_CONCURRENCY", "3"))
PER_RUN_MAX_WORKERS = int(os.environ.get("COMPANY_ENRICHMENT_PER_RUN_MAX_CONCURRENCY", "2"))
FETCH_MAX_WORKERS = int(os.environ.get("COMPANY_ENRICHMENT_FETCH_MAX_CONCURRENCY", str(max(GLOBAL_MAX_WORKERS, 1))))
MAX_INFLIGHT_ROWS = int(os.environ.get("COMPANY_ENRICHMENT_MAX_INFLIGHT_ROWS", str(max(GLOBAL_MAX_WORKERS, FETCH_MAX_WORKERS))))
CACHE_TTL_HOURS = int(os.environ.get("COMPANY_ENRICHMENT_CACHE_TTL_HOURS", "168"))
ROW_STALE_AFTER_SECONDS = int(os.environ.get("COMPANY_ENRICHMENT_ROW_STALE_AFTER_SEC", "900"))
POLL_INTERVAL = 1.0
OUTPUT_COLUMNS = ["company", "country", "job_page_url", "job_count", "comment"]
WORKER_ID = f"{os.uname().nodename}:{os.getpid()}"
CODEX_SEMAPHORE = threading.Semaphore(max(GLOBAL_MAX_WORKERS, 1))
FETCH_SEMAPHORE = threading.Semaphore(max(FETCH_MAX_WORKERS, 1))

PRIMARY_PROMPT = """You are a specialist at finding company careers and job listing pages.

For the company "{company}" in "{country}", do ALL of the following, in order:
1. Identify the company's official website domain.
2. On that domain or clearly associated subdomains (e.g. jobs.*, careers.*, workwithus.*, or ATS hosts), find the official job listing or careers page.
   Prefer URLs whose path contains one of: /careers, /career-opportunities, /jobs, /vacancies, /join-us, /work-with-us.
3. If no dedicated job-listing page exists but there is a general careers page, return that.
4. If absolutely nothing exists, return "not found".

Only consider official company or ATS-hosted pages that clearly belong to this company.
Do NOT return generic job boards like Indeed, LinkedIn, or SEEK unless they are the company's clearly official posting page.

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

If you find an official job listing or careers page (even if on a third-party ATS), return that URL.
If no such page exists, keep "not found".

Respond ONLY with JSON:
{{
  "job_page_url": "URL or 'not found'",
  "job_count": "exact number like '5', or 'approx N', or 'not found'",
  "comment": "brief note <=200 chars explaining your choice or why not found"
}}
"""


def _extract_json_object(output: str) -> str:
    text_value = (output or "").strip()
    if not text_value:
        return ""
    try:
        json.loads(text_value)
        return text_value
    except json.JSONDecodeError:
        pass
    fenced = re.search(r"```(?:json)?\s*\n?(.*?)\n?```", text_value, re.DOTALL)
    if fenced:
        candidate = fenced.group(1).strip()
        try:
            json.loads(candidate)
            return candidate
        except json.JSONDecodeError:
            pass
    depth = 0
    start = None
    for idx, char in enumerate(text_value):
        if char == "{":
            if depth == 0:
                start = idx
            depth += 1
        elif char == "}":
            if depth > 0:
                depth -= 1
                if depth == 0 and start is not None:
                    candidate = text_value[start:idx + 1]
                    try:
                        json.loads(candidate)
                        return candidate
                    except json.JSONDecodeError:
                        start = None
    return ""


def _parse_result(content: str) -> dict[str, str] | None:
    try:
        data = json.loads(_extract_json_object(content) or content)
    except json.JSONDecodeError:
        return None
    if not isinstance(data, dict):
        return None
    values = {
        "job_page_url": str(data.get("job_page_url", "")).strip(),
        "job_count": str(data.get("job_count", "")).strip(),
        "comment": str(data.get("comment", "")).strip(),
    }
    if not values["job_page_url"] or not values["job_count"] or not values["comment"]:
        return None
    return values


def _clean_count_value(value: str | int | None) -> str | None:
    if value is None:
        return None
    text_value = str(value).strip()
    match = re.search(r"\b(\d{1,6})\b", text_value.replace(",", ""))
    if not match:
        return None
    number = int(match.group(1))
    if number < 0:
        return None
    return str(number)


def _fetch_page_content(url: str) -> dict[str, str] | None:
    if not url or url.strip().lower() == "not found":
        return None
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return None

    with FETCH_SEMAPHORE:
        proc = subprocess.run(
            [
                "curl",
                "-L",
                "--compressed",
                "--silent",
                "--show-error",
                "--max-time",
                str(PAGE_FETCH_TIMEOUT),
                "-A",
                "Mozilla/5.0 (JobHarvest company enrichment worker)",
                url,
            ],
            capture_output=True,
            cwd=str(PROJECT_ROOT),
            env={**os.environ},
        )
    if proc.returncode != 0:
        return None

    raw_bytes = proc.stdout or b""
    if not raw_bytes.strip():
        return None
    try:
        raw_html = raw_bytes.decode("utf-8")
    except UnicodeDecodeError:
        raw_html = raw_bytes.decode("utf-8", errors="ignore")
    if not raw_html.strip():
        return None

    without_scripts = re.sub(r"(?is)<script\b[^>]*>.*?</script>", " ", raw_html)
    without_styles = re.sub(r"(?is)<style\b[^>]*>.*?</style>", " ", without_scripts)
    text_content = re.sub(r"(?is)<[^>]+>", " ", without_styles)
    normalized_text = re.sub(r"\s+", " ", html.unescape(text_content)).strip()
    return {"html": raw_html, "text": normalized_text}


def _extract_exact_job_count(page: dict[str, str] | None) -> str | None:
    if not page:
        return None

    html_content = page.get("html", "")
    text_content = page.get("text", "")
    compact_html = re.sub(r"\s+", " ", html_content)

    signal_patterns = [
        r"\b(\d{1,6})\s+(?:open\s+)?(?:jobs|roles|positions|openings|vacancies|opportunities)\b",
        r"\b(?:jobs|roles|positions|openings|vacancies|opportunities)\s*[:\-]?\s*(\d{1,6})\b",
        r"\bshowing\s+\d{1,6}\s*(?:-|to)\s*\d{1,6}\s+of\s+(\d{1,6})\b",
        r"\b(\d{1,6})\s+results\b",
        r"\b(\d{1,6})\s+job\s+results\b",
        r'"(?:totalJobs|jobCount|jobsCount|totalOpenings|openingCount|totalResults|jobResultsCount)"\s*:\s*"?(\d{1,6})"?',
        r'"(?:count|total)"\s*:\s*"?(\d{1,6})"?',
    ]

    for pattern in signal_patterns:
        match = re.search(pattern, text_content, re.IGNORECASE)
        if match:
            cleaned = _clean_count_value(match.group(1))
            if cleaned is not None:
                return cleaned

    for pattern in signal_patterns[4:]:
        match = re.search(pattern, compact_html, re.IGNORECASE)
        if match:
            cleaned = _clean_count_value(match.group(1))
            if cleaned is not None:
                return cleaned

    return None


def _estimate_job_count_from_links(page: dict[str, str] | None) -> str | None:
    if not page:
        return None

    compact_html = re.sub(r"\s+", " ", page.get("html", ""))
    job_link_patterns = [
        r'href="([^"]*(?:/jobs?/[^"#]+|/job/[^"#]+|/positions?/[^"#]+))"',
        r'href="(https?://[^"]*(?:greenhouse\.io|ashbyhq\.com|lever\.co|myworkdayjobs\.com|smartrecruiters\.com)[^"]+)"',
    ]
    unique_links: set[str] = set()
    for pattern in job_link_patterns:
        for href in re.findall(pattern, compact_html, re.IGNORECASE):
            unique_links.add(href)
    if len(unique_links) >= 10:
        return str(len(unique_links))
    return None


def _refine_result_with_page_data(result: dict[str, str]) -> dict[str, str]:
    if result["job_page_url"].strip().lower() == "not found":
        return result

    page = _fetch_page_content(result["job_page_url"])
    exact_count = _extract_exact_job_count(page)
    if exact_count:
        lowered_comment = result["comment"].lower()
        if "exact count" not in lowered_comment and "page-extracted count" not in lowered_comment:
            result["comment"] = f"{result['comment']} Page-extracted count: {exact_count}."
        result["job_count"] = exact_count
        return result

    estimated_count = _estimate_job_count_from_links(page)

    current_count = (result.get("job_count") or "").strip().lower()
    current_numeric = _clean_count_value(current_count)
    estimated_numeric = _clean_count_value(estimated_count)
    if current_count.startswith("approx") and estimated_numeric:
        if current_numeric is None or int(estimated_numeric) > int(current_numeric):
            result["job_count"] = f"approx {estimated_numeric}"
            result["comment"] = (
                f"{result['comment']} Page-derived listing estimate suggests about {estimated_numeric} job links; "
                "exact count was not exposed in fetched page content."
            )
            return result

    if current_count.startswith("approx"):
        result["comment"] = (
            f"{result['comment']} Exact count was not exposed in the fetched page content; "
            "retaining model estimate."
        )
    return result


def _database_url_sync() -> str:
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    if host == "postgres":
        host = "localhost"
        port = "5434"
    user = os.environ.get("POSTGRES_USER", "jobharvest")
    password = os.environ.get("POSTGRES_PASSWORD", "jobharvest")
    db = os.environ.get("POSTGRES_DB", "jobharvest")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


ENGINE = create_engine(_database_url_sync(), pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=ENGINE, expire_on_commit=False, future=True)


def _write_status(message: str, active_workers: int) -> None:
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "last_heartbeat": datetime.now(timezone.utc).isoformat(),
        "message": message,
        "pid": os.getpid(),
        "worker_id": WORKER_ID,
        "model": CODEX_MODEL,
        "active_workers": active_workers,
        "global_max_concurrency": GLOBAL_MAX_WORKERS,
        "per_run_max_concurrency": PER_RUN_MAX_WORKERS,
    }
    STATUS_FILE.write_text(json.dumps(payload), encoding="utf-8")


def _extract_text_from_jsonl(stdout: str) -> str:
    latest_text = ""
    for line in (stdout or "").strip().splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            event = json.loads(line)
        except Exception:
            continue
        if event.get("type") == "item.completed":
            item = event.get("item", {})
            text_value = item.get("text", "")
            if text_value:
                latest_text = text_value
        elif event.get("type") == "response.completed":
            text_value = event.get("response", {}).get("output_text", "")
            if text_value:
                latest_text = text_value
    return latest_text


def _run_codex_prompt(prompt: str) -> dict[str, Any]:
    started = time.time()
    with CODEX_SEMAPHORE:
        proc = subprocess.run(
            ["codex", "exec", "--json", "-m", CODEX_MODEL, prompt],
            capture_output=True,
            text=True,
            timeout=CODEX_TIMEOUT,
            cwd=str(PROJECT_ROOT),
            env={**os.environ},
        )
    content = _extract_text_from_jsonl(proc.stdout) or proc.stdout or proc.stderr or ""
    return {
        "ok": proc.returncode == 0 and bool(content.strip()),
        "content": content,
        "returncode": proc.returncode,
        "stderr": proc.stderr[-1000:],
        "stdout_size": len(proc.stdout or ""),
        "elapsed_seconds": round(time.time() - started, 2),
        "model": CODEX_MODEL,
    }


def _execute_row(company: str, country: str) -> tuple[dict[str, Any], dict[str, Any]]:
    attempts: list[dict[str, Any]] = []
    prompts = [
        PRIMARY_PROMPT.format(company=company, country=country),
        SECOND_PASS_PROMPT.format(company=company, country=country),
    ]

    for idx, prompt in enumerate(prompts):
        last_error: str | None = None
        for transport_attempt in range(2):
            try:
                result = _run_codex_prompt(prompt)
                if not result.get("ok"):
                    raise RuntimeError(str(result.get("stderr") or "Codex returned no content").strip() or "Codex execution failed")
                parsed = _parse_result(str(result.get("content") or ""))
                attempts.append({
                    "prompt": prompt,
                    "content": result.get("content"),
                    "parsed": parsed,
                    "model": CODEX_MODEL,
                    "transport_attempts": transport_attempt + 1,
                    "worker_meta": result,
                })
                if parsed and (idx == 1 or parsed["job_page_url"].strip().lower() != "not found"):
                    parsed = _refine_result_with_page_data(parsed)
                    attempts[-1]["parsed"] = parsed
                    return parsed, {"attempts": attempts}
                break
            except Exception as exc:
                last_error = str(exc)
                if transport_attempt == 0:
                    time.sleep(1)
                    continue
                attempts.append({
                    "prompt": prompt,
                    "content": "",
                    "parsed": None,
                    "model": CODEX_MODEL,
                    "transport_attempts": transport_attempt + 1,
                    "worker_meta": {"ok": False, "error": last_error},
                })
                if idx == 1:
                    raise RuntimeError(last_error)
        if attempts and attempts[-1].get("parsed") and attempts[-1]["parsed"].get("job_page_url", "").strip().lower() != "not found":
            break

    parsed = attempts[-1].get("parsed") if attempts else None
    if parsed:
        return parsed, {"attempts": attempts}
    raise RuntimeError("Codex returned no valid structured response")


def _refresh_run_progress(session: Session, run_id: str) -> dict[str, int]:
    rows = session.execute(
        text("""
            SELECT status, COUNT(*) AS n
            FROM company_enrichment_rows
            WHERE run_id = :run_id
            GROUP BY status
        """),
        {"run_id": run_id},
    ).all()
    by_status = {row.status: int(row.n) for row in rows}
    session.execute(
        text("""
            UPDATE company_enrichment_runs
            SET completed_rows = :completed,
                failed_rows = :failed,
                skipped_rows = :skipped
            WHERE id = :run_id
        """),
        {
            "run_id": run_id,
            "completed": by_status.get("completed", 0),
            "failed": by_status.get("failed", 0),
            "skipped": by_status.get("skipped", 0),
        },
    )
    session.commit()
    return by_status


def _load_cached_result(session: Session, company: str, country: str) -> dict[str, Any] | None:
    row = session.execute(
        text("""
            SELECT
                id,
                run_id,
                job_page_url,
                job_count,
                comment,
                completed_at
            FROM company_enrichment_rows
            WHERE status = 'completed'
              AND lower(trim(company)) = lower(trim(:company))
              AND lower(trim(country)) = lower(trim(:country))
              AND completed_at IS NOT NULL
              AND completed_at >= (now() - make_interval(hours => :cache_ttl_hours))
              AND (job_page_url IS NOT NULL OR job_count IS NOT NULL OR comment IS NOT NULL)
            ORDER BY completed_at DESC
            LIMIT 1
        """),
        {
            "company": company,
            "country": country,
            "cache_ttl_hours": CACHE_TTL_HOURS,
        },
    ).mappings().first()
    return dict(row) if row else None


def _write_output_csv(session: Session, run_id: str) -> str:
    rows = session.execute(
        text("""
            SELECT company, country, job_page_url, job_count, comment, error_message
            FROM company_enrichment_rows
            WHERE run_id = :run_id
            ORDER BY row_number ASC
        """),
        {"run_id": run_id},
    ).mappings().all()
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / f"{run_id}.csv"
    with output_path.open("w", encoding="utf-8", newline="") as f:
        import csv
        writer = csv.writer(f)
        writer.writerow(OUTPUT_COLUMNS)
        for row in rows:
            writer.writerow([
                row["company"],
                row["country"],
                row["job_page_url"] or "not found",
                row["job_count"] or "not found",
                row["comment"] or (row["error_message"] or "not found"),
            ])
    return output_path.name


def _finalize_run_if_complete(session: Session, run_id: str) -> None:
    run = session.execute(
        text("""
            SELECT total_rows, output_filename
            FROM company_enrichment_runs
            WHERE id = :run_id
        """),
        {"run_id": run_id},
    ).mappings().first()
    if not run:
        return

    by_status = _refresh_run_progress(session, run_id)
    terminal = by_status.get("completed", 0) + by_status.get("failed", 0) + by_status.get("skipped", 0)
    processing = by_status.get("processing", 0)
    pending = by_status.get("pending", 0)
    if run["total_rows"] and terminal >= int(run["total_rows"]) and processing == 0 and pending == 0:
        output_filename = run["output_filename"] or _write_output_csv(session, run_id)
        session.execute(
            text("""
                UPDATE company_enrichment_runs
                SET run_status = 'completed',
                    run_completed_at = now(),
                    output_filename = :output_filename
                WHERE id = :run_id
            """),
            {"run_id": run_id, "output_filename": output_filename},
        )
        session.commit()


def _finalize_ready_runs(session: Session) -> None:
    run_ids = session.execute(
        text("""
            SELECT r.id
            FROM company_enrichment_runs r
            WHERE r.run_status = 'running'
              AND NOT EXISTS (
                  SELECT 1
                  FROM company_enrichment_rows cer
                  WHERE cer.run_id = r.id
                    AND cer.status IN ('pending', 'processing')
              )
        """)
    ).scalars().all()
    for run_id in run_ids:
        _finalize_run_if_complete(session, str(run_id))


def _requeue_stale_rows(session: Session) -> None:
    session.execute(
        text("""
            UPDATE company_enrichment_rows
            SET status = 'pending',
                worker_id = NULL,
                started_at = NULL
            WHERE status = 'processing'
              AND started_at IS NOT NULL
              AND started_at < (now() - make_interval(secs => :stale_after))
        """),
        {"stale_after": ROW_STALE_AFTER_SECONDS},
    )
    session.commit()


def claim_rows(session: Session, worker_id: str, limit: int) -> list[dict[str, Any]]:
    if limit <= 0:
        return []
    rows = session.execute(
        text("""
            WITH run_load AS (
                SELECT
                    r.id AS run_id,
                    r.created_at,
                    GREATEST(:per_run_cap - COALESCE(p.processing_n, 0), 0) AS available_slots
                FROM company_enrichment_runs r
                LEFT JOIN (
                    SELECT run_id, COUNT(*) AS processing_n
                    FROM company_enrichment_rows
                    WHERE status = 'processing'
                    GROUP BY run_id
                ) p ON p.run_id = r.id
                WHERE r.run_status = 'running'
            ),
            ranked AS (
                SELECT
                    cer.id,
                    cer.run_id,
                    cer.row_number,
                    rl.created_at,
                    rl.available_slots,
                    ROW_NUMBER() OVER (PARTITION BY cer.run_id ORDER BY cer.row_number ASC) AS rn
                FROM company_enrichment_rows cer
                JOIN run_load rl ON rl.run_id = cer.run_id
                WHERE cer.status = 'pending'
                  AND rl.available_slots > 0
            ),
            candidate AS (
                SELECT cer.id
                FROM company_enrichment_rows cer
                JOIN ranked r ON r.id = cer.id
                WHERE r.rn <= r.available_slots
                ORDER BY r.created_at ASC, r.run_id ASC, r.row_number ASC
                LIMIT :limit
                FOR UPDATE OF cer SKIP LOCKED
            )
            UPDATE company_enrichment_rows cer
            SET status = 'processing',
                started_at = now(),
                completed_at = NULL,
                worker_id = :worker_id,
                attempt_count = COALESCE(cer.attempt_count, 0) + 1
            FROM candidate
            WHERE cer.id = candidate.id
            RETURNING cer.id, cer.run_id, cer.row_number, cer.company, cer.country, cer.attempt_count
        """),
        {
            "worker_id": worker_id,
            "limit": limit,
            "per_run_cap": PER_RUN_MAX_WORKERS,
        },
    ).mappings().all()
    session.commit()
    return [dict(row) for row in rows]


def process_claimed_row(row: dict[str, Any]) -> None:
    row_id = str(row["id"])
    run_id = str(row["run_id"])
    try:
        with SessionLocal() as session:
            cached = _load_cached_result(session, str(row["company"]), str(row["country"]))
            if cached:
                cache_payload = {
                    "cache_hit": True,
                    "source_row_id": str(cached["id"]),
                    "source_run_id": str(cached["run_id"]),
                    "cached_completed_at": cached["completed_at"].isoformat() if cached.get("completed_at") else None,
                }
                session.execute(
                    text("""
                        UPDATE company_enrichment_rows
                        SET status = 'completed',
                            job_page_url = :job_page_url,
                            job_count = :job_count,
                            comment = :comment,
                            raw_response_text = :raw_response_text,
                            raw_response_json = CAST(:raw_response_json AS JSONB),
                            error_message = NULL,
                            completed_at = now(),
                            worker_id = NULL
                        WHERE id = :row_id
                    """),
                    {
                        "row_id": row_id,
                        "job_page_url": cached.get("job_page_url"),
                        "job_count": cached.get("job_count"),
                        "comment": cached.get("comment"),
                        "raw_response_text": "CACHE_HIT",
                        "raw_response_json": json.dumps(cache_payload),
                    },
                )
                session.commit()
                _finalize_run_if_complete(session, run_id)
                return

        result, debug = _execute_row(row["company"], row["country"])
        with SessionLocal() as session:
            session.execute(
                text("""
                    UPDATE company_enrichment_rows
                    SET status = 'completed',
                        job_page_url = :job_page_url,
                        job_count = :job_count,
                        comment = :comment,
                        raw_response_text = :raw_response_text,
                        raw_response_json = CAST(:raw_response_json AS JSONB),
                        error_message = NULL,
                        completed_at = now(),
                        worker_id = NULL
                    WHERE id = :row_id
                """),
                {
                    "row_id": row_id,
                    "job_page_url": result["job_page_url"],
                    "job_count": result["job_count"],
                    "comment": result["comment"],
                    "raw_response_text": "\n\n".join(a.get("content", "") for a in debug.get("attempts", [])),
                    "raw_response_json": json.dumps(debug),
                },
            )
            session.commit()
            _finalize_run_if_complete(session, run_id)
    except Exception as exc:
        with SessionLocal() as session:
            session.execute(
                text("""
                    UPDATE company_enrichment_rows
                    SET status = 'failed',
                        error_message = :error_message,
                        completed_at = now(),
                        worker_id = NULL
                    WHERE id = :row_id
                """),
                {"row_id": row_id, "error_message": str(exc)[:500]},
            )
            session.commit()
            _finalize_run_if_complete(session, run_id)


def main() -> None:
    STORAGE_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    executor = ThreadPoolExecutor(max_workers=max(MAX_INFLIGHT_ROWS, 1))
    active: dict[str, Future[Any]] = {}

    try:
        while True:
            completed = [key for key, fut in active.items() if fut.done()]
            for key in completed:
                try:
                    active[key].result()
                except Exception:
                    pass
                active.pop(key, None)

            with SessionLocal() as session:
                _requeue_stale_rows(session)
                _finalize_ready_runs(session)
                available_slots = max(0, MAX_INFLIGHT_ROWS - len(active))
                claimed = claim_rows(session, WORKER_ID, available_slots)

            for row in claimed:
                active[str(row["id"])] = executor.submit(process_claimed_row, row)

            _write_status(
                "Company enrichment worker active" if active else "Company enrichment worker idle",
                active_workers=len(active),
            )
            time.sleep(POLL_INTERVAL)
    except KeyboardInterrupt:
        _write_status("Company enrichment worker stopped", active_workers=0)
    finally:
        try:
            STATUS_FILE.unlink(missing_ok=True)
        except Exception:
            pass
        executor.shutdown(wait=False)


if __name__ == "__main__":
    main()
