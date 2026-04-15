import uuid
import json
import asyncio
from datetime import datetime, timedelta, timezone
from contextlib import contextmanager
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from app.core.config import settings
from app.api.v1.endpoints.company_enrichment import _build_output_csv, _reconcile_running_runs, _validate_csv
from app.db.base import AsyncSessionLocal
from app.models.company_enrichment_row import CompanyEnrichmentRow
from app.services.company_enrichment_codex import CompanyEnrichmentCodexClient, extract_json_object
from scripts.company_enrichment_worker import _extract_exact_job_count, _fetch_page_content, _refine_result_with_page_data, claim_rows


def test_validate_csv_accepts_company_country_shape():
    content = b"company,country\nAcme,Australia\n"
    result = _validate_csv(content)
    assert result["valid"] is True
    assert result["total_rows"] == 1
    assert result["columns"] == ["company", "country"]


def test_validate_csv_rejects_missing_required_headers():
    content = b"name,country\nAcme,Australia\n"
    result = _validate_csv(content)
    assert result["valid"] is False
    assert "Missing required columns: company" in result["errors"][0]


def test_build_output_csv_uses_legacy_column_order():
    row = CompanyEnrichmentRow(
        id=uuid.uuid4(),
        run_id=uuid.uuid4(),
        row_number=1,
        company="Acme",
        country="Australia",
        status="completed",
        job_page_url="https://acme.com/careers",
        job_count="12",
        comment="Official careers page found",
    )
    csv_text = _build_output_csv([row])
    lines = csv_text.strip().splitlines()
    assert lines[0] == "company,country,job_page_url,job_count,comment"
    assert lines[1] == "Acme,Australia,https://acme.com/careers,12,Official careers page found"


def test_extract_json_object_handles_fenced_output():
    output = 'Here you go\\n```json\\n{"job_page_url":"https://acme.com/jobs","job_count":"3","comment":"Official jobs page"}\\n```'
    assert extract_json_object(output) == '{"job_page_url":"https://acme.com/jobs","job_count":"3","comment":"Official jobs page"}'


def test_worker_health_detects_stale_heartbeat(tmp_path):
    status_file = tmp_path / "worker_status.json"
    status_file.write_text(json.dumps({
        "last_heartbeat": (datetime.now(timezone.utc) - timedelta(seconds=300)).isoformat(),
        "message": "stale worker",
    }))
    client = CompanyEnrichmentCodexClient()
    client.status_file = str(status_file)
    client.stale_after_seconds = 90
    health = client.get_worker_health()
    assert health["alive"] is False
    assert "stale" in health["message"].lower()


def test_worker_health_reports_alive_worker(tmp_path):
    status_file = tmp_path / "worker_status.json"
    status_file.write_text(json.dumps({
        "last_heartbeat": datetime.now(timezone.utc).isoformat(),
        "message": "worker healthy",
        "pid": 123,
    }))
    client = CompanyEnrichmentCodexClient()
    client.status_file = str(status_file)
    client.stale_after_seconds = 90
    health = client.get_worker_health()
    assert health["alive"] is True
    assert health["pid"] == 123


def test_extract_exact_job_count_prefers_visible_job_total():
    page = {
        "html": '<div><h2>76 jobs</h2><p>View all open roles</p></div>',
        "text": "76 jobs View all open roles",
    }
    assert _extract_exact_job_count(page) == "76"


def test_refine_result_with_page_data_overrides_approx_with_exact(monkeypatch):
    monkeypatch.setattr(
        "scripts.company_enrichment_worker._fetch_page_content",
        lambda url: {
            "html": '<script>{"jobCount":76}</script><div>Careers</div>',
            "text": "Careers",
        },
    )
    result = _refine_result_with_page_data({
        "job_page_url": "https://example.com/careers",
        "job_count": "approx 8",
        "comment": "Official careers page",
    })
    assert result["job_count"] == "76"
    assert "Page-extracted count: 76." in result["comment"]


def test_refine_result_with_page_data_upgrades_to_page_estimate_when_better(monkeypatch):
    monkeypatch.setattr(
        "scripts.company_enrichment_worker._fetch_page_content",
        lambda url: {
            "html": '<a href="/jobs/1"></a><a href="/jobs/2"></a><a href="/jobs/3"></a><a href="/jobs/4"></a>'
                    '<a href="/jobs/5"></a><a href="/jobs/6"></a><a href="/jobs/7"></a><a href="/jobs/8"></a>'
                    '<a href="/jobs/9"></a><a href="/jobs/10"></a><a href="/jobs/11"></a><a href="/jobs/12"></a>',
            "text": "Join our team",
        },
    )
    result = _refine_result_with_page_data({
        "job_page_url": "https://example.com/careers",
        "job_count": "approx 8",
        "comment": "Official careers page",
    })
    assert result["job_count"] == "approx 12"
    assert "Page-derived listing estimate suggests about 12 job links" in result["comment"]


def test_refine_result_with_page_data_keeps_approx_when_no_exact_count(monkeypatch):
    monkeypatch.setattr("scripts.company_enrichment_worker._fetch_page_content", lambda url: {"html": "<html></html>", "text": "Join our team"})
    result = _refine_result_with_page_data({
        "job_page_url": "https://example.com/careers",
        "job_count": "approx 8",
        "comment": "Official careers page",
    })
    assert result["job_count"] == "approx 8"
    assert "retaining model estimate" in result["comment"]


def test_fetch_page_content_tolerates_non_utf8_bytes(monkeypatch):
    monkeypatch.setattr(
        "scripts.company_enrichment_worker.subprocess.run",
        lambda *args, **kwargs: SimpleNamespace(returncode=0, stdout=b"\x1f\x8b<html><body>15 jobs</body></html>", stderr=b""),
    )
    page = _fetch_page_content("https://example.com/jobs")
    assert page is not None
    assert "15 jobs" in page["html"]
    assert "15 jobs" in page["text"]


@contextmanager
def _sync_session():
    engine = create_engine(settings.DATABASE_URL_SYNC, future=True)
    session = Session(engine, future=True)
    try:
        yield session
    finally:
        session.close()
        engine.dispose()


def _skip_if_company_enrichment_tables_missing():
    with _sync_session() as session:
        count = session.execute(text("""
            SELECT COUNT(*)
            FROM information_schema.columns
            WHERE table_name = 'company_enrichment_rows'
              AND column_name IN ('status', 'worker_id')
        """)).scalar() or 0
        if count < 2:
            pytest.skip("company enrichment worker migration not applied")


def _cleanup_company_enrichment(run_ids: list[str]) -> None:
    if not run_ids:
        return
    with _sync_session() as session:
        for run_id in run_ids:
            session.execute(text("DELETE FROM company_enrichment_rows WHERE run_id = CAST(:run_id AS UUID)"), {"run_id": run_id})
            session.execute(text("DELETE FROM company_enrichment_runs WHERE id = CAST(:run_id AS UUID)"), {"run_id": run_id})
        session.commit()


def test_claim_rows_never_returns_same_row_twice():
    _skip_if_company_enrichment_tables_missing()
    run_id = str(uuid.uuid4())
    _cleanup_company_enrichment([run_id])
    try:
        with _sync_session() as session:
            session.execute(text("""
                INSERT INTO company_enrichment_runs (id, filename, original_filename, total_rows, validation_status, run_status)
                VALUES (:run_id, 'a.csv', 'a.csv', 3, 'valid', 'running')
            """), {"run_id": run_id})
            session.execute(text("""
                INSERT INTO company_enrichment_rows (id, run_id, row_number, company, country, status, attempt_count)
                VALUES
                  (:id1, :run_id, 1, 'Acme', 'AU', 'pending', 0),
                  (:id2, :run_id, 2, 'Beta', 'AU', 'pending', 0),
                  (:id3, :run_id, 3, 'Gamma', 'AU', 'pending', 0)
            """), {"run_id": run_id, "id1": str(uuid.uuid4()), "id2": str(uuid.uuid4()), "id3": str(uuid.uuid4())})
            session.commit()

            first_claim = claim_rows(session, "worker-a", 2)
            second_claim = claim_rows(session, "worker-b", 2)

        first_ids = {str(row["id"]) for row in first_claim}
        second_ids = {str(row["id"]) for row in second_claim}
        assert first_ids
        assert first_ids.isdisjoint(second_ids)
    finally:
        _cleanup_company_enrichment([run_id])


def test_claim_rows_respects_per_run_cap():
    _skip_if_company_enrichment_tables_missing()
    run_id = str(uuid.uuid4())
    _cleanup_company_enrichment([run_id])
    try:
        with _sync_session() as session:
            session.execute(text("""
                INSERT INTO company_enrichment_runs (id, filename, original_filename, total_rows, validation_status, run_status)
                VALUES (:run_id, 'b.csv', 'b.csv', 4, 'valid', 'running')
            """), {"run_id": run_id})
            session.execute(text("""
                INSERT INTO company_enrichment_rows (id, run_id, row_number, company, country, status, attempt_count)
                VALUES
                  (:id1, :run_id, 1, 'Acme', 'AU', 'processing', 1),
                  (:id2, :run_id, 2, 'Beta', 'AU', 'pending', 0),
                  (:id3, :run_id, 3, 'Gamma', 'AU', 'pending', 0),
                  (:id4, :run_id, 4, 'Delta', 'AU', 'pending', 0)
            """), {
                "run_id": run_id,
                "id1": str(uuid.uuid4()),
                "id2": str(uuid.uuid4()),
                "id3": str(uuid.uuid4()),
                "id4": str(uuid.uuid4()),
            })
            session.commit()

            claimed = claim_rows(session, "worker-c", 5)

        assert len(claimed) == 1
        assert claimed[0]["row_number"] == 2
    finally:
        _cleanup_company_enrichment([run_id])


def test_reconcile_running_runs_keeps_pending_rows_when_worker_stale(monkeypatch):
    _skip_if_company_enrichment_tables_missing()
    run_id = str(uuid.uuid4())
    _cleanup_company_enrichment([run_id])
    try:
        with _sync_session() as session:
            session.execute(text("""
                INSERT INTO company_enrichment_runs (id, filename, original_filename, total_rows, validation_status, run_status, run_started_at)
                VALUES (:run_id, 'stale.csv', 'stale.csv', 2, 'valid', 'running', now() - interval '10 minutes')
            """), {"run_id": run_id})
            session.execute(text("""
                INSERT INTO company_enrichment_rows (id, run_id, row_number, company, country, status, attempt_count)
                VALUES
                  (:id1, :run_id, 1, 'Acme', 'AU', 'pending', 0),
                  (:id2, :run_id, 2, 'Beta', 'AU', 'pending', 0)
            """), {"run_id": run_id, "id1": str(uuid.uuid4()), "id2": str(uuid.uuid4())})
            session.commit()

        monkeypatch.setattr(
            "app.api.v1.endpoints.company_enrichment.CompanyEnrichmentCodexClient.get_worker_health",
            lambda self: {"alive": False, "message": "stale worker"},
        )

        async def _run():
            async with AsyncSessionLocal() as db:
                await _reconcile_running_runs(db, run_id)

        asyncio.run(_run())

        with _sync_session() as session:
            rows = session.execute(text("""
                SELECT status
                FROM company_enrichment_rows
                WHERE run_id = CAST(:run_id AS UUID)
                ORDER BY row_number
            """), {"run_id": run_id}).scalars().all()
            run_error = session.execute(text("""
                SELECT error_message
                FROM company_enrichment_runs
                WHERE id = CAST(:run_id AS UUID)
            """), {"run_id": run_id}).scalar_one()

        assert rows == ["pending", "pending"]
        assert run_error == "Waiting for host enrichment worker heartbeat"
    finally:
        _cleanup_company_enrichment([run_id])
