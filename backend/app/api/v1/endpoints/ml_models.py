"""ML model CRUD and test-run endpoints."""

import logging
import re
from datetime import datetime, timezone

logger = logging.getLogger(__name__)
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.db.base import get_db
from app.models.ml_model import MLModel, MLModelTestRun, CodexImprovementRun

router = APIRouter()


# ---------------------------------------------------------------------------
# Pydantic schemas
# ---------------------------------------------------------------------------

class ModelCreate(BaseModel):
    name: str
    model_type: str
    description: Optional[str] = None
    config: Optional[dict] = None
    status: str = "new"
    is_active: bool = True


class ModelUpdate(BaseModel):
    name: Optional[str] = None
    model_type: Optional[str] = None
    description: Optional[str] = None
    config: Optional[dict] = None
    status: Optional[str] = None
    version: Optional[int] = None
    is_active: Optional[bool] = None


class TestRunCreate(BaseModel):
    test_name: Optional[str] = None
    test_config: Optional[dict] = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _serialize_test_run(tr: MLModelTestRun, *, compact: bool = False) -> dict:
    rd = tr.results_detail or {}
    summary = rd.get("summary", {}) if isinstance(rd, dict) else {}

    # Build column summaries for 3-column display
    baseline_col = None
    model_col = None
    sites = rd.get("sites", []) if isinstance(rd, dict) else []
    if sites:
        total_sites = len(sites)
        # Baseline (test data) column
        b_extracted = sum(1 for s in sites if s.get("baseline", {}).get("jobs", 0) > 0)
        b_total_jobs = sum(s.get("baseline", {}).get("jobs", 0) for s in sites)
        b_core = sum(s.get("baseline", {}).get("fields", {}).get("_core_complete", 0) for s in sites)
        b_fields_total = sum(
            sum(v for k, v in s.get("baseline", {}).get("fields", {}).items() if not k.startswith("_"))
            for s in sites
        )
        baseline_col = {
            "sites_tested": total_sites,
            "sites_extracted": b_extracted,
            "total_jobs": b_total_jobs,
            "core_complete": b_core,
            "quality_score": round(b_fields_total / max(1, b_total_jobs * 6) * 100),
            "stat_sig": _statistical_significance(b_extracted, total_sites),
        }

        # Model (variant) column — sites_extracted = sites that MATCHED baseline (>=90%)
        m_extracted_any = sum(1 for s in sites if s.get("model", {}).get("jobs", 0) > 0)
        m_matched = sum(1 for s in sites if s.get("match") in ("model_equal_or_better", "model_only"))
        m_total_jobs = sum(s.get("model", {}).get("jobs", 0) for s in sites)
        m_core = sum(s.get("model", {}).get("fields", {}).get("_core_complete", 0) for s in sites)
        m_fields_total = sum(
            sum(v for k, v in s.get("model", {}).get("fields", {}).items() if not k.startswith("_"))
            for s in sites
        )
        m_quality_jobs = sum(s.get("model", {}).get("jobs_quality", s.get("model", {}).get("jobs", 0)) for s in sites)
        m_quality_extracted = sum(1 for s in sites if s.get("model", {}).get("jobs_quality", 0) > 0)
        m_quality_warnings = sum(1 for s in sites if s.get("model", {}).get("quality_warning"))
        model_col = {
            "sites_tested": total_sites,
            "sites_extracted": m_matched,  # Only count genuine matches
            "sites_extracted_any": m_extracted_any,  # Sites with any jobs (for reference)
            "sites_extracted_quality": m_matched,
            "total_jobs": m_total_jobs,
            "total_jobs_quality": m_quality_jobs,
            "core_complete": m_core,
            "quality_score": round(m_fields_total / max(1, m_quality_jobs * 6) * 100),
            "quality_warnings": m_quality_warnings,
            "stat_sig": _statistical_significance(m_matched, total_sites),
        }

        # Champion column (if 3-way test)
        champion_col = None
        has_champion = any(s.get("champion") for s in sites)
        if has_champion:
            c_extracted_any = sum(1 for s in sites if (s.get("champion") or {}).get("jobs", 0) > 0)
            # Champion "matched" = sites where champion gets >=90% of baseline
            c_matched = 0
            for s in sites:
                cj = (s.get("champion") or {}).get("jobs_quality", (s.get("champion") or {}).get("jobs", 0))
                cbj = s.get("baseline", {}).get("jobs", 0)
                if cbj > 0 and cj >= cbj * 0.9:
                    c_matched += 1
                elif cbj == 0 and cj > 0:
                    c_matched += 1
            c_total_jobs = sum((s.get("champion") or {}).get("jobs", 0) for s in sites)
            c_core = sum((s.get("champion") or {}).get("fields", {}).get("_core_complete", 0) for s in sites)
            c_fields_total = sum(
                sum(v for k, v in (s.get("champion") or {}).get("fields", {}).items() if not k.startswith("_"))
                for s in sites
            )
            c_quality_jobs = sum((s.get("champion") or {}).get("jobs_quality", (s.get("champion") or {}).get("jobs", 0)) for s in sites)
            c_quality_extracted = sum(1 for s in sites if (s.get("champion") or {}).get("jobs_quality", 0) > 0)
            c_quality_warnings = sum(1 for s in sites if (s.get("champion") or {}).get("quality_warning"))
            champion_col = {
                "sites_tested": total_sites,
                "sites_extracted": c_matched,
                "sites_extracted_any": c_extracted_any,
                "sites_extracted_quality": c_matched,
                "total_jobs": c_total_jobs,
                "total_jobs_quality": c_quality_jobs,
                "core_complete": c_core,
                "quality_score": round(c_fields_total / max(1, c_quality_jobs * 6) * 100),
                "quality_warnings": c_quality_warnings,
                "stat_sig": _statistical_significance(c_matched, total_sites),
            }

    # Get labels from test_config
    test_config = tr.test_config or {}

    return {
        "id": str(tr.id),
        "model_id": str(tr.model_id),
        "test_name": tr.test_name,
        "total_tests": tr.total_tests,
        "tests_passed": tr.tests_passed,
        "tests_failed": tr.tests_failed,
        "accuracy": tr.accuracy,
        "precision_score": tr.precision_score,
        "recall": tr.recall,
        "f1_score": tr.f1_score,
        "test_config": tr.test_config,
        "results_detail": (
            # Compact mode: include only summary (with composite scores), not full site data
            {"summary": (tr.results_detail or {}).get("summary", {})}
            if compact and tr.results_detail
            else None if compact
            else tr.results_detail
        ),
        "status": tr.status,
        "started_at": tr.started_at.isoformat() if tr.started_at else None,
        "completed_at": tr.completed_at.isoformat() if tr.completed_at else None,
        "error_message": tr.error_message,
        "created_at": tr.created_at.isoformat() if tr.created_at else None,
        "baseline_summary": baseline_col,
        "champion_summary": champion_col if 'champion_col' in dir() else None,
        "model_summary": model_col,
        "labels": {
            "baseline": "Test Data",
            "champion": test_config.get("champion"),
            "challenger": test_config.get("challenger", "Challenger"),
        },
    }


def _serialize_model(m: MLModel, *, latest_run: Optional[MLModelTestRun] = None, compact: bool = False) -> dict:
    run_data = _serialize_test_run(latest_run, compact=compact) if latest_run else None
    return {
        "id": str(m.id),
        "name": m.name,
        "model_type": m.model_type,
        "description": m.description,
        "config": m.config,
        "status": m.status,
        "version": m.version,
        "is_active": m.is_active,
        "created_at": m.created_at.isoformat() if m.created_at else None,
        "updated_at": m.updated_at.isoformat() if m.updated_at else None,
        "latest_test_run": run_data,
        # 3-column summaries for table display
        "baseline_summary": run_data["baseline_summary"] if run_data else None,
        "champion_summary": run_data.get("champion_summary") if run_data else None,
        "model_summary": run_data["model_summary"] if run_data else None,
        "labels": run_data.get("labels") if run_data else None,
    }


async def _get_model_or_404(db: AsyncSession, model_id: UUID) -> MLModel:
    model = await db.get(MLModel, model_id)
    if not model:
        raise HTTPException(status_code=404, detail="Model not found")
    return model


async def _get_latest_run(db: AsyncSession, model_id) -> Optional[MLModelTestRun]:
    return await db.scalar(
        select(MLModelTestRun)
        .where(MLModelTestRun.model_id == model_id)
        .order_by(MLModelTestRun.created_at.desc())
        .limit(1)
    )


# ---------------------------------------------------------------------------
# Model CRUD
# ---------------------------------------------------------------------------

@router.get("")
async def list_models(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    search: Optional[str] = None,
    model_type: Optional[str] = None,
    status: Optional[str] = None,
    is_active: Optional[bool] = None,
    db: AsyncSession = Depends(get_db),
):
    q = select(MLModel)
    count_q = select(func.count()).select_from(MLModel)

    if search:
        q = q.where(MLModel.name.ilike(f"%{search}%"))
        count_q = count_q.where(MLModel.name.ilike(f"%{search}%"))
    if model_type:
        q = q.where(MLModel.model_type == model_type)
        count_q = count_q.where(MLModel.model_type == model_type)
    if status:
        if ',' in status:
            status_list = [s.strip() for s in status.split(',') if s.strip()]
            q = q.where(MLModel.status.in_(status_list))
            count_q = count_q.where(MLModel.status.in_(status_list))
        else:
            q = q.where(MLModel.status == status)
            count_q = count_q.where(MLModel.status == status)
    if is_active is not None:
        q = q.where(MLModel.is_active == is_active)
        count_q = count_q.where(MLModel.is_active == is_active)

    total = await db.scalar(count_q) or 0
    models = list(await db.scalars(
        q.order_by(MLModel.created_at.desc())
         .offset((page - 1) * page_size)
         .limit(page_size)
    ))

    # Fetch latest test run for each model in one query
    model_ids = [m.id for m in models]
    latest_runs: dict = {}
    if model_ids:
        from sqlalchemy import distinct
        # Use a lateral-join-style subquery: get max created_at per model, then fetch those runs
        sub = (
            select(
                MLModelTestRun.model_id,
                func.max(MLModelTestRun.created_at).label("max_created"),
            )
            .where(MLModelTestRun.model_id.in_(model_ids))
            .group_by(MLModelTestRun.model_id)
            .subquery()
        )
        runs = list(await db.scalars(
            select(MLModelTestRun).join(
                sub,
                (MLModelTestRun.model_id == sub.c.model_id)
                & (MLModelTestRun.created_at == sub.c.max_created),
            )
        ))
        for r in runs:
            latest_runs[r.model_id] = r

    items = [_serialize_model(m, latest_run=latest_runs.get(m.id), compact=True) for m in models]
    return {"items": items, "total": total, "page": page, "page_size": page_size}


@router.post("", status_code=201)
async def create_model(body: ModelCreate, db: AsyncSession = Depends(get_db)):
    model = MLModel(
        name=body.name,
        model_type=body.model_type,
        description=body.description,
        config=body.config,
        status=body.status,
        is_active=body.is_active,
    )
    db.add(model)
    await db.commit()
    await db.refresh(model)
    return _serialize_model(model)


# ---------------------------------------------------------------------------
# Codex Improvement Runs (must be before /{model_id} catch-all)
# ---------------------------------------------------------------------------

def _serialize_improvement_run(r: CodexImprovementRun) -> dict:
    return {
        "id": str(r.id),
        "source_model_id": str(r.source_model_id) if r.source_model_id else None,
        "test_run_id": str(r.test_run_id) if r.test_run_id else None,
        "output_model_id": str(r.output_model_id) if r.output_model_id else None,
        "status": r.status,
        "description": r.description,
        "source_model_name": r.source_model_name,
        "output_model_name": r.output_model_name,
        "test_winner": r.test_winner,
        "error_message": r.error_message,
        "started_at": r.started_at.isoformat() if r.started_at else None,
        "completed_at": r.completed_at.isoformat() if r.completed_at else None,
        "created_at": r.created_at.isoformat() if r.created_at else None,
    }


class ImprovementRunCreate(BaseModel):
    source_model_id: Optional[str] = None
    test_run_id: Optional[str] = None
    source_model_name: Optional[str] = None
    test_winner: Optional[str] = None
    status: str = "analysing"


class ImprovementRunUpdate(BaseModel):
    output_model_id: Optional[str] = None
    output_model_name: Optional[str] = None
    status: Optional[str] = None
    description: Optional[str] = None
    error_message: Optional[str] = None
    completed_at: Optional[str] = None


@router.get("/improvement-runs")
async def list_improvement_runs(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    status: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    q = select(CodexImprovementRun)
    count_q = select(func.count()).select_from(CodexImprovementRun)

    if status:
        q = q.where(CodexImprovementRun.status == status)
        count_q = count_q.where(CodexImprovementRun.status == status)

    total = await db.scalar(count_q) or 0
    rows = list(await db.scalars(
        q.order_by(CodexImprovementRun.created_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    ))

    return {
        "items": [_serialize_improvement_run(r) for r in rows],
        "total": total,
        "page": page,
        "page_size": page_size,
    }


@router.get("/recent-test-runs")
async def recent_test_runs(
    page_size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    """Return the most recent test runs across ALL models, for the unified timeline."""
    runs = list(await db.scalars(
        select(MLModelTestRun)
        .order_by(MLModelTestRun.created_at.desc())
        .limit(page_size)
    ))
    # Attach model info to each run
    model_ids = list({r.model_id for r in runs})
    models_by_id = {}
    if model_ids:
        models = list(await db.scalars(select(MLModel).where(MLModel.id.in_(model_ids))))
        models_by_id = {m.id: m for m in models}

    items = []
    for r in runs:
        m = models_by_id.get(r.model_id)
        item = _serialize_test_run(r, compact=True)
        item["model_id"] = str(r.model_id)
        item["model_name"] = m.name if m else "Unknown"
        item["model_description"] = m.description if m else None
        item["model_status"] = m.status if m else None
        items.append(item)

    return {"items": items}


@router.post("/improvement-runs")
async def create_improvement_run(
    body: ImprovementRunCreate,
    db: AsyncSession = Depends(get_db),
):
    run = CodexImprovementRun(
        source_model_id=body.source_model_id,
        test_run_id=body.test_run_id,
        source_model_name=body.source_model_name,
        test_winner=body.test_winner,
        status=body.status,
        started_at=datetime.now(timezone.utc),
    )
    db.add(run)
    await db.commit()
    await db.refresh(run)
    return _serialize_improvement_run(run)


@router.patch("/improvement-runs/{run_id}")
async def update_improvement_run(
    run_id: UUID,
    body: ImprovementRunUpdate,
    db: AsyncSession = Depends(get_db),
):
    run = await db.get(CodexImprovementRun, run_id)
    if not run:
        raise HTTPException(status_code=404, detail="Improvement run not found")

    for field in ("output_model_id", "output_model_name", "status", "description", "error_message"):
        val = getattr(body, field, None)
        if val is not None:
            setattr(run, field, val)

    if body.completed_at:
        run.completed_at = datetime.fromisoformat(body.completed_at)
    elif body.status in ("completed", "failed", "skipped") and not run.completed_at:
        run.completed_at = datetime.now(timezone.utc)

    await db.commit()
    await db.refresh(run)
    return _serialize_improvement_run(run)


# ---------------------------------------------------------------------------

@router.get("/{model_id}")
async def get_model(model_id: UUID, db: AsyncSession = Depends(get_db)):
    model = await _get_model_or_404(db, model_id)
    latest_run = await _get_latest_run(db, model_id)
    return _serialize_model(model, latest_run=latest_run)


@router.patch("/{model_id}")
async def update_model(model_id: UUID, body: ModelUpdate, db: AsyncSession = Depends(get_db)):
    model = await _get_model_or_404(db, model_id)
    update_data = body.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(model, field, value)
    await db.commit()
    await db.refresh(model)
    latest_run = await _get_latest_run(db, model_id)
    return _serialize_model(model, latest_run=latest_run)


@router.delete("/{model_id}", status_code=200)
async def delete_model(model_id: UUID, db: AsyncSession = Depends(get_db)):
    model = await _get_model_or_404(db, model_id)
    await db.delete(model)
    await db.commit()
    return {"id": str(model_id), "deleted": True}


@router.post("/{model_id}/auto-improve", status_code=202)
async def trigger_auto_improve(model_id: UUID, db: AsyncSession = Depends(get_db)):
    """Trigger the automated improvement loop for a model.

    Writes a trigger file that the host-side watcher picks up to run
    `codex exec` (which must run on the host, not inside the container).
    """
    import os

    model = await _get_model_or_404(db, model_id)

    # Write trigger file to shared storage (host can read it)
    trigger_dir = "/storage/auto_improve_triggers"
    os.makedirs(trigger_dir, exist_ok=True)
    trigger_file = os.path.join(trigger_dir, f"{model_id}.trigger")
    with open(trigger_file, "w") as f:
        import json
        json.dump({
            "model_id": str(model_id),
            "model_name": model.name,
            "triggered_at": datetime.now(timezone.utc).isoformat(),
        }, f)

    return {"status": "triggered", "model_id": str(model_id), "model_name": model.name,
            "message": "Trigger file written. Run 'python backend/scripts/auto_improve.py --watch' on the host to process."}


@router.get("/auto-improve/health")
async def get_auto_improve_health():
    """Check if the auto-improve daemon is alive."""
    import os as _os
    status_file = "/storage/auto_improve_status.json"
    if not _os.path.exists(status_file):
        return {"alive": False, "message": "Daemon not running", "last_activity": None, "stale": True}
    try:
        with open(status_file) as f:
            status = json.loads(f.read())
        last = datetime.fromisoformat(status.get("last_activity", "2000-01-01"))
        now = datetime.now()
        stale = (now - last).total_seconds() > 120
        status["stale"] = stale
        if stale:
            status["message"] = f"Daemon may be stuck (last activity {int((now - last).total_seconds())}s ago)"
        return status
    except Exception as e:
        return {"alive": False, "message": str(e), "last_activity": None, "stale": True}


@router.get("/auto-improve/activity")
async def get_auto_improve_activity(
    offset: int = Query(0, ge=0),
):
    """Get the latest auto-improve activity log across all models."""
    import os
    import glob

    log_dir = "/storage/auto_improve_logs"
    if not os.path.isdir(log_dir):
        return {"lines": [], "offset": 0, "running": False, "model": None}

    # Find the most recently modified log file
    log_files = glob.glob(os.path.join(log_dir, "*.log"))
    if not log_files:
        return {"lines": [], "offset": 0, "running": False, "model": None}

    latest = max(log_files, key=os.path.getmtime)
    model_id = os.path.basename(latest).replace(".log", "")

    with open(latest, "r") as f:
        all_lines = f.readlines()

    new_lines = all_lines[offset:]

    # Determine if auto-improve is actually running:
    # 1. Check if the log indicates completion/exit
    log_says_done = any(
        ("exited with code" in line or "Auto-improve stopped" in line)
        for line in all_lines[-5:]
    ) if all_lines else False

    # 2. Check if actual processes exist (ground truth)
    import subprocess
    try:
        ps_result = subprocess.run(
            ["pgrep", "-f", "auto_improve_daemon|codex exec"],
            capture_output=True, timeout=3,
        )
        process_alive = ps_result.returncode == 0
    except Exception:
        process_alive = False

    # 3. Check if log file is stale (no writes in 60s = likely dead)
    import time
    log_age = time.time() - os.path.getmtime(latest)
    log_stale = log_age > 60

    # 4. Check daemon status file (written by host-side daemon every 30s)
    #    This is the most reliable signal since pgrep can't see host processes from Docker.
    #    Use file mtime (OS-level, timezone-safe) rather than parsing timestamps.
    daemon_alive = False
    status_file = "/storage/auto_improve_status.json"
    if os.path.exists(status_file):
        try:
            status_age = time.time() - os.path.getmtime(status_file)
            if status_age < 120:  # File modified within 2 minutes = daemon alive
                import json as _json
                with open(status_file) as sf:
                    status = _json.load(sf)
                daemon_alive = status.get("alive", False)
        except Exception:
            pass

    # Process check runs inside Docker and can't see host processes,
    # so also treat a fresh (recently written) log as "running"
    log_is_fresh = not log_stale  # modified within last 60s
    # Daemon alive (from status file) takes priority over log_says_done,
    # because "exited with code" in the log just means a previous Codex run
    # finished — the daemon itself may still be running and waiting.
    running = daemon_alive or ((process_alive or log_is_fresh) and not log_says_done)

    # Also check supervisor log for recent health check messages
    supervisor_lines = []
    supervisor_log = "/tmp/auto_improve_supervisor.log"
    if os.path.exists(supervisor_log):
        try:
            with open(supervisor_log, "r") as f:
                sup_lines = f.readlines()
            # Only include actionable supervisor messages (not routine checks)
            for l in sup_lines[-20:]:
                stripped = l.rstrip("\n")
                if any(kw in stripped for kw in ["⚠️", "✅", "❌", "📝", "🚀"]):
                    if stripped not in [nl.rstrip("\n") for nl in all_lines[-50:]]:
                        supervisor_lines.append(f"[supervisor] {stripped}")
        except Exception:
            pass

    combined = [l.rstrip("\n") for l in new_lines] + supervisor_lines

    return {
        "lines": combined,
        "offset": len(all_lines),
        "running": running,
        "model": model_id,
        "log_file": os.path.basename(latest),
    }


@router.post("/auto-improve/stop")
async def stop_auto_improve():
    """Stop all running auto-improve processes (daemon, codex exec)."""
    import subprocess, glob
    killed = []
    for pattern in ["auto_improve_daemon", "auto_improve_supervisor", "codex exec"]:
        try:
            result = subprocess.run(
                ["pkill", "-9", "-f", pattern],
                capture_output=True, text=True, timeout=5,
            )
            if result.returncode == 0:
                killed.append(pattern)
        except Exception:
            pass

    # Write stop marker to the latest log file so the UI sees it immediately
    log_dir = "/storage/auto_improve_logs"
    if os.path.isdir(log_dir):
        log_files = glob.glob(os.path.join(log_dir, "*.log"))
        if log_files:
            latest = max(log_files, key=os.path.getmtime)
            with open(latest, "a") as f:
                f.write(f"\n[{datetime.now(timezone.utc).strftime('%H:%M:%S')}] Auto-improve stopped by user\n")

    return {"stopped": killed, "message": f"Stopped {len(killed)} process(es)"}


@router.post("/auto-improve/start")
async def start_auto_improve(db: AsyncSession = Depends(get_db)):
    """Start the auto-improve loop by finding the latest tested model and triggering improvement."""
    # Find the most recently tested model
    result = await db.execute(
        select(MLModel)
        .where(MLModel.status.in_(["tested", "live"]))
        .order_by(MLModel.updated_at.desc())
        .limit(1)
    )
    model = result.scalar_one_or_none()
    if not model:
        return {"started": False, "message": "No tested/live model found"}

    # Write trigger file
    import os as _os
    trigger_dir = "/storage/auto_improve_triggers"
    _os.makedirs(trigger_dir, exist_ok=True)
    trigger_path = _os.path.join(trigger_dir, f"{model.id}.trigger")
    with open(trigger_path, "w") as f:
        f.write(f'{{"model_id": "{model.id}", "model_name": "{model.name}", "auto_improve": true}}')

    return {"started": True, "model_id": str(model.id), "model_name": model.name}


@router.get("/{model_id}/auto-improve/log")
async def get_auto_improve_log(
    model_id: UUID,
    offset: int = Query(0, ge=0),
    db: AsyncSession = Depends(get_db),
):
    """Stream the auto-improve Codex log for a model. Returns new lines since offset."""
    import os
    log_path = f"/storage/auto_improve_logs/{model_id}.log"
    if not os.path.exists(log_path):
        return {"lines": [], "offset": 0, "running": False}

    with open(log_path, "r") as f:
        all_lines = f.readlines()

    new_lines = all_lines[offset:]
    # Check if still running (no "exited with code" in last line)
    running = True
    if all_lines and "exited with code" in all_lines[-1]:
        running = False

    return {
        "lines": [l.rstrip("\n") for l in new_lines],
        "offset": len(all_lines),
        "running": running,
    }


@router.post("/{model_id}/promote", status_code=200)
async def promote_model(model_id: UUID, db: AsyncSession = Depends(get_db)):
    """Promote a model to 'live' status. Demotes any existing live model of the same type."""
    model = await _get_model_or_404(db, model_id)

    # Demote any existing live model of the same type
    existing_live = list(await db.scalars(
        select(MLModel).where(
            MLModel.model_type == model.model_type,
            MLModel.status == "live",
            MLModel.id != model_id,
        )
    ))
    for m in existing_live:
        m.status = "tested" if m.status == "live" else m.status

    model.status = "live"
    await db.commit()
    await db.refresh(model)
    latest_run = await _get_latest_run(db, model_id)
    return _serialize_model(model, latest_run=latest_run)


# ---------------------------------------------------------------------------
# Test Run endpoints
# ---------------------------------------------------------------------------

@router.get("/{model_id}/test-runs")
async def list_test_runs(
    model_id: UUID,
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    db: AsyncSession = Depends(get_db),
):
    await _get_model_or_404(db, model_id)

    count_q = select(func.count()).select_from(MLModelTestRun).where(MLModelTestRun.model_id == model_id)
    total = await db.scalar(count_q) or 0

    runs = list(await db.scalars(
        select(MLModelTestRun)
        .where(MLModelTestRun.model_id == model_id)
        .order_by(MLModelTestRun.created_at.desc())
        .offset((page - 1) * page_size)
        .limit(page_size)
    ))
    items = [_serialize_test_run(r) for r in runs]
    return {"items": items, "total": total, "page": page, "page_size": page_size}


@router.post("/{model_id}/test-runs", status_code=201)
async def create_test_run(model_id: UUID, body: TestRunCreate, db: AsyncSession = Depends(get_db)):
    await _get_model_or_404(db, model_id)
    run = MLModelTestRun(
        model_id=model_id,
        test_name=body.test_name,
        test_config=body.test_config,
        status="pending",
        started_at=datetime.now(timezone.utc),
    )
    db.add(run)
    await db.commit()
    await db.refresh(run)
    return _serialize_test_run(run)


@router.get("/{model_id}/test-runs/{run_id}")
async def get_test_run(model_id: UUID, run_id: UUID, db: AsyncSession = Depends(get_db)):
    await _get_model_or_404(db, model_id)
    run = await db.get(MLModelTestRun, run_id)
    if not run or run.model_id != model_id:
        raise HTTPException(status_code=404, detail="Test run not found")
    return _serialize_test_run(run)


import math

CORE_FIELDS = ["title", "source_url", "location_raw"]
ALL_FIELDS = ["title", "source_url", "location_raw", "salary_raw", "employment_type", "description"]
DISPLAY_FIELDS = [
    "title", "source_url", "location_raw", "salary_raw", "employment_type",
    "description", "closing_date", "listed_date", "department",
    "extraction_method", "extraction_confidence", "has_detail_page",
]


def _composite_score_standalone(results: list[dict], phase_key: str) -> dict:
    """Compute composite score for a model phase. Standalone version for use by Celery tasks."""
    total = len(results)
    if total == 0:
        return {"composite": 0, "discovery": 0, "quality_extraction": 0,
                "field_completeness": 0, "volume_accuracy": 0}

    discovered = sum(
        1 for s in results
        if (s.get(phase_key) or {}).get("url_found")
        and not ((s.get(phase_key) or {}).get("error") or "").startswith("Could not discover")
    )
    discovery_rate = discovered / total * 100

    quality_extracted = sum(
        1 for s in results
        if (s.get(phase_key) or {}).get("jobs_quality",
           (s.get(phase_key) or {}).get("jobs", 0)) > 0
    )
    quality_warnings = sum(
        1 for s in results
        if (s.get(phase_key) or {}).get("quality_warning")
    )
    quality_extraction_rate = max(0, (quality_extracted - quality_warnings) / max(1, total) * 100)

    total_jobs_quality = sum(
        (s.get(phase_key) or {}).get("jobs_quality",
         (s.get(phase_key) or {}).get("jobs", 0))
        for s in results
    )
    total_fields = sum(
        sum(v for k, v in (s.get(phase_key) or {}).get("fields", {}).items()
            if not k.startswith("_"))
        for s in results
    )
    field_completeness = total_fields / max(1, total_jobs_quality * 6) * 100

    baseline_total = sum(s.get("baseline", {}).get("jobs", 0) for s in results)
    model_total = sum(
        (s.get(phase_key) or {}).get("jobs_quality",
         (s.get(phase_key) or {}).get("jobs", 0))
        for s in results
    )
    if baseline_total > 0:
        ratio = model_total / baseline_total
        if ratio >= 1.0:
            volume_accuracy = max(0, min(100, 100 - max(0, (ratio - 1.5) * 100)))
        else:
            volume_accuracy = max(0, 100 * ratio)
    else:
        volume_accuracy = 50 if model_total > 0 else 0

    composite = (
        0.20 * discovery_rate
        + 0.30 * quality_extraction_rate
        + 0.25 * field_completeness
        + 0.25 * volume_accuracy
    )

    return {
        "composite": round(composite, 1),
        "discovery": round(discovery_rate, 1),
        "quality_extraction": round(quality_extraction_rate, 1),
        "field_completeness": round(field_completeness, 1),
        "volume_accuracy": round(volume_accuracy, 1),
    }


def _truncate_jobs(jobs: list[dict], max_jobs: int = 50) -> list[dict]:
    """Truncate job list for storage — keep all fields but cap description length."""
    result = []
    for j in jobs[:max_jobs]:
        entry = {}
        for field in DISPLAY_FIELDS:
            val = j.get(field)
            if val is not None:
                if field == "description" and isinstance(val, str) and len(val) > 500:
                    entry[field] = val[:500] + "..."
                else:
                    entry[field] = val
        result.append(entry)
    return result


def _statistical_significance(successes: int, total: int) -> dict:
    """Compute confidence interval and significance for a binomial proportion.

    Uses Wilson score interval — accurate for small samples unlike normal approx.
    Returns: {"p": proportion, "ci_low": float, "ci_high": float,
              "margin_of_error": float, "significance": str}
    """
    if total == 0:
        return {"p": 0, "ci_low": 0, "ci_high": 0, "margin_of_error": 0, "significance": "insufficient_data"}

    p = successes / total
    z = 1.96  # 95% confidence

    # Wilson score interval
    denom = 1 + z * z / total
    center = (p + z * z / (2 * total)) / denom
    spread = z * math.sqrt((p * (1 - p) + z * z / (4 * total)) / total) / denom

    ci_low = max(0, center - spread)
    ci_high = min(1, center + spread)
    margin = ci_high - ci_low

    # Significance assessment
    if total < 10:
        sig = "too_small"
    elif total < 30:
        sig = "indicative"
    elif margin <= 0.15:
        sig = "significant"
    elif margin <= 0.25:
        sig = "moderate"
    else:
        sig = "low"

    return {
        "p": round(p, 3),
        "ci_low": round(ci_low, 3),
        "ci_high": round(ci_high, 3),
        "margin_of_error": round(margin, 3),
        "significance": sig,
    }


def _field_coverage(jobs: list[dict]) -> dict:
    """For a list of job dicts, count how many have each field populated."""
    total = len(jobs)
    result = {"_total": total}
    for f in ALL_FIELDS:
        result[f] = sum(1 for j in jobs if j.get(f))
    result["_core_complete"] = sum(
        1 for j in jobs if all(j.get(f) for f in CORE_FIELDS)
    )
    return result


async def _execute_baseline_with_steps(url: str, known: dict, client) -> str:
    """Execute baseline crawl following the production crawl steps from test data.

    Looks up the crawl steps for this site's crawler and follows them:
    - url_opener: open the URL
    - sleeper: wait N seconds (use Playwright for JS rendering)
    - link_navigator: click elements (cookie banners, tabs, load-more)
    - frame_switcher: switch into iframes
    - form_locator/form_submitter: submit search forms

    Two-pass approach:
    1. Try plain HTTP first. If wrapper selectors match and extract jobs, use that.
    2. If HTTP yields 0 jobs or site has Playwright steps, use Playwright rendering.
    """
    import asyncio

    # Look up crawl steps for this URL's crawler
    steps = []
    try:
        from sqlalchemy import text as _sa_text
        from app.db.base import AsyncSessionLocal
        async with AsyncSessionLocal() as step_db:
            result = await step_db.execute(_sa_text("""
                SELECT cs.step_name, cs.step_index, cs.options
                FROM crawl_steps_test_data cs
                JOIN crawler_test_data ct ON ct.external_id = cs.crawler_id
                JOIN site_url_test_data su ON su.site_id = ct.job_site_id
                WHERE su.url = :url
                ORDER BY cs.step_index
            """), {"url": url})
            steps = [{"name": r[0], "index": r[1], "options": r[2] or {}} for r in result.fetchall()]
    except Exception:
        pass

    needs_playwright = any(
        s["name"] in ("sleeper", "link_navigator", "frame_switcher", "form_locator", "form_submitter")
        for s in steps
    )

    # ── Pass 1: Try plain HTTP (fast, works for ~65% of sites) ──
    plain_html = ""
    try:
        resp = await client.get(url)
        plain_html = resp.text or ""
    except Exception:
        pass

    if plain_html and len(plain_html) >= 200 and not needs_playwright:
        # Quick check: do the wrapper selectors actually match anything?
        boundary = known.get("record_boundary_path", "")
        if boundary:
            from app.crawlers.job_extractor import JobExtractor
            test_jobs = JobExtractor._static_extract_wrapper(plain_html, url, known)
            if test_jobs:
                return plain_html  # Selectors work on static HTML — use it

    # ── Pass 2: Playwright rendering (handles JS sites + step execution) ──
    try:
        from playwright.async_api import async_playwright
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            pw_page = await browser.new_page(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            )
            try:
                await pw_page.goto(url, wait_until="networkidle", timeout=25000)

                if needs_playwright:
                    # Group steps by step_index — same index = alternatives/parallel
                    from collections import defaultdict
                    step_groups: dict[int, list] = defaultdict(list)
                    for step in steps:
                        step_groups[step["index"]].append(step)

                    for idx in sorted(step_groups.keys()):
                        group = step_groups[idx]
                        for step in group:
                            name = step["name"]
                            opts = step["options"] if isinstance(step["options"], dict) else {}

                            if name == "url_opener":
                                # Already opened — skip unless it's a different URL
                                opener_url = opts.get("url", "")
                                if opener_url and opener_url != url:
                                    try:
                                        await pw_page.goto(opener_url, wait_until="networkidle", timeout=20000)
                                    except Exception:
                                        pass

                            elif name == "sleeper":
                                secs = 5
                                try:
                                    secs = int(opts.get("seconds", 5))
                                except (ValueError, TypeError):
                                    secs = 5
                                secs = min(secs, 15)
                                await pw_page.wait_for_timeout(secs * 1000)

                            elif name == "link_navigator":
                                selector = opts.get("selector", "")
                                if selector:
                                    try:
                                        if selector.startswith("//") or selector.startswith(".//") or selector.startswith("/html"):
                                            el = pw_page.locator(f"xpath={selector}").first
                                        else:
                                            el = pw_page.locator(selector).first
                                        if await el.is_visible(timeout=3000):
                                            await el.click()
                                            await pw_page.wait_for_timeout(2000)
                                    except Exception:
                                        pass

                            elif name == "frame_switcher":
                                frame_path = opts.get("frame_path", "")
                                if frame_path:
                                    try:
                                        if frame_path.startswith("//") or frame_path.startswith("/html"):
                                            frame_el = pw_page.locator(f"xpath={frame_path}").first
                                        else:
                                            frame_el = pw_page.locator(frame_path).first
                                        frame = await frame_el.content_frame()
                                        if frame:
                                            pw_page = frame
                                    except Exception:
                                        pass

                            elif name == "form_locator":
                                # Locate and focus the form element (using opts selector)
                                form_path = opts.get("form_path", "")
                                if form_path:
                                    try:
                                        if form_path.startswith("//"):
                                            el = pw_page.locator(f"xpath={form_path}").first
                                        else:
                                            el = pw_page.locator(form_path).first
                                        if await el.is_visible(timeout=3000):
                                            await el.click()
                                            await pw_page.wait_for_timeout(1000)
                                    except Exception:
                                        pass

                            elif name == "form_submitter":
                                submit_sel = opts.get("submit_button_selector", "")
                                try:
                                    if submit_sel:
                                        if submit_sel.startswith("//"):
                                            btn = pw_page.locator(f"xpath={submit_sel}").first
                                        else:
                                            btn = pw_page.locator(submit_sel).first
                                    else:
                                        btn = pw_page.locator("button[type=submit], input[type=submit], button:has-text('Search'), button:has-text('Go')").first
                                    if await btn.is_visible(timeout=3000):
                                        await btn.click()
                                        await pw_page.wait_for_timeout(3000)
                                except Exception:
                                    pass
                else:
                    # No explicit steps but HTTP didn't work — just wait for JS render
                    await pw_page.wait_for_timeout(5000)

                    # Try cookie dismissal
                    for sel in ["button:has-text('Accept')", "button:has-text('OK')", "[class*=consent] button", "button:has-text('Agree')"]:
                        try:
                            btn = pw_page.locator(sel).first
                            if await btn.is_visible(timeout=1000):
                                await btn.click()
                                await pw_page.wait_for_timeout(500)
                                break
                        except Exception:
                            pass

                html = await pw_page.content()
                return html if html and len(html) > 200 else plain_html

            except Exception:
                try:
                    return await pw_page.content()
                except Exception:
                    return plain_html
            finally:
                await browser.close()

    except Exception:
        return plain_html


_JOB_NOUNS_FOR_VALIDATION = {
    "accountant", "administrator", "advisor", "analyst", "apprentice", "architect",
    "assistant", "associate", "auditor", "barista", "bookkeeper", "broker", "builder",
    "buyer", "carpenter", "cashier", "carer", "chef", "cleaner", "clerk", "coach",
    "consultant", "coordinator", "counsellor", "designer", "developer", "director",
    "dispatcher", "doctor", "driver", "editor", "electrician", "engineer", "estimator",
    "executive", "facilitator", "fitter", "foreman", "guard", "handler", "helper",
    "inspector", "instructor", "intern", "investigator", "labourer", "lawyer",
    "lecturer", "librarian", "machinist", "manager", "mechanic", "merchandiser",
    "midwife", "miner", "nurse", "officer", "operator", "optometrist", "painter",
    "paralegal", "paramedic", "pharmacist", "photographer", "physiotherapist", "pilot",
    "planner", "plumber", "porter", "president", "principal", "processor", "producer",
    "programmer", "receptionist", "recruiter", "registrar", "representative", "researcher",
    "scaffolder", "scientist", "secretary", "solicitor", "specialist", "strategist",
    "superintendent", "supervisor", "surgeon", "surveyor", "teacher", "technician",
    "technologist", "therapist", "trader", "trainer", "treasurer", "tutor",
    "underwriter", "veterinarian", "waiter", "welder", "worker", "writer",
}

_NOT_JOB_TITLES = {
    "about us", "contact", "home", "menu", "search", "login", "sign up", "register",
    "our team", "meet the team", "leadership", "management", "our culture",
    "latest news", "blog", "news", "events", "press", "media",
    "our services", "products", "solutions", "partners", "clients",
    "privacy", "terms", "cookie", "disclaimer", "sitemap",
    "working at", "life at", "why join", "our values", "benefits",
    "view more", "read more", "learn more", "see all", "load more",
}


def _count_real_jobs(jobs: list[dict]) -> int:
    """Count how many extracted items look like real job listings (not nav/blog/noise)."""
    real = 0
    for j in jobs:
        title = (j.get("title") or "").lower().strip()
        if not title or len(title) < 5:
            continue
        # Check if title IS a known non-job
        if any(nj in title for nj in _NOT_JOB_TITLES):
            continue
        # Check if title contains at least one job noun
        words = set(re.split(r'[\s/,\-–—()]+', title))
        has_job_noun = bool(words & _JOB_NOUNS_FOR_VALIDATION)
        # Also accept if URL looks like a job page
        url = (j.get("source_url") or "").lower()
        has_job_url = bool(re.search(r"/(?:job|position|vacancy|career|opening|role|apply)", url))
        if has_job_noun or has_job_url:
            real += 1
    return real


def _parse_html_safe(html: str):
    """Parse HTML with lxml, return root or None."""
    try:
        from lxml import etree
        parser = etree.HTMLParser(encoding="utf-8")
        return etree.fromstring(html.encode("utf-8", errors="replace"), parser)
    except Exception:
        return None


def _find_next_url(root, base_url: str, selector: str) -> Optional[str]:
    """Find next page URL using a CSS or XPath selector."""
    from urllib.parse import urljoin
    try:
        is_xpath = selector.startswith("//") or selector.startswith(".//")
        els = root.xpath(selector) if is_xpath else root.cssselect(selector)
        if els:
            href = els[0].get("href")
            if href and href != "#" and not href.startswith("javascript:"):
                return urljoin(base_url, href)
    except Exception:
        pass
    return None


class ExecuteTestBody(BaseModel):
    sample_size: int = 50
    champion_model_id: Optional[str] = None
    auto_improve: bool = False
    use_fixed_set: bool = True  # Phase 1: regression suite (fixed sites)
    include_exploration: bool = True  # Phase 2: also test on random unseen sites


@router.post("/{model_id}/test-runs/execute", status_code=202)
async def execute_test_run(
    model_id: UUID,
    body: ExecuteTestBody,
    db: AsyncSession = Depends(get_db),
):
    """Execute an A/B comparison test of the model against known-good test data.

    For each site:
      Phase A (Baseline): Apply the known wrapper selectors from test data to extract jobs.
      Phase B (Model):    Give the model only the URL. It creates its own site config and extracts jobs.
      Compare:            Job count, field coverage, and quality between A and B.
    """
    import asyncio
    import httpx
    from app.crawlers.tiered_extractor import TieredExtractor
    from app.crawlers.job_extractor import JobExtractor

    model = await _get_model_or_404(db, model_id)

    model_name = model.name or ""
    sample_size = min(body.sample_size, 200)

    # Resolve champion model (for 3-way test)
    champion_cls = None
    champion_name = None
    if body.champion_model_id:
        champ = await db.get(MLModel, UUID(body.champion_model_id))
        if champ:
            champion_name = champ.name
    else:
        # Auto-detect live model as champion
        champ = await db.scalar(
            select(MLModel).where(
                MLModel.model_type == model.model_type,
                MLModel.status == "live",
                MLModel.id != model_id,
            )
        )
        if champ:
            champion_name = champ.name

    def _pick_extractor(name: str):
        """Select extractor by model name. Dynamic import to support Codex-created versions."""
        import importlib, re
        # Extract version like "v1.6" or "v2.1" from model name
        match = re.search(r"v(\d+)\.(\d+)", name)
        if match:
            major, minor = int(match.group(1)), int(match.group(2))
            # Map model version to file version (v1.6 → v16, v2.1 → v21)
            file_ver = major * 10 + minor
            module_name = f"app.crawlers.tiered_extractor_v{file_ver}"
            class_name = f"TieredExtractorV{file_ver}"
            try:
                mod = importlib.import_module(module_name)
                return getattr(mod, class_name)
            except (ImportError, AttributeError):
                pass
        return TieredExtractor

    # Finder version mapping: model version → finder file version.
    # Post-2026-04-14 reset — only v69 (champion), v60 (reference), and stable bases
    # remain. Next challenger versions will add themselves via _FINDER_MAP updates
    # when their extractor + finder files are created.
    _FINDER_MAP = {
        69: 69,  # v6.9 champion
        60: 60,  # v6.0 consolidated reference
        20: 20, 17: 5, 16: 4, 15: 3, 14: 2, 13: 2, 12: 2,
    }

    def _pick_finder(name: str):
        """Select career page finder by model name. Dynamic import."""
        import importlib, re
        match = re.search(r"v(\d+)\.(\d+)", name)
        if match:
            major, minor = int(match.group(1)), int(match.group(2))
            file_ver = major * 10 + minor
            # Check explicit mapping first, then try direct version match
            finder_ver = _FINDER_MAP.get(file_ver, file_ver)
            module_name = f"app.crawlers.career_page_finder_v{finder_ver}"
            class_name = f"CareerPageFinderV{finder_ver}"
            try:
                mod = importlib.import_module(module_name)
                return getattr(mod, class_name)
            except (ImportError, AttributeError):
                pass
        from app.crawlers.career_page_finder import CareerPageFinder
        return CareerPageFinder

    extractor_cls = _pick_extractor(model_name)

    if champion_name:
        champion_cls = _pick_extractor(champion_name)

    from sqlalchemy import text as sa_text
    pages_q = sa_text("""
        SELECT su.url, js.name as company_name, sw.selectors as known_selectors
        FROM site_url_test_data su
        JOIN job_site_test_data js ON js.external_id = su.site_id
        JOIN crawler_test_data ct ON ct.job_site_id = su.site_id
        JOIN site_wrapper_test_data sw ON sw.crawler_id = ct.external_id
        WHERE su.url LIKE 'http%'
          AND su.url NOT LIKE 'file://%'
          AND js.site_type IN ('employer', 'recruiter')
          AND (js.uncrawlable_reason IS NULL OR js.uncrawlable_reason IN ('', 'null'))
        ORDER BY random()
        LIMIT :limit
    """)
    # ── Site selection: ALWAYS use the same fixed test set ──
    # Consistent comparison requires testing every model on the SAME sites.
    # No adaptive sizing, no exploration — just the fixed set, deterministically ordered.
    import random as _rng

    all_fixed = []
    try:
        fixed_q = sa_text("SELECT url, company_name, known_selectors FROM fixed_test_sites ORDER BY md5(url)")
        fixed_result = await db.execute(fixed_q)
        all_fixed = fixed_result.fetchall()
    except Exception:
        pass

    if all_fixed:
        # Use consistent 50 sites from the fixed pool (deterministic order via md5)
        pages = all_fixed[:min(sample_size, len(all_fixed))]
    else:
        # Fallback: random from test data
        result = await db.execute(pages_q, {"limit": sample_size})
        pages = result.fetchall()

    if not pages:
        raise HTTPException(status_code=400, detail="No test data sites available")
    fixed_count = len(pages)
    explore_count = 0

    challenger_label = model.name or "Challenger"
    champion_label = champion_name or None

    auto_improve = body.auto_improve

    run = MLModelTestRun(
        model_id=model_id,
        test_name=f"Test x {len(pages)} fixed sites",
        test_config={
            "sample_size": sample_size,
            "page_count": len(pages),
            "fixed_count": fixed_count,
            "explore_count": explore_count,
            "challenger": challenger_label,
            "champion": champion_label,
            "auto_improve": auto_improve,
        },
        status="running",
        started_at=datetime.now(timezone.utc),
        total_tests=len(pages),
    )
    db.add(run)
    await db.commit()
    await db.refresh(run)
    run_id = run.id

    # Serialize pages for Celery (Row objects aren't JSON-serializable)
    pages_serialized = []
    for row in pages:
        sel = row[2]
        if isinstance(sel, str):
            pages_serialized.append([row[0], row[1], sel])
        elif isinstance(sel, dict):
            import json as _json
            pages_serialized.append([row[0], row[1], _json.dumps(sel)])
        else:
            pages_serialized.append([row[0], row[1], str(sel) if sel else "{}"])

    # Fan out: one Celery task per site, then aggregate (parallel across workers)
    from app.tasks.ml_tasks import execute_model_test
    execute_model_test(
        run_id=str(run_id),
        model_id=str(model_id),
        model_name=model_name or "",
        champion_name=champion_name,
        pages_data=pages_serialized,
        fixed_count=fixed_count,
        auto_improve=auto_improve,
    )
    return _serialize_test_run(run)


# ---------------------------------------------------------------------------
# Feedback CRUD
# ---------------------------------------------------------------------------


class FeedbackCreate(BaseModel):
    site_url: str
    comment: str


class FeedbackUpdate(BaseModel):
    comment: str


@router.get("/{model_id}/test-runs/{run_id}/feedback")
async def list_feedback(
    model_id: UUID, run_id: UUID, site_url: Optional[str] = None,
    db: AsyncSession = Depends(get_db),
):
    """List feedback for a test run, optionally filtered by site URL."""
    from app.models.ml_model import MLTestFeedback
    q = select(MLTestFeedback).where(MLTestFeedback.test_run_id == run_id)
    if site_url:
        q = q.where(MLTestFeedback.site_url == site_url)
    q = q.order_by(MLTestFeedback.created_at)
    items = list(await db.scalars(q))
    return [
        {
            "id": str(f.id), "site_url": f.site_url, "comment": f.comment,
            "screenshot_path": f.screenshot_path,
            "screenshots": [p.strip() for p in (f.screenshot_path or "").split(",") if p.strip()],
            "created_at": f.created_at.isoformat() if f.created_at else None,
        }
        for f in items
    ]


@router.post("/{model_id}/test-runs/{run_id}/feedback", status_code=201)
async def create_feedback(
    model_id: UUID, run_id: UUID, body: FeedbackCreate,
    db: AsyncSession = Depends(get_db),
):
    from app.models.ml_model import MLTestFeedback
    fb = MLTestFeedback(test_run_id=run_id, site_url=body.site_url, comment=body.comment)
    db.add(fb)
    await db.commit()
    await db.refresh(fb)
    return {"id": str(fb.id), "site_url": fb.site_url, "comment": fb.comment, "screenshot_path": fb.screenshot_path}


@router.patch("/{model_id}/test-runs/{run_id}/feedback/{feedback_id}")
async def update_feedback(
    model_id: UUID, run_id: UUID, feedback_id: UUID, body: FeedbackUpdate,
    db: AsyncSession = Depends(get_db),
):
    from app.models.ml_model import MLTestFeedback
    fb = await db.get(MLTestFeedback, feedback_id)
    if not fb or fb.test_run_id != run_id:
        raise HTTPException(status_code=404, detail="Feedback not found")
    fb.comment = body.comment
    await db.commit()
    return {"id": str(fb.id), "comment": fb.comment}


@router.delete("/{model_id}/test-runs/{run_id}/feedback/{feedback_id}")
async def delete_feedback(
    model_id: UUID, run_id: UUID, feedback_id: UUID,
    db: AsyncSession = Depends(get_db),
):
    from app.models.ml_model import MLTestFeedback
    fb = await db.get(MLTestFeedback, feedback_id)
    if not fb or fb.test_run_id != run_id:
        raise HTTPException(status_code=404, detail="Feedback not found")
    await db.delete(fb)
    await db.commit()
    return {"deleted": True}


@router.post("/{model_id}/test-runs/{run_id}/feedback/{feedback_id}/screenshot")
async def upload_screenshot(
    model_id: UUID, run_id: UUID, feedback_id: UUID,
    db: AsyncSession = Depends(get_db),
    file: bytes = None,
):
    """Upload a screenshot image for a feedback entry. Accepts multipart/form-data."""
    from fastapi import UploadFile, File as FastAPIFile
    # This endpoint is handled below with proper file upload
    pass


# Separate file upload endpoint using UploadFile
from fastapi import UploadFile, File as FastAPIFile


@router.post("/{model_id}/test-runs/{run_id}/feedback/{feedback_id}/upload")
async def upload_feedback_screenshot(
    model_id: UUID, run_id: UUID, feedback_id: UUID,
    file: UploadFile = FastAPIFile(...),
    db: AsyncSession = Depends(get_db),
):
    """Upload a screenshot for a feedback entry."""
    import os
    from app.models.ml_model import MLTestFeedback

    fb = await db.get(MLTestFeedback, feedback_id)
    if not fb or fb.test_run_id != run_id:
        raise HTTPException(status_code=404, detail="Feedback not found")

    # Save to storage
    upload_dir = "/storage/feedback_screenshots"
    os.makedirs(upload_dir, exist_ok=True)
    ext = os.path.splitext(file.filename or "img.png")[1] or ".png"
    filename = f"{feedback_id}{ext}"
    filepath = os.path.join(upload_dir, filename)

    content = await file.read()
    with open(filepath, "wb") as f:
        f.write(content)

    new_path = f"/storage/feedback_screenshots/{filename}"
    # Append to existing screenshots (stored as comma-separated paths or single path)
    existing = fb.screenshot_path or ""
    if existing:
        fb.screenshot_path = existing + "," + new_path
    else:
        fb.screenshot_path = new_path
    await db.commit()
    return {"screenshot_path": new_path}
