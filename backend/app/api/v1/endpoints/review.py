"""Review endpoints — human-in-the-loop review queue for quality and duplicates."""

import uuid

from fastapi import APIRouter, Depends, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from app.db.base import get_db
from app.models.job import Job

router = APIRouter()

# ── helpers ──────────────────────────────────────────────────────────────────

def _job_dict(j: Job, include_description: bool = True) -> dict:
    return {
        "id": str(j.id),
        "title": j.title,
        "description": j.description if include_description else None,
        "location_raw": j.location_raw,
        "employment_type": j.employment_type,
        "salary_raw": j.salary_raw,
        "requirements": j.requirements,
        "date_posted": j.date_posted.isoformat() if j.date_posted else None,
        "source_url": j.source_url,
        "extraction_method": j.extraction_method,
        "extraction_confidence": j.extraction_confidence,
        "quality_score": j.quality_score,
        "quality_completeness": j.quality_completeness,
        "quality_description": j.quality_description,
        "quality_issues": j.quality_issues,
        "quality_flags": j.quality_flags,
        "quality_override": j.quality_override,
        "is_canonical": j.is_canonical,
        "canonical_job_id": str(j.canonical_job_id) if j.canonical_job_id else None,
        "dedup_score": j.dedup_score,
        "company_id": str(j.company_id),
        "company_name": j.company.name if j.company else None,
        "company_domain": j.company.domain if j.company else None,
        "first_seen_at": j.first_seen_at.isoformat() if j.first_seen_at else None,
    }


# ── Quality queue ─────────────────────────────────────────────────────────────

QUALITY_THRESHOLD = 40.0


@router.get("/quality-queue")
async def quality_queue(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    """Return jobs with low quality scores that haven't been reviewed yet."""
    from sqlalchemy import text
    count_result = await db.execute(text("""
        SELECT COUNT(*) FROM jobs j
        WHERE j.is_active = true
          AND j.quality_score IS NOT NULL
          AND j.quality_score < :threshold
          AND j.quality_override IS NOT TRUE
          AND NOT EXISTS (
            SELECT 1 FROM review_feedback rf
            WHERE rf.job_id = j.id AND rf.review_type = 'quality'
          )
    """), {"threshold": QUALITY_THRESHOLD})
    total = count_result.scalar() or 0

    rows_result = await db.execute(text("""
        SELECT j.id FROM jobs j
        WHERE j.is_active = true
          AND j.quality_score IS NOT NULL
          AND j.quality_score < :threshold
          AND j.quality_override IS NOT TRUE
          AND NOT EXISTS (
            SELECT 1 FROM review_feedback rf
            WHERE rf.job_id = j.id AND rf.review_type = 'quality'
          )
        ORDER BY j.quality_score ASC
        LIMIT :limit OFFSET :offset
    """), {"threshold": QUALITY_THRESHOLD, "limit": page_size, "offset": (page - 1) * page_size})
    job_ids = [row[0] for row in rows_result]

    jobs = []
    if job_ids:
        q = await db.scalars(
            select(Job).options(joinedload(Job.company)).where(Job.id.in_(job_ids)).order_by(Job.quality_score.asc())
        )
        jobs = list(q)

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": [_job_dict(j) for j in jobs],
    }


@router.post("/quality/{job_id}/feedback/")
@router.post("/quality/{job_id}/feedback")
async def quality_feedback(
    job_id: str,
    decision: str = Query(..., pattern="^(confirm|overrule)$"),
    db: AsyncSession = Depends(get_db),
):
    """Record a human quality review decision and extract training signal."""
    from sqlalchemy import text
    from datetime import datetime, timezone

    job = await db.scalar(select(Job).options(joinedload(Job.company)).where(Job.id == uuid.UUID(job_id)))
    if not job:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Job not found")

    # Build training signal snapshot
    desc = job.description or ""
    features = {
        "title": job.title,
        "description_length": len(desc),
        "has_location": bool(job.location_raw),
        "has_employment_type": bool(job.employment_type),
        "has_salary": bool(job.salary_raw),
        "has_requirements": bool(job.requirements),
        "has_date_posted": bool(job.date_posted),
        "quality_score": job.quality_score,
        "quality_issues": job.quality_issues,
        "quality_flags": job.quality_flags,
        "extraction_method": job.extraction_method,
        "extraction_confidence": job.extraction_confidence,
        "human_decision": decision,
        "reviewed_at": datetime.now(timezone.utc).isoformat(),
    }

    # Record feedback
    await db.execute(text("""
        INSERT INTO review_feedback (id, job_id, review_type, decision, features_snapshot)
        VALUES (:id, :job_id, 'quality', :decision, CAST(:features AS JSONB))
    """), {
        "id": str(uuid.uuid4()),
        "job_id": job_id,
        "decision": decision,
        "features": __import__('json').dumps(features),
    })

    # confirm = poor quality confirmed → deactivate the job
    # overrule = human says it IS quality → set override flag and keep active
    if decision == "confirm":
        await db.execute(text("""
            UPDATE jobs SET is_active = false WHERE id = :id
        """), {"id": job_id})
    elif decision == "overrule":
        await db.execute(text("""
            UPDATE jobs SET quality_override = true, quality_score = 60.0 WHERE id = :id
        """), {"id": job_id})

    await db.commit()
    return {"status": "ok", "decision": decision, "job_id": job_id}


# ── Duplicate queue ───────────────────────────────────────────────────────────

@router.get("/duplicate-queue")
async def duplicate_queue(
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    db: AsyncSession = Depends(get_db),
):
    """Return duplicate jobs (non-canonical) that haven't been reviewed yet."""
    from sqlalchemy import text

    count_result = await db.execute(text("""
        SELECT COUNT(*) FROM jobs j
        WHERE j.is_canonical = false
          AND j.canonical_job_id IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM review_feedback rf
            WHERE rf.job_id = j.id AND rf.review_type = 'duplicate'
          )
    """))
    total = count_result.scalar() or 0

    rows_result = await db.execute(text("""
        SELECT j.id FROM jobs j
        WHERE j.is_canonical = false
          AND j.canonical_job_id IS NOT NULL
          AND NOT EXISTS (
            SELECT 1 FROM review_feedback rf
            WHERE rf.job_id = j.id AND rf.review_type = 'duplicate'
          )
        ORDER BY j.dedup_score DESC NULLS LAST
        LIMIT :limit OFFSET :offset
    """), {"limit": page_size, "offset": (page - 1) * page_size})
    job_ids = [row[0] for row in rows_result]

    items = []
    if job_ids:
        dup_jobs = await db.scalars(
            select(Job).options(joinedload(Job.company)).where(Job.id.in_(job_ids))
        )
        dup_map = {j.id: j for j in dup_jobs}

        # fetch canonical jobs
        canonical_ids = [j.canonical_job_id for j in dup_map.values() if j.canonical_job_id]
        canonical_jobs_q = await db.scalars(
            select(Job).options(joinedload(Job.company)).where(Job.id.in_(canonical_ids))
        )
        canonical_map = {j.id: j for j in canonical_jobs_q}

        for jid in job_ids:
            dup = dup_map.get(jid)
            if not dup:
                continue
            canonical = canonical_map.get(dup.canonical_job_id) if dup.canonical_job_id else None
            items.append({
                "duplicate": _job_dict(dup),
                "canonical": _job_dict(canonical) if canonical else None,
                "dedup_score": dup.dedup_score,
            })

    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "items": items,
    }


@router.post("/duplicate/{job_id}/feedback/")
@router.post("/duplicate/{job_id}/feedback")
async def duplicate_feedback(
    job_id: str,
    decision: str = Query(..., pattern="^(confirm|overrule)$"),
    db: AsyncSession = Depends(get_db),
):
    """Record a human duplicate review decision."""
    from sqlalchemy import text
    import json
    from datetime import datetime, timezone

    job = await db.scalar(select(Job).options(joinedload(Job.company)).where(Job.id == uuid.UUID(job_id)))
    if not job:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Job not found")

    canonical = None
    if job.canonical_job_id:
        canonical = await db.scalar(
            select(Job).options(joinedload(Job.company)).where(Job.id == job.canonical_job_id)
        )

    def _desc_excerpt(d: str | None) -> str:
        if not d:
            return ""
        return d[:500]

    features = {
        "duplicate_title": job.title,
        "duplicate_company": job.company.name if job.company else None,
        "duplicate_description_excerpt": _desc_excerpt(job.description),
        "canonical_title": canonical.title if canonical else None,
        "canonical_company": canonical.company.name if canonical and canonical.company else None,
        "canonical_description_excerpt": _desc_excerpt(canonical.description if canonical else None),
        "dedup_score": job.dedup_score,
        "same_company": (job.company_id == canonical.company_id) if canonical else None,
        "human_decision": decision,
        "reviewed_at": datetime.now(timezone.utc).isoformat(),
    }

    await db.execute(text("""
        INSERT INTO review_feedback (id, job_id, review_type, decision, canonical_job_id, features_snapshot)
        VALUES (:id, :job_id, 'duplicate', :decision, :canonical_id, CAST(:features AS JSONB))
    """), {
        "id": str(uuid.uuid4()),
        "job_id": job_id,
        "decision": decision,
        "canonical_id": str(job.canonical_job_id) if job.canonical_job_id else None,
        "features": json.dumps(features),
    })

    # confirm = duplicate confirmed → keep inactive, just record signal
    # overrule = human says NOT a duplicate → reinstate as canonical
    if decision == "confirm":
        await db.execute(text("""
            UPDATE jobs SET is_active = false WHERE id = :id
        """), {"id": job_id})

    # If overruling (human says it is NOT a duplicate), reinstate as canonical
    if decision == "overrule":
        await db.execute(text("""
            UPDATE jobs SET is_canonical = true, canonical_job_id = NULL WHERE id = :id
        """), {"id": job_id})

    await db.commit()
    return {"status": "ok", "decision": decision, "job_id": job_id}
