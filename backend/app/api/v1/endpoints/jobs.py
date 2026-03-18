"""Job endpoints."""

import csv
import io
import json
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Response
from sqlalchemy import func, select, or_
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.base import get_db
from app.models.job import Job

router = APIRouter()


@router.get("/stats")
async def job_stats(db: AsyncSession = Depends(get_db)):
    total = await db.scalar(select(func.count(Job.id)))
    active = await db.scalar(select(func.count(Job.id)).where(Job.is_active == True))
    from datetime import date, timedelta
    today = date.today()
    week_ago = today - timedelta(days=7)
    new_today = await db.scalar(
        select(func.count(Job.id)).where(Job.first_seen_at >= today)
    )
    new_this_week = await db.scalar(
        select(func.count(Job.id)).where(Job.first_seen_at >= week_ago)
    )
    return {
        "total": total,
        "active": active,
        "new_today": new_today,
        "new_this_week": new_this_week,
    }


@router.get("/")
async def list_jobs(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    search: Optional[str] = None,
    company_id: Optional[UUID] = None,
    location_country: Optional[str] = None,
    location_city: Optional[str] = None,
    remote_type: Optional[str] = None,
    employment_type: Optional[str] = None,
    seniority_level: Optional[str] = None,
    is_active: Optional[bool] = True,
    salary_min: Optional[float] = None,
    salary_max: Optional[float] = None,
    db: AsyncSession = Depends(get_db),
):
    q = select(Job)
    if search:
        q = q.where(or_(Job.title.ilike(f"%{search}%"), Job.description.ilike(f"%{search}%")))
    if company_id:
        q = q.where(Job.company_id == company_id)
    if location_country:
        q = q.where(Job.location_country == location_country)
    if location_city:
        q = q.where(Job.location_city.ilike(f"%{location_city}%"))
    if remote_type:
        q = q.where(Job.remote_type == remote_type)
    if employment_type:
        q = q.where(Job.employment_type == employment_type)
    if seniority_level:
        q = q.where(Job.seniority_level == seniority_level)
    if is_active is not None:
        q = q.where(Job.is_active == is_active)
    if salary_min is not None:
        q = q.where(Job.salary_min >= salary_min)
    if salary_max is not None:
        q = q.where(Job.salary_max <= salary_max)

    total = await db.scalar(select(func.count()).select_from(q.subquery()))
    items = await db.scalars(q.order_by(Job.first_seen_at.desc()).offset((page - 1) * page_size).limit(page_size))
    return {"items": list(items), "total": total, "page": page, "page_size": page_size}


@router.get("/export")
async def export_jobs(
    format: str = Query("csv", pattern="^(csv|json)$"),
    is_active: Optional[bool] = True,
    db: AsyncSession = Depends(get_db),
):
    q = select(Job)
    if is_active is not None:
        q = q.where(Job.is_active == is_active)
    jobs = list(await db.scalars(q))

    if format == "json":
        data = [
            {
                "id": str(j.id),
                "title": j.title,
                "company_id": str(j.company_id),
                "location_raw": j.location_raw,
                "employment_type": j.employment_type,
                "salary_raw": j.salary_raw,
                "source_url": j.source_url,
                "first_seen_at": j.first_seen_at.isoformat() if j.first_seen_at else None,
            }
            for j in jobs
        ]
        return Response(json.dumps(data), media_type="application/json")

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["id", "title", "company_id", "location_raw", "employment_type", "salary_raw", "source_url", "first_seen_at"])
    for j in jobs:
        writer.writerow([str(j.id), j.title, str(j.company_id), j.location_raw, j.employment_type, j.salary_raw, j.source_url, j.first_seen_at])
    return Response(output.getvalue(), media_type="text/csv", headers={"Content-Disposition": "attachment; filename=jobs.csv"})


@router.get("/{job_id}")
async def get_job(job_id: UUID, db: AsyncSession = Depends(get_db)):
    job = await db.get(Job, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return job
