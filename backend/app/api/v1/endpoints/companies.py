"""Company endpoints."""

import csv
import io
from typing import Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, UploadFile, File
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.base import get_db
from app.models.company import Company
from app.schemas.company import CompanyCreate, CompanyRead, CompanyUpdate, CompanyList

router = APIRouter()


@router.get("/", response_model=CompanyList)
async def list_companies(
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=200),
    search: Optional[str] = None,
    ats_platform: Optional[str] = None,
    is_active: Optional[bool] = None,
    db: AsyncSession = Depends(get_db),
):
    q = select(Company)
    if search:
        q = q.where(Company.name.ilike(f"%{search}%") | Company.domain.ilike(f"%{search}%"))
    if ats_platform:
        q = q.where(Company.ats_platform == ats_platform)
    if is_active is not None:
        q = q.where(Company.is_active == is_active)

    total = await db.scalar(select(func.count()).select_from(q.subquery()))
    items = await db.scalars(q.offset((page - 1) * page_size).limit(page_size))
    return {"items": list(items), "total": total, "page": page, "page_size": page_size}


@router.post("/", response_model=CompanyRead, status_code=201)
async def create_company(payload: CompanyCreate, db: AsyncSession = Depends(get_db)):
    from urllib.parse import urlparse
    domain = urlparse(str(payload.root_url)).netloc.lstrip("www.")
    existing = await db.scalar(select(Company).where(Company.domain == domain))
    if existing:
        raise HTTPException(status_code=409, detail="Company with this domain already exists")
    company = Company(
        name=payload.name,
        domain=domain,
        root_url=str(payload.root_url),
        market_code=payload.market_code,
        crawl_priority=payload.crawl_priority,
        notes=payload.notes,
        discovered_via="manual",
    )
    db.add(company)
    await db.commit()
    await db.refresh(company)
    return company


@router.post("/bulk", status_code=202)
async def bulk_import_companies(file: UploadFile = File(...), db: AsyncSession = Depends(get_db)):
    content = await file.read()
    reader = csv.DictReader(io.StringIO(content.decode("utf-8")))
    imported, skipped = 0, 0
    for row in reader:
        url = row.get("url") or row.get("root_url", "")
        name = row.get("name", url)
        if not url:
            skipped += 1
            continue
        from urllib.parse import urlparse
        domain = urlparse(url).netloc.lstrip("www.")
        existing = await db.scalar(select(Company).where(Company.domain == domain))
        if existing:
            skipped += 1
            continue
        db.add(Company(name=name, domain=domain, root_url=url, discovered_via="bulk_import"))
        imported += 1
    await db.commit()
    return {"imported": imported, "skipped": skipped}


@router.get("/{company_id}", response_model=CompanyRead)
async def get_company(company_id: UUID, db: AsyncSession = Depends(get_db)):
    company = await db.get(Company, company_id)
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    return company


@router.patch("/{company_id}", response_model=CompanyRead)
async def update_company(company_id: UUID, payload: CompanyUpdate, db: AsyncSession = Depends(get_db)):
    company = await db.get(Company, company_id)
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    for field, value in payload.model_dump(exclude_unset=True).items():
        setattr(company, field, value)
    await db.commit()
    await db.refresh(company)
    return company


@router.post("/{company_id}/crawl", status_code=202)
async def trigger_crawl(company_id: UUID, db: AsyncSession = Depends(get_db)):
    company = await db.get(Company, company_id)
    if not company:
        raise HTTPException(status_code=404, detail="Company not found")
    # Enqueue Celery task
    from app.tasks.crawl_tasks import crawl_company
    task = crawl_company.delay(str(company_id))
    return {"task_id": task.id, "status": "queued"}


@router.get("/{company_id}/jobs")
async def get_company_jobs(company_id: UUID, db: AsyncSession = Depends(get_db)):
    from app.models.job import Job
    jobs = await db.scalars(select(Job).where(Job.company_id == company_id, Job.is_active == True))
    return list(jobs)


@router.get("/{company_id}/crawl-history")
async def get_company_crawl_history(company_id: UUID, db: AsyncSession = Depends(get_db)):
    from app.models.crawl_log import CrawlLog
    logs = await db.scalars(
        select(CrawlLog).where(CrawlLog.company_id == company_id).order_by(CrawlLog.started_at.desc()).limit(50)
    )
    return list(logs)
