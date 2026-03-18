"""Career page endpoints."""

from uuid import UUID
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.base import get_db
from app.models.career_page import CareerPage

router = APIRouter()


@router.get("/")
async def list_career_pages(db: AsyncSession = Depends(get_db)):
    pages = await db.scalars(select(CareerPage).order_by(CareerPage.created_at.desc()))
    return list(pages)


@router.get("/{page_id}")
async def get_career_page(page_id: UUID, db: AsyncSession = Depends(get_db)):
    page = await db.get(CareerPage, page_id)
    if not page:
        raise HTTPException(status_code=404, detail="Career page not found")
    return page


@router.post("/{page_id}/recrawl", status_code=202)
async def recrawl_page(page_id: UUID, db: AsyncSession = Depends(get_db)):
    page = await db.get(CareerPage, page_id)
    if not page:
        raise HTTPException(status_code=404, detail="Career page not found")
    from app.tasks.crawl_tasks import crawl_career_page
    task = crawl_career_page.delay(str(page_id))
    return {"task_id": task.id, "status": "queued"}


@router.get("/{page_id}/template")
async def get_template(page_id: UUID, db: AsyncSession = Depends(get_db)):
    from app.models.site_template import SiteTemplate
    template = await db.scalar(
        select(SiteTemplate).where(SiteTemplate.career_page_id == page_id, SiteTemplate.is_active == True)
    )
    if not template:
        raise HTTPException(status_code=404, detail="No active template for this career page")
    return template


@router.post("/{page_id}/validate-template", status_code=202)
async def validate_template(page_id: UUID, db: AsyncSession = Depends(get_db)):
    page = await db.get(CareerPage, page_id)
    if not page:
        raise HTTPException(status_code=404, detail="Career page not found")
    from app.tasks.crawl_tasks import validate_page_template
    task = validate_page_template.delay(str(page_id))
    return {"task_id": task.id, "status": "queued"}
