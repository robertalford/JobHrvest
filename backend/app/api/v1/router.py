"""API v1 router — aggregates all endpoint groups."""

from fastapi import APIRouter

from app.api.v1.endpoints import health, companies, career_pages, jobs, crawl, analytics, system, lead_imports

api_router = APIRouter()

api_router.include_router(health.router, tags=["health"])
api_router.include_router(companies.router, prefix="/companies", tags=["companies"])
api_router.include_router(career_pages.router, prefix="/career-pages", tags=["career-pages"])
api_router.include_router(jobs.router, prefix="/jobs", tags=["jobs"])
api_router.include_router(crawl.router, prefix="/crawl", tags=["crawl"])
api_router.include_router(analytics.router, prefix="/analytics", tags=["analytics"])
api_router.include_router(system.router, prefix="/system", tags=["system"])
api_router.include_router(lead_imports.router, prefix="/lead-imports", tags=["lead-imports"])
