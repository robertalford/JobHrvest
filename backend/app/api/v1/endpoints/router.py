"""API v1 router — aggregates all endpoint groups."""

from fastapi import APIRouter, Depends

from app.api.v1.endpoints import (
    health, companies, career_pages, jobs, crawl, analytics, system,
    lead_imports, settings as settings_endpoints, excluded_sites, review,
    discovery_sources, geocoder, domain_imports, test_data, ml_models,
)
from app.api.v1.endpoints.auth import router as auth_router, get_current_user

api_router = APIRouter()

# Public routes — no auth required
api_router.include_router(health.router, tags=["health"])
api_router.include_router(auth_router, prefix="/auth", tags=["auth"])

# Protected routes — all require a valid JWT
_auth = [Depends(get_current_user)]
api_router.include_router(companies.router,          prefix="/companies",         tags=["companies"],          dependencies=_auth)
api_router.include_router(career_pages.router,       prefix="/career-pages",      tags=["career-pages"],       dependencies=_auth)
api_router.include_router(jobs.router,               prefix="/jobs",              tags=["jobs"],               dependencies=_auth)
api_router.include_router(crawl.router,              prefix="/crawl",             tags=["crawl"],              dependencies=_auth)
api_router.include_router(analytics.router,          prefix="/analytics",         tags=["analytics"],          dependencies=_auth)
api_router.include_router(system.router,             prefix="/system",            tags=["system"],             dependencies=_auth)
api_router.include_router(lead_imports.router,       prefix="/lead-imports",      tags=["lead-imports"],       dependencies=_auth)
api_router.include_router(settings_endpoints.router, prefix="/settings",          tags=["settings"],           dependencies=_auth)
api_router.include_router(excluded_sites.router,     prefix="/excluded-sites",    tags=["excluded-sites"],     dependencies=_auth)
api_router.include_router(review.router,             prefix="/review",            tags=["review"],             dependencies=_auth)
api_router.include_router(discovery_sources.router,  prefix="/discovery-sources", tags=["discovery-sources"],  dependencies=_auth)
api_router.include_router(geocoder.router,           prefix="/geocoder",          tags=["geocoder"],           dependencies=_auth)
api_router.include_router(domain_imports.router,     prefix="/domain-imports",    tags=["domain-imports"],     dependencies=_auth)
api_router.include_router(test_data.router,          prefix="/test-data",         tags=["test-data"],          dependencies=_auth)
api_router.include_router(ml_models.router,          prefix="/ml-models",         tags=["ml-models"],          dependencies=_auth)
