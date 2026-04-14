"""JobHarvest FastAPI application."""

from contextlib import asynccontextmanager
import logging
from typing import Callable

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.types import ASGIApp, Receive, Scope, Send

from app.core.config import settings
from app.api.v1.router import api_router

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StripTrailingSlashMiddleware:
    """ASGI middleware that strips trailing slashes from request paths.

    Avoids FastAPI's automatic 307 redirect which produces an http:// Location
    when running behind an HTTPS reverse proxy, causing browser CORS failures.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            path: str = scope.get("path", "/")
            if path != "/" and path.endswith("/"):
                scope = dict(scope)
                scope["path"] = path.rstrip("/")
                if "raw_path" in scope:
                    scope["raw_path"] = scope["path"].encode("utf-8")
        await self.app(scope, receive, send)


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("JobHarvest API starting up")
    from app.crawlers.domain_blocklist import refresh_from_db_async
    await refresh_from_db_async()
    logger.info("Domain blocklist loaded from excluded_sites")
    yield
    logger.info("JobHarvest API shutting down")


app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    openapi_url=f"{settings.API_V1_PREFIX}/openapi.json",
    docs_url=f"{settings.API_V1_PREFIX}/docs",
    lifespan=lifespan,
    redirect_slashes=False,
)

app.add_middleware(StripTrailingSlashMiddleware)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(api_router, prefix=settings.API_V1_PREFIX)

# Serve uploaded files (feedback screenshots, etc.)
import os
from fastapi.staticfiles import StaticFiles
_storage_dir = "/storage"
if os.path.isdir(_storage_dir):
    app.mount("/storage", StaticFiles(directory=_storage_dir), name="storage")
