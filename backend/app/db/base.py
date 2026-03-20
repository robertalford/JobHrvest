"""SQLAlchemy async engine and session factory."""

import os

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.pool import NullPool

from app.core.config import settings

# Celery workers call asyncio.run() once per task, creating a new event loop each time.
# A pooled engine holds connections bound to the *previous* loop, causing:
#   "Future attached to a different loop" errors.
# NullPool avoids this by never reusing connections across event loop boundaries.
# The API process uses the default pool (set CELERY_WORKER=false for FastAPI).
_use_null_pool = os.getenv("CELERY_WORKER", "false").lower() == "true"

# Crawl tasks hold open DB transactions while making external HTTP requests (up to 200s).
# Without this override the Postgres server-level idle_in_transaction_session_timeout
# (default: 30s) closes the connection mid-task, causing InterfaceError cascades.
# Also set at the DB level: ALTER DATABASE jobharvest SET idle_in_transaction_session_timeout = 0
_connect_args = {"server_settings": {"idle_in_transaction_session_timeout": "0"}}

engine = create_async_engine(
    settings.DATABASE_URL,
    echo=settings.DEBUG,
    pool_pre_ping=True,
    connect_args=_connect_args,
    **({} if _use_null_pool else {"pool_size": 20, "max_overflow": 40, "pool_recycle": 3600}),
    **({"poolclass": NullPool} if _use_null_pool else {}),
)

AsyncSessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,
)


class Base(DeclarativeBase):
    pass


async def get_db() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        yield session
