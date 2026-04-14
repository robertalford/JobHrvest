"""SQLAlchemy models for the universality-gate infrastructure.

Backs the auto-improve loop's redesigned promotion gate:
  - EverPassedSite      : monotonic 'has any version ever passed this site?' set.
  - SiteResultHistory   : append-only per-site/per-run verdict log used by the
                          oscillation detector.

See migration 0028_universality_gate for the schema of record.
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import BigInteger, Boolean, DateTime, Double, Integer, Text, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class EverPassedSite(Base):
    __tablename__ = "ever_passed_sites"

    url: Mapped[str] = mapped_column(Text, primary_key=True)
    company: Mapped[Optional[str]] = mapped_column(Text)
    ats_platform: Mapped[Optional[str]] = mapped_column(Text)
    best_composite: Mapped[float] = mapped_column(Double, nullable=False)
    best_version_name: Mapped[str] = mapped_column(Text, nullable=False)
    best_run_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True))
    jobs_quality: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    baseline_jobs: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    first_passed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    last_updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class SiteResultHistory(Base):
    __tablename__ = "site_result_history"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    run_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    model_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True))
    model_name: Mapped[Optional[str]] = mapped_column(Text)
    ats_platform: Mapped[Optional[str]] = mapped_column(Text)
    match: Mapped[str] = mapped_column(Text, nullable=False)
    passed: Mapped[bool] = mapped_column(Boolean, nullable=False)
    baseline_jobs: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    model_jobs: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    jobs_quality: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    composite_pts: Mapped[Optional[float]] = mapped_column(Double)
    observed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
