"""Test data models — raw imported CSV data for ML experiments."""

import uuid
from datetime import datetime
from typing import Optional
from sqlalchemy import Boolean, DateTime, Integer, Text, func
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class CrawlerTestData(Base):
    __tablename__ = "crawler_test_data"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    external_id: Mapped[str] = mapped_column(Text, nullable=False)
    job_site_id: Mapped[str] = mapped_column(Text, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    crawler_type: Mapped[str] = mapped_column(Text, nullable=False)
    country: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    country_code: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    frequency: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    status: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    current_status: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    disabled: Mapped[bool] = mapped_column(Boolean, default=False)
    statistics_data: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class JobSiteTestData(Base):
    __tablename__ = "job_site_test_data"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    external_id: Mapped[str] = mapped_column(Text, nullable=False)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    site_type: Mapped[str] = mapped_column(Text, nullable=False)
    num_of_jobs: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    expected_job_count: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    disabled: Mapped[bool] = mapped_column(Boolean, default=False)
    uncrawlable_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    tags: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class SiteUrlTestData(Base):
    __tablename__ = "site_url_test_data"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    site_id: Mapped[str] = mapped_column(Text, nullable=False)
    url: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())


class SiteWrapperTestData(Base):
    __tablename__ = "site_wrapper_test_data"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    external_id: Mapped[str] = mapped_column(Text, nullable=False)
    crawler_id: Mapped[str] = mapped_column(Text, nullable=False)
    selectors: Mapped[dict] = mapped_column(JSONB, nullable=False)
    paths_config: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    has_detail_page: Mapped[bool] = mapped_column(Boolean, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
