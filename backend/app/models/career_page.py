"""Career page model."""

import uuid
from datetime import datetime
from typing import Optional
from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Text, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class CareerPage(Base):
    __tablename__ = "career_pages"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    company_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("companies.id", ondelete="CASCADE"))
    url: Mapped[str] = mapped_column(Text, nullable=False)
    page_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # listing_page, department_page, etc.
    discovery_method: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # heuristic, llm_classification, etc.
    discovery_confidence: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    is_primary: Mapped[bool] = mapped_column(Boolean, default=False)
    is_paginated: Mapped[bool] = mapped_column(Boolean, default=False)
    pagination_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    pagination_selector: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    requires_js_rendering: Mapped[bool] = mapped_column(Boolean, default=False)
    last_content_hash: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    last_crawled_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    last_extraction_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    company: Mapped["Company"] = relationship("Company", back_populates="career_pages")
    jobs: Mapped[list["Job"]] = relationship("Job", back_populates="career_page")
    site_templates: Mapped[list["SiteTemplate"]] = relationship("SiteTemplate", back_populates="career_page")
    crawl_logs: Mapped[list["CrawlLog"]] = relationship("CrawlLog", back_populates="career_page")
    extraction_comparisons: Mapped[list["ExtractionComparison"]] = relationship("ExtractionComparison", back_populates="career_page")
