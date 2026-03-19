"""Job and JobTag models."""

import uuid
from datetime import date, datetime
from typing import Optional
from sqlalchemy import Boolean, Date, DateTime, Float, ForeignKey, Index, Numeric, Text, func
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class Job(Base):
    __tablename__ = "jobs"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    company_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("companies.id", ondelete="CASCADE"))
    career_page_id: Mapped[Optional[uuid.UUID]] = mapped_column(UUID(as_uuid=True), ForeignKey("career_pages.id", ondelete="SET NULL"), nullable=True)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    external_id: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    description_html: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    location_raw: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    location_city: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    location_state: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    location_country: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    is_remote: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    remote_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # fully_remote, hybrid, onsite, flexible
    employment_type: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # full_time, part_time, etc.
    seniority_level: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    department: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    team: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    salary_raw: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    salary_min: Mapped[Optional[float]] = mapped_column(Numeric(12, 2), nullable=True)
    salary_max: Mapped[Optional[float]] = mapped_column(Numeric(12, 2), nullable=True)
    salary_currency: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    salary_period: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # hourly, daily, weekly, monthly, annual
    requirements: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    benefits: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    application_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    date_posted: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    date_expires: Mapped[Optional[date]] = mapped_column(Date, nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False, server_default=func.now())
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    extraction_method: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # structural, llm, schema_org, ats_api, hybrid
    extraction_confidence: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    raw_data: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    # Quality scoring
    quality_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    quality_completeness: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    quality_description: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    quality_issues: Mapped[Optional[list]] = mapped_column(JSONB, nullable=True)
    quality_flags: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    quality_scored_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    company: Mapped["Company"] = relationship("Company", back_populates="jobs")
    career_page: Mapped[Optional["CareerPage"]] = relationship("CareerPage", back_populates="jobs")
    tags: Mapped[list["JobTag"]] = relationship("JobTag", back_populates="job", cascade="all, delete-orphan")
    extraction_comparisons: Mapped[list["ExtractionComparison"]] = relationship("ExtractionComparison", back_populates="job")

    __table_args__ = (
        Index("ix_jobs_company_id", "company_id"),
        Index("ix_jobs_is_active", "is_active"),
        Index("ix_jobs_first_seen_at", "first_seen_at"),
        Index("ix_jobs_last_seen_at", "last_seen_at"),
        Index("ix_jobs_location", "location_country", "location_city"),
    )


class JobTag(Base):
    __tablename__ = "job_tags"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"))
    tag_type: Mapped[str] = mapped_column(Text, nullable=False)  # skill, technology, qualification, industry, category
    tag_value: Mapped[str] = mapped_column(Text, nullable=False)
    confidence: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Relationships
    job: Mapped["Job"] = relationship("Job", back_populates="tags")
