"""Company model."""

import uuid
from datetime import datetime
from typing import Optional
from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, String, Text, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class Company(Base):
    __tablename__ = "companies"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    domain: Mapped[str] = mapped_column(Text, unique=True, nullable=False)
    root_url: Mapped[str] = mapped_column(Text, nullable=False)
    market_code: Mapped[str] = mapped_column(String(10), ForeignKey("markets.code"), default="AU")
    discovered_via: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    ats_platform: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    ats_confidence: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    crawl_priority: Mapped[int] = mapped_column(Integer, default=5)
    crawl_frequency_hours: Mapped[int] = mapped_column(Integer, default=24)
    last_crawl_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    next_crawl_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    requires_js_rendering: Mapped[bool] = mapped_column(Boolean, default=False)
    anti_bot_level: Mapped[str] = mapped_column(Text, default="none")
    notes: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    market: Mapped["Market"] = relationship("Market", back_populates="companies")
    career_pages: Mapped[list["CareerPage"]] = relationship("CareerPage", back_populates="company")
    jobs: Mapped[list["Job"]] = relationship("Job", back_populates="company")
    crawl_logs: Mapped[list["CrawlLog"]] = relationship("CrawlLog", back_populates="company")
