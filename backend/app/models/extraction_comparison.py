"""Extraction comparison model — cross-validation logs."""

import uuid
from datetime import datetime
from typing import Optional
from sqlalchemy import DateTime, Float, ForeignKey, Text, func
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class ExtractionComparison(Base):
    __tablename__ = "extraction_comparisons"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    job_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("jobs.id", ondelete="CASCADE"))
    career_page_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("career_pages.id", ondelete="CASCADE"))
    method_a: Mapped[str] = mapped_column(Text, nullable=False)
    method_b: Mapped[str] = mapped_column(Text, nullable=False)
    method_a_result: Mapped[dict] = mapped_column(JSONB, default=dict)
    method_b_result: Mapped[dict] = mapped_column(JSONB, default=dict)
    agreement_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    resolved_result: Mapped[Optional[dict]] = mapped_column(JSONB, nullable=True)
    resolution_method: Mapped[Optional[str]] = mapped_column(Text, nullable=True)  # auto, llm_tiebreak, manual
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    # Relationships
    job: Mapped["Job"] = relationship("Job", back_populates="extraction_comparisons")
    career_page: Mapped["CareerPage"] = relationship("CareerPage", back_populates="extraction_comparisons")
