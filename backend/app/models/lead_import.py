"""SQLAlchemy model for lead_imports — tracks CSV lead ingestion status."""

import uuid
from sqlalchemy import Column, String, Text, Integer, Float, Boolean, DateTime, ForeignKey
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.sql import func
from app.db.base import Base


class LeadImport(Base):
    __tablename__ = "lead_imports"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)

    # Source data from CSV
    country_id = Column(String(10), nullable=False, index=True)
    advertiser_name = Column(Text, nullable=False)
    origin_domain = Column(Text, nullable=False, index=True)
    sample_linkout_url = Column(Text)
    ad_origin_category = Column(Text, index=True)
    expected_job_count = Column(Integer)
    origin_rank = Column(Integer)

    # Import outcome
    status = Column(Text, nullable=False, default="pending")
    # pending | success | failed | skipped | blocked
    company_id = Column(UUID(as_uuid=True), ForeignKey("companies.id", ondelete="SET NULL"))
    career_pages_found = Column(Integer, default=0)
    jobs_extracted = Column(Integer, default=0)
    error_message = Column(Text)
    error_details = Column(JSONB)
    skip_reason = Column(Text)

    # Timestamps
    imported_at = Column(DateTime(timezone=True), server_default=func.now())
    processed_at = Column(DateTime(timezone=True))
