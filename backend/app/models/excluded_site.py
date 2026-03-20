"""ExcludedSite — sites that are known but should not be crawled or extracted from."""

import uuid
from sqlalchemy import Column, String, Text, Integer, DateTime
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.sql import func
from app.db.base import Base


class ExcludedSite(Base):
    __tablename__ = "excluded_sites"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    domain = Column(Text, nullable=False, unique=True, index=True)
    company_name = Column(Text)
    site_url = Column(Text)
    site_type = Column(String(50))       # employer / job_board / recruiter
    country_code = Column(String(10))
    expected_job_count = Column(Integer)
    reason = Column(Text)                # why excluded (e.g. "site_disabled", "manual")
    source_file = Column(Text)           # which import file added this
    created_at = Column(DateTime(timezone=True), server_default=func.now())
