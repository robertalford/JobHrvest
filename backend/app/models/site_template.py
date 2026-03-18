"""Site template model — learned CSS/XPath selector maps."""

import uuid
from datetime import datetime
from typing import Optional
from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Text, func
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class SiteTemplate(Base):
    __tablename__ = "site_templates"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    company_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("companies.id", ondelete="CASCADE"))
    career_page_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("career_pages.id", ondelete="CASCADE"))
    template_type: Mapped[str] = mapped_column(Text, nullable=False)  # listing_page, detail_page
    selectors: Mapped[dict] = mapped_column(JSONB, default=dict)
    learned_via: Mapped[str] = mapped_column(Text, default="llm_bootstrapped")
    accuracy_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    last_validated_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    # Relationships
    company: Mapped["Company"] = relationship("Company")
    career_page: Mapped["CareerPage"] = relationship("CareerPage", back_populates="site_templates")
