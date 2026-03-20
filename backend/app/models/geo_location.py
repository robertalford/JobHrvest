"""GeoLocation and GeocodeCache models — hierarchical geocoder database."""

import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy import Boolean, DateTime, Float, ForeignKey, Integer, Numeric, String, Text, func
from sqlalchemy.dialects.postgresql import ARRAY, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class GeoLocation(Base):
    """Hierarchical location record.

    Level 1 = Country
    Level 2 = State / Province / Region
    Level 3 = City / Town
    Level 4 = Suburb / Neighbourhood / Village
    """

    __tablename__ = "geo_locations"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    level: Mapped[int] = mapped_column(Integer, nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    ascii_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    alt_names: Mapped[Optional[list]] = mapped_column(ARRAY(Text), nullable=True)
    parent_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("geo_locations.id"), nullable=True
    )
    market_code: Mapped[Optional[str]] = mapped_column(
        String(10), ForeignKey("markets.code"), nullable=True
    )
    country_code: Mapped[Optional[str]] = mapped_column(String(2), nullable=True)
    geonames_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    lat: Mapped[Optional[float]] = mapped_column(Numeric(10, 7), nullable=True)
    lng: Mapped[Optional[float]] = mapped_column(Numeric(10, 7), nullable=True)
    population: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    timezone: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    admin1_code: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    feature_code: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # Self-referential hierarchy
    parent: Mapped[Optional["GeoLocation"]] = relationship(
        "GeoLocation", remote_side="GeoLocation.id", back_populates="children"
    )
    children: Mapped[list["GeoLocation"]] = relationship(
        "GeoLocation", back_populates="parent", viewonly=True
    )


class GeocodeCache(Base):
    """Caches raw-text → geo_location resolutions to avoid redundant lookups."""

    __tablename__ = "geocode_cache"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    raw_text: Mapped[str] = mapped_column(Text, nullable=False)
    market_code: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    geo_location_id: Mapped[Optional[uuid.UUID]] = mapped_column(
        UUID(as_uuid=True), ForeignKey("geo_locations.id"), nullable=True
    )
    confidence: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    resolution_method: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_used_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    use_count: Mapped[int] = mapped_column(Integer, default=1)

    geo_location: Mapped[Optional["GeoLocation"]] = relationship("GeoLocation")
