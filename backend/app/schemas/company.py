"""Company Pydantic schemas."""

from datetime import datetime
from typing import Optional
from uuid import UUID
from pydantic import BaseModel, HttpUrl


class CompanyCreate(BaseModel):
    name: str
    root_url: HttpUrl
    market_code: str = "AU"
    crawl_priority: int = 5
    notes: Optional[str] = None


class CompanyUpdate(BaseModel):
    name: Optional[str] = None
    crawl_priority: Optional[int] = None
    crawl_frequency_hours: Optional[int] = None
    is_active: Optional[bool] = None
    requires_js_rendering: Optional[bool] = None
    notes: Optional[str] = None


class CompanyRead(BaseModel):
    id: UUID
    name: str
    domain: str
    root_url: str
    market_code: str
    ats_platform: Optional[str]
    ats_confidence: Optional[float]
    crawl_priority: Optional[int]
    crawl_frequency_hours: Optional[int]
    last_crawl_at: Optional[datetime]
    next_crawl_at: Optional[datetime]
    is_active: bool
    requires_js_rendering: Optional[bool]
    anti_bot_level: Optional[str]
    discovered_via: Optional[str]
    notes: Optional[str]
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class CompanyList(BaseModel):
    items: list[CompanyRead]
    total: int
    page: int
    page_size: int
