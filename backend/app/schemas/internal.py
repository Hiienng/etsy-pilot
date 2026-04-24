from pydantic import BaseModel
from datetime import datetime


# ── Upload response ──────────────────────────────────────
class UploadResponse(BaseModel):
    batch_id: str
    file_count: int


# ── Extract response ─────────────────────────────────────
class ListingReportRow(BaseModel):
    listing_id: str
    title: str | None = None
    no_vm: str | None = None
    price: float | None = None
    stock: int | None = None
    category: str | None = None
    lifetime_orders: int | None = None
    lifetime_revenue: float | None = None
    period: str
    views: int = 0
    clicks: int = 0
    orders: int = 0
    revenue: float = 0
    spend: float = 0
    roas: float = 0


class KeywordReportRow(BaseModel):
    listing_id: str
    keyword: str
    no_vm: str | None = None
    currently_status: str | None = None
    period: str
    roas: float = 0
    orders: int = 0
    spend: float = 0
    revenue: float = 0
    clicks: int = 0
    click_rate: str | None = None
    views: int = 0


class ExtractResponse(BaseModel):
    batch_id: str
    status: str
    listing_report: list[ListingReportRow] = []
    keyword_report: list[KeywordReportRow] = []
    progress: int = 0
    total_files: int = 0


# ── Confirm request / response ───────────────────────────
class ConfirmRequest(BaseModel):
    batch_id: str
    no_vm: str | None = None
    importer: str | None = None
    listing_report: list[ListingReportRow]
    keyword_report: list[KeywordReportRow]


class ConfirmResponse(BaseModel):
    imported: bool
    rows: dict  # {"listing": N, "keyword": M}


# ── Discard / Rollback response ──────────────────────────
class BatchActionResponse(BaseModel):
    batch_id: str
    status: str


# ── History ──────────────────────────────────────────────
class BatchHistoryItem(BaseModel):
    batch_id: str
    status: str
    file_count: int = 0
    listing_count: int = 0
    keyword_count: int = 0
    created_at: datetime | None = None
    confirmed_at: datetime | None = None
    note: str | None = None
    error_message: str | None = None
