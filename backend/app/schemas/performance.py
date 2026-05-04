from datetime import datetime
from decimal import Decimal
from pydantic import BaseModel


class ListingDashboardItem(BaseModel):
    listing_id: str | None = None
    url: str | None = None
    title: str | None = None
    product: str | None = None
    no_vm: str | None = None
    period: str | None = None
    reference_date: datetime | None = None
    # performance (from listing_report latest)
    ctr: Decimal | None = None
    cr: Decimal | None = None
    roas: Decimal | None = None
    views: int | None = None
    clicks: int | None = None
    orders: int | None = None
    revenue: Decimal | None = None
    spend: Decimal | None = None
    # own market data (market_listing WHERE id = listing_id, latest)
    price: int | None = None
    discount_price: int | None = None
    rating: float | None = None
    review_count: int | None = None
    badge: str | None = None
    free_shipping: bool | None = None
    is_ad: bool | None = None
    tag_ranking: int | None = None
    # scenario (from scenarios_rules JOIN)
    scenario_action: str | None = None
    scenario_label: str | None = None
    scenario_cause: str | None = None
    scenario_fix_listing: str | None = None
    scenario_fix_ads: str | None = None
    # references: top-N market listings from references_engine (ordered by ref_rank)
    references: list[dict] | None = None
    # keywords: list of dicts from keyword_report at latest import_time
    keywords: list[dict] | None = None
