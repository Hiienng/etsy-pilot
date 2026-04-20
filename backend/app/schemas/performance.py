from decimal import Decimal
from pydantic import BaseModel


class ListingDashboardItem(BaseModel):
    listing_id: str | None = None
    title: str | None = None
    product: str | None = None
    ctr: Decimal | None = None
    cr: Decimal | None = None
    roas: Decimal | None = None
    url: str | None = None
    views: int | None = None
    clicks: int | None = None
    orders: int | None = None
    revenue: Decimal | None = None
    spend: Decimal | None = None
    # scenario (from scenarios_rules JOIN)
    scenario_action: str | None = None       # keep / improve / improve_or_off
    scenario_label: str | None = None        # e.g. "Có sales và đang lời"
    scenario_cause: str | None = None
    scenario_fix_listing: str | None = None
    scenario_fix_ads: str | None = None
    # reference (from market_listing LATERAL)
    ref_title: str | None = None
    ref_shop: str | None = None
