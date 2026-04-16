from pydantic import BaseModel


class ListingDashboardItem(BaseModel):
    listing_id: str | None = None
    title: str | None = None
    product: str | None = None
    ctr: float | None = None
    cr: float | None = None
    roas: float | None = None
    url: str | None = None
    scenario_key: str | None = None
    scenario_label: str | None = None
    scenario_priority: str | None = None
    ref_title: str | None = None
    ref_url: str | None = None
