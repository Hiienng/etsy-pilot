from datetime import datetime
from pydantic import BaseModel, Field


class ListingBase(BaseModel):
    title: str
    idea_sku: str | None = None
    store: str | None = None
    personalization: str | None = None
    description: str | None = None
    tag: str | None = None
    attribute: str | None = None
    trang_thai: str | None = None
    listing_id: str | None = None
    listing_link: str | None = None


class ListingCreate(ListingBase):
    pass


class ListingUpdate(BaseModel):
    title: str | None = None
    store: str | None = None
    personalization: str | None = None
    description: str | None = None
    tag: str | None = None
    trang_thai: str | None = None


class ListingOut(ListingBase):
    id: str
    optimized_title: str | None = None
    optimized_tags: str | None = None
    optimized_description: str | None = None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


class OptimizeRequest(BaseModel):
    listing_id: str
    targets: list[str] = Field(
        default=["title", "tags", "description"],
        description="Các trường cần optimize: title | tags | description",
    )


class OptimizeResult(BaseModel):
    listing_id: str
    optimized_title: str | None = None
    optimized_tags: str | None = None
    optimized_description: str | None = None
