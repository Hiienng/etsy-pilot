import uuid
from datetime import datetime
from sqlalchemy import String, Text, DateTime, func
from sqlalchemy.orm import Mapped, mapped_column
from ..core.database import Base


class Listing(Base):
    __tablename__ = "listings"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
    idea_sku: Mapped[str | None] = mapped_column(String(64), index=True)
    ma_tam_listing: Mapped[str | None] = mapped_column(String(64))
    sample_sku: Mapped[str | None] = mapped_column(Text)

    title: Mapped[str] = mapped_column(Text, nullable=False)
    store: Mapped[str | None] = mapped_column(String(64), index=True)
    personalization: Mapped[str | None] = mapped_column(Text)

    description: Mapped[str | None] = mapped_column(Text)
    tag: Mapped[str | None] = mapped_column(Text)      # comma-separated tags
    attribute: Mapped[str | None] = mapped_column(Text)

    trang_thai: Mapped[str | None] = mapped_column(String(32), index=True)  # Open / Closed
    listing_id: Mapped[str | None] = mapped_column(String(32))
    listing_link: Mapped[str | None] = mapped_column(Text)
    media_link: Mapped[str | None] = mapped_column(Text)

    # AI-generated optimizations
    optimized_title: Mapped[str | None] = mapped_column(Text)
    optimized_tags: Mapped[str | None] = mapped_column(Text)
    optimized_description: Mapped[str | None] = mapped_column(Text)

    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
