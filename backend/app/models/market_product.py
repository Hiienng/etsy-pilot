from sqlalchemy import Column, Integer, String, Text, Numeric, Boolean, DateTime
from sqlalchemy.sql import func
from ..core.database import Base


class MarketProduct(Base):
    __tablename__ = "market_product"

    id = Column(Integer, primary_key=True, autoincrement=True)
    batch_id = Column(String(64), nullable=True)
    source_screenshot = Column(String(256), nullable=True)
    search_tag = Column(String(128), nullable=True, index=True)
    etsy_best = Column(String(32), nullable=True)
    product_type = Column(String(128), nullable=True, index=True)
    category = Column(String(128), nullable=True, index=True)
    title = Column(Text, nullable=True)
    price = Column(Integer, nullable=True)
    original_price = Column(Integer, nullable=True)
    discount = Column(Integer, nullable=True)
    shop_name = Column(String(256), nullable=True)
    rating = Column(Numeric(3, 1), nullable=True)
    review_count = Column(Integer, nullable=True)
    badge = Column(String(32), nullable=True)
    is_ad = Column(Boolean, default=False)
    free_shipping = Column(Boolean, default=False)
    image_description = Column(Text, nullable=True)
    scroll_position = Column(Integer, nullable=True)
    crawled_at = Column(DateTime(timezone=True), server_default=func.now())
