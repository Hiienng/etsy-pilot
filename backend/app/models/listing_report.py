from sqlalchemy import Column, Integer, String, Text, Numeric, DateTime, ForeignKey
from sqlalchemy.sql import func
from ..core.database import Base


class ListingReport(Base):
    __tablename__ = "listing_report"

    id = Column(Integer, primary_key=True, autoincrement=True)
    batch_id = Column(String(32), ForeignKey("import_batch.batch_id"), nullable=False)
    listing_id = Column(String(32), nullable=False)
    title = Column(Text, nullable=True)
    no_vm = Column(String(16), nullable=True)
    price = Column(Numeric(10, 2), nullable=True)
    stock = Column(Integer, nullable=True)
    category = Column(String(64), nullable=True)
    lifetime_orders = Column(Integer, nullable=True)
    lifetime_revenue = Column(Numeric(12, 2), nullable=True)
    period = Column(String(32), nullable=False)
    views = Column(Integer, default=0)
    clicks = Column(Integer, default=0)
    orders = Column(Integer, default=0)
    revenue = Column(Numeric(12, 2), default=0)
    spend = Column(Numeric(12, 2), default=0)
    roas = Column(Numeric(8, 2), default=0)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
