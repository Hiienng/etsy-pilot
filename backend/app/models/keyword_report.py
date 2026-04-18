from sqlalchemy import Column, Integer, String, Text, Numeric, DateTime, ForeignKey
from sqlalchemy.sql import func
from ..core.database import Base


class KeywordReport(Base):
    __tablename__ = "keyword_report"

    id = Column(Integer, primary_key=True, autoincrement=True)
    batch_id = Column(String(32), ForeignKey("import_batch.batch_id"), nullable=False)
    listing_id = Column(String(32), nullable=False)
    keyword = Column(Text, nullable=False)
    no_vm = Column(String(16), nullable=True)
    period = Column(String(32), nullable=False)
    roas = Column(Numeric(8, 2), default=0)
    orders = Column(Integer, default=0)
    spend = Column(Numeric(12, 2), default=0)
    revenue = Column(Numeric(12, 2), default=0)
    clicks = Column(Integer, default=0)
    click_rate = Column(String(8), nullable=True)
    views = Column(Integer, default=0)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
