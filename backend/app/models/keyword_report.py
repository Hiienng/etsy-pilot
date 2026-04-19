from sqlalchemy import Column, Integer, String, Text, Numeric, DateTime
from ..core.database import Base


class KeywordReport(Base):
    __tablename__ = "keyword_report"

    id = Column(Integer, primary_key=True, autoincrement=True)
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
    import_time = Column(DateTime(timezone=True), nullable=True)
    importer = Column(String(64), nullable=True)
