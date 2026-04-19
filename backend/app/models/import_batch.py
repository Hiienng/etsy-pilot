from sqlalchemy import Column, String, Integer, Text, DateTime
from sqlalchemy.sql import func
from ..core.database import Base


class ImportBatch(Base):
    __tablename__ = "import_batch"

    batch_id = Column(String(32), primary_key=True)  # YYYYMMDD_HHMM
    status = Column(String(16), nullable=False, default="uploaded")
    file_count = Column(Integer, default=0)
    listing_count = Column(Integer, default=0)
    keyword_count = Column(Integer, default=0)
    progress = Column(Integer, default=0)
    total_files = Column(Integer, default=0)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    confirmed_at = Column(DateTime(timezone=True), nullable=True)
    note = Column(Text, nullable=True)
    error_message = Column(Text, nullable=True)
