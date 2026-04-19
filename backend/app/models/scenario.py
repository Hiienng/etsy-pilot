from sqlalchemy import String, Text, Integer
from sqlalchemy.orm import Mapped, mapped_column
from ..core.database import Base


class ScenarioRule(Base):
    __tablename__ = "scenarios_rules"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    roas_band: Mapped[str] = mapped_column(String(32), nullable=False)   # profitable / slight_loss / heavy_loss / no_sales
    cr_level: Mapped[str] = mapped_column(String(8), nullable=False)     # high / low
    ctr_level: Mapped[str] = mapped_column(String(8), nullable=False)    # high / low
    case_name: Mapped[str] = mapped_column(Text, nullable=False)         # Có sales và đang lời
    action: Mapped[str] = mapped_column(String(32), nullable=False)      # keep / improve / improve_or_off
    cause: Mapped[str | None] = mapped_column(Text)
    fix_listing: Mapped[str | None] = mapped_column(Text)
    fix_ads: Mapped[str | None] = mapped_column(Text)
