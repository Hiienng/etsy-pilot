from sqlalchemy import String, Text
from sqlalchemy.orm import Mapped, mapped_column
from ..core.database import Base


class ScenarioRule(Base):
    __tablename__ = "scenarios_rules"

    scenario_key: Mapped[str] = mapped_column(String(32), primary_key=True)
    label: Mapped[str] = mapped_column(Text, nullable=False)
    priority: Mapped[str] = mapped_column(String(16), nullable=False)  # low/medium/high/critical
