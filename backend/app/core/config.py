from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # Database (Neon PostgreSQL) — internal data
    DATABASE_URL: str = ""
    # Market data DB (etsy_star_engine output)
    ETSY_MARKET_DB: str = ""

    # AI Vision — Gemini for screenshot extraction
    GEMINI_API_KEY: str = ""
    GEMINI_MODEL: str = "gemini-2.0-flash"

    # App
    APP_ENV: str = "development"
    APP_HOST: str = "0.0.0.0"
    APP_PORT: int = 8000
    SECRET_KEY: str = ""

    # CORS
    ALLOWED_ORIGINS: str = "http://localhost:3000"

    @property
    def origins(self) -> list[str]:
        return [o.strip() for o in self.ALLOWED_ORIGINS.split(",")]

    @staticmethod
    def _normalize_asyncpg_url(raw: str) -> str:
        import re
        url = raw.replace("postgresql://", "postgresql+asyncpg://", 1)
        url = url.replace("postgres://", "postgresql+asyncpg://", 1)
        url = re.sub(r"[?&]sslmode=[^&]*", "", url)
        url = re.sub(r"[?&]ssl=[^&]*", "", url)
        url = re.sub(r"[?&]channel_binding=[^&]*", "", url)
        return url

    @property
    def async_db_url(self) -> str:
        return self._normalize_asyncpg_url(self.DATABASE_URL)

    @property
    def async_market_db_url(self) -> str:
        return self._normalize_asyncpg_url(self.ETSY_MARKET_DB or self.DATABASE_URL)

    class Config:
        env_file = ("../.env", ".env")
        env_file_encoding = "utf-8"
        extra = "ignore"


@lru_cache
def get_settings() -> Settings:
    return Settings()
