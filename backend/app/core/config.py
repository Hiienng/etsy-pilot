from pydantic_settings import BaseSettings
from functools import lru_cache


class Settings(BaseSettings):
    # Database (Neon PostgreSQL)
    DATABASE_URL: str = ""

    # Claude API — optional if Claude features are not used
    ANTHROPIC_API_KEY: str = ""
    CLAUDE_MODEL: str = "claude-sonnet-4-6"

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

    @property
    def async_db_url(self) -> str:
        """Build asyncpg-compatible URL. SSL is passed via connect_args, not URL param."""
        import re
        url = self.DATABASE_URL
        # Normalize driver prefix
        url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
        url = url.replace("postgres://", "postgresql+asyncpg://", 1)
        # Strip params unsupported by asyncpg — SSL handled via connect_args
        url = re.sub(r"[?&]sslmode=[^&]*", "", url)
        url = re.sub(r"[?&]ssl=[^&]*", "", url)
        url = re.sub(r"[?&]channel_binding=[^&]*", "", url)
        return url

    class Config:
        env_file = ("../.env", ".env")
        env_file_encoding = "utf-8"
        extra = "ignore"


@lru_cache
def get_settings() -> Settings:
    return Settings()
