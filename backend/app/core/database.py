from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from .config import get_settings

settings = get_settings()

engine = create_async_engine(
    settings.async_db_url,
    echo=settings.APP_ENV == "development",
    pool_size=5,
    max_overflow=10,
    connect_args={"ssl": True},
)

AsyncSessionLocal = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
)

# Second engine — market data DB (ETSY_MARKET_DB / etsy_star_engine output)
# Built lazily so env var is read at first use, not at import time.
_market_engine = None
_MarketSessionLocal = None


def _get_market_session_factory() -> async_sessionmaker:
    global _market_engine, _MarketSessionLocal
    if _MarketSessionLocal is None:
        _market_engine = create_async_engine(
            get_settings().async_market_db_url,
            echo=get_settings().APP_ENV == "development",
            pool_size=3,
            max_overflow=5,
            connect_args={"ssl": True},
        )
        _MarketSessionLocal = async_sessionmaker(
            bind=_market_engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )
    return _MarketSessionLocal


class MarketSessionLocal:
    """Context manager proxy — creates engine lazily on first use."""
    def __new__(cls):
        return _get_market_session_factory()()


class Base(DeclarativeBase):
    pass


async def get_db() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


async def create_tables() -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # Safe migrations — add columns that don't exist yet.
    # Each entry: (table, column, DDL type)
    _MIGRATIONS = [
        ("import_batch", "error_message", "TEXT"),
        ("listing_report", "import_time", "TIMESTAMPTZ"),
        ("listing_report", "importer", "VARCHAR(64)"),
        ("keyword_report", "import_time", "TIMESTAMPTZ"),
        ("keyword_report", "importer", "VARCHAR(64)"),
        ("keyword_report", "relevant", "VARCHAR(8)"),
    ]
    async with engine.begin() as conn:
        for table, col, ddl_type in _MIGRATIONS:
            await conn.execute(
                text(
                    f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {col} {ddl_type}"
                )
            )
