from fastapi import APIRouter
from sqlalchemy import text
from ...core.database import AsyncSessionLocal

router = APIRouter(prefix="/market", tags=["market"])


@router.get("/samples")
async def get_market_samples():
    """
    Trả về map {product_type: url} — lấy link đầu tiên có url != null
    cho mỗi product_type trong bảng market_listing.
    Nếu chưa có link nào thì trả về null cho product_type đó.
    """
    async with AsyncSessionLocal() as session:
        result = await session.execute(text("""
            SELECT DISTINCT ON (product_type)
                product_type,
                url
            FROM market_listing
            WHERE product_type IS NOT NULL
            ORDER BY product_type, url NULLS LAST
        """))
        rows = result.fetchall()

    return {row.product_type: row.url for row in rows}
