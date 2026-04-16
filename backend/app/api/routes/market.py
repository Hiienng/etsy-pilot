from fastapi import APIRouter
from sqlalchemy import text
from ...core.database import AsyncSessionLocal

router = APIRouter(prefix="/market", tags=["market"])


@router.get("/samples")
async def get_market_samples():
    """
    Trả về map {product_type: listing_link} — lấy link đầu tiên có listing_link != null
    cho mỗi product_type trong bảng market_listing.
    Nếu chưa có link nào thì trả về null cho product_type đó.
    """
    async with AsyncSessionLocal() as session:
        result = await session.execute(text("""
            SELECT DISTINCT ON (product_type)
                product_type,
                listing_link
            FROM market_listing
            WHERE product_type IS NOT NULL
            ORDER BY product_type, listing_link NULLS LAST
        """))
        rows = result.fetchall()

    return {row.product_type: row.listing_link for row in rows}
