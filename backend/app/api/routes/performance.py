from fastapi import APIRouter, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from ...core.database import get_db
from ...schemas.performance import ListingDashboardItem
from ...services import performance_service

router = APIRouter(prefix="/performance", tags=["performance"])


@router.get("/listings", response_model=list[ListingDashboardItem])
async def get_listings_dashboard(db: AsyncSession = Depends(get_db)):
    """
    Danh sách listing kèm scenario (từ scenarios_rules) và
    reference sản phẩm market có tiềm năng doanh thu cao nhất (từ market_listing).
    """
    return await performance_service.get_dashboard_listings(db)
