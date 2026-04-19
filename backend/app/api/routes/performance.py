import traceback
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession
from ...core.database import get_db
from ...schemas.performance import ListingDashboardItem
from ...services import performance_service

router = APIRouter(prefix="/performance", tags=["performance"])


@router.get("/listings", response_model=list[ListingDashboardItem])
async def get_listings_dashboard(db: AsyncSession = Depends(get_db)):
    try:
        return await performance_service.get_dashboard_listings(db)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "trace": traceback.format_exc()})
