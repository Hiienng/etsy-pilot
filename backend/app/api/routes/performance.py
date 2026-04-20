import asyncio
import traceback
from pathlib import Path
from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from sqlalchemy.ext.asyncio import AsyncSession
from ...core.database import get_db
from ...schemas.performance import ListingDashboardItem
from ...services import performance_service

router = APIRouter(prefix="/performance", tags=["performance"])

_DASHBOARD_JSON = Path(__file__).resolve().parents[4] / "data" / "processed" / "performance_dashboard.json"


@router.get("/listings", response_model=list[ListingDashboardItem])
async def get_listings_dashboard(db: AsyncSession = Depends(get_db)):
    try:
        return await performance_service.get_dashboard_listings(db)
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "trace": traceback.format_exc()})


@router.post("/refresh")
async def refresh_dashboard(db: AsyncSession = Depends(get_db)):
    try:
        listings = await performance_service.get_dashboard_listings(db)
        await asyncio.to_thread(performance_service.write_dashboard_json, listings, _DASHBOARD_JSON)
        return {"status": "ok", "count": len(listings)}
    except Exception as e:
        return JSONResponse(status_code=500, content={"error": str(e), "trace": traceback.format_exc()})
