from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from ...core.database import get_db
from ...schemas.listing import OptimizeRequest, OptimizeResult
from ...services import listing_service, claude_service

router = APIRouter(prefix="/optimize", tags=["optimize"])


@router.post("/", response_model=OptimizeResult)
async def optimize_listing(req: OptimizeRequest, db: AsyncSession = Depends(get_db)):
    listing = await listing_service.get_listing(db, req.listing_id)
    if not listing:
        raise HTTPException(status_code=404, detail="Listing not found")

    result = OptimizeResult(listing_id=req.listing_id)

    if "title" in req.targets:
        result.optimized_title = await claude_service.optimize_title(
            listing.title, listing.tag, listing.description
        )

    if "tags" in req.targets:
        result.optimized_tags = await claude_service.optimize_tags(
            listing.title, listing.tag, listing.description
        )

    if "description" in req.targets:
        result.optimized_description = await claude_service.optimize_description(
            listing.title, listing.tag, listing.description, listing.personalization
        )

    await listing_service.save_optimizations(
        db,
        req.listing_id,
        result.optimized_title,
        result.optimized_tags,
        result.optimized_description,
    )

    return result
