from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.ext.asyncio import AsyncSession
from ...core.database import get_db
from ...schemas.listing import ListingCreate, ListingUpdate, ListingOut
from ...services import listing_service

router = APIRouter(prefix="/listings", tags=["listings"])


@router.get("/", response_model=list[ListingOut])
async def list_listings(
    skip: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    store: str | None = Query(None),
    status: str | None = Query(None),
    db: AsyncSession = Depends(get_db),
):
    return await listing_service.get_listings(db, skip, limit, store, status)


@router.get("/{listing_id}", response_model=ListingOut)
async def get_listing(listing_id: str, db: AsyncSession = Depends(get_db)):
    listing = await listing_service.get_listing(db, listing_id)
    if not listing:
        raise HTTPException(status_code=404, detail="Listing not found")
    return listing


@router.post("/", response_model=ListingOut, status_code=201)
async def create_listing(data: ListingCreate, db: AsyncSession = Depends(get_db)):
    return await listing_service.create_listing(db, data)


@router.patch("/{listing_id}", response_model=ListingOut)
async def update_listing(listing_id: str, data: ListingUpdate, db: AsyncSession = Depends(get_db)):
    listing = await listing_service.update_listing(db, listing_id, data)
    if not listing:
        raise HTTPException(status_code=404, detail="Listing not found")
    return listing


@router.delete("/{listing_id}", status_code=204)
async def delete_listing(listing_id: str, db: AsyncSession = Depends(get_db)):
    deleted = await listing_service.delete_listing(db, listing_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Listing not found")


@router.get("/stats/count")
async def listing_count(db: AsyncSession = Depends(get_db)):
    count = await listing_service.count_listings(db)
    return {"count": count}
