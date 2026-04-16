from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from ..models.listing import Listing
from ..schemas.listing import ListingCreate, ListingUpdate


async def get_listings(db: AsyncSession, skip: int = 0, limit: int = 50, store: str | None = None, status: str | None = None) -> list[Listing]:
    q = select(Listing)
    if store:
        q = q.where(Listing.store == store)
    if status:
        q = q.where(Listing.trang_thai == status)
    q = q.offset(skip).limit(limit).order_by(Listing.updated_at.desc())
    result = await db.execute(q)
    return list(result.scalars().all())


async def get_listing(db: AsyncSession, listing_id: str) -> Listing | None:
    result = await db.execute(select(Listing).where(Listing.id == listing_id))
    return result.scalar_one_or_none()


async def create_listing(db: AsyncSession, data: ListingCreate) -> Listing:
    listing = Listing(**data.model_dump())
    db.add(listing)
    await db.flush()
    await db.refresh(listing)
    return listing


async def update_listing(db: AsyncSession, listing_id: str, data: ListingUpdate) -> Listing | None:
    listing = await get_listing(db, listing_id)
    if not listing:
        return None
    for field, value in data.model_dump(exclude_none=True).items():
        setattr(listing, field, value)
    await db.flush()
    await db.refresh(listing)
    return listing


async def delete_listing(db: AsyncSession, listing_id: str) -> bool:
    listing = await get_listing(db, listing_id)
    if not listing:
        return False
    await db.delete(listing)
    return True


async def save_optimizations(db: AsyncSession, listing_id: str, optimized_title: str | None, optimized_tags: str | None, optimized_description: str | None) -> Listing | None:
    listing = await get_listing(db, listing_id)
    if not listing:
        return None
    if optimized_title:
        listing.optimized_title = optimized_title
    if optimized_tags:
        listing.optimized_tags = optimized_tags
    if optimized_description:
        listing.optimized_description = optimized_description
    await db.flush()
    await db.refresh(listing)
    return listing


async def count_listings(db: AsyncSession) -> int:
    result = await db.execute(select(func.count()).select_from(Listing))
    return result.scalar_one()
