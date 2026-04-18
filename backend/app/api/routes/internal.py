"""
Internal Ads Data Pipeline — API routes.

Prefix: /api/v1/internal
"""
import asyncio
from fastapi import APIRouter, Depends, File, HTTPException, UploadFile
from sqlalchemy.ext.asyncio import AsyncSession

from ...core.database import get_db, AsyncSessionLocal
from ...schemas.internal import (
    UploadResponse,
    ExtractResponse,
    ConfirmRequest,
    ConfirmResponse,
    BatchActionResponse,
    BatchHistoryItem,
)
from ...services import internal_service

router = APIRouter(prefix="/internal", tags=["internal"])


# ── POST /upload — receive images, create batch ─────────────────────────────

@router.post("/upload", response_model=UploadResponse)
async def upload_screenshots(
    files: list[UploadFile] = File(...),
    db: AsyncSession = Depends(get_db),
):
    if not files:
        raise HTTPException(400, "No files provided")
    if len(files) > 100:
        raise HTTPException(400, "Maximum 100 files per batch")

    allowed = {".png", ".jpg", ".jpeg", ".webp"}
    for f in files:
        ext = "." + f.filename.rsplit(".", 1)[-1].lower() if "." in f.filename else ""
        if ext not in allowed:
            raise HTTPException(400, f"File type not allowed: {f.filename}")

    batch_id, count, _ = await internal_service.save_uploaded_files(files, db)
    return UploadResponse(batch_id=batch_id, file_count=count)


# ── POST /extract — run Claude Vision on batch images ────────────────────────

@router.post("/extract", response_model=ExtractResponse)
async def extract_batch(batch_id: str, db: AsyncSession = Depends(get_db)):
    """
    Start extraction. For batches with many images, this runs Claude Vision
    concurrently (5 at a time) and updates progress in the DB.
    """
    try:
        preview = await internal_service.run_extraction(batch_id, db)
    except FileNotFoundError:
        raise HTTPException(404, f"Batch directory not found: {batch_id}")
    except ValueError as e:
        raise HTTPException(400, str(e))

    return ExtractResponse(
        batch_id=batch_id,
        status="extracted",
        listing_report=preview.get("listing_report", []),
        keyword_report=preview.get("keyword_report", []),
        progress=len(preview.get("listing_report", []) + preview.get("keyword_report", [])),
        total_files=preview.get("total_files", 0),
    )


# ── POST /confirm — write reviewed data to DB ───────────────────────────────

@router.post("/confirm", response_model=ConfirmResponse)
async def confirm_import(req: ConfirmRequest, db: AsyncSession = Depends(get_db)):
    try:
        result = await internal_service.confirm_import(
            batch_id=req.batch_id,
            listing_report=[r.model_dump() for r in req.listing_report],
            keyword_report=[r.model_dump() for r in req.keyword_report],
            db=db,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))

    return ConfirmResponse(imported=result["imported"], rows=result["rows"])


# ── POST /discard — cancel pending batch ─────────────────────────────────────

@router.post("/discard", response_model=BatchActionResponse)
async def discard_batch(batch_id: str, db: AsyncSession = Depends(get_db)):
    try:
        await internal_service.discard_batch(batch_id, db)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return BatchActionResponse(batch_id=batch_id, status="discarded")


# ── POST /rollback — revert confirmed batch ──────────────────────────────────

@router.post("/rollback", response_model=BatchActionResponse)
async def rollback_batch(batch_id: str, db: AsyncSession = Depends(get_db)):
    try:
        await internal_service.rollback_batch(batch_id, db)
    except ValueError as e:
        raise HTTPException(400, str(e))
    return BatchActionResponse(batch_id=batch_id, status="rolled_back")


# ── GET /history — list import batches ───────────────────────────────────────

@router.get("/history", response_model=list[BatchHistoryItem])
async def import_history(
    limit: int = 20,
    db: AsyncSession = Depends(get_db),
):
    return await internal_service.get_history(db, limit)


# ── GET /snapshot/{batch_id} — view confirmed data ──────────────────────────

@router.get("/snapshot/{batch_id}")
async def get_snapshot(batch_id: str):
    data = internal_service.get_snapshot(batch_id)
    if data is None:
        raise HTTPException(404, f"Snapshot not found for batch {batch_id}")
    return data
