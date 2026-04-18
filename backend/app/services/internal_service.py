"""
Internal Ads Data Pipeline — business logic.

Handles: upload → extract → confirm → discard → rollback → history → snapshot.
"""
import json
import shutil
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import delete, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from ..models.import_batch import ImportBatch
from ..models.listing_report import ListingReport
from ..models.keyword_report import KeywordReport

# Base paths relative to project root
_PROJECT_ROOT = Path(__file__).resolve().parents[3]  # backend/app/services -> project root
RAW_DIR = _PROJECT_ROOT / "data" / "raw" / "internal"
SNAPSHOT_DIR = _PROJECT_ROOT / "data" / "processed" / "snapshots"


def _now_batch_id() -> str:
    """Generate batch_id as YYYYMMDD_HHMM."""
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")


# ── Upload ───────────────────────────────────────────────────────────────────

async def save_uploaded_files(
    files: list,
    db: AsyncSession,
) -> tuple[str, int, Path]:
    """
    Save uploaded image files to data/raw/internal/{batch_id}/.
    Create import_batch record with status=uploaded.
    Returns (batch_id, file_count, batch_dir).
    """
    batch_id = _now_batch_id()
    batch_dir = RAW_DIR / batch_id
    batch_dir.mkdir(parents=True, exist_ok=True)

    count = 0
    for f in files:
        content = await f.read()
        dest = batch_dir / f.filename
        dest.write_bytes(content)
        count += 1

    batch = ImportBatch(
        batch_id=batch_id,
        status="uploaded",
        file_count=count,
        total_files=count,
    )
    db.add(batch)
    await db.flush()

    return batch_id, count, batch_dir


# ── Extract ──────────────────────────────────────────────────────────────────

async def run_extraction(batch_id: str, db: AsyncSession) -> dict:
    """
    Run Claude Vision extraction on all images in the batch.
    Updates progress in import_batch. Saves preview JSON.
    Returns {listing_report, keyword_report}.
    """
    from .internal_extractor import extract_batch

    batch_dir = RAW_DIR / batch_id
    if not batch_dir.exists():
        raise FileNotFoundError(f"Batch dir not found: {batch_dir}")

    image_paths = sorted(
        str(p) for p in batch_dir.iterdir()
        if p.suffix.lower() in (".png", ".jpg", ".jpeg", ".webp")
    )
    if not image_paths:
        raise ValueError("No images found in batch directory")

    # Update status to extracting
    result = await db.execute(
        select(ImportBatch).where(ImportBatch.batch_id == batch_id)
    )
    batch = result.scalar_one_or_none()
    if not batch:
        raise ValueError(f"Batch {batch_id} not found")

    batch.status = "extracting"
    batch.total_files = len(image_paths)
    batch.progress = 0
    await db.flush()

    # Progress callback updates DB
    async def on_progress(done: int, total: int):
        batch.progress = done
        await db.flush()

    listing_rows, keyword_rows = await extract_batch(
        image_paths, batch_id, on_progress=on_progress
    )

    # Save preview JSON to batch dir
    preview = {
        "batch_id": batch_id,
        "listing_report": listing_rows,
        "keyword_report": keyword_rows,
    }
    preview_path = batch_dir / "preview.json"
    preview_path.write_text(json.dumps(preview, ensure_ascii=False, default=str))

    # Update batch
    batch.status = "extracted"
    batch.listing_count = len(listing_rows)
    batch.keyword_count = len(keyword_rows)
    batch.progress = len(image_paths)
    await db.flush()

    return preview


# ── Confirm ──────────────────────────────────────────────────────────────────

async def confirm_import(
    batch_id: str,
    listing_report: list[dict],
    keyword_report: list[dict],
    db: AsyncSession,
) -> dict:
    """
    Import user-reviewed data into DB.
    1. Delete old records with same listing_id + period (from other batches)
    2. Insert new records
    3. Save snapshot JSON
    4. Delete raw images
    5. Update batch status
    """
    result = await db.execute(
        select(ImportBatch).where(ImportBatch.batch_id == batch_id)
    )
    batch = result.scalar_one_or_none()
    if not batch:
        raise ValueError(f"Batch {batch_id} not found")
    if batch.status not in ("extracted", "uploaded"):
        raise ValueError(f"Batch {batch_id} status is {batch.status}, expected extracted")

    # 1. Deduplicate: delete old records with same listing_id + period from OTHER batches
    if listing_report:
        listing_ids = list({r["listing_id"] for r in listing_report})
        periods = list({r["period"] for r in listing_report})
        await db.execute(
            text(
                "DELETE FROM listing_report "
                "WHERE listing_id = ANY(:lids) AND period = ANY(:periods) "
                "AND batch_id != :bid"
            ),
            {"lids": listing_ids, "periods": periods, "bid": batch_id},
        )

    if keyword_report:
        kw_listing_ids = list({r["listing_id"] for r in keyword_report})
        kw_periods = list({r.get("period", "") for r in keyword_report})
        await db.execute(
            text(
                "DELETE FROM keyword_report "
                "WHERE listing_id = ANY(:lids) AND period = ANY(:periods) "
                "AND batch_id != :bid"
            ),
            {"lids": kw_listing_ids, "periods": kw_periods, "bid": batch_id},
        )

    # 2. Insert new records
    lr_count = 0
    for row in listing_report:
        db.add(ListingReport(
            batch_id=batch_id,
            listing_id=row["listing_id"],
            title=row.get("title"),
            no_vm=row.get("no_vm"),
            price=row.get("price"),
            stock=row.get("stock"),
            category=row.get("category"),
            lifetime_orders=row.get("lifetime_orders"),
            lifetime_revenue=row.get("lifetime_revenue"),
            period=row["period"],
            views=row.get("views", 0),
            clicks=row.get("clicks", 0),
            orders=row.get("orders", 0),
            revenue=row.get("revenue", 0),
            spend=row.get("spend", 0),
            roas=row.get("roas", 0),
        ))
        lr_count += 1

    kw_count = 0
    for row in keyword_report:
        db.add(KeywordReport(
            batch_id=batch_id,
            listing_id=row["listing_id"],
            keyword=row["keyword"],
            no_vm=row.get("no_vm"),
            period=row.get("period", ""),
            roas=row.get("roas", 0),
            orders=row.get("orders", 0),
            spend=row.get("spend", 0),
            revenue=row.get("revenue", 0),
            clicks=row.get("clicks", 0),
            click_rate=row.get("click_rate"),
            views=row.get("views", 0),
        ))
        kw_count += 1

    await db.flush()

    # 3. Save snapshot
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    snapshot = {
        "batch_id": batch_id,
        "confirmed_at": datetime.now(timezone.utc).isoformat(),
        "listing_report": listing_report,
        "keyword_report": keyword_report,
    }
    snapshot_path = SNAPSHOT_DIR / f"{batch_id}.json"
    snapshot_path.write_text(json.dumps(snapshot, ensure_ascii=False, default=str))

    # 4. Delete raw images
    batch_dir = RAW_DIR / batch_id
    if batch_dir.exists():
        shutil.rmtree(batch_dir)

    # 5. Update batch
    batch.status = "confirmed"
    batch.listing_count = lr_count
    batch.keyword_count = kw_count
    batch.confirmed_at = datetime.now(timezone.utc)
    await db.flush()

    return {"imported": True, "rows": {"listing": lr_count, "keyword": kw_count}}


# ── Discard ──────────────────────────────────────────────────────────────────

async def discard_batch(batch_id: str, db: AsyncSession) -> None:
    """Cancel a pending batch: delete raw images, mark discarded."""
    result = await db.execute(
        select(ImportBatch).where(ImportBatch.batch_id == batch_id)
    )
    batch = result.scalar_one_or_none()
    if not batch:
        raise ValueError(f"Batch {batch_id} not found")

    batch_dir = RAW_DIR / batch_id
    if batch_dir.exists():
        shutil.rmtree(batch_dir)

    batch.status = "discarded"
    await db.flush()


# ── Rollback ─────────────────────────────────────────────────────────────────

async def rollback_batch(batch_id: str, db: AsyncSession) -> None:
    """Revert a confirmed batch: delete DB rows, keep snapshot."""
    result = await db.execute(
        select(ImportBatch).where(ImportBatch.batch_id == batch_id)
    )
    batch = result.scalar_one_or_none()
    if not batch:
        raise ValueError(f"Batch {batch_id} not found")
    if batch.status != "confirmed":
        raise ValueError(f"Batch {batch_id} status is {batch.status}, expected confirmed")

    await db.execute(delete(ListingReport).where(ListingReport.batch_id == batch_id))
    await db.execute(delete(KeywordReport).where(KeywordReport.batch_id == batch_id))

    batch.status = "rolled_back"
    await db.flush()


# ── History ──────────────────────────────────────────────────────────────────

async def get_history(db: AsyncSession, limit: int = 20) -> list[dict]:
    """Return recent import batches."""
    result = await db.execute(
        select(ImportBatch)
        .order_by(ImportBatch.created_at.desc())
        .limit(limit)
    )
    batches = result.scalars().all()
    return [
        {
            "batch_id": b.batch_id,
            "status": b.status,
            "file_count": b.file_count or 0,
            "listing_count": b.listing_count or 0,
            "keyword_count": b.keyword_count or 0,
            "created_at": b.created_at,
            "confirmed_at": b.confirmed_at,
            "note": b.note,
        }
        for b in batches
    ]


# ── Snapshot ─────────────────────────────────────────────────────────────────

def get_snapshot(batch_id: str) -> dict | None:
    """Read the snapshot JSON for a confirmed/rolled_back batch."""
    path = SNAPSHOT_DIR / f"{batch_id}.json"
    if not path.exists():
        return None
    return json.loads(path.read_text())
