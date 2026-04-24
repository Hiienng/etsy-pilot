"""
Internal Ads Data Pipeline — business logic.

Handles: upload → extract → confirm → discard → rollback → history → snapshot.
"""
import json
import shutil
import struct
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import delete, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from ..models.import_batch import ImportBatch
from ..models.listing_report import ListingReport
from ..models.keyword_report import KeywordReport

# ── Image validation constants ──────────────────────────────────────────────
MIN_IMAGE_SIZE = 10 * 1024       # 10 KB — smaller is likely corrupt or icon
MAX_IMAGE_SIZE = 20 * 1024 * 1024  # 20 MB
MIN_DIMENSION = 200              # px — screenshots should be at least 200×200

# Magic bytes for supported formats
_MAGIC = {
    b"\x89PNG\r\n\x1a\n": "png",
    b"\xff\xd8\xff": "jpeg",
    b"RIFF": "webp",  # WebP starts with RIFF....WEBP
}

# Base paths relative to project root
_PROJECT_ROOT = Path(__file__).resolve().parents[3]  # backend/app/services -> project root
RAW_DIR = _PROJECT_ROOT / "data" / "raw" / "internal"
SNAPSHOT_DIR = _PROJECT_ROOT / "data" / "processed" / "snapshots"


def _now_batch_id() -> str:
    """Generate batch_id as YYYYMMDD_HHMM."""
    return datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")


# ── Image validation ─────────────────────────────────────────────────────────

def _detect_format(header: bytes) -> str | None:
    """Detect image format from first bytes (magic bytes)."""
    if header[:8] == b"\x89PNG\r\n\x1a\n":
        return "png"
    if header[:3] == b"\xff\xd8\xff":
        return "jpeg"
    if header[:4] == b"RIFF" and header[8:12] == b"WEBP":
        return "webp"
    return None


def _read_dimensions(data: bytes, fmt: str) -> tuple[int, int] | None:
    """Extract (width, height) from image bytes without PIL."""
    try:
        if fmt == "png":
            # IHDR chunk starts at byte 16: width(4B) + height(4B)
            if len(data) < 24:
                return None
            w = struct.unpack(">I", data[16:20])[0]
            h = struct.unpack(">I", data[20:24])[0]
            return w, h
        if fmt == "jpeg":
            # Scan for SOF0/SOF2 marker (0xFF 0xC0 or 0xFF 0xC2)
            i = 2
            while i < len(data) - 9:
                if data[i] != 0xFF:
                    break
                marker = data[i + 1]
                if marker in (0xC0, 0xC2):
                    h = struct.unpack(">H", data[i + 5 : i + 7])[0]
                    w = struct.unpack(">H", data[i + 7 : i + 9])[0]
                    return w, h
                # Skip to next marker
                seg_len = struct.unpack(">H", data[i + 2 : i + 4])[0]
                i += 2 + seg_len
            return None
        if fmt == "webp":
            # VP8 lossy: width/height at bytes 26-29
            if len(data) >= 30 and data[12:16] == b"VP8 ":
                w = struct.unpack("<H", data[26:28])[0] & 0x3FFF
                h = struct.unpack("<H", data[28:30])[0] & 0x3FFF
                return w, h
            # VP8L lossless: width/height packed in bytes 21-24
            if len(data) >= 25 and data[12:16] == b"VP8L":
                bits = struct.unpack("<I", data[21:25])[0]
                w = (bits & 0x3FFF) + 1
                h = ((bits >> 14) & 0x3FFF) + 1
                return w, h
            return None
    except Exception:
        return None
    return None


async def validate_image(filename: str, content: bytes) -> str | None:
    """
    Validate image content. Returns error message string, or None if valid.

    Checks:
    1. File size (10 KB – 20 MB)
    2. Magic bytes match actual image format (not just extension)
    3. Minimum dimensions (200×200) — screenshot should be readable
    """
    size = len(content)
    if size < MIN_IMAGE_SIZE:
        return f"{filename}: quá nhỏ ({size:,} bytes < {MIN_IMAGE_SIZE:,}). Có thể file bị lỗi."
    if size > MAX_IMAGE_SIZE:
        mb = size / (1024 * 1024)
        return f"{filename}: quá lớn ({mb:.1f} MB > 20 MB)."

    fmt = _detect_format(content[:12])
    if fmt is None:
        return f"{filename}: không phải ảnh hợp lệ (PNG/JPEG/WebP). File có thể bị đổi extension."

    dims = _read_dimensions(content, fmt)
    if dims is not None:
        w, h = dims
        if w < MIN_DIMENSION or h < MIN_DIMENSION:
            return (
                f"{filename}: ảnh quá nhỏ ({w}×{h}px). "
                f"Screenshot cần ít nhất {MIN_DIMENSION}×{MIN_DIMENSION}px."
            )

    return None


# ── Upload ───────────────────────────────────────────────────────────────────

async def save_uploaded_files(
    file_contents: list[tuple],
    db: AsyncSession,
) -> tuple[str, int, Path]:
    """
    Save pre-validated image files to data/raw/internal/{batch_id}/.
    Create import_batch record with status=uploaded.

    Args:
        file_contents: list of (UploadFile, bytes) tuples — content already read & validated.

    Returns (batch_id, file_count, batch_dir).
    """
    batch_id = _now_batch_id()
    batch_dir = RAW_DIR / batch_id
    batch_dir.mkdir(parents=True, exist_ok=True)

    count = 0
    for f, content in file_contents:
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

    On failure: resets status to "uploaded" with error_message so the user
    can retry instead of the batch being stuck at "extracting" forever.
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
    batch.error_message = None
    await db.flush()

    # Progress callback updates DB
    async def on_progress(done: int, total: int):
        batch.progress = done
        await db.flush()

    try:
        listing_rows, keyword_rows = await extract_batch(
            image_paths, on_progress=on_progress
        )
    except Exception as e:
        # Recovery: reset status so user can retry
        batch.status = "uploaded"
        batch.error_message = f"Extraction failed: {e}"
        batch.progress = 0
        await db.flush()
        raise ValueError(f"Extraction failed (batch reset to uploaded): {e}") from e

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
    batch.error_message = None
    await db.flush()

    return preview


# ── Confirm ──────────────────────────────────────────────────────────────────

async def confirm_import(
    batch_id: str,
    listing_report: list[dict],
    keyword_report: list[dict],
    no_vm: str | None,
    importer: str | None,
    db: AsyncSession,
) -> dict:
    """
    Import user-reviewed data into DB.
    1. Dedup: delete old records with same listing_id + period
    2. Insert new records with import_time + importer + no_vm
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

    now = datetime.now(timezone.utc)

    # 1. Deduplicate: delete old records with same listing_id + period
    if listing_report:
        listing_ids = list({r["listing_id"] for r in listing_report})
        periods = list({r["period"] for r in listing_report})
        await db.execute(
            text(
                "DELETE FROM listing_report "
                "WHERE listing_id = ANY(:lids) AND period = ANY(:periods)"
            ),
            {"lids": listing_ids, "periods": periods},
        )

    if keyword_report:
        kw_listing_ids = list({r["listing_id"] for r in keyword_report})
        kw_periods = list({r.get("period", "") for r in keyword_report})
        await db.execute(
            text(
                "DELETE FROM keyword_report "
                "WHERE listing_id = ANY(:lids) AND period = ANY(:periods)"
            ),
            {"lids": kw_listing_ids, "periods": kw_periods},
        )

    # 2. Insert new records — apply no_vm + importer + import_time to all rows
    vm = no_vm.strip() if no_vm else None

    lr_count = 0
    for row in listing_report:
        db.add(ListingReport(
            listing_id=row["listing_id"],
            title=row.get("title"),
            no_vm=vm or row.get("no_vm"),
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
            import_time=now,
            importer=importer,
        ))
        lr_count += 1

    kw_count = 0
    for row in keyword_report:
        db.add(KeywordReport(
            listing_id=row["listing_id"],
            keyword=row["keyword"],
            no_vm=vm or row.get("no_vm"),
            currently_status=row.get("currently_status"),
            period=row.get("period", ""),
            roas=row.get("roas", 0),
            orders=row.get("orders", 0),
            spend=row.get("spend", 0),
            revenue=row.get("revenue", 0),
            clicks=row.get("clicks", 0),
            click_rate=row.get("click_rate"),
            views=row.get("views", 0),
            import_time=now,
            importer=importer,
        ))
        kw_count += 1

    await db.flush()

    # 3. Save snapshot
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    snapshot = {
        "batch_id": batch_id,
        "confirmed_at": now.isoformat(),
        "no_vm": vm,
        "importer": importer,
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
    batch.confirmed_at = now
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
    """Revert a confirmed batch: delete DB rows by import_time, keep snapshot."""
    result = await db.execute(
        select(ImportBatch).where(ImportBatch.batch_id == batch_id)
    )
    batch = result.scalar_one_or_none()
    if not batch:
        raise ValueError(f"Batch {batch_id} not found")
    if batch.status != "confirmed":
        raise ValueError(f"Batch {batch_id} status is {batch.status}, expected confirmed")
    if not batch.confirmed_at:
        raise ValueError(f"Batch {batch_id} has no confirmed_at timestamp")

    # Delete rows matching the exact import_time (= batch.confirmed_at)
    await db.execute(
        delete(ListingReport).where(ListingReport.import_time == batch.confirmed_at)
    )
    await db.execute(
        delete(KeywordReport).where(KeywordReport.import_time == batch.confirmed_at)
    )

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
            "error_message": b.error_message,
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
