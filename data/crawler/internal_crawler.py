"""
Internal Listing Crawler
Đọc screenshots từ Etsy Ads dashboard (data/raw/internal/<date>-<vm>/),
extract performance metrics bằng Claude Vision, upsert vào bảng listing_report.

Cách dùng:
    cd data/crawler
    python internal_crawler.py --folder ../raw/internal/20-04-2026-VM01

Mỗi ảnh chứa 1 tab metric của 1 listing — header luôn hiển thị đủ 6 metrics tổng.
Script dedup theo listing_id (giữ record đầu đủ data nhất) trước khi upsert.
"""

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import google.generativeai as genai
from PIL import Image

# ── Config ─────────────────────────────────────────────────────────────────────

import os

_env_path = Path(__file__).parents[2] / ".env"
if _env_path.exists():
    for _line in _env_path.read_text().splitlines():
        if "=" in _line and not _line.startswith("#"):
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")
DATABASE_URL   = os.environ.get("DATABASE_URL", "")

IMAGE_EXTS  = {".jpg", ".jpeg", ".png", ".webp"}

# ── Extraction prompt ──────────────────────────────────────────────────────────

EXTRACTION_PROMPT = """Đây là screenshot từ trang Etsy Ads > Listing stats.

Hãy extract các thông tin sau và trả về JSON object (không có text thêm):

- listing_id: số ID trong URL (ví dụ URL ".../listings/4409031723?..." → "4409031723")
- title: tiêu đề listing đầy đủ
- price_usd: giá bán, số thực, đơn vị USD (ví dụ "$24.27" → 24.27, null nếu không thấy)
- stock: số lượng tồn kho (ví dụ "310 in stock" → 310, null nếu không thấy)
- section: danh mục/section (ví dụ "Sweater", "Blanket", null nếu không thấy)
- lifetime_orders: số đơn hàng lifetime (ví dụ "Lifetime ad orders: 315" → 315, null nếu không thấy)
- lifetime_revenue_usd: doanh thu lifetime, số thực USD (ví dụ "Lifetime ad revenue: $11,851.83" → 11851.83, null nếu không thấy)
- period: chuỗi ngày tháng của date range hiển thị (ví dụ "04/11/2026 - 04/18/2026")
- views: tổng views trong period (số nguyên, null nếu không thấy — "12.1K" → 12100)
- clicks: tổng clicks trong period (số nguyên)
- orders: tổng orders trong period (số nguyên)
- revenue_usd: tổng revenue trong period, số thực USD (ví dụ "$517.85" → 517.85)
- spend_usd: tổng spend trong period, số thực USD
- roas: ROAS trong period, số thực (ví dụ "2.34" → 2.34, "0" → 0.0)

Quy tắc:
- Tất cả 6 metrics summary (views/clicks/orders/revenue/spend/roas) luôn hiển thị ở header.
- Nếu giá trị là "0" hoặc "$0" → trả về 0 (không phải null).
- Nếu một field thực sự không đọc được → null.

Chỉ trả về JSON object, không có markdown hay text thêm.
"""

# ── Gemini Vision ──────────────────────────────────────────────────────────────

def _get_client():
    if not GEMINI_API_KEY:
        raise SystemExit("[!] Thiếu GEMINI_API_KEY trong .env")
    genai.configure(api_key=GEMINI_API_KEY)
    return genai.GenerativeModel("gemini-1.5-flash")


def extract_from_screenshot(path: Path, client) -> dict | None:
    """Gọi Gemini Vision, trả về dict hoặc None nếu thất bại."""
    print(f"  Extracting: {path.name}")
    img = Image.open(path)

    try:
        response = client.generate_content([EXTRACTION_PROMPT, img])
        text = response.text.strip()
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
        data = json.loads(text)
        print(f"    → listing_id={data.get('listing_id')} | "
              f"views={data.get('views')} clicks={data.get('clicks')} "
              f"orders={data.get('orders')} roas={data.get('roas')}")
        return data
    except json.JSONDecodeError as e:
        print(f"    Lỗi JSON: {e} | response: {text[:200]}")
        return None
    except Exception as e:
        print(f"    Lỗi API: {e}")
        return None


# ── Merge / dedup ──────────────────────────────────────────────────────────────

def _completeness(r: dict) -> int:
    """Đếm số field có giá trị (không None) để chọn record đầy đủ nhất."""
    keys = ["title", "price_usd", "stock", "section",
            "lifetime_orders", "lifetime_revenue_usd",
            "period", "views", "clicks", "orders",
            "revenue_usd", "spend_usd", "roas"]
    return sum(1 for k in keys if r.get(k) is not None)


def merge_records(extractions: list[dict]) -> list[dict]:
    """
    Merge nhiều extractions của cùng listing_id thành 1 record.
    Với mỗi listing, giữ record có nhiều field nhất, sau đó fill-in
    các field còn null từ các record khác.
    """
    by_id: dict[str, list[dict]] = {}
    for r in extractions:
        lid = str(r.get("listing_id", "")).strip()
        if not lid:
            continue
        by_id.setdefault(lid, []).append(r)

    merged = []
    for lid, records in by_id.items():
        # Sắp xếp theo completeness giảm dần
        records.sort(key=_completeness, reverse=True)
        base = dict(records[0])
        # Fill null fields từ các records khác
        for fallback in records[1:]:
            for k, v in fallback.items():
                if base.get(k) is None and v is not None:
                    base[k] = v
        merged.append(base)

    return merged


# ── PostgreSQL upsert ──────────────────────────────────────────────────────────

def _pg_dsn() -> str:
    url = DATABASE_URL
    if not url:
        raise SystemExit("[!] DATABASE_URL chưa cấu hình")
    url = url.replace("postgresql+asyncpg://", "postgresql://", 1)
    url = url.replace("postgres://", "postgresql://", 1)
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    qs.pop("channel_binding", None)
    if "sslmode" not in qs:
        qs["sslmode"] = ["require"]
    return urlunparse(parsed._replace(query=urlencode({k: v[0] for k, v in qs.items()})))


def upsert_listing_reports(records: list[dict], no_vm: str) -> int:
    """
    Upsert vào bảng listing_report dùng SELECT + INSERT/UPDATE
    (không cần unique constraint trên listing_id + period).
    """
    if not records:
        return 0

    import psycopg2
    dsn  = _pg_dsn()
    conn = psycopg2.connect(dsn)
    cur  = conn.cursor()
    now  = datetime.now(timezone.utc)
    count = 0

    for r in records:
        lid    = str(r.get("listing_id", "")).strip()
        period = str(r.get("period", "")).strip()
        if not lid or not period:
            continue

        price    = _to_numeric(r.get("price_usd"))
        lt_rev   = _to_numeric(r.get("lifetime_revenue_usd"))
        revenue  = _to_numeric(r.get("revenue_usd"))
        spend    = _to_numeric(r.get("spend_usd"))
        roas_val = _to_numeric(r.get("roas"))

        cur.execute(
            "SELECT id FROM listing_report WHERE listing_id = %s AND period = %s",
            (lid, period),
        )
        existing = cur.fetchone()

        if existing:
            cur.execute("""
                UPDATE listing_report SET
                    title            = COALESCE(%s, title),
                    no_vm            = %s,
                    price            = COALESCE(%s, price),
                    stock            = COALESCE(%s, stock),
                    category         = COALESCE(%s, category),
                    lifetime_orders  = COALESCE(%s, lifetime_orders),
                    lifetime_revenue = COALESCE(%s, lifetime_revenue),
                    views            = COALESCE(%s, views),
                    clicks           = COALESCE(%s, clicks),
                    orders           = COALESCE(%s, orders),
                    revenue          = COALESCE(%s, revenue),
                    spend            = COALESCE(%s, spend),
                    roas             = COALESCE(%s, roas),
                    import_time      = %s
                WHERE id = %s
            """, (
                r.get("title"), no_vm, price, r.get("stock"), r.get("section"),
                r.get("lifetime_orders"), lt_rev,
                r.get("views"), r.get("clicks"), r.get("orders"),
                revenue, spend, roas_val,
                now, existing[0],
            ))
        else:
            cur.execute("""
                INSERT INTO listing_report
                    (listing_id, title, no_vm, price, stock, category,
                     lifetime_orders, lifetime_revenue,
                     period, views, clicks, orders, revenue, spend, roas,
                     import_time, importer)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'internal_crawler')
            """, (
                lid, r.get("title"), no_vm, price, r.get("stock"), r.get("section"),
                r.get("lifetime_orders"), lt_rev, period,
                r.get("views"), r.get("clicks"), r.get("orders"),
                revenue, spend, roas_val, now,
            ))
        count += cur.rowcount

    conn.commit()
    conn.close()
    return count


def _to_numeric(val):
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


# ── Main ────────────────────────────────────────────────────────────────────────

def parse_vm(folder: Path) -> str:
    """Lấy VM id từ tên folder: '20-04-2026-VM01' → 'VM01'."""
    parts = folder.name.split("-")
    for p in reversed(parts):
        if p.upper().startswith("VM"):
            return p.upper()
    return folder.name


def run(folder: Path, dry_run: bool = False):
    print("=" * 62)
    print(f"  Internal Listing Crawler — {folder.name}")
    print("=" * 62)

    images = sorted(
        p for p in folder.iterdir()
        if p.is_file() and p.suffix.lower() in IMAGE_EXTS
    )
    if not images:
        print("[!] Không tìm thấy ảnh trong folder.")
        sys.exit(1)

    print(f"\n[1/3] Tìm thấy {len(images)} ảnh. Bắt đầu extract...")
    client = _get_client()
    extractions = []
    for img in images:
        result = extract_from_screenshot(img, client)
        if result:
            extractions.append(result)

    print(f"\n[2/3] Extracted {len(extractions)}/{len(images)} ảnh thành công. "
          f"Merging by listing_id...")
    merged = merge_records(extractions)
    print(f"      → {len(merged)} listings unique")

    if dry_run:
        print("\n[DRY RUN] Không ghi DB. Kết quả:")
        for r in merged:
            print(f"  {r.get('listing_id'):>15} | {str(r.get('title',''))[:50]}")
            print(f"  {'':>15}   views={r.get('views')} clicks={r.get('clicks')} "
                  f"orders={r.get('orders')} roas={r.get('roas')}")
        return

    no_vm = parse_vm(folder)
    print(f"\n[3/3] Upserting vào listing_report (no_vm={no_vm})...")
    n = upsert_listing_reports(merged, no_vm)
    print(f"      → {n} rows upserted")

    print("\n" + "=" * 62)
    print("  Done.")
    print("=" * 62)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Internal listing screenshot crawler")
    parser.add_argument(
        "--folder",
        required=True,
        help="Path tới folder chứa screenshots, ví dụ: ../raw/internal/20-04-2026-VM01",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Chỉ extract + print, không ghi DB",
    )
    args = parser.parse_args()

    folder_path = Path(args.folder).resolve()
    if not folder_path.is_dir():
        print(f"[!] Folder không tồn tại: {folder_path}")
        sys.exit(1)

    run(folder_path, dry_run=args.dry_run)
