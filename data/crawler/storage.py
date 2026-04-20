"""
Storage Module
Lưu dữ liệu đã extract vào JSON, CSV, PostgreSQL (Neon).
SQLite giữ lại làm fallback nếu DATABASE_URL không có.
"""
import json
import csv
import sqlite3
import re as _re
from pathlib import Path
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from config import OUTPUT_DIR, DB_PATH, DATABASE_URL


def ensure_output_dir():
    Path(OUTPUT_DIR).mkdir(parents=True, exist_ok=True)


def normalize_product(p: dict) -> dict:
    """Chuẩn hóa kiểu dữ liệu — chạy trước khi lưu CSV/SQLite"""
    import re

    def to_int_price(val):
        if val is None:
            return None
        s = str(val).lower().replace(",", "").replace(".", "")
        s = re.sub(r"[^\d]", "", s)
        return int(s) if s else None

    def to_int_discount(val):
        if val is None:
            return None
        s = re.sub(r"[^\d]", "", str(val))
        return int(s) if s else None

    def to_float_rating(val):
        if val is None:
            return None
        s = str(val).replace(",", ".")
        s = re.sub(r"[^\d.]", "", s)
        try:
            return float(s)
        except ValueError:
            return None

    def to_int_review(val):
        if val is None:
            return None
        s = str(val).lower().strip()
        # "8.3k" → 8300, "1,234" → 1234
        match = re.match(r"([\d.,]+)\s*k", s)
        if match:
            num = float(match.group(1).replace(",", "."))
            return int(num * 1000)
        s = re.sub(r"[^\d]", "", s)
        return int(s) if s else None

    return {
        **p,
        "price": to_int_price(p.get("price")),
        "original_price": to_int_price(p.get("original_price")),
        "discount": to_int_discount(p.get("discount")),
        "rating": to_float_rating(p.get("rating")),
        "review_count": to_int_review(p.get("review_count")),
    }


def save_json(data: dict | list, filename: str):
    """Lưu vào file JSON"""
    ensure_output_dir()
    path = Path(OUTPUT_DIR) / filename
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"[Storage] JSON saved: {path} ({_count_items(data)} items)")
    return str(path)


def save_csv(products: list[dict], filename: str):
    """Lưu danh sách sản phẩm vào CSV"""
    if not products:
        print("[Storage] Không có dữ liệu để lưu CSV")
        return None

    ensure_output_dir()
    path = Path(OUTPUT_DIR) / filename

    fieldnames = [
        "batch_id", "source_screenshot", "search_tag", "etsy_best",
        "product_type", "title", "price", "original_price", "discount",
        "shop_name", "rating", "review_count",
        "badge", "free_shipping", "is_ad",
        "image_description", "scroll_position", "category", "import_date",
    ]

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(normalize_product(p) for p in products)

    print(f"[Storage] CSV saved: {path} ({len(products)} rows)")
    return str(path)


def save_sqlite(products: list[dict], db_path: str = None):
    """Lưu vào SQLite database"""
    if not products:
        print("[Storage] Không có dữ liệu để lưu SQLite")
        return

    if db_path is None:
        db_path = DB_PATH

    ensure_output_dir()
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT,
            source_screenshot TEXT,
            search_tag TEXT,
            etsy_best TEXT,
            product_type TEXT,
            category TEXT,
            title TEXT,
            price INTEGER,
            original_price INTEGER,
            discount INTEGER,
            shop_name TEXT,
            rating REAL,
            review_count INTEGER,
            badge TEXT,
            is_ad INTEGER,
            free_shipping INTEGER,
            image_description TEXT,
            scroll_position INTEGER,
            crawled_at TEXT
        )
    """)

    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_category ON products(category)
    """)
    cursor.execute("""
        CREATE INDEX IF NOT EXISTS idx_title ON products(title)
    """)

    now = datetime.now().isoformat()
    rows = []
    for p in (normalize_product(x) for x in products):
        rows.append((
            p.get("batch_id"),
            p.get("source_screenshot"),
            p.get("search_tag"),
            p.get("etsy_best"),
            p.get("product_type"),
            p.get("category"),
            p.get("title"),
            p.get("price"),
            p.get("original_price"),
            p.get("discount"),
            p.get("shop_name"),
            p.get("rating"),
            p.get("review_count"),
            p.get("badge"),
            1 if p.get("is_ad") else 0,
            1 if p.get("free_shipping") else 0,
            p.get("image_description"),
            p.get("scroll_position"),
            now,
        ))

    cursor.executemany("""
        INSERT INTO products (
            batch_id, source_screenshot, search_tag, etsy_best,
            product_type, category, title, price, original_price, discount,
            shop_name, rating, review_count, badge, is_ad, free_shipping,
            image_description, scroll_position, crawled_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, rows)

    conn.commit()

    # Thống kê
    cursor.execute("SELECT COUNT(*) FROM products")
    total = cursor.fetchone()[0]
    conn.close()

    print(f"[Storage] SQLite saved: {db_path} (total {total} records)")
    return db_path


def _pg_dsn() -> str | None:
    """Convert DATABASE_URL to psycopg2-compatible DSN. Returns None if unavailable."""
    url = DATABASE_URL
    if not url:
        return None
    # Normalize prefix
    url = url.replace("postgresql+asyncpg://", "postgresql://", 1)
    url = url.replace("postgres://", "postgresql://", 1)
    # Strip params unsupported by psycopg2 (channel_binding)
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    qs.pop("channel_binding", None)
    if "sslmode" not in qs:
        qs["sslmode"] = ["require"]
    clean_query = urlencode({k: v[0] for k, v in qs.items()})
    return urlunparse(parsed._replace(query=clean_query))


def save_postgres(products: list[dict]):
    """Deprecated: market_product table đã bị drop. Vision flow ghi vào CSV/JSON/SQLite local."""
    return save_sqlite(products)


def save_all(category_products: dict, timestamp: str = None):
    """
    Lưu tất cả dữ liệu cùng lúc.
    Input: {category: [products]}
    """
    if timestamp is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    # Flatten tất cả sản phẩm
    all_products = []
    for products in category_products.values():
        all_products.extend(products)

    print(f"\n[Storage] Tổng sản phẩm: {len(all_products)}")

    # Lưu JSON gộp
    save_json(category_products, f"etsy_by_category_{timestamp}.json")
    save_json(all_products, f"etsy_all_products_{timestamp}.json")

    # Lưu CSV
    save_csv(all_products, f"etsy_products_{timestamp}.csv")

    # Lưu PostgreSQL (fallback SQLite nếu không có DATABASE_URL)
    save_postgres(all_products)

    return {
        "total_products": len(all_products),
        "categories": {k: len(v) for k, v in category_products.items()},
        "timestamp": timestamp,
    }


def query_top_products(
    category: str = None,
    limit: int = 20,
    sort_by: str = "rating",
) -> list[dict]:
    """Query top sản phẩm từ SQLite local (market_product đã bị drop)."""
    return _query_sqlite(category, limit, sort_by)


def _query_sqlite(category, limit, sort_by) -> list[dict]:
    if not Path(DB_PATH).exists():
        print(f"Database chưa tồn tại: {DB_PATH}")
        return []

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    where_clause = "WHERE is_ad = 0"
    params = []
    if category:
        where_clause += " AND category = ?"
        params.append(category)

    order_map = {
        "rating": "rating DESC, review_count DESC",
        "scroll": "scroll_position ASC",
        "recent": "crawled_at DESC",
    }
    order = order_map.get(sort_by, "scroll_position ASC")

    cursor.execute(f"""
        SELECT * FROM products
        {where_clause}
        ORDER BY {order}
        LIMIT ?
    """, params + [limit])

    rows = [dict(row) for row in cursor.fetchall()]
    conn.close()
    return rows


def _count_items(data):
    if isinstance(data, list):
        return len(data)
    if isinstance(data, dict):
        return sum(len(v) if isinstance(v, list) else 1 for v in data.values())
    return 1
