"""
ETL: listing_report → listings
================================
Chạy mỗi thứ 2 chiều via GitHub Actions.

Logic:
  - Với mỗi listing_id trong listing_report, lấy latest non-null value
    riêng cho từng field (title, category, no_vm, importer) bằng
    correlated subquery ORDER BY import_time DESC.
  - UPSERT vào listings:
      INSERT nếu listing_id chưa có
      UPDATE nếu đã có: COALESCE(new, existing) — chỉ ghi đè khi field
      hiện tại đang null, không bao giờ xoá dữ liệu đã có.
  - Yêu cầu: UNIQUE constraint trên listings.listing_id (đã tồn tại).

Env vars:
  DATABASE_URL — Neon PostgreSQL connection string
"""

import os
import sys
from datetime import datetime, timezone
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse


def pg_dsn() -> str:
    raw = os.environ.get("DATABASE_URL", "")
    if not raw:
        env_path = os.path.join(os.path.dirname(__file__), "../../.env")
        if os.path.exists(env_path):
            for line in open(env_path):
                if line.startswith("DATABASE_URL="):
                    raw = line.split("=", 1)[1].strip()
    if not raw:
        raise SystemExit("[!] DATABASE_URL chưa cấu hình")
    url = raw.replace("postgresql+asyncpg://", "postgresql://", 1).replace("postgres://", "postgresql://", 1)
    parsed = urlparse(url)
    qs = parse_qs(parsed.query)
    qs.pop("channel_binding", None)
    if "sslmode" not in qs:
        qs["sslmode"] = ["require"]
    return urlunparse(parsed._replace(query=urlencode({k: v[0] for k, v in qs.items()})))


def run_etl(dsn: str) -> dict:
    import psycopg2
    import psycopg2.extras

    conn = psycopg2.connect(dsn)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Lấy latest non-null value per field per listing_id
    cur.execute("""
        SELECT
            listing_id,
            (SELECT title    FROM listing_report r2 WHERE r2.listing_id = r.listing_id AND r2.title    IS NOT NULL ORDER BY r2.import_time DESC NULLS LAST LIMIT 1) AS title,
            (SELECT category FROM listing_report r2 WHERE r2.listing_id = r.listing_id AND r2.category IS NOT NULL ORDER BY r2.import_time DESC NULLS LAST LIMIT 1) AS category,
            (SELECT no_vm    FROM listing_report r2 WHERE r2.listing_id = r.listing_id AND r2.no_vm    IS NOT NULL ORDER BY r2.import_time DESC NULLS LAST LIMIT 1) AS no_vm,
            (SELECT importer FROM listing_report r2 WHERE r2.listing_id = r.listing_id AND r2.importer IS NOT NULL ORDER BY r2.import_time DESC NULLS LAST LIMIT 1) AS importer,
            MAX(import_time) AS import_time
        FROM listing_report r
        WHERE listing_id IS NOT NULL
        GROUP BY listing_id
    """)
    source_rows = cur.fetchall()
    print(f"[ETL] Source: {len(source_rows)} unique listings từ listing_report")

    upserted = 0

    for row in source_rows:
        url = f"https://www.etsy.com/listing/{row['listing_id']}"
        cur.execute("""
            INSERT INTO listings (listing_id, title, category, no_vm, url, import_time, importer)
            VALUES (%(listing_id)s, %(title)s, %(category)s, %(no_vm)s, %(url)s, %(import_time)s, %(importer)s)
            ON CONFLICT (listing_id) DO UPDATE SET
                title       = COALESCE(EXCLUDED.title,       listings.title),
                category    = COALESCE(EXCLUDED.category,    listings.category),
                no_vm       = COALESCE(EXCLUDED.no_vm,       listings.no_vm),
                url         = COALESCE(EXCLUDED.url,         listings.url),
                import_time = COALESCE(EXCLUDED.import_time, listings.import_time),
                importer    = COALESCE(EXCLUDED.importer,    listings.importer)
        """, {
            "listing_id":  row["listing_id"],
            "title":       row["title"],
            "category":    row["category"],
            "no_vm":       row["no_vm"],
            "url":         url,
            "import_time": row["import_time"],
            "importer":    row["importer"],
        })
        if cur.rowcount:
            upserted += 1

    conn.commit()

    # Đếm thực tế
    cur.execute("SELECT COUNT(*) AS n FROM listings")
    total = cur.fetchone()["n"]
    conn.close()

    return {"source": len(source_rows), "upserted": upserted, "total_listings": total}


def main():
    print("=" * 56)
    print("  ETL: listing_report → listings")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 56)

    dsn = pg_dsn()
    result = run_etl(dsn)

    print(f"\n  Source (listing_report unique):  {result['source']}")
    print(f"  Upserted vào listings:                    {result['upserted']}")
    print(f"  Total listings sau ETL:                   {result['total_listings']}")
    print("\n  Done.")


if __name__ == "__main__":
    main()
