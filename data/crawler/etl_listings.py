"""
ETL: listing_report → listings
================================
Chạy mỗi thứ 2 chiều via GitHub Actions.

Logic:
  - DISTINCT ON (listing_id) lấy row mới nhất từ listing_report
  - UPSERT vào listings:
      INSERT nếu listing_id chưa có
      UPDATE nếu đã có (COALESCE giữ giá trị cũ nếu field mới null)
  - importer = 'ETL_automation'

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

    conn.commit()

    # Lấy unique listing mới nhất từ listing_report
    cur.execute("""
        SELECT DISTINCT ON (listing_id)
            listing_id,
            title,
            category,
            no_vm,
            import_time
        FROM listing_report
        WHERE listing_id IS NOT NULL
        ORDER BY listing_id, import_time DESC NULLS LAST
    """)
    source_rows = cur.fetchall()
    print(f"[ETL] Source: {len(source_rows)} unique listings từ listing_report")

    inserted = updated = 0
    now = datetime.now(timezone.utc)

    for row in source_rows:
        url = f"https://www.etsy.com/listing/{row['listing_id']}"
        cur.execute("""
            INSERT INTO listings (listing_id, title, category, no_vm, url, import_time, importer)
            VALUES (%(listing_id)s, %(title)s, %(category)s, %(no_vm)s, %(url)s, %(import_time)s, 'ETL_automation')
            ON CONFLICT (listing_id) DO UPDATE SET
                title       = COALESCE(EXCLUDED.title,    listings.title),
                category    = COALESCE(EXCLUDED.category, listings.category),
                no_vm       = COALESCE(EXCLUDED.no_vm,    listings.no_vm),
                url         = COALESCE(EXCLUDED.url,      listings.url),
                import_time = EXCLUDED.import_time,
                importer    = 'ETL_automation'
        """, {
            "listing_id": row["listing_id"],
            "title":      row["title"],
            "category":   row["category"],
            "no_vm":      row["no_vm"],
            "url":        url,
            "import_time": row["import_time"] or now,
        })
        if cur.rowcount:
            # xorshift: INSERT = 1 row affected, UPDATE = 1 row affected
            # dùng xid để phân biệt INSERT vs UPDATE
            updated += 1

    conn.commit()

    # Đếm thực tế
    cur.execute("SELECT COUNT(*) AS n FROM listings")
    total = cur.fetchone()["n"]
    conn.close()

    return {"source": len(source_rows), "upserted": updated, "total_listings": total}


def main():
    print("=" * 56)
    print("  ETL: listing_report → listings")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 56)

    dsn = pg_dsn()
    result = run_etl(dsn)

    print(f"\n  Source (listing_report unique): {result['source']}")
    print(f"  Upserted vào listings:          {result['upserted']}")
    print(f"  Total listings sau ETL:         {result['total_listings']}")
    print("\n  Done.")


if __name__ == "__main__":
    main()
