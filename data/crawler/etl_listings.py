"""
ETL: listing_report → listings → etl_references
================================================
Chạy mỗi thứ 2 chiều via GitHub Actions.

Pipeline:
  1. run_etl()            — sync listing_report → listings (latest non-null per field)
  2. run_etl_references() — build top-3 market reference per internal listing,
                            match category by ILIKE qua (search_tag / product_type / title),
                            rank theo tag_ranking ASC (thấp = xuất hiện sớm hơn).

UPSERT rule (cả 2 bước): INSERT mới, ON CONFLICT DO UPDATE với COALESCE
— không bao giờ xoá dữ liệu đã có, chỉ fill null.

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


def run_etl_references(dsn: str, top_n: int = 3) -> dict:
    """
    Build etl_references: top-N market listings per internal listing.
    Match: ILIKE category qua (search_tag / product_type / title).
    Rank: tag_ranking ASC NULLS LAST (thấp = xuất hiện sớm hơn trong search).
    """
    import psycopg2
    import psycopg2.extras

    conn = psycopg2.connect(dsn)
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS etl_references (
            listing_id           VARCHAR(32)  NOT NULL,
            reference_listing_id TEXT         NOT NULL,
            ref_rank             SMALLINT     NOT NULL,
            ref_title            TEXT,
            ref_shop             TEXT,
            ref_url              TEXT,
            ref_price            INTEGER,
            ref_rating           REAL,
            ref_review_count     INTEGER,
            ref_tag_ranking      INTEGER,
            ref_badge            TEXT,
            match_method         VARCHAR(16)  DEFAULT 'category',
            refreshed_at         TIMESTAMPTZ  DEFAULT now(),
            PRIMARY KEY (listing_id, reference_listing_id)
        )
    """)
    conn.commit()

    cur.execute(f"""
        WITH ranked AS (
            SELECT
                l.listing_id,
                ml.id        AS reference_listing_id,
                ml.title     AS ref_title,
                ml.shop_name AS ref_shop,
                ml.url       AS ref_url,
                ml.price     AS ref_price,
                ml.rating    AS ref_rating,
                ml.review_count AS ref_review_count,
                ml.tag_ranking  AS ref_tag_ranking,
                ml.badge        AS ref_badge,
                ROW_NUMBER() OVER (
                    PARTITION BY l.listing_id
                    ORDER BY ml.tag_ranking ASC NULLS LAST, ml.review_count DESC NULLS LAST
                ) AS rnk
            FROM listings l
            JOIN market_listing ml ON (
                LOWER(ml.search_tag)   LIKE '%%' || LOWER(l.category) || '%%'
             OR LOWER(ml.product_type) LIKE '%%' || LOWER(l.category) || '%%'
             OR LOWER(ml.title)        LIKE '%%' || LOWER(l.category) || '%%'
            )
            WHERE l.category IS NOT NULL
              AND ml.tag_ranking IS NOT NULL
        )
        SELECT * FROM ranked WHERE rnk <= {top_n}
    """)
    rows = cur.fetchall()
    print(f"[ETL refs] {len(rows)} candidate references (top-{top_n} per listing)")

    upserted = 0
    for r in rows:
        cur.execute("""
            INSERT INTO etl_references (
                listing_id, reference_listing_id, ref_rank,
                ref_title, ref_shop, ref_url, ref_price,
                ref_rating, ref_review_count, ref_tag_ranking, ref_badge,
                match_method, refreshed_at
            ) VALUES (
                %(listing_id)s, %(reference_listing_id)s, %(rnk)s,
                %(ref_title)s, %(ref_shop)s, %(ref_url)s, %(ref_price)s,
                %(ref_rating)s, %(ref_review_count)s, %(ref_tag_ranking)s, %(ref_badge)s,
                'category', now()
            )
            ON CONFLICT (listing_id, reference_listing_id) DO UPDATE SET
                ref_rank         = EXCLUDED.ref_rank,
                ref_title        = COALESCE(EXCLUDED.ref_title,        etl_references.ref_title),
                ref_shop         = COALESCE(EXCLUDED.ref_shop,         etl_references.ref_shop),
                ref_url          = COALESCE(EXCLUDED.ref_url,          etl_references.ref_url),
                ref_price        = COALESCE(EXCLUDED.ref_price,        etl_references.ref_price),
                ref_rating       = COALESCE(EXCLUDED.ref_rating,       etl_references.ref_rating),
                ref_review_count = COALESCE(EXCLUDED.ref_review_count, etl_references.ref_review_count),
                ref_tag_ranking  = COALESCE(EXCLUDED.ref_tag_ranking,  etl_references.ref_tag_ranking),
                ref_badge        = COALESCE(EXCLUDED.ref_badge,        etl_references.ref_badge),
                refreshed_at     = now()
        """, r)
        upserted += cur.rowcount

    conn.commit()

    cur.execute("SELECT COUNT(DISTINCT listing_id) AS n FROM etl_references")
    covered = cur.fetchone()["n"]
    cur.execute("SELECT COUNT(*) AS n FROM etl_references")
    total = cur.fetchone()["n"]
    conn.close()

    return {"upserted": upserted, "listings_with_ref": covered, "total_refs": total}


def main():
    print("=" * 56)
    print("  ETL: listing_report → listings → etl_references")
    print(f"  {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print("=" * 56)

    dsn = pg_dsn()

    # Step 1: listing_report → listings
    r1 = run_etl(dsn)
    print(f"\n[1] listing_report → listings")
    print(f"    Source unique:  {r1['source']}")
    print(f"    Upserted:       {r1['upserted']}")
    print(f"    Total listings: {r1['total_listings']}")

    # Step 2: listings × market_listing → etl_references (chỉ chạy nếu step 1 ok)
    r2 = run_etl_references(dsn, top_n=3)
    print(f"\n[2] listings × market_listing → etl_references")
    print(f"    Upserted refs:            {r2['upserted']}")
    print(f"    Listings có reference:    {r2['listings_with_ref']}")
    print(f"    Total rows etl_references: {r2['total_refs']}")

    print("\n  Done.")


if __name__ == "__main__":
    main()
