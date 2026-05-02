"""
References service — on-demand: pair internal listings với top-N market
listings cùng category, rank theo tag_ranking ASC.

Gọi từ route POST /api/v1/references/refresh. Mỗi lần gọi chạy lại toàn
bộ pipeline rồi UPSERT vào bảng references_engine (composite PK
(listing_id, reference_listing_id)).
"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS references_engine (
    listing_id           VARCHAR(32)  NOT NULL,
    reference_listing_id TEXT         NOT NULL,
    ref_rank             SMALLINT     NOT NULL,
    ref_title            TEXT,
    ref_shop             TEXT,
    ref_url              TEXT,
    ref_price            INTEGER,
    ref_discount         INTEGER,
    ref_rating           REAL,
    ref_review_count     INTEGER,
    ref_tag_ranking      INTEGER,
    ref_badge            TEXT,
    ref_free_shipping    BOOLEAN,
    ref_product_type     TEXT,
    ref_import_date      DATE,
    match_method         VARCHAR(16)  DEFAULT 'category',
    refreshed_at         TIMESTAMPTZ  DEFAULT now(),
    PRIMARY KEY (listing_id, reference_listing_id)
)
"""

# Migration: add columns if table đã tồn tại với schema cũ.
_MIGRATE_COLUMNS_SQL = """
ALTER TABLE references_engine
    ADD COLUMN IF NOT EXISTS ref_discount      INTEGER,
    ADD COLUMN IF NOT EXISTS ref_free_shipping BOOLEAN,
    ADD COLUMN IF NOT EXISTS ref_product_type  TEXT,
    ADD COLUMN IF NOT EXISTS ref_import_date   DATE
"""


_UPSERT_SQL = """
INSERT INTO references_engine (
    listing_id, reference_listing_id, ref_rank,
    ref_title, ref_shop, ref_url, ref_price, ref_discount,
    ref_rating, ref_review_count, ref_tag_ranking, ref_badge,
    ref_free_shipping, ref_product_type, ref_import_date,
    match_method, refreshed_at
) VALUES (
    :listing_id, :reference_listing_id, :rnk,
    :ref_title, :ref_shop, :ref_url, :ref_price, :ref_discount,
    :ref_rating, :ref_review_count, :ref_tag_ranking, :ref_badge,
    :ref_free_shipping, :ref_product_type, :ref_import_date,
    'category', now()
)
ON CONFLICT (listing_id, reference_listing_id) DO UPDATE SET
    ref_rank          = EXCLUDED.ref_rank,
    ref_title         = COALESCE(EXCLUDED.ref_title,         references_engine.ref_title),
    ref_shop          = COALESCE(EXCLUDED.ref_shop,          references_engine.ref_shop),
    ref_url           = COALESCE(EXCLUDED.ref_url,           references_engine.ref_url),
    ref_price         = COALESCE(EXCLUDED.ref_price,         references_engine.ref_price),
    ref_discount      = COALESCE(EXCLUDED.ref_discount,      references_engine.ref_discount),
    ref_rating        = COALESCE(EXCLUDED.ref_rating,        references_engine.ref_rating),
    ref_review_count  = COALESCE(EXCLUDED.ref_review_count,  references_engine.ref_review_count),
    ref_tag_ranking   = COALESCE(EXCLUDED.ref_tag_ranking,   references_engine.ref_tag_ranking),
    ref_badge         = COALESCE(EXCLUDED.ref_badge,         references_engine.ref_badge),
    ref_free_shipping = COALESCE(EXCLUDED.ref_free_shipping, references_engine.ref_free_shipping),
    ref_product_type  = COALESCE(EXCLUDED.ref_product_type,  references_engine.ref_product_type),
    ref_import_date   = COALESCE(EXCLUDED.ref_import_date,   references_engine.ref_import_date),
    refreshed_at      = now()
"""


async def refresh_references(
    db: AsyncSession,
    market_db: AsyncSession,
    top_n: int = 3,
    listing_id: str | None = None,
) -> dict:
    await db.execute(text(_CREATE_TABLE_SQL))
    await db.execute(text(_MIGRATE_COLUMNS_SQL))

    # Xoá refs cũ của scope đang refresh — tránh tồn đọng khi top-N thay đổi.
    await db.execute(
        text(
            """
            DELETE FROM references_engine
            WHERE (CAST(:listing_id AS VARCHAR) IS NULL OR listing_id = CAST(:listing_id AS VARCHAR))
            """
        ),
        {"listing_id": listing_id},
    )

    # Fetch candidates từ market_db (market_listing), listings từ db
    listings_result = await db.execute(
        text("SELECT listing_id, category FROM listings WHERE category IS NOT NULL"
             + (" AND listing_id = :lid" if listing_id else "")),
        {"lid": listing_id} if listing_id else {},
    )
    internal_listings = [dict(r._mapping) for r in listings_result]

    cat_filter = " OR ".join(
        [f"LOWER(ml.search_tag) LIKE '%' || LOWER(:cat_{i}) || '%'"
         f" OR LOWER(ml.product_type) LIKE '%' || LOWER(:cat_{i}) || '%'"
         f" OR LOWER(ml.title) LIKE '%' || LOWER(:cat_{i}) || '%'"
         for i in range(len(internal_listings))]
    ) or "1=0"
    cat_params = {f"cat_{i}": r["category"] for i, r in enumerate(internal_listings)}

    mkt_sql = text(f"""
        SELECT id AS reference_listing_id, title, shop_name, url, price, discount,
               rating, review_count, tag_ranking, badge, free_shipping, product_type,
               search_tag, import_date
        FROM market_listing
        WHERE tag_ranking IS NOT NULL AND ({cat_filter})
    """)
    mkt_result = await market_db.execute(mkt_sql, cat_params)
    market_rows = [dict(r._mapping) for r in mkt_result]

    # Match & rank in Python
    rows = []
    for lst in internal_listings:
        cat = (lst["category"] or "").lower()
        matches = [
            m for m in market_rows
            if cat in (m.get("search_tag") or "").lower()
            or cat in (m.get("product_type") or "").lower()
            or cat in (m.get("title") or "").lower()
        ]
        matches.sort(key=lambda m: (m.get("tag_ranking") or 99999, -(m.get("review_count") or 0)))
        for rnk, m in enumerate(matches[:top_n], start=1):
            rows.append({
                "listing_id": lst["listing_id"],
                "reference_listing_id": m["reference_listing_id"],
                "rnk": rnk,
                "ref_title": m.get("title"),
                "ref_shop": m.get("shop_name"),
                "ref_url": m.get("url"),
                "ref_price": m.get("price"),
                "ref_discount": m.get("discount"),
                "ref_rating": m.get("rating"),
                "ref_review_count": m.get("review_count"),
                "ref_tag_ranking": m.get("tag_ranking"),
                "ref_badge": m.get("badge"),
                "ref_free_shipping": m.get("free_shipping"),
                "ref_product_type": m.get("product_type"),
                "ref_import_date": m.get("import_date"),
            })

    for row in rows:
        await db.execute(text(_UPSERT_SQL), row)

    await db.commit()

    scope = await db.execute(
        text(
            """
            SELECT COUNT(DISTINCT listing_id) AS covered, COUNT(*) AS total
            FROM references_engine
            WHERE (CAST(:listing_id AS VARCHAR) IS NULL OR listing_id = CAST(:listing_id AS VARCHAR))
            """
        ),
        {"listing_id": listing_id},
    )
    r = scope.one()._mapping

    return {
        "upserted":          len(rows),
        "listings_with_ref": r["covered"],
        "total_refs":        r["total"],
        "top_n":             top_n,
        "scope":             listing_id or "all",
    }


async def get_references(
    db: AsyncSession,
    listing_id: str | None = None,
) -> list[dict]:
    result = await db.execute(
        text(
            """
            SELECT listing_id, reference_listing_id, ref_rank,
                   ref_title, ref_shop, ref_url, ref_price, ref_discount,
                   ref_rating, ref_review_count, ref_tag_ranking, ref_badge,
                   ref_free_shipping, ref_product_type, ref_import_date,
                   refreshed_at
            FROM references_engine
            WHERE (CAST(:listing_id AS VARCHAR) IS NULL OR listing_id = CAST(:listing_id AS VARCHAR))
            ORDER BY listing_id, ref_rank
            """
        ),
        {"listing_id": listing_id},
    )
    return [dict(r._mapping) for r in result]
