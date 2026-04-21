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
    ref_rating           REAL,
    ref_review_count     INTEGER,
    ref_tag_ranking      INTEGER,
    ref_badge            TEXT,
    match_method         VARCHAR(16)  DEFAULT 'category',
    refreshed_at         TIMESTAMPTZ  DEFAULT now(),
    PRIMARY KEY (listing_id, reference_listing_id)
)
"""

_SELECT_CANDIDATES_SQL = """
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
        LOWER(ml.search_tag)   LIKE '%' || LOWER(l.category) || '%'
     OR LOWER(ml.product_type) LIKE '%' || LOWER(l.category) || '%'
     OR LOWER(ml.title)        LIKE '%' || LOWER(l.category) || '%'
    )
    WHERE l.category IS NOT NULL
      AND ml.tag_ranking IS NOT NULL
      AND ml.id != l.listing_id
      AND NOT EXISTS (SELECT 1 FROM listings l2 WHERE l2.listing_id = ml.id)
      AND (CAST(:listing_id AS VARCHAR) IS NULL OR l.listing_id = CAST(:listing_id AS VARCHAR))
)
SELECT * FROM ranked WHERE rnk <= :top_n
"""

_UPSERT_SQL = """
INSERT INTO references_engine (
    listing_id, reference_listing_id, ref_rank,
    ref_title, ref_shop, ref_url, ref_price,
    ref_rating, ref_review_count, ref_tag_ranking, ref_badge,
    match_method, refreshed_at
) VALUES (
    :listing_id, :reference_listing_id, :rnk,
    :ref_title, :ref_shop, :ref_url, :ref_price,
    :ref_rating, :ref_review_count, :ref_tag_ranking, :ref_badge,
    'category', now()
)
ON CONFLICT (listing_id, reference_listing_id) DO UPDATE SET
    ref_rank         = EXCLUDED.ref_rank,
    ref_title        = COALESCE(EXCLUDED.ref_title,        references_engine.ref_title),
    ref_shop         = COALESCE(EXCLUDED.ref_shop,         references_engine.ref_shop),
    ref_url          = COALESCE(EXCLUDED.ref_url,          references_engine.ref_url),
    ref_price        = COALESCE(EXCLUDED.ref_price,        references_engine.ref_price),
    ref_rating       = COALESCE(EXCLUDED.ref_rating,       references_engine.ref_rating),
    ref_review_count = COALESCE(EXCLUDED.ref_review_count, references_engine.ref_review_count),
    ref_tag_ranking  = COALESCE(EXCLUDED.ref_tag_ranking,  references_engine.ref_tag_ranking),
    ref_badge        = COALESCE(EXCLUDED.ref_badge,        references_engine.ref_badge),
    refreshed_at     = now()
"""


async def refresh_references(
    db: AsyncSession,
    top_n: int = 3,
    listing_id: str | None = None,
) -> dict:
    await db.execute(text(_CREATE_TABLE_SQL))

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

    result = await db.execute(
        text(_SELECT_CANDIDATES_SQL),
        {"top_n": top_n, "listing_id": listing_id},
    )
    rows = [dict(r._mapping) for r in result]

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
                   ref_title, ref_shop, ref_url, ref_price,
                   ref_rating, ref_review_count, ref_tag_ranking, ref_badge,
                   refreshed_at
            FROM references_engine
            WHERE (CAST(:listing_id AS VARCHAR) IS NULL OR listing_id = CAST(:listing_id AS VARCHAR))
            ORDER BY listing_id, ref_rank
            """
        ),
        {"listing_id": listing_id},
    )
    return [dict(r._mapping) for r in result]
