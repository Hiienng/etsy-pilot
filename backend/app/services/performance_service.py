from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

# Default scenario rules — seeded once on startup
_SCENARIO_DEFAULTS = [
    ("top",       "Nhân đôi sản phẩm tương tự",                    "low"),
    ("quick_win", "Thumbnail yếu — cần cải thiện ảnh bìa",         "medium"),
    ("fix_cr",    "Traffic đủ nhưng listing chưa thuyết phục mua", "high"),
    ("fix_roas",  "Metrics tốt nhưng margin mỏng",                 "high"),
    ("fix_ctr",   "Listing yếu toàn diện — cần tái cấu trúc",     "critical"),
]

# SQL fragment reused in both SELECT and ORDER BY
_SCENARIO_CASE = """
    CASE
        WHEN il.ctr >= 2 AND il.cr >= 4 AND il.roas >= 2 THEN 'top'
        WHEN il.cr  >= 4 AND il.ctr < 2                  THEN 'quick_win'
        WHEN il.ctr >= 2 AND il.cr  < 4                  THEN 'fix_cr'
        WHEN il.ctr >= 2 AND il.cr  >= 4                 THEN 'fix_roas'
        ELSE 'fix_ctr'
    END
"""


async def seed_scenarios(db: AsyncSession) -> None:
    """Insert default scenario rules if the table is empty."""
    result = await db.execute(text("SELECT COUNT(*) FROM scenarios_rules"))
    if result.scalar_one() == 0:
        for key, label, priority in _SCENARIO_DEFAULTS:
            await db.execute(
                text(
                    "INSERT INTO scenarios_rules (scenario_key, label, priority) "
                    "VALUES (:k, :l, :p) ON CONFLICT DO NOTHING"
                ),
                {"k": key, "l": label, "p": priority},
            )
        await db.commit()


async def get_dashboard_listings(db: AsyncSession) -> list[dict]:
    """
    JOIN internal_listing × scenarios_rules × market_listing.

    Scenario: computed via CASE expression, then looked up in scenarios_rules.
    Reference: LATERAL join to market_listing — single row per product_type
                with highest revenue potential (price × review_count).
    Order: critical first → top last; within same scenario: roas DESC.
    """
    sql = text(f"""
        SELECT
            il.listing_id,
            il.title,
            il.product,
            il.ctr,
            il.cr,
            il.roas,
            il.listing_link                AS url,
            {_SCENARIO_CASE}               AS scenario_key,
            sr.label                       AS scenario_label,
            sr.priority                    AS scenario_priority,
            ref.title                      AS ref_title,
            ref.listing_link               AS ref_url
        FROM internal_listing il
        LEFT JOIN scenarios_rules sr
            ON sr.scenario_key = ({_SCENARIO_CASE})
        LEFT JOIN LATERAL (
            SELECT title, listing_link
            FROM market_listing
            WHERE product_type = il.product
            ORDER BY (price::float * review_count) DESC NULLS LAST
            LIMIT 1
        ) ref ON true
        ORDER BY
            CASE ({_SCENARIO_CASE})
                WHEN 'fix_ctr'   THEN 1
                WHEN 'fix_cr'    THEN 2
                WHEN 'fix_roas'  THEN 3
                WHEN 'quick_win' THEN 4
                WHEN 'top'       THEN 5
            END ASC,
            il.roas DESC NULLS LAST
    """)
    result = await db.execute(sql)
    return [dict(r) for r in result.mappings().all()]
