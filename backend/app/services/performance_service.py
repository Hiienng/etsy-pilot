from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

# ── Scenario matrix from senarious_rules.xlsx (sheet "Kịch bản Ads") ───────
# Thresholds: CTR ≥ 1.5% = Cao, CR ≥ 3% = Cao, ROAS breakeven default = 2.0
# Columns: (roas_band, cr_level, ctr_level, case_name, action, cause, fix_listing, fix_ads)
_SCENARIO_SEED = [
    # --- ROAS trên huề vốn (profitable) ---
    ("profitable", "high", "high",
     "Có sales và đang lời", "keep", None, None, None),
    ("profitable", "low", "high",
     "Có sales và đang lời", "improve",
     "Listing chưa tốt, keywords trong ads chưa tối ưu",
     "Kiểm tra giá bán, giá ship, review, hình thông tin sp, option sản phẩm",
     "Tắt bớt keywords không hiệu quả"),
    ("profitable", "high", "low",
     "Có sales và đang lời", "improve",
     "Hình main chưa đúng intent, keywords chưa chuẩn, giá mòi cao",
     "Tăng CTR: xem lại keywords, hình main, alt, giá mòi",
     "Tắt bớt keywords không hiệu quả"),
    ("profitable", "low", "low",
     "Có sales và đang lời", "improve",
     "Keyword không hiệu quả, listing chưa đúng intent + chưa hấp dẫn",
     "Tăng CTR: keywords, hình main, alt, giá mòi. Tăng CR: giá bán, offers, ship, reviews. Giảm CPC: long-tailed keywords",
     "Tắt keywords không đúng intent hoặc target rộng, cpc cao"),

    # --- Lỗ nhẹ (1 ≤ ROAS < breakeven) ---
    ("slight_loss", "high", "high",
     "Có sales, đang lỗ nhẹ", "improve",
     "Views/clicks thấp hoặc AOV chưa cover ads, CPC > $0.8",
     "Views thấp: xem lại keywords, giá mòi, hình main. Tăng AOV: thêm Offer hoặc related products",
     "Tắt keywords không đúng intent hoặc target rộng, cpc cao"),
    ("slight_loss", "high", "low",
     "Có sales, đang lỗ nhẹ", "improve",
     "Keywords chưa đúng intent hoặc target quá rộng, cạnh tranh cao",
     "Tối ưu keywords, hình main, alt, giá mòi",
     "Tắt keywords không đúng intent hoặc target rộng, cpc cao"),
    ("slight_loss", "low", "high",
     "Có sales, đang lỗ nhẹ", "improve",
     "Listing chưa đủ hấp dẫn",
     "Xem lại keywords, đổi long-tailed, hình main, alt, giảm giá mòi",
     "Tắt keywords không đúng intent hoặc target rộng, cpc cao"),
    ("slight_loss", "low", "low",
     "Có sales, đang lỗ nhẹ", "improve",
     "Listing chưa tối ưu, keywords trong ads chưa hiệu quả",
     "Tăng CTR: sửa keywords, alt, hình main, video, giảm giá mòi. Tăng CR: giá bán, ship, offer, reviews",
     "Tắt keywords không hiệu quả"),

    # --- Lỗ nặng (0 < ROAS < 1) ---
    ("heavy_loss", "high", "high",
     "Có sales, lỗ nặng", "improve",
     "AOV chưa cover ads spend, CPC > $0.8, views thấp",
     "Views thấp: kiểm tra keywords, hình main. AOV thấp: thêm offer, related products",
     "Tắt keywords không đúng intent, target rộng, cạnh tranh cao"),
    ("heavy_loss", "low", "high",
     "Có sales, lỗ nặng", "improve",
     "CR thấp do listing chưa tốt, keywords ads chưa đúng intent",
     "Xem lại keywords, đổi long-tailed, hình main, alt, giảm giá mòi",
     "Tắt keywords không đúng intent, target rộng, cạnh tranh cao"),
    ("heavy_loss", "high", "low",
     "Có sales, lỗ nặng", "improve",
     "Keywords trong listing/ads cạnh tranh cao hoặc target rộng",
     "Xem lại keywords, đổi long-tailed, hình main, alt, giảm giá mòi",
     "Tắt keywords không đúng intent, target rộng, cạnh tranh cao"),
    ("heavy_loss", "low", "low",
     "Có sales, lỗ nặng", "improve_or_off",
     "Listing chưa tối ưu hoặc đã tối ưu nhưng không cải thiện → tắt",
     "Xem lại keywords, hình main, alt, giá mòi, giá bán, ship, reviews",
     "Tắt keywords không hiệu quả"),

    # --- Không có sale (ROAS = 0) ---
    ("no_sales", "zero", "high",
     "Không có sale, có clicks", "improve_or_off",
     "Listing mới: theo dõi. Listing cũ đã tối ưu: tắt. Chưa tối ưu: keywords, giá, hình, reviews",
     "Đổi keywords thành long-tailed, up thêm ảnh chi tiết, xin reviews",
     "Tắt keywords không đúng intent, target rộng, cạnh tranh cao"),
    ("no_sales", "zero", "low",
     "Không có sale, có clicks", "improve_or_off",
     "Listing mới: theo dõi. Listing cũ: tắt. Có thể do listing mất index",
     "Đổi keywords thành long-tailed, thêm ảnh, xin reviews, thêm offers. Mất index: deactive → reactive",
     "Tắt keywords không đúng intent, target rộng, cạnh tranh cao"),
]

# Thresholds
CTR_THRESHOLD = 1.5
CR_THRESHOLD = 3.0
ROAS_BREAKEVEN = 2.0


async def seed_scenarios(db: AsyncSession) -> None:
    """Drop & re-create scenarios_rules with correct schema, then seed."""
    await db.execute(text("DROP TABLE IF EXISTS scenarios_rules CASCADE"))
    await db.execute(text("""
        CREATE TABLE scenarios_rules (
            id SERIAL PRIMARY KEY,
            roas_band VARCHAR(32) NOT NULL,
            cr_level  VARCHAR(8)  NOT NULL,
            ctr_level VARCHAR(8)  NOT NULL,
            case_name TEXT        NOT NULL,
            action    VARCHAR(32) NOT NULL,
            cause     TEXT,
            fix_listing TEXT,
            fix_ads   TEXT
        )
    """))
    for row in _SCENARIO_SEED:
        await db.execute(
            text("""
                INSERT INTO scenarios_rules
                    (roas_band, cr_level, ctr_level, case_name, action, cause, fix_listing, fix_ads)
                VALUES (:rb, :cr, :ctr, :cn, :act, :cause, :fl, :fa)
            """),
            dict(rb=row[0], cr=row[1], ctr=row[2], cn=row[3],
                 act=row[4], cause=row[5], fl=row[6], fa=row[7]),
        )
    await db.commit()


async def get_dashboard_listings(db: AsyncSession) -> list[dict]:
    """
    listing_report → compute CTR/CR → classify roas_band/cr_level/ctr_level
    → JOIN scenarios_rules on 3 dimensions
    → LATERAL JOIN market_listing for reference product.
    """
    sql = text(f"""
        WITH lr AS (
            SELECT
                lr.listing_id,
                lr.title,
                lr.category                        AS product,
                CASE WHEN lr.views > 0
                     THEN ROUND(lr.clicks::numeric / lr.views * 100, 2)
                     ELSE 0 END                    AS ctr,
                CASE WHEN lr.clicks > 0
                     THEN ROUND(lr.orders::numeric / lr.clicks * 100, 2)
                     ELSE 0 END                    AS cr,
                COALESCE(lr.roas, 0)               AS roas,
                lr.views,
                lr.clicks,
                lr.orders,
                lr.revenue,
                lr.spend,
                -- classify roas_band
                CASE
                    WHEN COALESCE(lr.orders, 0) = 0           THEN 'no_sales'
                    WHEN lr.roas >= {ROAS_BREAKEVEN}           THEN 'profitable'
                    WHEN lr.roas >= 1                           THEN 'slight_loss'
                    ELSE 'heavy_loss'
                END                                AS roas_band,
                -- classify cr_level
                CASE
                    WHEN COALESCE(lr.orders, 0) = 0           THEN 'zero'
                    WHEN lr.clicks > 0
                         AND (lr.orders::numeric / lr.clicks * 100) >= {CR_THRESHOLD}
                                                               THEN 'high'
                    ELSE 'low'
                END                                AS cr_level,
                -- classify ctr_level
                CASE
                    WHEN lr.views > 0
                         AND (lr.clicks::numeric / lr.views * 100) >= {CTR_THRESHOLD}
                                                               THEN 'high'
                    ELSE 'low'
                END                                AS ctr_level
            FROM (
                SELECT DISTINCT ON (listing_id) *
                FROM listing_report
                ORDER BY listing_id, import_date DESC
            ) lr
        )
        SELECT
            lr.listing_id,
            lr.title,
            lr.product,
            lr.ctr,
            lr.cr,
            lr.roas,
            'https://www.etsy.com/listing/' || lr.listing_id AS url,
            lr.views,
            lr.clicks,
            lr.orders,
            lr.revenue,
            lr.spend,
            -- scenario fields
            sr.action                              AS scenario_action,
            sr.case_name                           AS scenario_label,
            sr.cause                               AS scenario_cause,
            sr.fix_listing                         AS scenario_fix_listing,
            sr.fix_ads                             AS scenario_fix_ads,
            -- reference: best revenue-potential alike product from market
            ref.title                              AS ref_title,
            ref.listing_link                       AS ref_url
        FROM lr
        LEFT JOIN scenarios_rules sr
            ON  sr.roas_band  = lr.roas_band
            AND sr.cr_level   = lr.cr_level
            AND sr.ctr_level  = lr.ctr_level
        LEFT JOIN LATERAL (
            SELECT title, listing_link
            FROM market_listing
            WHERE product_type = lr.product
            ORDER BY (price::float * review_count) DESC NULLS LAST
            LIMIT 1
        ) ref ON true
        ORDER BY
            CASE sr.action
                WHEN 'improve_or_off' THEN 1
                WHEN 'improve'        THEN 2
                WHEN 'keep'           THEN 3
                ELSE 4
            END ASC,
            lr.roas ASC NULLS LAST
    """)
    result = await db.execute(sql)
    return [dict(r) for r in result.mappings().all()]
