import json
from decimal import Decimal
from pathlib import Path
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
     "Listing: Khách clicks vào đúng intent nhưng listing chưa tốt.\nAds: Keywords trong ads chưa tối ưu.",
     "Kiểm tra giá bán, giá ship, review, hình thông tin sp đã rõ chưa, option sản phẩm có thể thêm gì không.",
     "Tắt bớt các keywords không hiệu quả đang chạy trong listing."),
    ("profitable", "high", "low",
     "Có sales và đang lời", "improve",
     "Listing: hình main chưa đúng intent, keywords dùng trong listing chưa chuẩn, giá mòi đang cao.\nAds: Keywords dùng trong ads đang chưa đúng intent.",
     "Tăng CTR: xem lại keywords, hình main, alt trong hình main, giá mòi.",
     "Tắt bớt các keywords không hiệu quả đang chạy trong listing."),
    ("profitable", "low", "low",
     "Có sales và đang lời", "improve",
     "Ads: Keyword đang không hiệu quả, giá mòi đang cao.\nListing: vừa chưa đúng intent vừa chưa đủ hấp dẫn để khách mua.\nViews cao, CTR thấp: Keywords trong ads và listing quá rộng, cạnh tranh cao.",
     "Tăng CTR: xem lại keywords, hình main, alt trong hình main, giá mòi.\nTăng CR: tối ưu giá bán, offers, giá ship, reviews.\nGiảm CPC: đổi keywords rộng thành long-tailed.",
     "Tắt bớt các keywords không đúng intent hoặc target rộng, cpc cao."),

    # --- Lỗ nhẹ (1 ≤ ROAS < breakeven) ---
    ("slight_loss", "high", "high",
     "Có sales, đang lỗ nhẹ", "improve",
     "Khi views và clicks quá thấp.\nAOV chưa đủ cover tiền ads, cpc quá cao (>$0.8).",
     "Views và clicks thấp: xem lại keywords, giá mòi, hình main, alt trong hình main.\nTăng AOV: thêm Offer hoặc dùng related products.",
     "Tắt bớt các keywords không đúng intent hoặc target rộng, cpc cao."),
    ("slight_loss", "high", "low",
     "Có sales, đang lỗ nhẹ", "improve",
     "Do keywords trong listing chưa đúng intent hoặc target quá rộng, do hình main, giá mòi.\nDo phần keywords trong ads chưa đúng intent hoặc quá rộng, cạnh tranh cao.",
     "Tối ưu keywords, hình main, alt trong hình main, giá mòi.",
     "Tắt bớt các keywords không đúng intent hoặc target rộng, cpc cao."),
    ("slight_loss", "low", "high",
     "Có sales, đang lỗ nhẹ", "improve",
     "Do listing chưa đủ hấp dẫn.",
     "Xem lại keywords dùng trong listing, đổi long-tailed keywords, bỏ bớt keywords target rộng.\nHình main, alt trong hình main.\nGiá mòi: giảm.",
     "Tắt bớt các keywords không đúng intent hoặc target rộng, cpc cao."),
    ("slight_loss", "low", "low",
     "Có sales, đang lỗ nhẹ", "improve",
     "Nếu có views: do listing, do keywords trong ads.\nViews thấp: do listing (xem lại keywords, hình main đúng intent), do keywords đang chạy trong ads.",
     "Tăng CTR: sửa keywords khi cần, kiểm tra alt trong hình main, cân nhắc đổi hình main, video, giảm giá mòi.\nTăng CR: xem giá bán, giá ship, offer, reviews.",
     "Tắt bớt các keywords không hiệu quả đang chạy trong listing."),

    # --- Lỗ nặng (0 < ROAS < 1) ---
    ("heavy_loss", "high", "high",
     "Có sales, lỗ nặng", "improve",
     "AOV chưa đủ cover ads spend.\nDo CPC quá cao (>$0.8).\nViews thấp.",
     "Views thấp: kiểm tra lại keywords trong listing và ads (loại bỏ từ không đúng intent, target quá rộng), hình main hoặc alt trong hình main.\nAOV thấp: thay đổi offer, thêm related products.",
     "Tắt keywords không đúng intent, target rộng, cạnh tranh cao trong ads."),
    ("heavy_loss", "low", "high",
     "Có sales, lỗ nặng", "improve",
     "CR thấp do listing chưa tốt, keywords trong ads chưa đúng intent hoặc quá rộng.",
     "Xem lại keywords dùng trong listing, đổi long-tailed keywords, bỏ bớt keywords target rộng.\nHình main, alt trong hình main.\nGiá mòi: giảm.",
     "Tắt keywords không đúng intent, target rộng, cạnh tranh cao trong ads."),
    ("heavy_loss", "high", "low",
     "Có sales, lỗ nặng", "improve",
     "Keywords (trong listing và trong ads), hình main, giá mòi.",
     "Xem lại keywords dùng trong listing, đổi long-tailed keywords, bỏ bớt keywords target rộng.\nHình main, alt trong hình main.\nGiá mòi: giảm.",
     "Tắt keywords không đúng intent, target rộng, cạnh tranh cao trong ads."),
    ("heavy_loss", "low", "low",
     "Có sales, lỗ nặng", "improve_or_off",
     "Listing chưa tối ưu: do keywords, hình main, giá mòi.\nĐã tối ưu nhưng không có xu hướng cải thiện: tắt.",
     "Xem lại keywords dùng trong listing, đổi long-tailed keywords, bỏ bớt keywords target rộng.\nHình main, alt trong hình main.\nGiá mòi: giảm.\nKiểm tra giá bán, giá ship, reviews xem có review xấu không.",
     "Tắt keywords không đúng intent, target rộng, cạnh tranh cao trong ads."),

    # --- Không có sale (ROAS = 0) ---
    ("no_sales", "zero", "high",
     "Không có sale, có clicks", "improve_or_off",
     "Listing mới: để theo dõi thêm, kiểm tra keywords trong ads.\nListing cũ, đã tối ưu nhưng không cải thiện: tắt.\nListing cũ, chưa tối ưu: keywords có thể không liên quan, giá bán, giá mòi, hình chi tiết hoặc reviews.",
     "Đổi keywords thành long-tailed, up thêm ảnh chi tiết, xin reviews, gỡ review xấu.",
     "Tắt keywords không đúng intent, target rộng, cạnh tranh cao trong ads."),
    ("no_sales", "zero", "low",
     "Không có sale, có clicks", "improve_or_off",
     "Listing mới: để theo dõi thêm, kiểm tra keywords trong ads.\nListing cũ, đã tối ưu nhưng không cải thiện: tắt.\nListing cũ, chưa tối ưu: keywords có thể không liên quan, giá bán, giá mòi, hình chi tiết hoặc reviews.\nDo listing mất index.",
     "Đổi keywords thành long-tailed, up thêm ảnh chi tiết, xin reviews, gỡ review xấu.\nThêm offers, giảm giá.\nNếu do listing mất index: deactive → reactive lại listing.",
     "Tắt keywords không đúng intent, target rộng, cạnh tranh cao trong ads."),
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


async def get_dashboard_listings(db: AsyncSession, market_db: AsyncSession) -> list[dict]:
    """
    listings (all) → JOIN listing_report (all range-period rows, dedup listing_id+period)
        → compute CTR/CR per row, classify bands
        → LEFT JOIN LATERAL market_listing (id = listing_id, latest) → own market data
        → LEFT JOIN scenarios_rules (3-dim)
        → LEFT JOIN LATERAL market_listing (competitor ref, better badge/review/rating)
    Sort: listing_id ASC
    """
    sql = text(f"""
        WITH lr AS (
            SELECT DISTINCT ON (listing_id, period)
                listing_id,
                title,
                no_vm,
                category                           AS product,
                period,
                import_time                        AS reference_date,
                views,
                clicks,
                orders,
                revenue,
                spend,
                COALESCE(roas, 0)                  AS roas,
                CASE WHEN views > 0
                     THEN ROUND(clicks::numeric / views * 100, 2)
                     ELSE 0 END                    AS ctr,
                CASE WHEN clicks > 0
                     THEN ROUND(orders::numeric / clicks * 100, 2)
                     ELSE 0 END                    AS cr,
                CASE
                    WHEN COALESCE(orders, 0) = 0           THEN 'no_sales'
                    WHEN COALESCE(roas, 0) >= {ROAS_BREAKEVEN} THEN 'profitable'
                    WHEN COALESCE(roas, 0) >= 1            THEN 'slight_loss'
                    ELSE 'heavy_loss'
                END                                AS roas_band,
                CASE
                    WHEN COALESCE(orders, 0) = 0           THEN 'zero'
                    WHEN clicks > 0
                         AND (orders::numeric / clicks * 100) >= {CR_THRESHOLD}
                                                            THEN 'high'
                    ELSE 'low'
                END                                AS cr_level,
                CASE
                    WHEN views > 0
                         AND (clicks::numeric / views * 100) >= {CTR_THRESHOLD}
                                                            THEN 'high'
                    ELSE 'low'
                END                                AS ctr_level
            FROM listing_report
            WHERE period LIKE '% - %' OR period LIKE '%/%-%'
            ORDER BY listing_id, period, import_time DESC
        )
        SELECT
            l.listing_id,
            COALESCE(lr.title, l.title)            AS title,
            COALESCE(lr.product, l.category)       AS product,
            lr.period,
            lr.reference_date,
            lr.ctr,
            lr.cr,
            lr.roas,
            COALESCE(l.url, 'https://www.etsy.com/listing/' || l.listing_id) AS url,
            lr.no_vm,
            lr.views,
            lr.clicks,
            lr.orders,
            lr.revenue,
            lr.spend,
            sr.action                              AS scenario_action,
            sr.case_name                           AS scenario_label,
            sr.cause                               AS scenario_cause,
            sr.fix_listing                         AS scenario_fix_listing,
            sr.fix_ads                             AS scenario_fix_ads,
            refs.references                        AS "references",
            kw.keywords                            AS keywords
        FROM listings l
        LEFT JOIN lr ON lr.listing_id = l.listing_id
        LEFT JOIN scenarios_rules sr
            ON  sr.roas_band = lr.roas_band
            AND sr.cr_level  = lr.cr_level
            AND sr.ctr_level = lr.ctr_level
        LEFT JOIN LATERAL (
            SELECT json_agg(
                json_build_object(
                    'reference_listing_id', re.reference_listing_id,
                    'ref_rank',             re.ref_rank,
                    'ref_title',            re.ref_title,
                    'ref_shop',             re.ref_shop,
                    'ref_url',              re.ref_url,
                    'ref_price',            re.ref_price,
                    'ref_discount',         re.ref_discount,
                    'ref_rating',           re.ref_rating,
                    'ref_review_count',     re.ref_review_count,
                    'ref_tag_ranking',      re.ref_tag_ranking,
                    'ref_badge',            re.ref_badge,
                    'ref_free_shipping',    re.ref_free_shipping,
                    'ref_product_type',     re.ref_product_type,
                    'ref_import_date',      re.ref_import_date
                ) ORDER BY re.ref_rank
            ) AS "references"
            FROM references_engine re
            WHERE re.listing_id = l.listing_id
        ) refs ON true
        LEFT JOIN LATERAL (
            SELECT json_agg(
                json_build_object(
                    'keyword',          kr.keyword,
                    'currently_status', kr.relevant,
                    'views',            kr.views,
                    'clicks',           kr.clicks,
                    'orders',           kr.orders,
                    'revenue',          kr.revenue,
                    'spend',            kr.spend,
                    'roas',             kr.roas,
                    'click_rate',       kr.click_rate
                ) ORDER BY COALESCE(kr.orders, 0) DESC, COALESCE(kr.clicks, 0) DESC
            ) AS keywords
            FROM keyword_report kr
            WHERE kr.listing_id = l.listing_id
              AND kr.import_time = (
                  SELECT MAX(import_time) FROM keyword_report
                  WHERE listing_id = l.listing_id
              )
        ) kw ON true
        ORDER BY l.listing_id ASC, lr.period ASC
    """)
    result = await db.execute(sql)
    rows = [dict(r) for r in result.mappings().all()]

    # Fetch own market data from ETSY_MARKET_DB, merge by listing_id
    listing_ids = list({r["listing_id"] for r in rows if r.get("listing_id")})
    market_map: dict = {}
    if listing_ids:
        mkt_sql = text("""
            SELECT DISTINCT ON (id)
                id, price, discount, rating, review_count, badge, free_shipping, is_ad, tag_ranking
            FROM market_listing
            WHERE id = ANY(:ids)
            ORDER BY id, import_date DESC
        """)
        mkt_result = await market_db.execute(mkt_sql, {"ids": listing_ids})
        for row in mkt_result.mappings().all():
            market_map[row["id"]] = dict(row)

    for r in rows:
        own = market_map.get(r.get("listing_id"), {})
        r["price"] = own.get("price")
        r["discount_price"] = own.get("discount")
        r["rating"] = own.get("rating")
        r["review_count"] = own.get("review_count")
        r["badge"] = own.get("badge")
        r["free_shipping"] = own.get("free_shipping")
        r["is_ad"] = own.get("is_ad")
        r["tag_ranking"] = own.get("tag_ranking")

    return rows


def write_dashboard_json(listings: list[dict], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(
        json.dumps(listings, default=lambda o: float(o) if isinstance(o, Decimal) else str(o), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
