"""
Internal Listing Extension Crawl — headless Playwright (GitHub Actions compatible)
====================================================================
Replicated from crawl_weekly.py with adjustments for single listing pages.
Flow:
  1. Load internal URLs từ listings table
  2. Crawl mỗi URL bằng headless Chromium + stealth
  3. Upsert kết quả vào listing_extense
  4. Tính original_price từ price/discount
"""

import asyncio
import csv
import json
import os
import random
import re
import sys
from datetime import date
from pathlib import Path
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse, unquote_plus

from playwright.async_api import async_playwright, Browser, Page

# ── config ────────────────────────────────────────────────────────────────────

CONCURRENCY     = 3
NAV_TIMEOUT_MS  = 35_000
RENDER_WAIT_MIN = 3.0
RENDER_WAIT_MAX = 5.5
DELAY_MIN       = 8
DELAY_MAX       = 18

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

# ── DB helpers ────────────────────────────────────────────────────────────────

def pg_dsn() -> str:
    raw = os.environ.get("DATABASE_URL", "")
    if not raw:
        env_path = os.path.join(os.path.dirname(__file__), "../../.env")
        if os.path.exists(env_path):
            for line in open(env_path):
                if line.startswith("DATABASE_URL="):
                    raw = line.split("=", 1)[1].strip().strip('"').strip("'")
    if not raw:
        raise SystemExit("[!] DATABASE_URL chưa cấu hình")
    url = raw.replace("postgresql+asyncpg://", "postgresql://", 1).replace("postgres://", "postgresql://", 1)
    parsed = urlparse(url)
    qs = parse_qs(parsed.query); qs.pop("channel_binding", None)
    if "sslmode" not in qs:
        qs["sslmode"] = ["require"]
    return urlunparse(parsed._replace(query=urlencode({k: v[0] for k, v in qs.items()})))


def get_conn(dsn: str):
    import psycopg2
    return psycopg2.connect(dsn)


def init_db(dsn: str):
    """Ensure listing_extense table exists."""
    conn = get_conn(dsn); cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS listing_extense (
        id               VARCHAR(32) PRIMARY KEY,
        search_tag       TEXT,
        product_type     TEXT,
        title            TEXT,
        price            BIGINT,
        original_price   BIGINT,
        shop_name        TEXT,
        rating           REAL,
        review_count     INTEGER,
        badge            TEXT,
        discount         INTEGER,
        free_shipping    BOOLEAN,
        is_ad            BOOLEAN DEFAULT FALSE,
        tag_ranking      INTEGER,
        url              TEXT,
        import_date      DATE,
        importer         VARCHAR(32),
        updated_at       TIMESTAMPTZ DEFAULT now()
    );
    """)
    conn.commit(); conn.close()


def load_internal_listings(dsn: str) -> list[dict]:
    """Load internal listings từ bảng listings."""
    conn = get_conn(dsn); cur = conn.cursor()
    cur.execute("SELECT listing_id, url, category FROM listings WHERE url IS NOT NULL")
    rows = cur.fetchall()
    conn.close()
    return [{"listing_id": r[0], "url": r[1], "category": r[2]} for r in rows if r[0]]


def upsert_extense(dsn: str, items: list[dict]):
    """Upsert vào listing_extense (INSERT nếu chưa có, UPDATE nếu đã có)."""
    if not items:
        return 0
    import psycopg2
    conn = get_conn(dsn); cur = conn.cursor()
    updated = inserted = 0
    today = date.today()

    for item in items:
        lid = item.get("listing_id")
        if not lid:
            continue
        cur.execute("SELECT id FROM listing_extense WHERE id = %s", (lid,))
        exists = cur.fetchone()

        if exists:
            cur.execute("""
                UPDATE listing_extense SET
                    price          = COALESCE(%s, price),
                    title          = COALESCE(%s, title),
                    rating         = COALESCE(%s, rating),
                    badge          = COALESCE(%s, badge),
                    discount       = COALESCE(%s, discount),
                    review_count   = COALESCE(%s, review_count),
                    free_shipping  = %s,
                    import_date    = %s,
                    updated_at     = now()
                WHERE id = %s
            """, (
                item.get("price"), item.get("title"),
                item.get("rating_score"), item.get("badge"),
                item.get("discount"), item.get("review_count"),
                bool(item.get("free_shipping")), today, lid,
            ))
            updated += cur.rowcount
        else:
            cur.execute("""
                INSERT INTO listing_extense
                    (id, search_tag, product_type, title, price, shop_name,
                     rating, review_count, badge, discount, free_shipping, is_ad,
                     tag_ranking, url, import_date, importer)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'crawl_internal')
            """, (
                lid, item.get("category"), item.get("category"),
                item.get("title"), item.get("price"),
                item.get("shop"), item.get("rating_score"),
                item.get("review_count"), item.get("badge"),
                item.get("discount"), bool(item.get("free_shipping")),
                False, 0, item.get("url"), today,
            ))
            inserted += cur.rowcount

    conn.commit(); conn.close()
    return inserted + updated


def fill_original_price(dsn: str) -> int:
    """Tính original_price = price / (1 - discount/100), overwrite mỗi lần crawl."""
    conn = get_conn(dsn); cur = conn.cursor()
    cur.execute("""
        UPDATE listing_extense
        SET original_price = ROUND(price::numeric / (1 - discount::numeric / 100))
        WHERE price IS NOT NULL AND discount IS NOT NULL
          AND discount > 0 AND discount < 100
    """)
    n = cur.rowcount
    conn.commit(); conn.close()
    return n


# ── JS extractor ──────────────────────────────────────────────────────────────

EXTRACT_JS = """
() => {
    const clean = s => s ? s.replace(/\\s+/g, ' ').trim() : '';

    // Title
    const titleEl = document.querySelector('h1[data-buy-box-listing-title], h1');
    const title = titleEl ? clean(titleEl.textContent) : '';

    // Price
    const currentPriceEl = document.querySelector('.wt-text-title-03, .wt-text-title-larger, .wt-pr-2 .wt-text-title-03');
    let priceText = currentPriceEl ? currentPriceEl.textContent : '';
    if (!priceText) {
        const prices = [...document.querySelectorAll('span.wt-text-title-03')];
        if (prices.length > 0) priceText = prices[0].textContent;
    }
    const price = priceText 
        ? Math.round(parseFloat(priceText.replace(/[^\\d.]/g,'')) * 10000) 
        : null;

    // Discount
    let discount = null;
    const discEl = document.querySelector('.wt-badge--sale, .wt-text-caption .wt-font-bold');
    if (discEl && discEl.textContent.includes('% off')) {
        const d = discEl.textContent.replace(/[^\\d]/g, '');
        if (d) discount = parseInt(d);
    }

    // Shop
    const shopEl = document.querySelector('a[href*="/shop/"] [class*="wt-text-body-01"], a[href*="/shop/"]');
    const shop = shopEl ? clean(shopEl.textContent) : '';

    // Rating & Reviews
    let rating_score = null, review_count = null;
    const ratingLink = document.querySelector('a[href*="#reviews"]');
    if (ratingLink) {
        const txt = ratingLink.textContent;
        const m = txt.match(/([\\d.]+)\\s*\\(([\\d,]+)\\)/);
        if (m) {
            rating_score = parseFloat(m[1]);
            review_count = parseInt(m[2].replace(/,/g,''));
        }
    }
    if (!rating_score) {
        const scoreEl = document.querySelector('.wt-badge--star-rating .wt-screen-reader-only');
        if (scoreEl) {
            const m = scoreEl.textContent.match(/([\\d.]+)/);
            if (m) rating_score = parseFloat(m[1]);
        }
    }

    // Badge
    const badgeEl = document.querySelector('.wt-badge--star-seller, .wt-badge--bestseller, .wt-badge--etsys-pick');
    const badge = badgeEl ? clean(badgeEl.textContent) : null;

    // Free shipping
    const shipText = (document.body.textContent || '');
    const freeShipping = /FREE shipping/i.test(shipText) || !!document.querySelector('.wt-badge--free-shipping');

    return [{
        title,
        price,
        discount,
        shop,
        rating_score,
        review_count,
        badge,
        free_shipping: freeShipping
    }];
}
"""

# ── stealth ───────────────────────────────────────────────────────────────────

STEALTH_JS = """
Object.defineProperty(navigator, 'webdriver', {get: () => undefined});
Object.defineProperty(navigator, 'languages', {get: () => ['en-US', 'en']});
Object.defineProperty(navigator, 'plugins',   {get: () => [1,2,3,4,5]});
window.chrome = {runtime: {}};
"""

# ── page helpers ──────────────────────────────────────────────────────────────

async def simulate_scroll(page: Page):
    height   = await page.evaluate("document.body.scrollHeight")
    viewport = await page.evaluate("window.innerHeight")
    if height <= viewport:
        return
    pos = 0; target = int(height * 0.75)
    while pos < target:
        pos = min(pos + random.randint(250, 500), target)
        await page.evaluate(f"window.scrollTo({{top:{pos},behavior:'smooth'}})")
        await asyncio.sleep(random.uniform(0.2, 0.5))
    await asyncio.sleep(random.uniform(0.5, 1.0))


async def is_blocked(page: Page) -> bool:
    try:
        url   = page.url or ""
        title = (await page.title()).lower()
        if "dd_referrer" in url or title in ("etsy.com", ""):
            return True
        return await page.evaluate("""
            () => !!(document.querySelector('[class*="captcha"]') ||
                     document.querySelector('[class*="challenge"]'))
        """)
    except Exception:
        return False


# ── core crawl ────────────────────────────────────────────────────────────────

async def crawl_all(targets: list[dict], dsn: str):
    sem  = asyncio.Semaphore(CONCURRENCY)
    lock = asyncio.Lock()
    total_upserted = 0
    ua = random.choice(USER_AGENTS)

    async with async_playwright() as p:
        browser: Browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox", "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
                f"--user-agent={ua}",
            ],
        )

        async def process(idx: int, row: dict):
            nonlocal total_upserted
            url = row["url"]
            lid = row["listing_id"]
            async with sem:
                page = await browser.new_page(
                    user_agent=ua,
                    locale="en-US",
                    viewport={"width": 1440, "height": 900},
                )
                await page.add_init_script(STEALTH_JS)
                try:
                    print(f"  [{idx}/{len(targets)}] ID:{lid} -> {url}")
                    await page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
                    await asyncio.sleep(random.uniform(RENDER_WAIT_MIN, RENDER_WAIT_MAX))

                    if await is_blocked(page):
                        print(f"  [{idx}] Blocked — skipping.")
                        return

                    await simulate_scroll(page)
                    raw = await page.evaluate(EXTRACT_JS)

                    if not raw or not raw[0].get("title"):
                        print(f"  [{idx}] Extraction failed or empty page.")
                        return

                    # Attach metadata
                    for item in raw:
                        item["listing_id"] = lid
                        item["url"] = url
                        item["category"] = row["category"]

                    n = upsert_extense(dsn, raw)
                    async with lock:
                        total_upserted += n
                    print(f"  [{idx}] Upserted {n} for {lid} (total {total_upserted})")

                    await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

                except Exception as e:
                    print(f"  [{idx}] Error: {e}")
                finally:
                    await page.close()

        tasks = [process(i, row) for i, row in enumerate(targets, 1)]
        await asyncio.gather(*tasks)
        await browser.close()

    return total_upserted


# ── entry ─────────────────────────────────────────────────────────────────────

async def main():
    print("=" * 62)
    print("  Internal Listing Extension Crawl")
    print("=" * 62)

    dsn = pg_dsn()
    
    # 0. Init Table
    init_db(dsn)

    # 1. Load URLs
    targets = load_internal_listings(dsn)
    print(f"\n[1/3] Loaded {len(targets)} internal listings from DB")

    if not targets:
        print("      → Nothing to crawl. Exit.")
        return

    # 2. Crawl
    print(f"\n[2/3] Crawling ({CONCURRENCY} tabs parallel)...")
    total = await crawl_all(targets, dsn)
    print(f"      → {total} rows upserted into listing_extense")

    # 3. original_price
    print("\n[3/3] Computing original_price from price/discount...")
    n = fill_original_price(dsn)
    print(f"      → {n} rows filled")

    print("\n" + "=" * 62)
    print("  Done.")
    print("=" * 62)


if __name__ == "__main__":
    asyncio.run(main())
