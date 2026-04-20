"""
Weekly Etsy Crawl — headless Playwright (GitHub Actions compatible)
====================================================================
Chạy mỗi thứ 6 via GitHub Actions. Không cần real Chrome / CDP.

Flow:
  1. Load search URLs từ data/crawler/search_urls.csv
  2. Crawl mỗi URL bằng headless Chromium + stealth
  3. Upsert kết quả vào market_listing
  4. Tính original_price từ price/discount

Env vars cần (GitHub Secrets):
  DATABASE_URL — Neon PostgreSQL connection string
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
MAX_ITEMS       = 48

USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]

# ── DB helpers ────────────────────────────────────────────────────────────────

def pg_dsn() -> str:
    raw = os.environ.get("DATABASE_URL", "")
    if not raw:
        # fallback: load từ .env local
        env_path = os.path.join(os.path.dirname(__file__), "../../.env")
        if os.path.exists(env_path):
            for line in open(env_path):
                if line.startswith("DATABASE_URL="):
                    raw = line.split("=", 1)[1].strip()
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


SEARCH_URLS_CSV = Path(__file__).parent / "search_urls.csv"


def load_search_urls(_dsn: str = None) -> list[dict]:
    """Load search URLs từ search_urls.csv (columns: category, url)."""
    with open(SEARCH_URLS_CSV, newline="", encoding="utf-8") as f:
        return [{"url": r["url"], "category": r["category"]} for r in csv.DictReader(f)]


def upsert_listings(dsn: str, items: list[dict]):
    """Upsert vào market_listing (INSERT nếu chưa có, UPDATE nếu đã có)."""
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
        cur.execute("SELECT id FROM market_listing WHERE id = %s", (lid,))
        exists = cur.fetchone()

        if exists:
            cur.execute("""
                UPDATE market_listing SET
                    badge          = COALESCE(%s, badge),
                    discount       = COALESCE(%s, discount),
                    tag_ranking    = COALESCE(%s, tag_ranking),
                    review_count   = COALESCE(review_count, %s),
                    is_ad          = %s,
                    free_shipping  = %s,
                    import_date    = %s
                WHERE id = %s
            """, (
                item.get("badge"), item.get("discount"),
                item.get("tag_ranking"), item.get("review_count"),
                bool(item.get("is_ad")), bool(item.get("free_shipping")),
                today, lid,
            ))
            updated += cur.rowcount
        else:
            q = unquote_plus(parse_qs(urlparse(item.get("source_url", "")).query).get("q", [""])[0])
            cur.execute("""
                INSERT INTO market_listing
                    (id, search_tag, product_type, title, price, shop_name,
                     rating, review_count, badge, discount, free_shipping, is_ad,
                     tag_ranking, url, import_date, importer)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'crawl_weekly')
            """, (
                lid, q, q,
                item.get("title"), item.get("price"),
                item.get("shop"), item.get("rating_score"),
                item.get("review_count"), item.get("badge"),
                item.get("discount"), bool(item.get("free_shipping")),
                bool(item.get("is_ad")), item.get("tag_ranking"),
                item.get("url"), today,
            ))
            inserted += cur.rowcount

    conn.commit(); conn.close()
    return inserted + updated


def fill_original_price(dsn: str) -> int:
    """Tính original_price = price / (1 - discount/100) cho rows chưa có."""
    conn = get_conn(dsn); cur = conn.cursor()
    cur.execute("""
        UPDATE market_listing
        SET original_price = ROUND(price::numeric / (1 - discount::numeric / 100))
        WHERE price IS NOT NULL AND discount IS NOT NULL
          AND discount > 0 AND discount < 100
          AND original_price IS NULL
    """)
    n = cur.rowcount
    conn.commit(); conn.close()
    return n


# ── JS extractor ──────────────────────────────────────────────────────────────

EXTRACT_JS = """
(maxItems) => {
    const clean = s => s ? s.replace(/\\s+/g, ' ').trim() : '';

    const allCards = [...document.querySelectorAll('[data-listing-id]')].filter(
        el => !el.parentElement?.closest('[data-listing-id]')
    );
    if (allCards.length === 0) return [];

    const seen = new Set();
    const results = [];

    allCards.forEach((card, idx) => {
        const listingId = card.getAttribute('data-listing-id') || '';
        if (!listingId || seen.has(listingId)) return;
        seen.add(listingId);

        const linkEl   = card.querySelector('a[href*="/listing/"]');
        const imgEl    = card.querySelector('img[src], img[data-src], picture img');

        // shop
        const shopLinkEl = card.querySelector('a[href*="/shop/"]');
        let shop = '';
        if (shopLinkEl) {
            const parts = shopLinkEl.pathname.split('/shop/');
            if (parts.length > 1) shop = parts[1].split('/')[0] || '';
        }
        if (!shop) {
            const m = (card.textContent || '').match(/From shop\\s+(\\S+)/) ||
                      (card.textContent || '').match(/\\bBy\\s+([A-Z]\\S+)/);
            if (m) shop = m[1];
        }

        // prices
        const priceEls = [...card.querySelectorAll('[class*="currency-value"]')];
        const price = priceEls[0]
            ? Math.round(parseFloat(priceEls[0].textContent.replace(/[^\\d.]/g,'')) * 10000) || null
            : null;

        // discount %
        let discount = null;
        const discEl = card.querySelector('[class*="percent-off"],[class*="sale-percent"],[class*="discount"]');
        if (discEl) {
            const d = discEl.textContent.replace(/[^\\d]/g, '');
            if (d) discount = parseInt(d);
        }

        // badge
        const badgeEl = card.querySelector('[class*="wt-badge"],[class*="listing-badge"]');
        let badge = badgeEl ? clean(badgeEl.textContent) : null;
        if (!badge) {
            const m = (card.textContent || '').match(/\\b(Bestseller|Popular now|Etsy's Pick)\\b/);
            if (m) badge = m[1];
        }

        // rating + reviews
        let rating_score = null, review_count = null;
        const ratingEl = card.querySelector('[aria-label*="star"],[aria-label*="review"]');
        if (ratingEl) {
            const lbl = ratingEl.getAttribute('aria-label') || '';
            const s = (lbl.match(/([\\d.]+)\\s+star/) || [])[1];
            const r = (lbl.match(/(\\d[\\d,]*)\\s+review/) || [])[1];
            if (s) rating_score = parseFloat(s);
            if (r) review_count = parseInt(r.replace(/,/g,''));
        }
        if (review_count === null) {
            const m = (card.textContent || '').match(/(\\d\\.\\d)\\s*\\(([\\d,.k]+)\\)/i);
            if (m) {
                rating_score = parseFloat(m[1]);
                const rs = m[2].replace(/,/g,'');
                review_count = rs.toLowerCase().endsWith('k')
                    ? Math.round(parseFloat(rs)*1000) : parseInt(rs);
            }
        }

        // is_ad / free_shipping
        const txt = card.textContent || '';
        const isAd = /\\bAd by\\b/i.test(txt) || !!card.querySelector('[class*="sponsored"],[data-ad-unit]');
        const freeShipping = /FREE shipping/i.test(txt);

        results.push({
            listing_id   : listingId,
            title        : (() => { const n = card.querySelector('h3,h2,[class*="title"]'); return n ? clean(n.textContent) : ''; })(),
            price,
            shop,
            rating_score,
            review_count,
            badge,
            discount,
            is_ad        : isAd,
            free_shipping: freeShipping,
            tag_ranking  : idx + 1,
            url          : linkEl ? linkEl.href.split('?')[0] : '',
        });

        if (results.length >= maxItems) return;
    });

    return results;
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

async def crawl_all(search_rows: list[dict], dsn: str):
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
            async with sem:
                page = await browser.new_page(
                    user_agent=ua,
                    locale="en-US",
                    viewport={"width": 1440, "height": 900},
                )
                await page.add_init_script(STEALTH_JS)
                try:
                    print(f"  [{idx}/{len(search_rows)}] {url.split('q=')[-1]}")
                    await page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
                    await asyncio.sleep(random.uniform(RENDER_WAIT_MIN, RENDER_WAIT_MAX))

                    if await is_blocked(page):
                        print(f"  [{idx}] Blocked — skipping.")
                        return

                    await simulate_scroll(page)
                    raw = await page.evaluate(EXTRACT_JS, MAX_ITEMS)

                    if not raw:
                        print(f"  [{idx}] No cards found.")
                        return

                    # Attach source_url cho upsert
                    for item in raw:
                        item["source_url"] = url

                    n = upsert_listings(dsn, raw)
                    async with lock:
                        total_upserted += n
                    print(f"  [{idx}] {len(raw)} cards → {n} upserted (total {total_upserted})")

                    await asyncio.sleep(random.uniform(DELAY_MIN, DELAY_MAX))

                except Exception as e:
                    print(f"  [{idx}] Error: {e}")
                finally:
                    await page.close()

        tasks = [process(i, row) for i, row in enumerate(search_rows, 1)]
        await asyncio.gather(*tasks)
        await browser.close()

    return total_upserted


# ── entry ─────────────────────────────────────────────────────────────────────

async def main():
    print("=" * 62)
    print("  Weekly Etsy Crawl")
    print("=" * 62)

    dsn = pg_dsn()

    # 1. Load URLs
    search_rows = load_search_urls()
    print(f"\n[1/3] Loaded {len(search_rows)} search URLs from {SEARCH_URLS_CSV.name}")

    # 2. Crawl
    print(f"\n[2/3] Crawling ({CONCURRENCY} tabs parallel)...")
    total = await crawl_all(search_rows, dsn)
    print(f"      → {total} rows upserted into market_listing")

    # 3. original_price
    print("\n[3/3] Computing original_price from price/discount...")
    n = fill_original_price(dsn)
    print(f"      → {n} rows filled")

    print("\n" + "=" * 62)
    print("  Done.")
    print("=" * 62)


if __name__ == "__main__":
    asyncio.run(main())
