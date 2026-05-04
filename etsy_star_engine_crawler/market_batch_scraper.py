"""
Etsy Market Batch Scraper — VM01 Keywords
==========================================
Đọc keywords từ vm01_keywords.json, crawl search results + detail pages,
lưu vào 4 bảng trên etsy_market_db:
  - market_listing          (overview từ search results)
  - market_listing_details  (chi tiết từ trang listing)
  - market_listing_reviews  (reviews từ trang listing)
  - market_shop             (thông tin shop/seller)

Kế thừa toàn bộ anti-detect engine từ batch_scraper.py:
  - CDP + real Chrome (no automation flags)
  - Human-like scroll
  - CAPTCHA pause
  - Checkpoint / resume

Modes:
  (default)   Human-in-loop: pause sau mỗi keyword
  --auto      Fully automated (delay 30–90s)
  --auto N    Automated N keywords rồi dừng
  --resume TS Resume từ checkpoint_TS.json

Usage:
    python3 market_batch_scraper.py
    python3 market_batch_scraper.py --auto
    python3 market_batch_scraper.py --auto 10
    python3 market_batch_scraper.py --resume 20260501_120000
    python3 market_batch_scraper.py --init-schema
"""

import asyncio
import json
import random
import re
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import psycopg2.extras
from playwright.async_api import async_playwright, Browser, Page

# ─────────────────────────── paths & config ───────────────────────────────────

HERE         = Path(__file__).parent
ROOT         = HERE.parent                               # etsy-pilot/
KEYWORDS_FILE = ROOT / "data/crawler/vm01_keywords.json"
OUTPUT_DIR   = HERE / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

CHROME_PATH  = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
DEBUG_PORT   = 9222
PROFILE_DIR  = Path.home() / ".etsy_cdp_profile"

# DB
_ENV_PATH = ROOT / ".env"
_DB_URL   = ""
if _ENV_PATH.exists():
    for _line in _ENV_PATH.read_text().splitlines():
        key, sep, value = _line.strip().partition("=")
        if sep and key.strip().lower() == "etsy_market_db":
            _DB_URL = value.strip()
            break

IMPORTER        = "hien_crawler"
MAX_ITEMS       = 48           # listing cards per keyword (1 trang search Etsy ~ 48)
DELAY_MIN       = 30
DELAY_MAX       = 90
NAV_TIMEOUT_MS  = 35_000
RENDER_WAIT_MIN = 2.5
RENDER_WAIT_MAX = 4.5
PAGE_DELAY_MIN  = 2.0
PAGE_DELAY_MAX  = 5.0
DETAIL_DELAY_MIN = 2.0
DETAIL_DELAY_MAX = 4.0

# ─────────────────────────── DB setup ─────────────────────────────────────────

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS market_listing (
    listing_id      BIGINT PRIMARY KEY,
    keyword         TEXT,
    title           TEXT,
    price           NUMERIC(12,2),
    currency        CHAR(3),
    shop_name       VARCHAR(100),
    rating          NUMERIC(3,1),
    review_count    INTEGER,
    badge           VARCHAR(50),
    discount        SMALLINT,
    free_shipping   BOOLEAN,
    is_ad           BOOLEAN,
    tag_ranking     SMALLINT,
    url             TEXT,
    image_url       TEXT,
    source_url      TEXT,
    crawled_at      TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS market_listing_details (
    listing_id          BIGINT PRIMARY KEY,
    product_name        TEXT,
    design              TEXT,
    base_price          NUMERIC(12,2),
    sale_price          NUMERIC(12,2),
    discount_percent    SMALLINT,
    currency            CHAR(3),
    materials           TEXT,
    highlights          TEXT,
    shipping_status     VARCHAR(100),
    origin_ship_from    VARCHAR(100),
    ship_time_max_days  SMALLINT,
    us_shipping         BOOLEAN,
    return_policy       BOOLEAN,
    ai_summary          TEXT,
    crawled_at          TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_market_listing_details_lid ON market_listing_details(listing_id);
CREATE INDEX IF NOT EXISTS idx_market_listing_reviews_lid ON market_listing_reviews(listing_id);

CREATE TABLE IF NOT EXISTS market_listing_reviews (
    id          SERIAL PRIMARY KEY,
    listing_id  BIGINT,
    reviewer    VARCHAR(100),
    review_date DATE,
    stars       SMALLINT,
    content     TEXT,
    crawled_at  TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS market_shop (
    shop_name       VARCHAR(100) PRIMARY KEY,
    owner_name      VARCHAR(100),
    location        VARCHAR(200),
    join_year       SMALLINT,
    total_sales     INTEGER,
    shop_rating     NUMERIC(3,1),
    badge           VARCHAR(50),
    smooth_shipping BOOLEAN,
    speedy_replies  BOOLEAN,
    last_crawled_at TIMESTAMPTZ
);
"""


def get_db_conn():
    if not _DB_URL:
        raise RuntimeError("etsy_market_db connection string not found in .env")
    return psycopg2.connect(_DB_URL)


def init_db():
    conn = get_db_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
            # drop FK constraints if they exist (safe for crawler)
            cur.execute("""
                ALTER TABLE market_listing_details
                    DROP CONSTRAINT IF EXISTS market_listing_details_listing_id_fkey;
                ALTER TABLE market_listing_reviews
                    DROP CONSTRAINT IF EXISTS market_listing_reviews_listing_id_fkey;
            """)
    conn.close()
    print("[DB] Schema ready.")


# ─────────────────────────── JS extractors ────────────────────────────────────

SEARCH_EXTRACT_JS = """
(maxItems) => {
    const clean = s => s ? s.replace(/\\s+/g, ' ').trim() : '';
    const getText = (el, sels) => {
        for (const s of sels) {
            const node = el.querySelector(s);
            if (node && node.textContent.trim()) return clean(node.textContent);
        }
        return '';
    };
    const parseRating = (aria) => {
        if (!aria) return { rating: null, review_count: null };
        const score   = (aria.match(/([\\d.]+)\\s+star/)   || [])[1];
        const reviews = (aria.match(/(\\d[\\d,]*)\\s+review/) || [])[1];
        return {
            rating:       score   ? parseFloat(score)                 : null,
            review_count: reviews ? parseInt(reviews.replace(',','')) : null,
        };
    };
    const parseDiscount = (text) => {
        if (!text) return null;
        const pats = [/\\b(\\d{1,3})\\s*%\\s*off\\b/i, /\\bsave\\s*(\\d{1,3})\\s*%\\b/i];
        for (const p of pats) {
            const m = text.match(p);
            if (m) { const v = parseInt(m[1]); if (v > 0 && v < 100) return v; }
        }
        return null;
    };
    // VND uses '.' as thousand separator (e.g. "378.788₫"), USD uses ',' as thousand separator.
    // Count dots vs commas to detect format.
    const parsePrice = (text) => {
        if (!text) return null;
        const t = text.trim();
        const dots   = (t.match(/\\./g)  || []).length;
        const commas = (t.match(/,/g) || []).length;
        let normalized;
        if (dots > 1) {
            // e.g. "1.107.163" — dots are thousand separators
            normalized = t.replace(/\\./g, '').replace(/,/g, '.');
        } else if (commas > 1) {
            // e.g. "1,107,163" — commas are thousand separators
            normalized = t.replace(/,/g, '');
        } else if (dots === 1 && commas === 1) {
            // ambiguous: whichever comes last is decimal separator
            normalized = t.lastIndexOf('.') > t.lastIndexOf(',')
                ? t.replace(/,/g, '')
                : t.replace(/\\./g, '').replace(',', '.');
        } else {
            // single separator or none — remove non-digit except last separator
            normalized = t.replace(/[^\\d.,]/g, '');
            // if ends like ".xxx" with 3 digits it's a thousand sep not decimal
            if (/\\.\\d{3}$/.test(normalized) && dots === 1 && commas === 0)
                normalized = normalized.replace('.', '');
        }
        const v = parseFloat(normalized.replace(/[^\\d.]/g, ''));
        return isFinite(v) && v > 0 ? v : null;
    };
    const allCards = [...document.querySelectorAll('[data-listing-id]')].filter(
        el => !el.parentElement?.closest('[data-listing-id]')
    );
    if (!allCards.length) return [];
    const seen = new Set(), results = [];
    for (const card of allCards) {
        const listingId = card.getAttribute('data-listing-id') || '';
        if (!listingId || seen.has(listingId)) continue;
        seen.add(listingId);
        const linkEl   = card.querySelector('a[href*="/listing/"]');
        const url      = linkEl ? linkEl.href.split('?')[0] : '';
        const imgEl    = card.querySelector('img[src], img[data-src], picture img');
        const imageUrl = imgEl ? (imgEl.src || imgEl.dataset.src || '') : '';
        // shop_name — try multiple selectors Etsy uses
        let shop_name = '';
        const shopLinkEl = card.querySelector('a[href*="/shop/"]');
        if (shopLinkEl) {
            const parts = shopLinkEl.pathname.split('/shop/');
            if (parts.length > 1) shop_name = parts[1].split('/')[0] || '';
        }
        if (!shop_name) {
            const shopNameEl = card.querySelector('[class*="shop-name"],[class*="seller-name"],[data-shop-name]');
            if (shopNameEl) shop_name = clean(shopNameEl.textContent);
        }
        if (!shop_name) {
            const m = (card.textContent || '').match(/From shop\\s+(\\S+)/);
            if (m) shop_name = m[1];
        }
        let rating = null, review_count = null;
        const ratingEl = card.querySelector('[aria-label*="star"], [aria-label*="review"]');
        if (ratingEl) ({ rating, review_count } = parseRating(ratingEl.getAttribute('aria-label') || ''));
        const priceEl = card.querySelector('[class*="currency-value"],[class*="price-value"],span[class*="price"]');
        const price   = priceEl ? parsePrice(priceEl.textContent) : null;
        const txt     = card.textContent || '';
        const discEl  = card.querySelector('[class*="percent-off"],[class*="sale-percent"],[class*="discount"]');
        let discount  = discEl ? parseDiscount(clean(discEl.textContent || '')) : null;
        if (discount === null) discount = parseDiscount(txt);
        const badgeEl = card.querySelector('[class*="wt-badge"],[class*="listing-badge"]');
        let badge = badgeEl ? clean(badgeEl.textContent) : null;
        if (!badge) { const bm = txt.match(/\\b(Bestseller|Popular now|Etsy's Pick)\\b/); if (bm) badge = bm[1]; }
        results.push({
            listing_id:   listingId,
            title:        getText(card, ['h3','h2','[class*="title"]']),
            price,
            currency:     getText(card, ['[class*="currency-symbol"]','abbr[class*="currency"]']) || 'USD',
            shop_name,
            rating,
            review_count,
            badge,
            discount,
            is_ad:        /\\bAd by\\b/i.test(txt) || !!card.querySelector('[class*="sponsored"]'),
            free_shipping:/FREE shipping/i.test(txt),
            tag_ranking:  results.length + 1,
            url,
            image_url:    imageUrl,
        });
        if (results.length >= maxItems) break;
    }
    return results;
}
"""

DETAIL_EXTRACT_JS = (HERE / "_detail_extract.js").read_text(encoding="utf-8")

# ─────────────────────────── helpers ──────────────────────────────────────────

def banner(msg: str):
    print(f"\n{'─'*64}\n  {msg}\n{'─'*64}")


def launch_chrome():
    PROFILE_DIR.mkdir(parents=True, exist_ok=True)
    subprocess.Popen(
        [CHROME_PATH, f"--remote-debugging-port={DEBUG_PORT}",
         f"--user-data-dir={PROFILE_DIR}", "--no-first-run",
         "--no-default-browser-check", "--disable-logging", "--log-level=3",
         "--start-maximized", "https://www.etsy.com"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    time.sleep(4)


async def simulate_scroll(page: Page):
    scroll_height = await page.evaluate("document.body.scrollHeight")
    viewport      = await page.evaluate("window.innerHeight")
    if scroll_height <= viewport:
        return
    pos = 0
    target = int(scroll_height * 0.75)
    while pos < target:
        pos = min(pos + random.randint(220, 480), target)
        await page.evaluate(f"window.scrollTo({{top:{pos},behavior:'smooth'}})")
        await asyncio.sleep(random.uniform(0.25, 0.65))
    await asyncio.sleep(random.uniform(0.6, 1.3))
    await page.evaluate("window.scrollTo({top:0,behavior:'smooth'})")
    await asyncio.sleep(random.uniform(0.3, 0.7))


async def check_blocked(page: Page) -> bool:
    try:
        url   = page.url or ""
        title = (await page.title()).lower().strip()
        if "dd_referrer" in url or title in ("etsy.com", "") or "verification" in title:
            return True
        return await page.evaluate("""
            () => !!(document.querySelector('[class*="captcha"]') ||
                     document.querySelector('[class*="challenge"]') ||
                     document.querySelector('[class*="slider-button"]'))
        """)
    except Exception:
        return False


async def handle_captcha(page: Page) -> bool:
    print("\n  [!] CAPTCHA detected — solve in Chrome then press ENTER...")
    try:
        await asyncio.get_event_loop().run_in_executor(None, input)
    except EOFError:
        print("  [!] No interactive stdin available; skipping this detail page.")
        return False
    await asyncio.sleep(2)
    return not await check_blocked(page)


async def wait_for_listings(page: Page, timeout_ms: int = 15_000) -> bool:
    try:
        await page.wait_for_load_state("networkidle", timeout=min(timeout_ms, 8_000))
    except Exception:
        pass
    deadline = time.monotonic() + timeout_ms / 1000
    while time.monotonic() < deadline:
        count = await page.evaluate(
            "() => document.querySelectorAll('[data-listing-id]').length"
        )
        if count >= 4:
            return True
        await asyncio.sleep(0.5)
    return False


# ─────────────────────────── checkpoint ───────────────────────────────────────

def load_checkpoint(path: Path) -> set[str]:
    if not path.exists():
        return set()
    try:
        return set(json.loads(path.read_text()).get("done", []))
    except Exception:
        return set()


def save_checkpoint(path: Path, done: set[str]):
    path.write_text(json.dumps({"done": list(done)}, indent=2))


# ─────────────────────────── DB writes ────────────────────────────────────────

def upsert_listing(cur, item: dict, keyword: str, source_url: str):
    cur.execute("""
        INSERT INTO market_listing
          (listing_id, keyword, title, price, currency, shop_name, rating,
           review_count, badge, discount, free_shipping, is_ad, tag_ranking,
           url, image_url, source_url, crawled_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (listing_id) DO UPDATE SET
          keyword=EXCLUDED.keyword, title=EXCLUDED.title,
          price=EXCLUDED.price, rating=EXCLUDED.rating,
          review_count=EXCLUDED.review_count, badge=EXCLUDED.badge,
          discount=EXCLUDED.discount, tag_ranking=EXCLUDED.tag_ranking,
          crawled_at=EXCLUDED.crawled_at
    """, (
        int(item['listing_id']), keyword,
        item.get('title'), item.get('price'),
        (item.get('currency') or 'USD')[:3],
        item.get('shop_name'), item.get('rating'), item.get('review_count'),
        item.get('badge'), item.get('discount'),
        bool(item.get('free_shipping')), bool(item.get('is_ad')),
        item.get('tag_ranking'), item.get('url'),
        item.get('image_url'), source_url,
        datetime.now(timezone.utc),
    ))


def upsert_details(cur, listing_id: int, d: dict):
    cur.execute("""
        INSERT INTO market_listing_details
          (listing_id, base_price, sale_price, discount_percent, currency,
           materials, highlights, shipping_status, origin_ship_from,
           ship_time_max_days, us_shipping, return_policy, design, ai_summary,
           crawled_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (listing_id) DO UPDATE SET
          base_price=EXCLUDED.base_price, sale_price=EXCLUDED.sale_price,
          discount_percent=EXCLUDED.discount_percent,
          materials=EXCLUDED.materials, highlights=EXCLUDED.highlights,
          shipping_status=EXCLUDED.shipping_status,
          origin_ship_from=EXCLUDED.origin_ship_from,
          ship_time_max_days=EXCLUDED.ship_time_max_days,
          us_shipping=EXCLUDED.us_shipping, return_policy=EXCLUDED.return_policy,
          design=EXCLUDED.design, ai_summary=EXCLUDED.ai_summary,
          crawled_at=EXCLUDED.crawled_at
    """, (
        listing_id,
        d.get('base_price'), d.get('sale_price'), d.get('discount_percent'),
        'USD',
        d.get('materials'), d.get('highlights'), d.get('shipping_status'),
        d.get('origin_ship_from'), d.get('ship_time_max_days'),
        bool(d.get('us_shipping')), bool(d.get('return_policy')),
        d.get('design'), d.get('ai_summary'),
        datetime.now(timezone.utc),
    ))


def insert_reviews(cur, listing_id: int, reviews: list):
    # Xóa reviews cũ của listing này trước khi insert mới (fresh snapshot)
    cur.execute("DELETE FROM market_listing_reviews WHERE listing_id = %s", (listing_id,))
    for r in reviews:
        review_date = None
        if r.get('review_date'):
            try:
                review_date = datetime.strptime(r['review_date'][:10], '%Y-%m-%d').date()
            except Exception:
                pass
        cur.execute("""
            INSERT INTO market_listing_reviews
              (listing_id, reviewer, review_date, stars, content, crawled_at)
            VALUES (%s,%s,%s,%s,%s,%s)
        """, (
            listing_id, r.get('reviewer'), review_date,
            r.get('stars'), r.get('content'),
            datetime.now(timezone.utc),
        ))


def upsert_shop(cur, shop_name: str, s: dict):
    if not shop_name:
        return
    cur.execute("""
        INSERT INTO market_shop
          (shop_name, owner_name, location, join_year, total_sales,
           shop_rating, badge, smooth_shipping, speedy_replies, last_crawled_at)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (shop_name) DO UPDATE SET
          owner_name=EXCLUDED.owner_name, location=EXCLUDED.location,
          total_sales=EXCLUDED.total_sales, shop_rating=EXCLUDED.shop_rating,
          badge=EXCLUDED.badge, smooth_shipping=EXCLUDED.smooth_shipping,
          speedy_replies=EXCLUDED.speedy_replies,
          last_crawled_at=EXCLUDED.last_crawled_at
    """, (
        shop_name, s.get('owner_name'), s.get('location'),
        s.get('join_year'), s.get('total_sales'), s.get('shop_rating'),
        s.get('badge'), bool(s.get('smooth_shipping')),
        bool(s.get('speedy_replies')), datetime.now(timezone.utc),
    ))


# ─────────────────────────── core scrape ──────────────────────────────────────

async def scrape_search(page: Page, keyword: str) -> list[dict]:
    url = f"https://www.etsy.com/search?q={keyword.replace(' ', '+')}&currency=USD"
    print(f"  [search] {url}")
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
        await asyncio.sleep(random.uniform(RENDER_WAIT_MIN, RENDER_WAIT_MAX))
    except Exception as e:
        print(f"  [!] Navigation failed: {e}")
        return []

    if await check_blocked(page):
        ok = await handle_captcha(page)
        if not ok:
            return []

    await wait_for_listings(page)
    await simulate_scroll(page)

    items = await page.evaluate(SEARCH_EXTRACT_JS, MAX_ITEMS) or []
    print(f"  [search] {len(items)} listings found")
    return items


async def scrape_detail(page: Page, url: str):
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
        await asyncio.sleep(random.uniform(RENDER_WAIT_MIN, RENDER_WAIT_MAX))
    except Exception as e:
        print(f"    [!] Detail page failed: {e}")
        return None

    if await check_blocked(page):
        ok = await handle_captcha(page)
        if not ok:
            return None

    await simulate_scroll(page)
    try:
        return await page.evaluate(DETAIL_EXTRACT_JS)
    except Exception as e:
        print(f"    [!] JS extract failed: {e}")
        return None


# ─────────────────────────── main loop ────────────────────────────────────────

async def run(keywords: list[str], run_ts: str, auto_mode: bool, auto_limit: int = 0, init_schema: bool = False):
    checkpoint_path = OUTPUT_DIR / f"checkpoint_{run_ts}.json"
    done_kw         = load_checkpoint(checkpoint_path)

    pending = [kw for kw in keywords if kw not in done_kw]
    if not pending:
        print("[+] All keywords already done.")
        return
    if auto_mode and auto_limit > 0:
        pending = pending[:auto_limit]

    mode_label = f"AUTO {auto_limit}" if (auto_mode and auto_limit) else ("AUTO ALL" if auto_mode else "HUMAN-IN-LOOP")
    banner(f"Market Batch Scraper [{mode_label}] — {len(pending)} keywords")

    if init_schema:
        init_db()
    conn = get_db_conn()

    launch_chrome()
    print("[>] Waiting 5s for Chrome to settle...")
    await asyncio.sleep(5)

    async with async_playwright() as p:
        try:
            browser: Browser = await p.chromium.connect_over_cdp(f"http://localhost:{DEBUG_PORT}")
        except Exception as e:
            print(f"[!] Cannot connect to Chrome: {e}")
            conn.close()
            return

        context = browser.contexts[0] if browser.contexts else None
        if not context:
            print("[!] No browser context found.")
            await browser.close()
            conn.close()
            return

        page: Page = await context.new_page()
        total_listings = 0
        total_details  = 0

        for i, keyword in enumerate(pending, 1):
            banner(f"[{i}/{len(pending)}] keyword: {keyword}")
            source_url = f"https://www.etsy.com/search?q={keyword.replace(' ', '+')}&currency=USD"

            # ── 1. Crawl search results ──────────────────────────────────────
            items = await scrape_search(page, keyword)
            if not items:
                print("  [!] No items — skipping.")
                done_kw.add(keyword)
                save_checkpoint(checkpoint_path, done_kw)
                continue

            if not auto_mode:
                print(f"\n  Preview ({len(items)} items):")
                for idx, it in enumerate(items[:5], 1):
                    print(f"    [{idx}] {str(it.get('title',''))[:55]}  ${it.get('price','?')}")
                if len(items) > 5:
                    print(f"    ... and {len(items)-5} more")
                loop = asyncio.get_event_loop()
                choice = await loop.run_in_executor(
                    None, lambda: input("\n  [A] Approve  [S] Skip  [Q] Quit > ").strip().upper()
                )
                if choice == 'Q':
                    print("  [Q] Quitting.")
                    break
                if choice == 'S':
                    print("  [S] Skipped.")
                    done_kw.add(keyword)
                    save_checkpoint(checkpoint_path, done_kw)
                    continue

            # ── 2. Insert search-level data ──────────────────────────────────
            cur = conn.cursor()
            for item in items:
                try:
                    cur.execute("SAVEPOINT sp_listing")
                    upsert_listing(cur, item, keyword, source_url)
                    cur.execute("RELEASE SAVEPOINT sp_listing")
                except Exception as e:
                    cur.execute("ROLLBACK TO SAVEPOINT sp_listing")
                    print(f"  [!] listing insert error {item.get('listing_id')}: {e}")
            conn.commit()
            cur.close()
            total_listings += len(items)
            print(f"  [+] {len(items)} listings saved (total: {total_listings})")

            # ── 3. Crawl detail pages ────────────────────────────────────────
            for j, item in enumerate(items, 1):
                detail_url = item.get('url')
                if not detail_url:
                    continue
                listing_id = int(item['listing_id'])
                print(f"  [detail {j}/{len(items)}] listing {listing_id}")

                detail = await scrape_detail(page, detail_url)
                if not detail:
                    await asyncio.sleep(random.uniform(2, 4))
                    continue

                cur = conn.cursor()
                # details
                try:
                    cur.execute("SAVEPOINT sp_details")
                    upsert_details(cur, listing_id, detail)
                    cur.execute("RELEASE SAVEPOINT sp_details")
                except Exception as e:
                    cur.execute("ROLLBACK TO SAVEPOINT sp_details")
                    print(f"    [!] details error: {e}")
                # reviews
                try:
                    cur.execute("SAVEPOINT sp_reviews")
                    insert_reviews(cur, listing_id, detail.get('reviews') or [])
                    cur.execute("RELEASE SAVEPOINT sp_reviews")
                except Exception as e:
                    cur.execute("ROLLBACK TO SAVEPOINT sp_reviews")
                    print(f"    [!] reviews error: {e}")
                # shop
                try:
                    cur.execute("SAVEPOINT sp_shop")
                    shop = detail.get('shop') or {}
                    shop_name = ((shop.get('page_shop_name') or '').strip() or
                                (item.get('shop_name') or '').strip())[:200]
                    if shop_name:
                        cur.execute(
                            "UPDATE market_listing SET shop_name=%s WHERE listing_id=%s AND (shop_name IS NULL OR shop_name='')",
                            (shop_name, listing_id)
                        )
                    upsert_shop(cur, shop_name, shop)
                    cur.execute("RELEASE SAVEPOINT sp_shop")
                except Exception as e:
                    cur.execute("ROLLBACK TO SAVEPOINT sp_shop")
                    print(f"    [!] shop error: {e}")
                conn.commit()
                cur.close()

                total_details += 1
                await asyncio.sleep(random.uniform(DETAIL_DELAY_MIN, DETAIL_DELAY_MAX))

            done_kw.add(keyword)
            save_checkpoint(checkpoint_path, done_kw)
            print(f"  [✓] keyword done. details crawled: {total_details}")

            if i < len(pending):
                if auto_mode:
                    delay = random.uniform(DELAY_MIN, DELAY_MAX)
                    print(f"  Sleeping {delay:.0f}s before next keyword...")
                    await asyncio.sleep(delay)
                else:
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(
                        None, lambda: input("\n  Press ENTER to continue...")
                    )

        await browser.close()

    conn.close()
    banner(f"Done — {total_listings} listings | {total_details} detail pages crawled")


# ─────────────────────────── entry point ──────────────────────────────────────

if __name__ == "__main__":
    args = sys.argv[1:]

    auto       = "--auto" in args
    auto_limit = 0
    if auto:
        idx = args.index("--auto")
        if idx + 1 < len(args) and args[idx + 1].isdigit():
            auto_limit = int(args[idx + 1])

    resume_ts = None
    if "--resume" in args:
        idx = args.index("--resume")
        if idx + 1 < len(args):
            m = re.search(r"(\d{8}_\d{6})", args[idx + 1])
            if m:
                resume_ts = m.group(1)

    init_schema = "--init-schema" in args

    # Load keywords
    if not KEYWORDS_FILE.exists():
        print(f"[!] Keywords file not found: {KEYWORDS_FILE}")
        sys.exit(1)

    data     = json.loads(KEYWORDS_FILE.read_text(encoding="utf-8"))
    keywords = data.get("keywords", [])
    if not keywords:
        print("[!] No keywords found in JSON.")
        sys.exit(1)

    print(f"[+] Loaded {len(keywords)} keywords from {KEYWORDS_FILE.name}")
    print(f"[+] Source: no_vm={data.get('no_vm')} exported_at={data.get('exported_at')}")

    run_ts = resume_ts or datetime.now().strftime("%Y%m%d_%H%M%S")
    asyncio.run(run(keywords, run_ts, auto_mode=auto, auto_limit=auto_limit, init_schema=init_schema))
