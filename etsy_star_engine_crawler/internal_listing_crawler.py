"""
Internal Listing Detail Crawler
================================
Đọc tất cả URLs từ bảng `listings` (etsy_pilot DB),
crawl detail page từng listing, lưu vào `internal_listing_details` (cùng DB).

Dùng chung anti-detect engine với market_batch_scraper.py:
  - CDP + real Chrome (no automation flags)
  - Human-like scroll
  - CAPTCHA pause
  - Checkpoint / resume

Fields crawled (tương tự market_listing_details):
  listing_id, base_price, sale_price, discount_percent, currency,
  materials, highlights, shipping_status, origin_ship_from,
  ship_time_max_days, us_shipping, return_policy, design, ai_summary,
  rating, review_count, badge,
  shop_name, owner_name, shop_location, join_year, total_sales,
  shop_rating, shop_badge, smooth_shipping, speedy_replies,
  crawled_at

Modes:
  (default)   Human-in-loop: pause sau mỗi listing
  --auto      Fully automated (delay 8–20s)
  --auto N    Automated N listings rồi dừng
  --resume TS Resume từ checkpoint_TS.json

Usage:
    python3 internal_listing_crawler.py
    python3 internal_listing_crawler.py --auto
    python3 internal_listing_crawler.py --auto 10
    python3 internal_listing_crawler.py --resume 20260504_120000
    python3 internal_listing_crawler.py --init-schema
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
from playwright.async_api import async_playwright, Browser, Page

# ─────────────────────────── paths & config ───────────────────────────────────

HERE       = Path(__file__).parent
ROOT       = HERE.parent
OUTPUT_DIR = HERE / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

CHROME_PATH = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
DEBUG_PORT  = 9222
PROFILE_DIR = Path.home() / ".etsy_cdp_profile"

_ENV_PATH = ROOT / ".env"
_APP_DB_URL = ""
if _ENV_PATH.exists():
    for _line in _ENV_PATH.read_text().splitlines():
        key, sep, value = _line.strip().partition("=")
        if sep and key.strip().lower() == "database_url":
            _APP_DB_URL = value.strip()
            break

NAV_TIMEOUT_MS   = 35_000
RENDER_WAIT_MIN  = 3.0
RENDER_WAIT_MAX  = 5.5
DELAY_MIN        = 8
DELAY_MAX        = 20

# ─────────────────────────── DB setup ─────────────────────────────────────────

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS internal_listing_details (
    listing_id          VARCHAR(20)   PRIMARY KEY,
    base_price          NUMERIC(14,2),
    sale_price          NUMERIC(14,2),
    discount_percent    SMALLINT,
    currency            VARCHAR(10),
    materials           TEXT,
    highlights          TEXT,
    shipping_status     VARCHAR(200),
    origin_ship_from    VARCHAR(200),
    ship_time_max_days  SMALLINT,
    us_shipping         BOOLEAN,
    return_policy       BOOLEAN,
    design              TEXT,
    ai_summary          TEXT,
    rating              NUMERIC(3,1),
    review_count        INTEGER,
    badge               VARCHAR(50),
    shop_name           VARCHAR(200),
    owner_name          VARCHAR(200),
    shop_location       VARCHAR(200),
    join_year           SMALLINT,
    total_sales         INTEGER,
    shop_rating         NUMERIC(3,1),
    shop_badge          VARCHAR(50),
    smooth_shipping     BOOLEAN,
    speedy_replies      BOOLEAN,
    crawled_at          TIMESTAMPTZ   NOT NULL
);
"""


def get_conn():
    if not _APP_DB_URL:
        raise RuntimeError("DATABASE_URL not found in .env")
    return psycopg2.connect(_APP_DB_URL)


def init_db():
    conn = get_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
    conn.close()
    print("[DB] Schema ready.")


def load_listings() -> list[dict]:
    conn = get_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT listing_id, url, title FROM listings ORDER BY import_time")
            rows = [{"listing_id": r[0], "url": r[1], "title": r[2]} for r in cur.fetchall()]
    conn.close()
    return rows


def upsert_detail(cur, listing_id: str, d: dict):
    shop = d.get("shop") or {}
    shop_name = (shop.get("page_shop_name") or "").strip()[:200] or None

    # detect currency from sale_price magnitude: VND >> 1000, others << 1000
    sale  = d.get("sale_price")
    base  = d.get("base_price")
    currency = "VND" if (sale and sale > 1000) or (base and base > 1000) else "USD"

    cur.execute("""
        INSERT INTO internal_listing_details (
            listing_id, base_price, sale_price, discount_percent, currency,
            materials, highlights, shipping_status, origin_ship_from,
            ship_time_max_days, us_shipping, return_policy, design, ai_summary,
            rating, review_count, badge,
            shop_name, owner_name, shop_location, join_year, total_sales,
            shop_rating, shop_badge, smooth_shipping, speedy_replies,
            crawled_at
        ) VALUES (
            %s,%s,%s,%s,%s,
            %s,%s,%s,%s,
            %s,%s,%s,%s,%s,
            %s,%s,%s,
            %s,%s,%s,%s,%s,
            %s,%s,%s,%s,
            %s
        )
        ON CONFLICT (listing_id) DO UPDATE SET
            base_price=EXCLUDED.base_price, sale_price=EXCLUDED.sale_price,
            discount_percent=EXCLUDED.discount_percent, currency=EXCLUDED.currency,
            materials=EXCLUDED.materials, highlights=EXCLUDED.highlights,
            shipping_status=EXCLUDED.shipping_status,
            origin_ship_from=EXCLUDED.origin_ship_from,
            ship_time_max_days=EXCLUDED.ship_time_max_days,
            us_shipping=EXCLUDED.us_shipping, return_policy=EXCLUDED.return_policy,
            design=EXCLUDED.design, ai_summary=EXCLUDED.ai_summary,
            rating=EXCLUDED.rating, review_count=EXCLUDED.review_count,
            badge=EXCLUDED.badge,
            shop_name=EXCLUDED.shop_name, owner_name=EXCLUDED.owner_name,
            shop_location=EXCLUDED.shop_location, join_year=EXCLUDED.join_year,
            total_sales=EXCLUDED.total_sales, shop_rating=EXCLUDED.shop_rating,
            shop_badge=EXCLUDED.shop_badge,
            smooth_shipping=EXCLUDED.smooth_shipping,
            speedy_replies=EXCLUDED.speedy_replies,
            crawled_at=EXCLUDED.crawled_at
    """, (
        listing_id,
        base, sale, d.get("discount_percent"), currency,
        d.get("materials"), d.get("highlights"), d.get("shipping_status"),
        d.get("origin_ship_from"), d.get("ship_time_max_days"),
        bool(d.get("us_shipping")), bool(d.get("return_policy")),
        d.get("design"), d.get("ai_summary"),
        d.get("rating"), d.get("review_count"), d.get("badge"),
        shop_name,
        (shop.get("owner_name") or "")[:200] or None,
        (shop.get("location") or "")[:200] or None,
        shop.get("join_year"), shop.get("total_sales"),
        shop.get("shop_rating"),
        (shop.get("badge") or "")[:50] or None,
        bool(shop.get("smooth_shipping")), bool(shop.get("speedy_replies")),
        datetime.now(timezone.utc),
    ))


# ─────────────────────────── JS extractor ─────────────────────────────────────

DETAIL_EXTRACT_JS = (HERE / "_detail_extract.js").read_text(encoding="utf-8")

# Extended: also extract rating, review_count, badge from listing page header
LISTING_HEADER_JS = """
() => {
    const clean = s => s ? s.replace(/\\s+/g, ' ').trim() : '';
    // Rating + review count
    let rating = null, review_count = null;
    const ratingEl = document.querySelector(
        '[class*="rating"] [class*="value"],[aria-label*="star"],[class*="stars-small"]'
    );
    if (ratingEl) {
        const aria = ratingEl.getAttribute('aria-label') || '';
        const m1 = aria.match(/([\\d.]+)\\s+star/);
        const m2 = aria.match(/([\\d,]+)\\s+review/);
        if (m1) rating = parseFloat(m1[1]);
        if (m2) review_count = parseInt(m2[1].replace(/,/g, ''));
    }
    if (!rating) {
        const rt = document.querySelector('[class*="ratingValue"],[class*="rating-value"]');
        if (rt) rating = parseFloat(rt.textContent) || null;
    }
    if (!review_count) {
        const rc = document.querySelector('[class*="reviewCount"],[class*="review-count"]');
        if (rc) review_count = parseInt((rc.textContent || '').replace(/[^\\d]/g, '')) || null;
    }
    // Badge
    const badgeEl = document.querySelector('[class*="wt-badge"],[class*="listing-badge"],[class*="badge"]');
    let badge = badgeEl ? clean(badgeEl.textContent) : null;
    if (!badge) {
        const bm = (document.body.textContent || '').match(/\\b(Bestseller|Popular now|Etsy\\'s Pick)\\b/);
        if (bm) badge = bm[1];
    }
    return { rating, review_count, badge };
}
"""

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
    target = int(scroll_height * random.uniform(0.6, 0.85))
    pos = 0
    while pos < target:
        step = random.randint(80, 180) if random.random() < 0.3 else random.randint(200, 420)
        pos  = min(pos + step, target)
        await page.evaluate(f"window.scrollTo({{top:{pos},behavior:'smooth'}})")
        if random.random() < 0.12:
            await asyncio.sleep(random.uniform(1.0, 2.5))
        else:
            await asyncio.sleep(random.uniform(0.15, 0.5))
    await asyncio.sleep(random.uniform(0.8, 1.8))
    await page.evaluate("window.scrollTo({top:0,behavior:'smooth'})")
    await asyncio.sleep(random.uniform(0.3, 0.8))


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
        print("  [!] No interactive stdin available; skipping this listing.")
        return False
    await asyncio.sleep(2)
    return not await check_blocked(page)


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


# ─────────────────────────── core scrape ──────────────────────────────────────

async def scrape_listing(page: Page, url: str) -> dict | None:
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
        await asyncio.sleep(random.uniform(RENDER_WAIT_MIN, RENDER_WAIT_MAX))
    except Exception as e:
        print(f"    [!] Navigation failed: {e}")
        return None

    if await check_blocked(page):
        ok = await handle_captcha(page)
        if not ok:
            return None

    await simulate_scroll(page)

    try:
        detail = await page.evaluate(DETAIL_EXTRACT_JS)
        header = await page.evaluate(LISTING_HEADER_JS)
        detail["rating"]       = header.get("rating")
        detail["review_count"] = header.get("review_count")
        detail["badge"]        = header.get("badge")
        return detail
    except Exception as e:
        print(f"    [!] JS extract failed: {e}")
        return None


# ─────────────────────────── main loop ────────────────────────────────────────

async def run(listings: list[dict], run_ts: str, auto_mode: bool, auto_limit: int = 0, init_schema: bool = False):
    checkpoint_path = OUTPUT_DIR / f"internal_checkpoint_{run_ts}.json"
    done_ids        = load_checkpoint(checkpoint_path)

    pending = [l for l in listings if l["listing_id"] not in done_ids]
    if not pending:
        print("[+] All listings already crawled.")
        return
    if auto_mode and auto_limit > 0:
        pending = pending[:auto_limit]

    mode_label = (f"AUTO {auto_limit}" if (auto_mode and auto_limit)
                  else ("AUTO ALL" if auto_mode else "HUMAN-IN-LOOP"))
    banner(f"Internal Listing Crawler [{mode_label}] — {len(pending)} listings")

    if init_schema:
        init_db()
    conn = get_conn()

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
        total = 0

        for i, listing in enumerate(pending, 1):
            lid   = listing["listing_id"]
            url   = listing["url"]
            title = listing.get("title") or ""
            banner(f"[{i}/{len(pending)}] {lid} — {title[:55]}")
            print(f"  {url}")

            detail = await scrape_listing(page, url)

            if not detail:
                print("  [!] Failed — skipping.")
                done_ids.add(lid)
                save_checkpoint(checkpoint_path, done_ids)
                continue

            if not auto_mode:
                sale  = detail.get("sale_price")
                base  = detail.get("base_price")
                disc  = detail.get("discount_percent")
                shop  = (detail.get("shop") or {}).get("page_shop_name", "?")
                print(f"\n  price: {sale} (base: {base}, disc: {disc}%)")
                print(f"  shop: {shop}")
                print(f"  shipping: {detail.get('shipping_status')}  from: {detail.get('origin_ship_from')}")
                loop = asyncio.get_event_loop()
                choice = await loop.run_in_executor(
                    None, lambda: input("\n  [A] Approve  [S] Skip  [Q] Quit > ").strip().upper()
                )
                if choice == "Q":
                    print("  [Q] Quitting.")
                    break
                if choice == "S":
                    print("  [S] Skipped.")
                    done_ids.add(lid)
                    save_checkpoint(checkpoint_path, done_ids)
                    continue

            cur = conn.cursor()
            try:
                cur.execute("SAVEPOINT sp")
                upsert_detail(cur, lid, detail)
                cur.execute("RELEASE SAVEPOINT sp")
                conn.commit()
                total += 1
                print(f"  [+] Saved (total: {total})")
            except Exception as e:
                cur.execute("ROLLBACK TO SAVEPOINT sp")
                conn.commit()
                print(f"  [!] DB error: {e}")
            finally:
                cur.close()

            done_ids.add(lid)
            save_checkpoint(checkpoint_path, done_ids)

            if i < len(pending):
                if auto_mode:
                    delay = random.uniform(DELAY_MIN, DELAY_MAX)
                    print(f"  Sleeping {delay:.0f}s...")
                    await asyncio.sleep(delay)
                else:
                    loop = asyncio.get_event_loop()
                    await loop.run_in_executor(None, lambda: input("\n  Press ENTER to continue..."))

        await browser.close()

    conn.close()
    banner(f"Done — {total} listings saved to internal_listing_details")


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

    listings = load_listings()
    if not listings:
        print("[!] No listings found in DB.")
        sys.exit(1)
    print(f"[+] Loaded {len(listings)} listings from etsy_pilot DB")

    run_ts = resume_ts or datetime.now().strftime("%Y%m%d_%H%M%S")
    asyncio.run(run(listings, run_ts, auto_mode=auto, auto_limit=auto_limit, init_schema=init_schema))
