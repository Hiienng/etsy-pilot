"""
Etsy Keyword Rank Crawler
=========================
Crawl search results cho từng keyword → lưu listing_id, rank, badge, product
vào bảng keyword_rank_snapshot trên etsy_market_db.

Schema (tối giản):
  keyword_rank_snapshot(keyword, listing_id, rank, badge, product, crawled_at)

Kế thừa anti-detect engine từ market_batch_scraper.py:
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
    python3 keyword_rank_crawler.py
    python3 keyword_rank_crawler.py --auto
    python3 keyword_rank_crawler.py --auto 10
    python3 keyword_rank_crawler.py --resume 20260501_120000
"""

import asyncio
import json
import os
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

HERE       = Path(__file__).parent
ROOT       = HERE.parent.parent
OUTPUT_DIR = HERE / "output"
OUTPUT_DIR.mkdir(exist_ok=True)

CHROME_PATH = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
DEBUG_PORT  = 9222
PROFILE_DIR = Path.home() / ".etsy_cdp_profile"

_ENV_PATH    = ROOT / "nguyenphamdieuhien.online/.env"
_DB_URL      = ""   # etsy_market_db  (destination)
_APP_DB_URL  = ""   # DATABASE_URL    (source of keywords)
_GROQ_KEY    = ""
if _ENV_PATH.exists():
    for _line in _ENV_PATH.read_text().splitlines():
        _kv = _line.strip()
        if _kv.startswith("ETSY_MARKET_DB"):
            _DB_URL = _kv.split("=", 1)[1].strip()
        elif _kv.startswith("DATABASE_URL"):
            _APP_DB_URL = _kv.split("=", 1)[1].strip()
        elif _kv.startswith("GROQ_API_KEY"):
            _GROQ_KEY = _kv.split("=", 1)[1].strip()

MAX_ITEMS        = 48
DELAY_MIN        = 35
DELAY_MAX        = 85
NAV_TIMEOUT_MS   = 35_000
RENDER_WAIT_MIN  = 3.0
RENDER_WAIT_MAX  = 6.0

# ─────────────────────────── DB setup ─────────────────────────────────────────

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS keyword_rank_snapshot (
    id          BIGSERIAL PRIMARY KEY,
    keyword     TEXT        NOT NULL,
    listing_id  BIGINT      NOT NULL,
    rank        SMALLINT    NOT NULL,
    badge       VARCHAR(50),
    product     TEXT,
    crawled_at  TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_krs_keyword    ON keyword_rank_snapshot(keyword);
CREATE INDEX IF NOT EXISTS idx_krs_listing_id ON keyword_rank_snapshot(listing_id);
CREATE INDEX IF NOT EXISTS idx_krs_crawled_at ON keyword_rank_snapshot(crawled_at);
"""


def get_db_conn():
    if not _DB_URL:
        raise RuntimeError("ETSY_MARKET_DB connection string not found in .env")
    return psycopg2.connect(_DB_URL)


def load_keywords_from_db() -> list[str]:
    if not _APP_DB_URL:
        raise RuntimeError("DATABASE_URL not found in .env")
    conn = psycopg2.connect(_APP_DB_URL)
    with conn:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT keyword FROM keyword_report ORDER BY keyword")
            keywords = [r[0] for r in cur.fetchall()]
    conn.close()
    return keywords


def init_db():
    conn = get_db_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
    conn.close()
    print("[DB] Schema ready.")


# ─────────────────────────── JS extractor ─────────────────────────────────────

SEARCH_EXTRACT_JS = """
(maxItems) => {
    const clean = s => s ? s.replace(/\\s+/g, ' ').trim() : '';
    const allCards = [...document.querySelectorAll('[data-listing-id]')].filter(
        el => !el.parentElement?.closest('[data-listing-id]')
    );
    if (!allCards.length) return [];
    const seen = new Set(), results = [];
    for (const card of allCards) {
        const listing_id = card.getAttribute('data-listing-id') || '';
        if (!listing_id || seen.has(listing_id)) continue;
        seen.add(listing_id);
        const txt = card.textContent || '';
        const badgeEl = card.querySelector('[class*="wt-badge"],[class*="listing-badge"]');
        let badge = badgeEl ? clean(badgeEl.textContent) : null;
        if (!badge) {
            const bm = txt.match(/\\b(Bestseller|Popular now|Etsy's Pick)\\b/);
            if (bm) badge = bm[1];
        }
        const titleEl = card.querySelector('h3,h2,[class*="title"],[class*="listing-title"]');
        const product = titleEl ? clean(titleEl.textContent) : '';
        results.push({ listing_id, rank: results.length + 1, badge: badge || null, product });
        if (results.length >= maxItems) break;
    }
    return results;
}
"""

# ─────────────────────────── keyword suggestion ───────────────────────────────

_SUGGEST_SYSTEM = (
    "You are an Etsy search expert. Given a product name, return a JSON array "
    "of ~50 distinct search queries that Etsy buyers are likely to use when "
    "looking for that product. Include variations: gift phrases, occasion-based, "
    "style/material modifiers, age/size qualifiers, personalised/custom variants. "
    "Output ONLY the raw JSON array — no markdown, no explanation."
)


def suggest_keywords(product: str) -> list[str]:
    import requests  # pip install requests (already a transitive dep via playwright)
    key = _GROQ_KEY or os.environ.get("GROQ_API_KEY", "")
    if not key:
        raise RuntimeError("GROQ_API_KEY not found in .env or environment")
    resp = requests.post(
        "https://api.groq.com/openai/v1/chat/completions",
        headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
        json={
            "model": "llama-3.1-8b-instant",
            "messages": [
                {"role": "system", "content": _SUGGEST_SYSTEM},
                {"role": "user",   "content": product},
            ],
            "max_tokens": 2048,
            "temperature": 0.7,
        },
        timeout=30,
    )
    resp.raise_for_status()
    text = resp.json()["choices"][0]["message"]["content"].strip()
    text = re.sub(r"^```[a-z]*\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    raw = json.loads(text)
    return list(dict.fromkeys(kw.strip().lower() for kw in raw if kw.strip()))


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

    # scroll to a random depth between 60-90% of page
    target = int(scroll_height * random.uniform(0.60, 0.90))
    pos = 0
    while pos < target:
        # vary step size: small steps (reading) mixed with larger (skimming)
        step = random.randint(80, 180) if random.random() < 0.3 else random.randint(200, 420)
        pos  = min(pos + step, target)
        await page.evaluate(f"window.scrollTo({{top:{pos},behavior:'smooth'}})")

        # occasional longer pause — simulates user reading a listing
        if random.random() < 0.15:
            await asyncio.sleep(random.uniform(1.2, 2.8))
        else:
            await asyncio.sleep(random.uniform(0.18, 0.55))

    # linger at bottom
    await asyncio.sleep(random.uniform(0.8, 2.0))

    # scroll back up in 1-2 jumps (not always smooth to top)
    if random.random() < 0.5:
        mid = int(pos * random.uniform(0.3, 0.6))
        await page.evaluate(f"window.scrollTo({{top:{mid},behavior:'smooth'}})")
        await asyncio.sleep(random.uniform(0.3, 0.7))
    await page.evaluate("window.scrollTo({top:0,behavior:'smooth'})")
    await asyncio.sleep(random.uniform(0.4, 1.0))


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
    await asyncio.get_event_loop().run_in_executor(None, input)
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


# ─────────────────────────── DB write ─────────────────────────────────────────

def insert_snapshot(cur, keyword: str, items: list[dict], crawled_at: datetime):
    for item in items:
        cur.execute("""
            INSERT INTO keyword_rank_snapshot (keyword, listing_id, rank, badge, product, crawled_at)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            keyword,
            int(item["listing_id"]),
            int(item["rank"]),
            item.get("badge"),
            item.get("product"),
            crawled_at,
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


# ─────────────────────────── main loop ────────────────────────────────────────

async def run(keywords: list[str], run_ts: str, auto_mode: bool, auto_limit: int = 0):
    checkpoint_path = OUTPUT_DIR / f"rank_checkpoint_{run_ts}.json"
    done_kw         = load_checkpoint(checkpoint_path)

    pending = [kw for kw in keywords if kw not in done_kw]
    if not pending:
        print("[+] All keywords already done.")
        return
    if auto_mode and auto_limit > 0:
        pending = pending[:auto_limit]

    mode_label = (f"AUTO {auto_limit}" if (auto_mode and auto_limit)
                  else ("AUTO ALL" if auto_mode else "HUMAN-IN-LOOP"))
    banner(f"Keyword Rank Crawler [{mode_label}] — {len(pending)} keywords")

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
        total_rows = 0

        for i, keyword in enumerate(pending, 1):
            banner(f"[{i}/{len(pending)}] keyword: {keyword}")

            items = await scrape_search(page, keyword)
            if not items:
                print("  [!] No items — skipping.")
                done_kw.add(keyword)
                save_checkpoint(checkpoint_path, done_kw)
                continue

            if not auto_mode:
                print(f"\n  Preview ({len(items)} items):")
                for it in items[:5]:
                    badge_str = f"  [{it.get('badge','')}]" if it.get('badge') else ""
                    print(f"    [{it['rank']:2d}]{badge_str} {str(it.get('product',''))[:55]}")
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

            crawled_at = datetime.now(timezone.utc)
            cur = conn.cursor()
            try:
                cur.execute("SAVEPOINT sp_snapshot")
                insert_snapshot(cur, keyword, items, crawled_at)
                cur.execute("RELEASE SAVEPOINT sp_snapshot")
                conn.commit()
                total_rows += len(items)
                print(f"  [+] {len(items)} rows saved (total: {total_rows})")
            except Exception as e:
                cur.execute("ROLLBACK TO SAVEPOINT sp_snapshot")
                conn.commit()
                print(f"  [!] Insert error: {e}")
            finally:
                cur.close()

            done_kw.add(keyword)
            save_checkpoint(checkpoint_path, done_kw)

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
    banner(f"Done — {total_rows} rows inserted into keyword_rank_snapshot")


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

    product = None
    if "--product" in args:
        idx = args.index("--product")
        if idx + 1 < len(args):
            product = args[idx + 1]

    if product:
        print(f"[LLM] Suggesting keywords for product: {product!r} ...")
        keywords = suggest_keywords(product)
        print(f"[+] {len(keywords)} keywords suggested")
        auto = True  # product mode always runs automated
    else:
        keywords = load_keywords_from_db()
        if not keywords:
            print("[!] No keywords found in keyword_report.")
            sys.exit(1)
        print(f"[+] Loaded {len(keywords)} distinct keywords from keyword_report")

    run_ts = resume_ts or datetime.now().strftime("%Y%m%d_%H%M%S")
    asyncio.run(run(keywords, run_ts, auto_mode=auto, auto_limit=auto_limit))
