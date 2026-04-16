"""
Etsy Screenshot Crawler
Chụp màn hình các trang Etsy theo từng viewport scroll
"""
import asyncio
import os
import time
from pathlib import Path
from playwright.async_api import async_playwright, Page
from config import (
    ETSY_URLS, SCREENSHOT_DIR, RAW_DATA_DIR, SCROLL_PAUSE_SEC,
    MAX_SCROLLS, VIEWPORT_WIDTH, VIEWPORT_HEIGHT, SEARCH_TAGS
)


async def setup_browser(playwright):
    """Khởi tạo browser với các settings anti-detection"""
    from playwright_stealth import Stealth

    browser = await playwright.chromium.launch(
        headless=False,   # headless=False giảm khả năng bị detect hơn
        slow_mo=150,
        args=[
            "--no-sandbox",
            "--disable-blink-features=AutomationControlled",
            "--disable-infobars",
            "--start-maximized",
        ]
    )
    context = await browser.new_context(
        viewport={"width": VIEWPORT_WIDTH, "height": VIEWPORT_HEIGHT},
        user_agent=(
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        locale="en-US",
        timezone_id="America/New_York",
        extra_http_headers={
            "Accept-Language": "en-US,en;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        },
    )
    page = await context.new_page()
    await Stealth().apply_stealth_async(page)
    return browser, context, page


async def accept_cookies(page: Page):
    """Đóng popup cookie nếu có"""
    try:
        btn = await page.wait_for_selector(
            "button[data-gdpr-single-choice-accept], button:has-text('Accept'), "
            "button:has-text('Allow All'), #gdpr-single-choice-accept",
            timeout=5000
        )
        if btn:
            await btn.click()
            await asyncio.sleep(1)
    except Exception:
        pass


async def close_popups(page: Page):
    """Đóng các popup khác (newsletter, location...)"""
    selectors = [
        "button[aria-label='Close']",
        "button.wt-btn--transparent[aria-label='Dismiss']",
        "[data-overlay-dismiss]",
    ]
    for sel in selectors:
        try:
            btn = await page.query_selector(sel)
            if btn and await btn.is_visible():
                await btn.click()
                await asyncio.sleep(0.5)
        except Exception:
            pass


async def scroll_and_screenshot(page: Page, category: str, url: str) -> list[str]:
    """
    Truy cập URL, cuộn xuống và chụp screenshot từng màn hình.
    Trả về danh sách đường dẫn screenshot.
    """
    screenshots = []
    save_dir = Path(SCREENSHOT_DIR) / category
    save_dir.mkdir(parents=True, exist_ok=True)

    print(f"[{category}] Đang mở: {url}")
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(2)
    except Exception as e:
        print(f"  Lỗi load trang: {e}")
        return screenshots

    await accept_cookies(page)
    await close_popups(page)
    await asyncio.sleep(1)

    # Chụp screenshot đầu tiên (viewport ban đầu)
    for scroll_idx in range(MAX_SCROLLS + 1):
        filename = save_dir / f"scroll_{scroll_idx:02d}.png"
        await page.screenshot(path=str(filename), full_page=False)
        screenshots.append(str(filename))
        print(f"  [{category}] Screenshot {scroll_idx}: {filename.name}")

        if scroll_idx < MAX_SCROLLS:
            # Cuộn xuống 1 viewport
            await page.evaluate(f"window.scrollBy(0, {VIEWPORT_HEIGHT - 100})")
            await asyncio.sleep(SCROLL_PAUSE_SEC)

            # Kiểm tra đã đến cuối trang chưa
            at_bottom = await page.evaluate(
                "(window.innerHeight + window.scrollY) >= document.body.scrollHeight - 200"
            )
            if at_bottom:
                print(f"  [{category}] Đã đến cuối trang sau {scroll_idx + 1} lần cuộn")
                break

    return screenshots


async def crawl_all_categories(categories: dict = None) -> dict:
    """
    Crawl tất cả categories, trả về dict {category: [screenshot_paths]}
    """
    if categories is None:
        categories = ETSY_URLS

    all_screenshots = {}

    async with async_playwright() as playwright:
        browser, context, page = await setup_browser(playwright)

        for category, url in categories.items():
            print(f"\n{'='*50}")
            print(f"Đang crawl category: {category}")
            print(f"{'='*50}")

            shots = await scroll_and_screenshot(page, category, url)
            all_screenshots[category] = shots
            print(f"  Tổng screenshots: {len(shots)}")

            await asyncio.sleep(3)

        await browser.close()

    return all_screenshots


async def crawl_single_category(category: str) -> list[str]:
    """Crawl 1 category cụ thể"""
    url = ETSY_URLS.get(category)
    if not url:
        raise ValueError(f"Category '{category}' không tồn tại. Các category: {list(ETSY_URLS.keys())}")

    async with async_playwright() as playwright:
        browser, context, page = await setup_browser(playwright)
        shots = await scroll_and_screenshot(page, category, url)
        await browser.close()
    return shots


async def search_and_screenshot(page: Page, tag: str) -> str | None:
    """
    Tìm kiếm tag trên Etsy, bật filter Star Seller,
    chụp 1 screenshot viewport đầu (6 listing đầu tiên).
    Lưu vào RAW_DATA_DIR/{tag_slug}_{timestamp}.png
    Trả về đường dẫn file, hoặc None nếu thất bại.
    """
    from datetime import datetime
    import re

    save_dir = Path(RAW_DATA_DIR)
    save_dir.mkdir(parents=True, exist_ok=True)

    tag_slug = re.sub(r"[^a-z0-9]+", "_", tag.lower().strip())
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = save_dir / f"{tag_slug}_{timestamp}.png"

    # Star Seller filter embed thẳng vào URL — không cần click UI
    q = tag.replace(' ', '+')
    url = f"https://www.etsy.com/search?q={q}&explicit=1&is_star_seller=1"
    print(f"[search] '{tag}' → {url}")

    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=30000)
        await asyncio.sleep(3)
    except Exception as e:
        print(f"  Lỗi load trang: {e}")
        return None

    # Kiểm tra bị block / captcha
    content = await page.content()
    if "Verification Required" in content or "Access is temporarily restricted" in content:
        print("  ✗ Etsy đang hiện captcha. Thử lại sau vài phút hoặc đổi IP.")
        await page.screenshot(path="/tmp/etsy_blocked.png")
        return None

    await accept_cookies(page)
    await close_popups(page)
    await asyncio.sleep(1)
    print("  ✓ Load trang thành công, filter Star Seller đã áp dụng qua URL")

    # --- Chụp màn hình ---
    await page.screenshot(path=str(filename), full_page=False)
    print(f"  ✓ Saved: {filename.name}")
    return str(filename)


async def run_search_tags(tags: list[str] = None) -> dict:
    """
    Chạy search_and_screenshot cho danh sách tags.
    Trả về {tag: screenshot_path}
    """
    if tags is None:
        tags = SEARCH_TAGS

    results = {}

    async with async_playwright() as playwright:
        browser, context, page = await setup_browser(playwright)

        for tag in tags:
            print(f"\n{'='*50}")
            path = await search_and_screenshot(page, tag)
            results[tag] = path
            await asyncio.sleep(3)

        await browser.close()

    return results


if __name__ == "__main__":
    import sys

    args = sys.argv[1:]

    if "--tag" in args:
        idx = args.index("--tag")
        tag = args[idx + 1]
        results = asyncio.run(run_search_tags([tag]))
    elif args:
        results = asyncio.run(run_search_tags(args))
    else:
        # Chạy tất cả tags trong config
        results = asyncio.run(run_search_tags())

    print("\nKết quả:")
    for tag, path in results.items():
        status = path if path else "FAILED"
        print(f"  {tag}: {status}")
