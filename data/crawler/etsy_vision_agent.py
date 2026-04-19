"""
EtsyVisionAgent
Agent chuyên đọc screenshot Etsy và xuất file CSV chuẩn hóa.

Cách dùng:
    # Từ file ảnh có sẵn
    agent = EtsyVisionAgent()
    agent.run_from_images(["data/raw/data1.png", "data/raw/data2.png"])

    # Từ URL (tự chụp màn hình)
    agent.run_from_url("keepsake", url="https://www.etsy.com/search?q=keepsake")
"""
import os
import asyncio
from pathlib import Path
from datetime import datetime

import re as _re
from vision_extractor import extract_products_from_screenshot
from storage import normalize_product, save_csv, save_json, save_postgres
from config import GEMINI_API_KEY


def _extract_batch_id(filename: str) -> str:
    """
    Lấy batch_id từ tên file ảnh.
    Ví dụ: 'keepsake_3.png' → '3', 'birth_announcement_12.png' → '12'
    """
    match = _re.search(r"_(\d+)\.\w+$", filename)
    return match.group(1) if match else filename


class EtsyVisionAgent:
    """
    Agent đọc ảnh Etsy → extract sản phẩm → lưu CSV/JSON/SQLite.
    """

    name = "EtsyVisionAgent"

    def __init__(self):
        if not GEMINI_API_KEY:
            raise ValueError("Thiếu GEMINI_API_KEY trong file .env")

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_from_images(
        self,
        image_paths: list[str],
        search_tag: str = None,
        out_prefix: str = None,
    ) -> dict:
        """
        Đọc danh sách ảnh → extract → lưu file.

        Args:
            image_paths:  Đường dẫn các file ảnh PNG/JPG
            search_tag:   Ghi đè search_tag nếu ảnh không rõ (tuỳ chọn)
            out_prefix:   Tiền tố tên file output (mặc định: timestamp)

        Returns:
            {"products": [...], "csv": "path", "json": "path", "total": N}
        """
        print(f"[{self.name}] Bắt đầu xử lý {len(image_paths)} ảnh...")
        products = self._extract(image_paths, search_tag)
        return self._save(products, out_prefix)

    def run_from_url(
        self,
        search_tag: str,
        url: str,
        out_prefix: str = None,
    ) -> dict:
        """Tự chụp màn hình URL rồi extract."""
        from screenshot_crawler import crawl_single_category
        from config import ETSY_URLS

        # Thêm URL tạm vào config nếu chưa có
        if search_tag not in ETSY_URLS:
            import config
            config.ETSY_URLS[search_tag] = url

        print(f"[{self.name}] Chụp màn hình: {url}")
        shots = asyncio.run(crawl_single_category(search_tag))
        return self.run_from_images(shots, search_tag=search_tag, out_prefix=out_prefix)

    def run_from_tags(
        self,
        tags: list[str] = None,
        out_prefix: str = None,
    ) -> dict:
        """
        Full pipeline: tìm kiếm từng tag trên Etsy → bật Star Seller filter
        → chụp màn hình → extract → lưu CSV/JSON/SQLite.

        Args:
            tags: Danh sách search tags. Mặc định dùng SEARCH_TAGS trong config.
            out_prefix: Tiền tố tên file output.

        Returns:
            {"products": [...], "csv": "path", "json": "path", "total": N,
             "screenshots": {tag: path}}
        """
        from screenshot_crawler import run_search_tags

        if tags is None:
            from config import SEARCH_TAGS
            tags = SEARCH_TAGS

        print(f"[{self.name}] Bắt đầu crawl {len(tags)} tags...")

        # Bước 1: Chụp màn hình
        screenshots = asyncio.run(run_search_tags(tags))

        # Chỉ lấy các ảnh chụp thành công
        valid = [(tag, path) for tag, path in screenshots.items() if path]
        print(f"[{self.name}] Chụp thành công: {len(valid)}/{len(tags)} tags")

        if not valid:
            print(f"[{self.name}] Không có ảnh nào để extract.")
            return {"products": [], "total": 0, "screenshots": screenshots}

        # Bước 2: Extract từng ảnh, gán đúng search_tag
        all_products = []
        seen_titles = set()

        for tag, path in valid:
            print(f"\n[{self.name}] Extracting: {tag}")
            raw = self._extract([path], search_tag=tag)
            for p in raw:
                title_key = p.get("title", "").strip().lower()[:80]
                if title_key and title_key not in seen_titles:
                    seen_titles.add(title_key)
                    all_products.append(p)

        # Bước 3: Lưu
        result = self._save(all_products, out_prefix)
        result["screenshots"] = screenshots
        return result

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _extract(self, image_paths: list[str], search_tag: str = None) -> list[dict]:
        all_products = []
        seen_titles = set()

        for idx, path in enumerate(image_paths):
            if not Path(path).exists():
                print(f"  [SKIP] Không tìm thấy file: {path}")
                continue

            raw = extract_products_from_screenshot(path)

            for p in raw:
                title_key = p.get("title", "").strip().lower()[:80]
                if not title_key or title_key in seen_titles:
                    continue
                seen_titles.add(title_key)

                p = normalize_product(p)
                p["source_screenshot"] = Path(path).name
                p["scroll_position"] = idx
                p["batch_id"] = _extract_batch_id(Path(path).name)
                p["import_date"] = datetime.now().strftime("%Y-%m-%d")

                if search_tag:
                    p["search_tag"] = search_tag

                all_products.append(p)

        print(f"[{self.name}] Extract xong: {len(all_products)} sản phẩm unique")
        return all_products

    def _save(self, products: list[dict], out_prefix: str = None) -> dict:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        prefix = out_prefix or f"etsy_{ts}"

        csv_path = save_csv(products, f"{prefix}.csv")
        json_path = save_json(products, f"{prefix}.json")
        save_postgres(products)

        return {
            "products": products,
            "csv": csv_path,
            "json": json_path,
            "total": len(products),
        }


# ------------------------------------------------------------------
# CLI
# ------------------------------------------------------------------

if __name__ == "__main__":
    import sys

    args = sys.argv[1:]

    if not args:
        print("Dùng:")
        print("  # Từ ảnh có sẵn:")
        print("  python etsy_vision_agent.py data/raw/data1.png --tag keepsake")
        print()
        print("  # Tự động tìm kiếm + chụp (Star Seller filter):")
        print("  python etsy_vision_agent.py --tags \"birth announcement\" \"baby girl gift\"")
        print("  python etsy_vision_agent.py --tags  # dùng danh sách mặc định trong config")
        sys.exit(0)

    agent = EtsyVisionAgent()

    # Mode 1: --tags → tự động tìm kiếm + chụp + extract

    if "--tags" in args:
        idx = args.index("--tags")
        tags = args[idx + 1:] or None  # None = dùng SEARCH_TAGS từ config
        result = agent.run_from_tags(tags)
        print(f"\nKết quả:")
        print(f"  Tổng sản phẩm : {result['total']}")
        print(f"  CSV            : {result.get('csv')}")
        print(f"  JSON           : {result.get('json')}")
        print(f"  Screenshots:")
        for tag, path in result.get("screenshots", {}).items():
            print(f"    {tag}: {path or 'FAILED'}")

    # Mode 2: đường dẫn ảnh → extract từ ảnh có sẵn
    else:
        search_tag = None
        if "--tag" in args:
            i = args.index("--tag")
            search_tag = args[i + 1]
            args = [a for a in args if a not in ("--tag", search_tag)]

        result = agent.run_from_images(args, search_tag=search_tag)
        print(f"\nKết quả:")
        print(f"  Tổng sản phẩm : {result['total']}")
        print(f"  CSV            : {result['csv']}")
        print(f"  JSON           : {result['json']}")
