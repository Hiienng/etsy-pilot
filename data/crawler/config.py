import os
from pathlib import Path

# Load .env
_env_path = Path(__file__).parents[2] / ".env"
if _env_path.exists():
    for line in _env_path.read_text().splitlines():
        if "=" in line and not line.startswith("#"):
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip())

# Etsy URLs để crawl
ETSY_URLS = {
    "bestsellers": "https://www.etsy.com/market/best_selling",
    "trending": "https://www.etsy.com/featured",
    "jewelry": "https://www.etsy.com/c/jewelry?ref=catnav-10923&order=most_relevant",
    "home_decor": "https://www.etsy.com/c/home-and-living?ref=catnav-10923&order=most_relevant",
    "clothing": "https://www.etsy.com/c/clothing?ref=catnav-10923&order=most_relevant",
    "art": "https://www.etsy.com/c/art-and-collectibles?ref=catnav-10923&order=most_relevant",
    "craft_supplies": "https://www.etsy.com/c/craft-supplies-and-tools?ref=catnav-10923&order=most_relevant",
    "wedding": "https://www.etsy.com/c/weddings?ref=catnav-10923&order=most_relevant",
}

# AI API keys
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
GEMINI_API_KEY = os.environ.get("GEMINI_API_KEY", "")

# Screenshot settings
SCREENSHOT_DIR = "screenshots"
RAW_DATA_DIR = "/Users/hienem/Downloads/hienprojects/nguyenphamdieuhien.online/data/raw"
OUTPUT_DIR = "output"
SCROLL_PAUSE_SEC = 2
MAX_SCROLLS = 5  # Số lần cuộn xuống mỗi trang
VIEWPORT_WIDTH = 1400
VIEWPORT_HEIGHT = 900

# Search tags để crawl (Star Seller filter)
SEARCH_TAGS = [
    "birth announcement",
    "baby girl gift",
    "keepsake",
    "personalized jewelry",
    "custom portrait",
    "wedding gift",
    "mother gift",
    "memorial gift",
]

# PostgreSQL (Neon) — shared with backend
DATABASE_URL = os.environ.get("DATABASE_URL", "")

# Output
OUTPUT_FORMAT = ["json", "csv"]  # json, csv, postgres
DB_PATH = "output/etsy_products.db"  # legacy SQLite fallback
