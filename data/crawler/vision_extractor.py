"""
Vision AI Extractor
Dùng Gemini Vision để đọc screenshots và extract dữ liệu sản phẩm
"""
import json
import re
from pathlib import Path

import google.generativeai as genai
from PIL import Image

from config import GEMINI_API_KEY


EXTRACTION_PROMPT = """Bạn là một AI chuyên phân tích trang thương mại điện tử Etsy.

Hãy nhìn vào screenshot này và trích xuất TẤT CẢ sản phẩm hiển thị trên màn hình.

Với mỗi sản phẩm, hãy lấy và CHUẨN HÓA theo đúng quy tắc sau:

- title: Tên sản phẩm
- price: Giá — CHỈ số nguyên, bỏ ký hiệu tiền tệ và dấu phẩy. Ví dụ: "111,194d" → 111194 / "$12.99" → 12 (làm tròn) / null nếu không thấy
- original_price: Giá gốc — cùng quy tắc với price, null nếu không có
- discount: Phần trăm giảm — CHỈ số nguyên, không kèm ký hiệu. Ví dụ: "55% off" → 55 / null nếu không có
- shop_name: Tên shop (sau chữ "By "), null nếu không thấy
- rating: Điểm đánh giá — số thực, dùng dấu chấm thập phân. Ví dụ: 4.9 / null nếu không thấy
- review_count: Số lượng đánh giá — CHỈ số nguyên, không có "k". Ví dụ: "8.3k" → 8300 / "1,234" → 1234 / null nếu không thấy
- badge: Badge GÓC TRÁI của card sản phẩm — CHỈ lấy "Popular now" hoặc "Bestseller", null nếu không có. KHÔNG nhầm với nhãn "Free shipping"
- free_shipping: true nếu có nhãn "Free shipping" hiển thị trên card, false nếu không
- is_ad: true nếu có chữ "Ad", false nếu không
- search_tag: Từ khóa trong thanh search bar (ví dụ "keepsake")
- etsy_best: Filter "Etsy's best" đang được tick trong sidebar — "star_seller" / "etsy_picks" / null
- product_type: Loại sản phẩm cụ thể — đọc từ tên + hình ảnh, viết thường, tiếng Anh. Ví dụ: "ring dish", "trinket dish", "jewelry tray", "crystal ball lamp", "photo frame", "necklace", "candle"... Cố gắng cụ thể nhất có thể, không để null
- image_description: Mô tả ngắn hình ảnh sản phẩm (1 câu)

Chỉ trả về JSON array, không có text thêm. Format:
[
  {
    "title": "...",
    "price": 111194,
    "original_price": 247097,
    "discount": 55,
    "shop_name": "...",
    "rating": 4.9,
    "review_count": 8300,
    "badge": "Popular now",
    "free_shipping": false,
    "is_ad": false,
    "search_tag": "keepsake",
    "etsy_best": "star_seller",
    "product_type": "ring dish",
    "image_description": "..."
  }
]

Nếu không thấy sản phẩm nào, trả về [].
"""


def _get_client():
    if not GEMINI_API_KEY:
        raise ValueError("Thiếu GEMINI_API_KEY trong file .env")
    genai.configure(api_key=GEMINI_API_KEY)
    return genai.GenerativeModel("gemini-1.5-flash")


def extract_products_from_screenshot(image_path: str, client=None) -> list[dict]:
    """
    Gửi 1 screenshot lên Gemini Vision và nhận lại danh sách sản phẩm.
    Tham số client giữ để tương thích với EtsyVisionAgent (không dùng).
    """
    print(f"  Extracting: {Path(image_path).name}")

    model = _get_client()
    img = Image.open(image_path)

    try:
        response = model.generate_content([EXTRACTION_PROMPT, img])
        response_text = response.text.strip()

        # Bỏ markdown code block nếu có
        response_text = re.sub(r"^```(?:json)?\s*", "", response_text)
        response_text = re.sub(r"\s*```$", "", response_text)

        json_match = re.search(r'\[[\s\S]*\]', response_text)
        if json_match:
            products = json.loads(json_match.group())
        else:
            products = json.loads(response_text)

        print(f"    -> {len(products)} sản phẩm tìm thấy")
        return products

    except json.JSONDecodeError as e:
        print(f"    Lỗi parse JSON: {e}")
        print(f"    Response: {response_text[:300]}")
        return []
    except Exception as e:
        print(f"    Lỗi API: {e}")
        return []


def extract_products_batch(
    screenshot_paths: list[str],
    category: str,
    scroll_offset: int = 0
) -> list[dict]:
    """Batch extract nhiều screenshots cùng category."""
    all_products = []
    seen_titles = set()

    for idx, path in enumerate(screenshot_paths):
        products = extract_products_from_screenshot(path)

        for product in products:
            title_key = product.get("title", "").strip().lower()[:80]
            if title_key and title_key not in seen_titles:
                seen_titles.add(title_key)
                product["category"] = category
                product["source_screenshot"] = Path(path).name
                product["scroll_position"] = idx + scroll_offset
                all_products.append(product)

    return all_products
