"""
Claude Vision extractor for Etsy Ads dashboard screenshots.

Two screenshot types:
  1. Listing performance (summary header + daily table)
  2. Keyword performance (keyword × metrics table)

Claude auto-classifies the type and returns structured JSON.
"""
import asyncio
import base64
import json
import re
from pathlib import Path

import anthropic

from ..core.config import get_settings

# ── Unified prompt — Claude classifies automatically ────────────────────────

EXTRACTION_PROMPT = """You are an AI that reads Etsy Ads dashboard screenshots and extracts structured data.

Look at this screenshot carefully and determine which type it is:

TYPE A — Listing performance page:
Contains a listing's advertising summary (views, clicks, orders, revenue, spend, ROAS) and a daily breakdown table.

If TYPE A, extract:
- listing_id: the Etsy listing ID from the URL bar (e.g. URL contains "/listings/4438217152" → "4438217152")
- title: the listing title shown on the page
- no_vm: if visible in URL or page, the "vm" code (e.g. "vm08"); otherwise null
- price: listing price (number, no $)
- stock: stock/quantity if visible; otherwise null
- category: product category if visible; otherwise null
- lifetime_orders: total orders if shown; otherwise null
- lifetime_revenue: total revenue if shown (number, no $); otherwise null
- period: the date range from the dropdown filter (e.g. "Mar 19 - Apr 18")
- summary: {views, clicks, orders, revenue, spend, roas} — numbers from the header
- metric_column: which metric the daily table shows (e.g. "views", "spend", "clicks")
- daily_data: array of {date, value} from the daily table. Date format: "DD/M/YY" (e.g. "19/3/26"). Value: integer for views/clicks/orders, float for revenue/spend/roas.

Return JSON:
{
  "type": "listing_daily",
  "listing_id": "4438217152",
  "title": "Applique embroidered baby sweater...",
  "no_vm": "vm08",
  "price": 24.27,
  "stock": 991,
  "category": "Sweater",
  "lifetime_orders": 3,
  "lifetime_revenue": 102.97,
  "period": "Mar 19 - Apr 18",
  "summary": {"views": 2474, "clicks": 33, "orders": 3, "revenue": 102.97, "spend": 27.99, "roas": 3.68},
  "metric_column": "views",
  "daily_data": [
    {"date": "19/3/26", "value": 68},
    {"date": "20/3/26", "value": 26}
  ]
}

TYPE B — Keyword performance table:
Contains a table of keywords with columns: keyword, ROAS, orders, spend, revenue, clicks, click rate, views.

If TYPE B, extract:
- listing_id: from URL bar
- no_vm: if visible; otherwise null
- keywords: array of objects with {keyword, roas, orders, spend, revenue, clicks, click_rate, views}
  - click_rate: keep as string with % (e.g. "1.1%")
  - spend/revenue: float, no $
  - roas: float

Return JSON:
{
  "type": "keyword_table",
  "listing_id": "4438225302",
  "no_vm": "vm08",
  "keywords": [
    {"keyword": "custom sweatshirts", "roas": 0, "orders": 0, "spend": 0.85, "revenue": 0, "clicks": 2, "click_rate": "1.1%", "views": 181}
  ]
}

IMPORTANT:
- Return ONLY valid JSON, no markdown, no text before or after.
- All numbers should be plain numbers (no commas, no $ signs).
- If you cannot determine a value, use null.
- Be precise with listing_id — it must be the exact numeric ID.
"""


async def _call_claude_vision(image_path: str, settings=None) -> dict | None:
    """Send one image to Claude Vision, return parsed JSON."""
    if settings is None:
        settings = get_settings()

    path = Path(image_path)
    media_type = "image/png" if path.suffix.lower() == ".png" else "image/jpeg"
    image_data = base64.b64encode(path.read_bytes()).decode("utf-8")

    client = anthropic.AsyncAnthropic(api_key=settings.ANTHROPIC_API_KEY)

    try:
        response = await client.messages.create(
            model=settings.CLAUDE_MODEL,
            max_tokens=4096,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": image_data}},
                    {"type": "text", "text": EXTRACTION_PROMPT},
                ],
            }],
        )

        text = response.content[0].text.strip()
        # Strip markdown code fences if present
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)

        json_match = re.search(r"\{[\s\S]*\}", text)
        if json_match:
            return json.loads(json_match.group())
        return json.loads(text)

    except json.JSONDecodeError:
        return None
    except Exception:
        return None


async def extract_batch(
    image_paths: list[str],
    batch_id: str,
    on_progress=None,
) -> tuple[list[dict], list[dict]]:
    """
    Extract all images in a batch concurrently (5 at a time).

    Returns (listing_report_rows, keyword_report_rows).
    on_progress(done, total) callback for progress tracking.
    """
    settings = get_settings()
    total = len(image_paths)
    done = 0
    results: list[dict | None] = [None] * total

    sem = asyncio.Semaphore(5)

    async def _process(idx: int, path: str):
        nonlocal done
        async with sem:
            result = await _call_claude_vision(path, settings)
            results[idx] = result
            done += 1
            if on_progress:
                await on_progress(done, total)

    tasks = [_process(i, p) for i, p in enumerate(image_paths)]
    await asyncio.gather(*tasks)

    return _merge_results(results, batch_id)


def _merge_results(
    raw_results: list[dict | None],
    batch_id: str,
) -> tuple[list[dict], list[dict]]:
    """
    Merge extracted data from multiple screenshots.

    Listing daily screenshots for the same listing_id are merged:
    - Summary row: taken from any screenshot (they share the same summary)
    - Daily rows: merged by date across different metric_columns

    Keyword tables are kept as-is.
    """
    # Group by listing_id for listing_daily type
    listing_groups: dict[str, dict] = {}  # listing_id -> merged data
    keyword_rows: list[dict] = []

    for r in raw_results:
        if r is None:
            continue

        rtype = r.get("type", "")

        if rtype == "listing_daily":
            lid = r.get("listing_id", "")
            if not lid:
                continue

            if lid not in listing_groups:
                listing_groups[lid] = {
                    "listing_id": lid,
                    "title": r.get("title"),
                    "no_vm": r.get("no_vm"),
                    "price": r.get("price"),
                    "stock": r.get("stock"),
                    "category": r.get("category"),
                    "lifetime_orders": r.get("lifetime_orders"),
                    "lifetime_revenue": r.get("lifetime_revenue"),
                    "period": r.get("period", ""),
                    "summary": r.get("summary", {}),
                    "daily": {},  # date -> {metric: value}
                }
            else:
                # Update metadata if missing
                grp = listing_groups[lid]
                for key in ("title", "no_vm", "price", "stock", "category",
                            "lifetime_orders", "lifetime_revenue"):
                    if grp.get(key) is None and r.get(key) is not None:
                        grp[key] = r[key]

            # Merge daily data
            metric_col = r.get("metric_column", "views")
            for day in r.get("daily_data", []):
                date_key = day.get("date", "")
                if not date_key:
                    continue
                if date_key not in listing_groups[lid]["daily"]:
                    listing_groups[lid]["daily"][date_key] = {}
                listing_groups[lid]["daily"][date_key][metric_col] = day.get("value", 0)

        elif rtype == "keyword_table":
            lid = r.get("listing_id", "")
            no_vm = r.get("no_vm")
            for kw in r.get("keywords", []):
                kw["listing_id"] = lid
                kw["no_vm"] = no_vm
                keyword_rows.append(kw)

    # Build flat listing_report rows
    listing_rows: list[dict] = []
    for lid, grp in listing_groups.items():
        base = {
            "listing_id": grp["listing_id"],
            "title": grp["title"],
            "no_vm": grp["no_vm"],
            "price": grp["price"],
            "stock": grp["stock"],
            "category": grp["category"],
            "lifetime_orders": grp["lifetime_orders"],
            "lifetime_revenue": grp["lifetime_revenue"],
        }

        # Summary row
        s = grp["summary"]
        listing_rows.append({
            **base,
            "batch_id": batch_id,
            "period": grp["period"],
            "views": s.get("views", 0),
            "clicks": s.get("clicks", 0),
            "orders": s.get("orders", 0),
            "revenue": s.get("revenue", 0),
            "spend": s.get("spend", 0),
            "roas": s.get("roas", 0),
        })

        # Daily rows
        for date_key, metrics in sorted(grp["daily"].items()):
            listing_rows.append({
                **base,
                "batch_id": batch_id,
                "period": date_key,
                "views": metrics.get("views", 0),
                "clicks": metrics.get("clicks", 0),
                "orders": metrics.get("orders", 0),
                "revenue": metrics.get("revenue", 0),
                "spend": metrics.get("spend", 0),
                "roas": metrics.get("roas", 0),
            })

    # Add batch_id + period to keyword rows
    # Period = "30 days ending on screenshot date" — use the period from
    # the first listing group, or leave blank for user to confirm
    default_period = ""
    if listing_groups:
        first = next(iter(listing_groups.values()))
        default_period = first.get("period", "")

    for kw in keyword_rows:
        kw["batch_id"] = batch_id
        kw.setdefault("period", default_period)

    return listing_rows, keyword_rows
