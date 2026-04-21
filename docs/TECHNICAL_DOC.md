# TECHNICAL DOCUMENT — Etsy Listing Manager

> Domain: `nguyenphamdieuhien.online`
> Mục đích: tổng hợp kiến trúc, data schemas và đặc tả từng feature. Sơ đồ luồng của mỗi feature nằm trong thư mục [flows/](flows/).

---

## 0. Kiến trúc tổng quan

```
Frontend (Vanilla HTML/CSS/JS)           Backend (FastAPI)              Storage
──────────────────────────────           ───────────────────            ───────
index.html                   ── fetch ──> /api/v1/listings              Postgres (Neon)
  · Performance Hub          ── fetch ──> /api/v1/performance             · listings
  · Research Hub             ── fetch ──> /api/v1/internal                · listing_report
  · Internal Import          ── fetch ──> /api/v1/market                  · keyword_report
  · Listings CRUD                                                         · import_batch
                                                                          · market_listing
data/processed/                                                           · scenarios_rules
  performance_dashboard.json <── write ── services/performance_service
data/raw/internal/{batch}/   <─── rw ──── services/internal_service
data/processed/snapshots/    <── write ── services/internal_service     External APIs
                                                                          · Claude Vision
data/crawler/output/         <── write ── data/crawler/*                  · Gemini Vision
```

### Thư mục chính

| Layer | Path | Nội dung |
|-------|------|----------|
| Frontend | `index.html`, `frontend/` | UI, charts, fetch API |
| Backend API | `backend/app/api/routes/` | Route handlers, FastAPI |
| Service | `backend/app/services/` | Business logic |
| Model | `backend/app/models/` | SQLAlchemy ORM |
| Schema | `backend/app/schemas/` | Pydantic DTO |
| Crawler | `data/crawler/` | Cron crawler → `market_listing` |
| Data | `data/raw/`, `data/processed/` | Nguồn gốc & output |

### Env vars quan trọng

| Biến | Mô tả |
|------|-------|
| `DATABASE_URL` | Neon Postgres (async `postgresql+asyncpg://`) |
| `ANTHROPIC_API_KEY` | Claude API (listing optimizer + vision) |
| `CLAUDE_MODEL` | Model ID (default `claude-sonnet-4-6`) |
| `GEMINI_API_KEY` | (tuỳ chọn) fallback vision |
| `ALLOWED_ORIGINS` | CORS |

---

## 1. Data schemas

Tất cả bảng cùng nằm trên Neon Postgres. Base = SQLAlchemy declarative (`backend/app/core/database.py`).

### 1.1 `listings` — catalog nội bộ (source of truth cho AI optimization)

> Schema thực tế trên Neon (được populate bởi ETL `data/crawler/etl_listings.py`).

| Column | Type | Null | Index | Ghi chú |
|---|---|---|---|---|
| `id` | `INTEGER` PK autoincrement | NO | PK | Sinh tự động |
| `listing_id` | `VARCHAR(32)` | NO | UNIQUE | ID listing trên Etsy |
| `title` | `TEXT` | YES |   | Tiêu đề listing |
| `category` | `VARCHAR(64)` | YES |   | Danh mục sản phẩm |
| `no_vm` | `VARCHAR(16)` | YES |   | Mã VM của importer |
| `url` | `TEXT` | YES |   | URL Etsy (tự sinh từ listing_id) |
| `import_time` | `TIMESTAMPTZ` | YES |   | Thời điểm import gần nhất |
| `importer` | `VARCHAR(64)` | YES |   | Tên người/hệ thống import |

**ETL logic** (`etl_listings.py`, chạy mỗi thứ 2 lúc 07:00 UTC):
- Nguồn: `listing_report` — lấy **latest non-null value per field** (correlated subquery per column).
- UPSERT: INSERT mới nếu `listing_id` chưa có; UPDATE nếu đã có nhưng field đang null (`COALESCE` — không bao giờ xoá dữ liệu có sẵn).
- Sau khi step 1 xong, tự động chạy step 2 sinh `etl_references` (xem §1.8).

### 1.8 `etl_references` — top-3 market reference per internal listing

Populate bởi `run_etl_references()` trong `data/crawler/etl_listings.py` (chạy nối tiếp sau bước ETL listings).

| Column | Type | Null | Ghi chú |
|---|---|---|---|
| `listing_id` | `VARCHAR(32)` | NO | PK (composite), khớp `listings.listing_id` |
| `reference_listing_id` | `TEXT` | NO | PK (composite), khớp `market_listing.id` |
| `ref_rank` | `SMALLINT` | NO | 1..3 — thứ tự ưu tiên |
| `ref_title` | `TEXT` | YES |   |
| `ref_shop` | `TEXT` | YES |   |
| `ref_url` | `TEXT` | YES |   |
| `ref_price` | `INTEGER` | YES | cents |
| `ref_rating` | `REAL` | YES | 0.0 – 5.0 |
| `ref_review_count` | `INTEGER` | YES |   |
| `ref_tag_ranking` | `INTEGER` | YES | Thấp hơn = xuất hiện sớm hơn trong search |
| `ref_badge` | `TEXT` | YES | `Popular`, `Bestseller`… |
| `match_method` | `VARCHAR(16)` | def `'category'` | Cơ chế match (hiện chỉ có `category`) |
| `refreshed_at` | `TIMESTAMPTZ` | def `now()` |   |

**Logic:**
- Match: `ILIKE` — `listings.category` xuất hiện trong bất kỳ field nào của market_listing (`search_tag` / `product_type` / `title`).
- Rank: `ORDER BY tag_ranking ASC NULLS LAST, review_count DESC NULLS LAST`, giữ top-3.
- UPSERT: ON CONFLICT (`listing_id`, `reference_listing_id`) DO UPDATE — refresh rank + fill null fields bằng COALESCE.
- Future work: thêm `match_method='embedding'` dùng `ListingEmbedder` khi cần match semantic thay vì category.

### 1.2 `listing_report` — snapshot hiệu suất theo listing × period

Model: [backend/app/models/listing_report.py](../backend/app/models/listing_report.py)

| Column | Type | Null | Ghi chú |
|---|---|---|---|
| `id` | `INTEGER` PK autoincrement | NO |   |
| `listing_id` | `VARCHAR(32)` | NO | Khớp với `listings.listing_id` (logical FK, không ràng buộc) |
| `title` | `TEXT` | YES |   |
| `no_vm` | `VARCHAR(16)` | YES | Mã VM của importer |
| `price` | `NUMERIC(10,2)` | YES | $ |
| `stock` | `INTEGER` | YES |   |
| `category` | `VARCHAR(64)` | YES |   |
| `lifetime_orders` | `INTEGER` | YES |   |
| `lifetime_revenue` | `NUMERIC(12,2)` | YES | $ |
| `period` | `VARCHAR(32)` | NO | Định dạng "Mar 19 - Apr 18" |
| `views` | `INTEGER` | def 0 |   |
| `clicks` | `INTEGER` | def 0 |   |
| `orders` | `INTEGER` | def 0 |   |
| `revenue` | `NUMERIC(12,2)` | def 0 |   |
| `spend` | `NUMERIC(12,2)` | def 0 |   |
| `roas` | `NUMERIC(8,2)` | def 0 | revenue/spend |
| `import_time` | `TIMESTAMPTZ` | YES |   |
| `importer` | `VARCHAR(64)` | YES | Tên người import |

Khoá tự nhiên: `(listing_id, period, no_vm)` — khi confirm import sẽ `DELETE` theo khoá này rồi `INSERT` lại.

### 1.3 `keyword_report` — hiệu suất theo keyword × listing × period

Model: [backend/app/models/keyword_report.py](../backend/app/models/keyword_report.py)

| Column | Type | Null | Ghi chú |
|---|---|---|---|
| `id` | `INTEGER` PK autoincrement | NO |   |
| `listing_id` | `VARCHAR(32)` | NO |   |
| `keyword` | `TEXT` | NO |   |
| `no_vm` | `VARCHAR(16)` | YES |   |
| `period` | `VARCHAR(32)` | NO |   |
| `roas` | `NUMERIC(8,2)` | def 0 |   |
| `orders` | `INTEGER` | def 0 |   |
| `spend` | `NUMERIC(12,2)` | def 0 |   |
| `revenue` | `NUMERIC(12,2)` | def 0 |   |
| `clicks` | `INTEGER` | def 0 |   |
| `click_rate` | `VARCHAR(8)` | YES | Lưu dạng text "1.1%" |
| `views` | `INTEGER` | def 0 |   |
| `import_time` | `TIMESTAMPTZ` | YES |   |
| `importer` | `VARCHAR(64)` | YES |   |

### 1.4 `import_batch` — quản lý batch Internal Ads Import

Model: [backend/app/models/import_batch.py](../backend/app/models/import_batch.py)

| Column | Type | Null | Ghi chú |
|---|---|---|---|
| `batch_id` | `VARCHAR(32)` PK | NO | Format `YYYYMMDD_HHMM` |
| `status` | `VARCHAR(16)` | NO | `uploaded` / `extracted` / `confirmed` / `discarded` / `rolled_back` |
| `file_count` | `INTEGER` | def 0 |   |
| `listing_count` | `INTEGER` | def 0 |   |
| `keyword_count` | `INTEGER` | def 0 |   |
| `progress` | `INTEGER` | def 0 | 0–100 |
| `total_files` | `INTEGER` | def 0 |   |
| `created_at` | `TIMESTAMPTZ` | NO | `server_default=now()` |
| `confirmed_at` | `TIMESTAMPTZ` | YES |   |
| `note` | `TEXT` | YES |   |
| `error_message` | `TEXT` | YES |   |

State machine: `uploaded → extracted → (confirmed | discarded)`; `confirmed → rolled_back`.

### 1.5 `market_listing` — dữ liệu crawl Etsy

Bảng được tạo thủ công trên Neon (model Python đã drop — xem commit `df2e314`). Schema hiện hành:

| Column | Type | Index | Ghi chú |
|---|---|---|---|
| `id` | `INTEGER` PK autoincrement |   |   |
| `batch_id` | `VARCHAR(64)` |   | Crawl batch |
| `source_screenshot` | `VARCHAR(256)` |   | Tên file ảnh nguồn |
| `search_tag` | `VARCHAR(128)` | ✅ | Keyword khi crawl |
| `etsy_best` | `VARCHAR(32)` |   | Badge "Etsy's Pick"… |
| `product_type` | `VARCHAR(128)` | ✅ | Ví dụ `baby romper` |
| `category` | `VARCHAR(128)` | ✅ |   |
| `title` | `TEXT` |   |   |
| `price` | `INTEGER` |   | Giá hiện tại (cents) |
| `original_price` | `INTEGER` |   | Giá gốc (cents) |
| `discount` | `INTEGER` |   | % |
| `shop_name` | `VARCHAR(256)` |   |   |
| `rating` | `NUMERIC(3,1)` |   | 0.0 – 5.0 |
| `review_count` | `INTEGER` |   |   |
| `badge` | `VARCHAR(32)` |   | `Popular`… |
| `is_ad` | `BOOLEAN` def F |   |   |
| `free_shipping` | `BOOLEAN` def F |   |   |
| `image_description` | `TEXT` |   | Alt / caption |
| `scroll_position` | `INTEGER` |   |   |
| `crawled_at` | `TIMESTAMPTZ` | def now |   |

Ghi chú: code crawler hiện tại ghi qua CSV/JSON/SQLite local (`data/crawler/output/products.db`); bảng Postgres được nạp bằng migrate ngoài luồng crawler. Performance service query `market_listing` bằng LATERAL JOIN để tìm reference competitor.

### 1.6 `scenarios_rules` — ma trận quyết định CTR × CR × ROAS

Model: [backend/app/models/scenario.py](../backend/app/models/scenario.py)

| Column | Type | Null | Ghi chú |
|---|---|---|---|
| `id` | `INTEGER` PK autoincrement | NO |   |
| `roas_band` | `VARCHAR(32)` | NO | `profitable` / `slight_loss` / `heavy_loss` / `no_sales` |
| `cr_level` | `VARCHAR(8)` | NO | `high` / `low` / `zero` |
| `ctr_level` | `VARCHAR(8)` | NO | `high` / `low` |
| `case_name` | `TEXT` | NO | Mô tả tiếng Việt |
| `action` | `VARCHAR(32)` | NO | `keep` / `improve` / `improve_or_off` |
| `cause` | `TEXT` | YES | Root cause tiếng Việt |
| `fix_listing` | `TEXT` | YES | Gợi ý sửa listing |
| `fix_ads` | `TEXT` | YES | Gợi ý chỉnh campaign |

Seeded bởi `performance_service.seed_scenarios()` (16 rows). Ngưỡng chuẩn: **CTR ≥ 1.5% = high**, **CR ≥ 3% = high**, **ROAS break-even = 2.0**.

### 1.7 Quan hệ logic

```
listings.listing_id ──┐
                      ├─► listing_report.listing_id     (1-N theo period)
                      └─► keyword_report.listing_id     (1-N theo keyword × period)

import_batch.batch_id ──► listing_report (logical, dùng period + importer để revert)
                      └─► keyword_report

listing_report.(roas_band, cr_level, ctr_level)
    ──► scenarios_rules.(roas_band, cr_level, ctr_level)  (N-1)

listing_report.category ──► market_listing.product_type  (LATERAL JOIN)
```

Không có ràng buộc FK cứng — join thực hiện ở tầng SQL/service để dễ import dữ liệu rời rạc.

---

## 2. Backend routes

| Prefix | File | Route | Method | Dùng cho feature |
|---|---|---|---|---|
| `/api/v1/listings` | [listings.py](../backend/app/api/routes/listings.py) | `/` | GET/POST | Listings CRUD |
|   |   | `/{listing_id}` | GET/PATCH/DELETE | Listings CRUD |
|   |   | `/stats/count` | GET | Listings CRUD |
| `/api/v1/performance` | [performance.py](../backend/app/api/routes/performance.py) | `/listings` | GET | Performance Hub |
|   |   | `/refresh` | POST | Performance Hub |
| `/api/v1/internal` | [internal.py](../backend/app/api/routes/internal.py) | `/upload` | POST | Internal Import |
|   |   | `/extract` | POST | Internal Import |
|   |   | `/confirm` | POST | Internal Import |
|   |   | `/discard` | POST | Internal Import |
|   |   | `/rollback` | POST | Internal Import |
|   |   | `/history` | GET | Internal Import |
|   |   | `/snapshot/{batch_id}` | GET | Internal Import |
| `/api/v1/market` | [market.py](../backend/app/api/routes/market.py) | `/samples` | GET | Research Hub |

---

## 3. Feature specs

Mỗi feature có một sơ đồ luồng Mermaid trong [flows/](flows/).

### 3.1 Performance Hub — Listing Performance (pill `perf-sub-performance`)
- **Mục tiêu:** snapshot hiệu suất listing theo 3 chỉ số CTR / CR / ROAS.
- **Backend nguồn dữ liệu:** `listing_report` (aggregated theo listing mới nhất) + `scenarios_rules` JOIN + `market_listing` LATERAL.
- **Output:** Hero KPI, bảng Best Performers (CTR ≥ 2%, CR ≥ 4%, ROAS ≥ 2), Quick Win (CR ≥ 4%, CTR < 2%), chart gap & portfolio.
- **Cache:** `data/processed/performance_dashboard.json`. Rebuild qua `POST /api/v1/performance/refresh`.
- **Flow diagram:** [flows/01_performance_hub.md](flows/01_performance_hub.md).

### 3.2 Performance Hub — Listing Improvement (pill `perf-sub-improvement`)
- **Mục tiêu:** gợi ý hành động (`keep` / `improve` / `improve_or_off`) cho từng listing dựa trên `scenarios_rules`.
- **Nguồn:** cùng JSON dashboard — filter theo product, VM, level CTR/CR/ROAS.
- **Hiển thị:** bảng kèm `cause`, `fix_listing`, `fix_ads`; sort theo mức độ ưu tiên action.
- **Flow diagram:** [flows/02_listing_improvement.md](flows/02_listing_improvement.md).

### 3.3 Performance Hub — Listing Scale Up (pill `perf-sub-scaleup`)
- **Mục tiêu:** xác định listing có tín hiệu lan toả (CTR/CR/ROAS đều pass) để mở rộng ngân sách & scale.
- **Nguồn:** cùng JSON dashboard — lọc `action='keep'` + ROAS ≥ 2.5 theo phân khúc product.
- **Hiển thị:** bubble chart (size = revenue, trục = CR × ROAS), bảng candidates.
- **Flow diagram:** [flows/03_listing_scaleup.md](flows/03_listing_scaleup.md).

### 3.4 Internal Ads Import (pill `perf-sub-import`)
- **Mục tiêu:** đưa screenshot báo cáo ads Etsy vào DB.
- **Pipeline:**
  1. **Upload** — drag-drop PNG/JPG/WebP, validate (10KB–20MB, ≥200×200).
  2. **Extract** — Claude/Gemini Vision phân loại "listing summary" vs "keyword table", trích xuất fields, merge theo `listing_id`.
  3. **Preview** — 2 bảng editable cho người QA chỉnh sửa.
  4. **Confirm** — `DELETE WHERE (listing_id, period, no_vm)` → `INSERT`; snapshot ra `data/processed/snapshots/{batch_id}.json`; xoá raw images.
  5. **Discard / Rollback** — huỷ trước khi confirm / revert sau khi confirm (load snapshot).
- **State:** `import_batch.status`.
- **Flow diagram:** [flows/04_internal_import.md](flows/04_internal_import.md).

### 3.5 Research Hub — Market Trend Intelligence
- **Mục tiêu:** phân tích dữ liệu Etsy crawl để chọn sản phẩm may được (sewable).
- **Nguồn:** bảng `market_listing` (crawler chạy weekly `data/crawler/crawl_weekly.py`).
- **Output:** 8 chart (market share, hot rate, price range, discount, emerging…) + filter "sewable only".
- **Flow diagram:** [flows/05_research_hub.md](flows/05_research_hub.md).

### 3.6 Listings CRUD
- **Mục tiêu:** quản lý catalog nội bộ (thêm/sửa/xoá/đánh dấu trạng thái) và lưu output AI optimize.
- **Backend:** `services/listing_service.py` + `routes/listings.py`.
- **Flow diagram:** [flows/06_listings_crud.md](flows/06_listings_crud.md).

---

## 4. Ngưỡng & quy ước nghiệp vụ

| Chỉ số | Ngưỡng "high" | Target business |
|---|---|---|
| CTR | ≥ 1.5% | ≥ 2% |
| CR | ≥ 3% | ≥ 4% |
| ROAS | ≥ 2.0 (break-even) | ≥ 2.5 (scale-up) |

Banding ROAS:
- `profitable` ≥ 2.0
- `slight_loss` 1.0 – 1.99
- `heavy_loss` > 0 và < 1.0
- `no_sales` = 0

---

## 5. Build & run

```bash
# Backend
cd backend
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000

# Crawler (weekly)
cd data/crawler
python crawl_weekly.py
```

Frontend được serve bằng FastAPI static (`backend/app/main.py`) — truy cập `/` load `index.html`.

---

## 6. Tham chiếu

- [CLAUDE.md](../CLAUDE.md) — quy tắc code theo layer
- [PERFORMANCE_HUB_RULES.md](../PERFORMANCE_HUB_RULES.md) — spec chi tiết Performance Hub
- [RESEARCH_HUB_RULES.md](../RESEARCH_HUB_RULES.md) — spec Research Hub
- [docs/DESIGN.md](DESIGN.md) — design system
- [docs/PLAN_INTERNAL_ADS_PIPELINE.md](PLAN_INTERNAL_ADS_PIPELINE.md) — spec gốc pipeline Internal Import
- [data-dictionary.html](../data-dictionary.html) — data dictionary dạng HTML
