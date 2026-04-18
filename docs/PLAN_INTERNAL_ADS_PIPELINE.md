# Plan: Internal Ads Data Pipeline

> Feature: Upload Etsy Ads screenshots -> Extract -> Review -> Import DB
> Date: 2026-04-18
> Status: DRAFT - pending review

---

## 1. Data Flow

```
User chụp Etsy Ads dashboard (mỗi ~7 ngày)
        |
        v
[index.html] Drag & Drop ảnh vào upload zone
        |
        v
POST /api/v1/internal/upload
  -> Lưu ảnh vào data/raw/internal/{batch_id}/
  -> Tạo record trong import_batch (status=uploaded)
  -> Return batch_id
        |
        v
POST /api/v1/internal/extract   {batch_id}
  -> Gemini Vision đọc từng ảnh
  -> Phân loại: listing_summary | keyword_table
  -> Merge data theo listing_id
  -> Lưu 2 JSON vào data/raw/internal/{batch_id}/
  -> Update import_batch (status=extracted)
  -> Return {listing_report: [...], keyword_report: [...]}
        |
        v
[index.html] Hiện preview 2 bảng (editable)
  -> User kiểm tra số, sửa nếu cần
        |
        v
POST /api/v1/internal/confirm   {batch_id, listing_report, keyword_report}
  -> Xoá data cũ trong DB cùng listing_id + period
  -> Insert data mới từ payload (đã qua user review)
  -> Lưu snapshot JSON vào data/processed/snapshots/{batch_id}.json
  -> Xoá ảnh raw trong data/raw/internal/{batch_id}/
  -> Update import_batch (status=confirmed)
  -> Return {imported: true, rows: {listing: N, keyword: M}}
        |
        v
[index.html] Hiện "Import thành công" + link xem history
```

### Discard flow (user thấy data sai, không muốn import):
```
POST /api/v1/internal/discard   {batch_id}
  -> Xoá ảnh raw trong data/raw/internal/{batch_id}/
  -> Update import_batch (status=discarded)
```

### Rollback flow (đã confirm nhưng phát hiện sai sau đó):
```
POST /api/v1/internal/rollback   {batch_id}
  -> Đọc snapshot JSON từ data/processed/snapshots/{batch_id}.json
  -> Xoá data batch đó trong DB (theo batch_id)
  -> Update import_batch (status=rolled_back)
  -> Snapshot giữ nguyên (không xoá)
```

---

## 2. DB Schema (3 bảng mới)

### 2.1 import_batch — tracking mỗi lần upload

```sql
CREATE TABLE import_batch (
    batch_id        VARCHAR(32) PRIMARY KEY,   -- format: YYYYMMDD_HHMM (e.g. "20260418_1435")
    status          VARCHAR(16) NOT NULL,       -- uploaded | extracted | confirmed | discarded | rolled_back
    file_count      INTEGER DEFAULT 0,          -- số ảnh upload
    listing_count   INTEGER DEFAULT 0,          -- số listing extracted
    keyword_count   INTEGER DEFAULT 0,          -- số keyword extracted
    created_at      TIMESTAMPTZ DEFAULT now(),
    confirmed_at    TIMESTAMPTZ,                -- NULL cho tới khi confirm
    note            TEXT                        -- user ghi chú (optional)
);
```

### 2.2 listing_report — performance theo listing + period

```sql
CREATE TABLE listing_report (
    id              SERIAL PRIMARY KEY,
    batch_id        VARCHAR(32) NOT NULL REFERENCES import_batch(batch_id),
    listing_id      VARCHAR(32) NOT NULL,       -- Etsy listing ID (e.g. "4438217152")
    title           TEXT,
    price           NUMERIC(10,2),
    stock           INTEGER,
    category        VARCHAR(64),
    lifetime_orders INTEGER,
    lifetime_revenue NUMERIC(12,2),
    period          VARCHAR(32) NOT NULL,        -- "Mar 19 - Apr 18" hoặc "19/3/26"
    views           INTEGER DEFAULT 0,
    clicks          INTEGER DEFAULT 0,
    orders          INTEGER DEFAULT 0,
    revenue         NUMERIC(12,2) DEFAULT 0,
    spend           NUMERIC(12,2) DEFAULT 0,
    roas            NUMERIC(8,2) DEFAULT 0,
    created_at      TIMESTAMPTZ DEFAULT now()
);

-- Index cho query phổ biến
CREATE INDEX idx_lr_listing_period ON listing_report(listing_id, period);
CREATE INDEX idx_lr_batch ON listing_report(batch_id);
```

### 2.3 keyword_report — performance theo keyword

```sql
CREATE TABLE keyword_report (
    id              SERIAL PRIMARY KEY,
    batch_id        VARCHAR(32) NOT NULL REFERENCES import_batch(batch_id),
    listing_id      VARCHAR(32) NOT NULL,
    keyword         TEXT NOT NULL,
    period          VARCHAR(32) NOT NULL,
    roas            NUMERIC(8,2) DEFAULT 0,
    orders          INTEGER DEFAULT 0,
    spend           NUMERIC(12,2) DEFAULT 0,
    revenue         NUMERIC(12,2) DEFAULT 0,
    clicks          INTEGER DEFAULT 0,
    click_rate      VARCHAR(8),                  -- "1.1%" — giữ string vì UI cần hiển thị %
    views           INTEGER DEFAULT 0,
    created_at      TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX idx_kr_listing_period ON keyword_report(listing_id, period);
CREATE INDEX idx_kr_batch ON keyword_report(batch_id);
```

### Quan hệ giữa 3 bảng

```
import_batch (1) ──< listing_report (N)
import_batch (1) ──< keyword_report (N)
```

- `batch_id` là FK duy nhất, dùng để rollback/delete theo batch
- Không FK tới bảng `listings` (vì listing_id ở đây là Etsy ID string, không phải UUID internal)

---

## 3. API Endpoints

### Đặt tại: `backend/app/api/routes/internal.py`
### Prefix: `/api/v1/internal`

| Method | Path | Input | Output | Mô tả |
|--------|------|-------|--------|-------|
| POST | `/upload` | `multipart/form-data` (files[]) | `{batch_id, file_count}` | Nhận ảnh, lưu raw, tạo batch |
| POST | `/extract` | `{batch_id}` | `{listing_report: [], keyword_report: []}` | Gemini extract, return preview |
| POST | `/confirm` | `{batch_id, listing_report: [], keyword_report: []}` | `{imported: true, rows: {...}}` | Import vào DB, xoá ảnh raw |
| POST | `/discard` | `{batch_id}` | `{discarded: true}` | Huỷ batch, xoá ảnh raw |
| POST | `/rollback` | `{batch_id}` | `{rolled_back: true}` | Revert DB, giữ snapshot |
| GET | `/history` | `?limit=20` | `[{batch_id, status, counts, dates}]` | Lịch sử import |
| GET | `/snapshot/{batch_id}` | — | `{listing_report: [], keyword_report: []}` | Xem lại data đã import |

### Xoá logic khi confirm:

```python
# Trong confirm endpoint:
# 1. Xoá records CŨ cùng listing_id + period (tránh duplicate)
await session.execute(
    delete(ListingReport).where(
        ListingReport.listing_id.in_(listing_ids),
        ListingReport.period.in_(periods),
        ListingReport.batch_id != batch_id  # chỉ xoá batch cũ
    )
)
# 2. Tương tự cho keyword_report
# 3. Insert records mới (từ payload user đã review)
# 4. Lưu snapshot
# 5. Xoá ảnh raw
```

---

## 4. Gemini Prompt — Internal Ads Screenshots

### Khác biệt với market prompt hiện có:

| | Market (hiện có) | Internal Ads (mới) |
|---|---|---|
| Source | Etsy search results page | Etsy Ads dashboard |
| Data | Product cards (title, price, rating...) | Performance metrics (views, clicks, ROAS...) |
| Layout | Grid of product cards | Summary header + daily table OR keyword table |
| Prompt file | `data/crawler/vision_extractor.py` | `backend/app/services/internal_extractor.py` (MỚI) |

### 4.1 Prompt cho Listing Performance screenshots

```
Bạn là AI chuyên đọc screenshot từ Etsy Ads dashboard.

Screenshot này là trang "Listing advertising" của Etsy. Hãy trích xuất:

1. listing_id: Số ID trong URL bar (ví dụ: URL chứa "/listings/4438217152" → "4438217152")

2. period: Khoảng thời gian trong dropdown filter (ví dụ: "Last 30 days (Mar 19 - Apr 18)" → "Mar 19 - Apr 18")

3. summary: Header tổng hợp phía trên, gồm:
   - views (số nguyên, bỏ dấu phẩy)
   - clicks (số nguyên)
   - orders (số nguyên)
   - revenue (số thực, bỏ $)
   - spend (số thực, bỏ $)
   - roas (số thực)

4. metric_column: Cột daily data đang hiển thị. Đọc tiêu đề cột bên phải
   (ví dụ: "Views from last 30 days" → "views", "Spend from last 30 days" → "spend")

5. daily_data: Array các dòng, mỗi dòng gồm:
   - date: ngày (format "MMM DD, YYYY", ví dụ "Mar 19, 2026")
   - value: giá trị (số nguyên cho views/clicks/orders, số thực cho revenue/spend/roas, bỏ $ nếu có)

Trả về JSON, không text thêm:
{
  "type": "listing_daily",
  "listing_id": "4438217152",
  "period": "Mar 19 - Apr 18",
  "summary": {"views": 2474, "clicks": 33, "orders": 3, "revenue": 102.97, "spend": 27.99, "roas": 3.68},
  "metric_column": "views",
  "daily_data": [
    {"date": "Mar 19, 2026", "value": 68},
    {"date": "Mar 20, 2026", "value": 26}
  ]
}
```

### 4.2 Prompt cho Keyword Performance screenshots

```
Bạn là AI chuyên đọc screenshot từ Etsy Ads dashboard.

Screenshot này là bảng keyword performance của một listing. Hãy trích xuất:

1. listing_id: Số ID trong URL bar

2. keywords: Array, mỗi keyword gồm:
   - keyword (text)
   - roas (số thực)
   - orders (số nguyên)
   - spend (số thực, bỏ $)
   - revenue (số thực, bỏ $)
   - clicks (số nguyên)
   - click_rate (string giữ nguyên %, ví dụ "1.1%")
   - views (số nguyên)

Trả về JSON:
{
  "type": "keyword_table",
  "listing_id": "4438225302",
  "keywords": [
    {"keyword": "custom sweatshirts", "roas": 0, "orders": 0, "spend": 0.85, "revenue": 0, "clicks": 2, "click_rate": "1.1%", "views": 181}
  ]
}
```

### 4.3 Auto-classify

Backend gửi ảnh kèm prompt ngắn để Gemini tự phân loại:
- Nếu thấy daily breakdown → dùng prompt 4.1
- Nếu thấy keyword table → dùng prompt 4.2
- Hoặc: **1 prompt gộp** với instruction "if you see daily chart return type=listing_daily, if you see keyword rows return type=keyword_table"

### 4.4 Merge logic (backend)

```python
# Sau khi extract tất cả ảnh:
# 1. Group listing_daily screenshots theo listing_id
# 2. Summary lấy từ bất kỳ screenshot nào (giống nhau)
# 3. daily_data merge theo date:
#    {"19/3/26": {"views": 68}} + {"19/3/26": {"clicks": 2}} → {"19/3/26": {"views": 68, "clicks": 2}}
# 4. Nếu thiếu metric cho 1 ngày → giữ null (screenshot scroll không đủ)
# 5. keyword_table giữ nguyên, không merge
```

---

## 5. Frontend UI

### Đặt tại: section mới trong `index.html` (tab "Internal Data" hoặc modal)

### 5.1 Upload Zone

```
┌─────────────────────────────────────────────────┐
│  📁 Internal Ads Data                           │
│                                                 │
│  ┌───────────────────────────────────────────┐  │
│  │                                           │  │
│  │    Drag & Drop screenshots here           │  │
│  │    or click to browse                     │  │
│  │                                           │  │
│  │    Accepted: PNG, JPG  •  Max: 20 files   │  │
│  └───────────────────────────────────────────┘  │
│                                                 │
│  Selected: 7 files (1.2 MB)                     │
│  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐ ...       │
│  │thumb1│ │thumb2│ │thumb3│ │thumb4│            │
│  └──────┘ └──────┘ └──────┘ └──────┘            │
│                                                 │
│  [ Upload & Extract ]                           │
└─────────────────────────────────────────────────┘
```

### 5.2 Preview & Edit (sau extract)

```
┌─────────────────────────────────────────────────┐
│  Batch: 20260418_1435  •  Status: extracted     │
│                                                 │
│  ── Listing Report (3 rows) ──────────────────  │
│  ┌─────────┬──────────┬───────┬────────┬─────┐  │
│  │listing  │ period   │ views │ clicks │ ... │  │
│  ├─────────┼──────────┼───────┼────────┼─────┤  │
│  │44382171 │Mar19-Ap18│ 2474  │  33    │ ... │  │  <- cell editable
│  │44382171 │19/3/26   │  68   │   2    │ ... │  │
│  │...      │          │       │        │     │  │
│  └─────────┴──────────┴───────┴────────┴─────┘  │
│                                                 │
│  ── Keyword Report (9 rows) ──────────────────  │
│  ┌──────────────────┬──────┬────────┬───────┐   │
│  │ keyword          │views │ clicks │ spend │   │
│  ├──────────────────┼──────┼────────┼───────┤   │
│  │custom sweatshirts│ 181  │   2    │ 0.85  │   │  <- cell editable
│  │...               │      │        │       │   │
│  └──────────────────┴──────┴────────┴───────┘   │
│                                                 │
│  [ Discard ]                    [ Confirm ]     │
└─────────────────────────────────────────────────┘
```

### 5.3 States & Transitions

```
(no batch)  ──[Upload]──>  uploaded  ──[Extract]──>  extracted
                                                        |
                                          [Discard] <───┤───> [Confirm]
                                              |                    |
                                          discarded            confirmed
                                                                   |
                                                          [Rollback] (from history)
                                                                   |
                                                              rolled_back
```

### 5.4 History Panel

```
┌─────────────────────────────────────────────────┐
│  Import History                                 │
│                                                 │
│  20260418_1435  ✅ confirmed  3 listings  9 kw  │
│  20260411_0920  ✅ confirmed  2 listings  7 kw  │
│  20260404_1100  🔙 rolled_back                  │
│                                                 │
│  Click to view snapshot or rollback             │
└─────────────────────────────────────────────────┘
```

---

## 6. Cơ chế Xoá & Cleanup

### 6.1 Xoá ảnh raw sau confirm

```python
# Khi confirm thành công:
batch_dir = Path(f"data/raw/internal/{batch_id}")
if batch_dir.exists():
    shutil.rmtree(batch_dir)  # xoá toàn bộ folder ảnh
```

**Lý do xoá:** Ảnh chỉ là input cho Gemini, sau confirm data đã nằm trong DB + snapshot JSON. Giữ ảnh tốn storage và không cần truy cập lại.

### 6.2 Xoá data cũ khi import trùng

```python
# Trước khi insert batch mới:
# Xoá records có cùng (listing_id, period) từ batch CŨ
# Giữ snapshot của batch cũ để rollback nếu cần
```

### 6.3 Discard — huỷ batch chưa confirm

```python
# Xoá ảnh raw + JSON preview
# Mark batch status = discarded
# Không ảnh hưởng DB
```

### 6.4 Rollback — revert batch đã confirm

```python
# 1. Đọc snapshot JSON
# 2. DELETE FROM listing_report WHERE batch_id = ?
# 3. DELETE FROM keyword_report WHERE batch_id = ?
# 4. Mark batch status = rolled_back
# 5. Giữ nguyên snapshot (cho audit trail)
```

### 6.5 Auto-cleanup (optional, phase 2)

- Snapshots > 90 ngày: archive hoặc xoá
- Batches status=discarded > 7 ngày: xoá record khỏi import_batch

---

## 7. File Structure (code mới cần tạo)

```
backend/app/
├── api/routes/
│   └── internal.py              ← NEW: 7 endpoints
├── models/
│   ├── import_batch.py          ← NEW: ImportBatch model
│   ├── listing_report.py        ← NEW: ListingReport model
│   └── keyword_report.py        ← NEW: KeywordReport model
├── schemas/
│   └── internal.py              ← NEW: request/response schemas
├── services/
│   └── internal_service.py      ← NEW: business logic
│   └── internal_extractor.py    ← NEW: Gemini prompt + extract + merge
```

### Sửa file có sẵn:

| File | Thay đổi |
|------|----------|
| `backend/app/main.py` | Thêm `include_router(internal_router)` |
| `backend/app/core/config.py` | Thêm `GEMINI_API_KEY` setting |
| `backend/requirements.txt` | Thêm `google-generativeai`, `Pillow`, `python-multipart` |
| `index.html` | Thêm Internal Data section/tab + JS upload/preview/confirm |
| `.env.example` | Thêm `GEMINI_API_KEY` |

---

## 8. Dependencies mới

```
# backend/requirements.txt — thêm:
google-generativeai>=0.8.0    # Gemini Vision API
Pillow>=10.0.0                # Image processing
python-multipart>=0.0.9       # File upload support (FastAPI)
aiofiles>=24.1.0              # Async file operations
```

---

## 9. Phân công Agent

| Phase | Agent | Scope | Output |
|-------|-------|-------|--------|
| 1 | **Backend** | Models + DB migration | 3 model files, alembic migration |
| 2 | **Backend** | Schemas + Service | internal.py schemas, internal_service.py |
| 3 | **Backend** | Gemini Extractor | internal_extractor.py (2 prompts + merge logic) |
| 4 | **Backend** | API Routes | internal.py routes (7 endpoints) |
| 5 | **Frontend** | UI Components | Upload zone, preview tables, history panel |
| 6 | **Testing** | Integration test | Upload → Extract → Confirm → Rollback flow |

### Thứ tự phụ thuộc:

```
Phase 1 (Models) ──> Phase 2 (Schemas+Service) ──> Phase 4 (Routes)
                                                        |
Phase 3 (Extractor) ────────────────────────────────────┘
                                                        |
                                                   Phase 5 (Frontend)
                                                        |
                                                   Phase 6 (Testing)
```

---

## 10. Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| Gemini đọc sai số từ screenshot | Data sai vào DB | User review + editable cells trước confirm |
| Screenshot format thay đổi (Etsy update UI) | Extract fail | Prompt flexible, có fallback manual input |
| Upload ảnh lớn (>5MB/file) | Slow, timeout | Resize client-side trước upload, limit 20 files |
| Confirm nhầm, phát hiện sau | Data sai trong DB | Rollback mechanism + snapshot |
| Trùng data (upload cùng period 2 lần) | Duplicate rows | Upsert logic: xoá cũ cùng listing+period trước insert |
| Gemini API rate limit | Extract fail giữa chừng | Retry with backoff, partial extract resume |

---

## 11. Câu hỏi mở (cần thảo luận)

1. **Daily rows**: Có cần lưu daily breakdown (31 dòng/listing) hay chỉ cần 30-day summary + yesterday?
   - Nếu chỉ summary: đơn giản hơn, ít ảnh hơn (chỉ cần 1 ảnh/listing + 1 ảnh keyword)
   - Nếu daily: cần 6 ảnh/listing (views, clicks, orders, revenue, spend, roas) + scroll đủ

2. **Listing metadata** (title, price, stock, category, lifetime): Lấy từ đâu?
   - Không có trong Ads screenshot — cần screenshot riêng từ listing page?
   - Hay nhập tay trên UI?
   - Hay pull từ Etsy API (nếu có API key)?

3. **Multi-listing per batch**: Mỗi lần upload có thể chứa ảnh từ nhiều listing không?
   - Nếu có: cần Gemini phân loại theo listing_id từ URL

4. **Period cho keyword_report**: Screenshot keyword không hiện period filter → assume cùng period với listing screenshots cùng batch?

5. **Tần suất**: 7 ngày/lần, nhưng period = 30 ngày → data overlap 23 ngày giữa 2 lần import. Có cần xử lý overlap hay ghi đè?
