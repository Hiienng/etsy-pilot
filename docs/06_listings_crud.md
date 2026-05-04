# Flow 06 — Listings CRUD

Feature: quản lý catalog nội bộ (`listings`) và lưu output AI optimization (`optimized_title` / `optimized_tags` / `optimized_description`).
UI: (chưa gắn vào index.html — endpoint dùng cho backfill & Listing Manager tương lai).

## Sequence — các thao tác CRUD

```mermaid
sequenceDiagram
    autonumber
    actor U as User / Backfill script
    participant FE as Frontend / curl
    participant API as /api/v1/listings
    participant SVC as listing_service
    participant DB as Postgres (listings)

    rect rgb(240,245,255)
    Note over U,DB: READ
    U->>API: GET /listings?store=X&status=Open
    API->>SVC: get_listings(db, skip, limit, store, status)
    SVC->>DB: SELECT * FROM listings<br/>WHERE store=:s AND trang_thai=:t<br/>OFFSET skip LIMIT limit
    DB-->>API: rows
    API-->>U: listings[]
    end

    rect rgb(240,255,245)
    Note over U,DB: CREATE
    U->>API: POST /listings (ListingCreate)
    API->>SVC: create_listing(db, data)
    SVC->>DB: INSERT INTO listings (...)
    DB-->>API: new row
    API-->>U: listing (id = UUID)
    end

    rect rgb(255,250,235)
    Note over U,DB: UPDATE
    U->>API: PATCH /listings/{id} (ListingUpdate)
    API->>SVC: update_listing(db, id, data)
    SVC->>DB: UPDATE listings SET ...<br/>WHERE id = :id
    DB-->>API: updated row
    API-->>U: listing
    end

    rect rgb(255,240,240)
    Note over U,DB: DELETE
    U->>API: DELETE /listings/{id}
    API->>SVC: delete_listing(db, id)
    SVC->>DB: DELETE FROM listings<br/>WHERE id = :id
    DB-->>API: rowcount
    API-->>U: {ok: true}
    end
```

## Sequence — AI optimization save

```mermaid
sequenceDiagram
    autonumber
    actor Sys as Optimizer<br/>(sklearn/Claude)
    participant SVC as listing_service
    participant DB as listings

    Sys->>SVC: save_optimizations(<br/>listing_id, title, tags, desc)
    SVC->>DB: UPDATE listings SET<br/>optimized_title = :t,<br/>optimized_tags = :g,<br/>optimized_description = :d,<br/>updated_at = now()<br/>WHERE id = :listing_id
    DB-->>SVC: updated row
    SVC-->>Sys: listing
```

## Routes

| Method | Path | Handler | Schema |
|---|---|---|---|
| GET | `/api/v1/listings/` | `list_listings` | query: `skip`, `limit`, `store`, `status` |
| GET | `/api/v1/listings/{listing_id}` | `get_listing` |   |
| POST | `/api/v1/listings/` | `create_listing` | body: `ListingCreate` |
| PATCH | `/api/v1/listings/{listing_id}` | `update_listing` | body: `ListingUpdate` |
| DELETE | `/api/v1/listings/{listing_id}` | `delete_listing` |   |
| GET | `/api/v1/listings/stats/count` | `listing_count` |   |

## Quan hệ với các feature khác

```mermaid
flowchart LR
    L[listings] -- "listing_id (logical)" --> LR[listing_report]
    L -- "listing_id (logical)" --> KR[keyword_report]
    L -- "input cho AI optimize" --> OPT[Optimizer<br/>(sklearn/Claude)]
    OPT -- "save_optimizations()" --> L
    CSV[data/raw/<br/>production_file_listing.csv] -- "seed ban đầu" --> L
```

## Schema chạm tới

- `listings` — bảng duy nhất
- (logical) `listing_report` / `keyword_report` — đọc/ghi qua feature khác, không thuộc CRUD này

## Ghi chú vận hành

- `id` là UUID v4, sinh ở tầng model (`default=lambda: str(uuid.uuid4())`).
- Index: `idea_sku`, `store`, `trang_thai` — đảm bảo query filter bằng 3 cột này nhanh.
- `updated_at` tự cập nhật qua `onupdate=func.now()`.
- Khi gọi `save_optimizations`, không ghi đè trường gốc (`title`, `tag`, `description`) — chỉ set các cột `optimized_*` để giữ reversible.
