# Flow 04 — Internal Ads Import

Feature: chuyển screenshot báo cáo ads Etsy → DB (`listing_report` + `keyword_report`) qua 3 stage + 2 đường hồi phục.
UI pill: `perf-sub-import`.

## High-level state machine

```mermaid
stateDiagram-v2
    [*] --> uploaded: POST /internal/upload
    uploaded --> extracted: POST /internal/extract<br/>(Claude/Gemini Vision)
    uploaded --> discarded: POST /internal/discard
    extracted --> confirmed: POST /internal/confirm
    extracted --> discarded: POST /internal/discard
    confirmed --> rolled_back: POST /internal/rollback
    discarded --> [*]
    rolled_back --> [*]
```

## Sequence — happy path

```mermaid
sequenceDiagram
    autonumber
    actor U as User
    participant FE as index.html (JS)
    participant API as /api/v1/internal
    participant SVC as internal_service
    participant EXT as internal_extractor
    participant VIS as Claude/Gemini Vision
    participant FS as data/raw/internal/<br/>{batch_id}/
    participant SNAP as data/processed/<br/>snapshots/{batch_id}.json
    participant DB as Postgres

    U->>FE: Drag PNG/JPG/WebP +<br/>nhập VM, importer name
    FE->>FE: Validate MIME, size, kích thước
    FE->>API: POST /upload (multipart)
    API->>SVC: save_uploaded_files(files, db)
    SVC->>FS: Ghi từng ảnh
    SVC->>DB: INSERT import_batch<br/>(status='uploaded')
    API-->>FE: {batch_id, file_count}

    U->>FE: Click "Extract"
    FE->>API: POST /extract?batch_id=...
    API->>SVC: run_extraction(batch_id)
    SVC->>EXT: extract_batch()
    loop mỗi ảnh (concurrency=5)
        EXT->>VIS: call vision API (retry 3×)
        VIS-->>EXT: JSON (type A / B)
    end
    EXT->>EXT: _merge_results()<br/>(aggregate theo listing_id)
    EXT->>FS: Lưu extracted.json
    SVC->>DB: UPDATE import_batch<br/>(status='extracted', progress=100)
    API-->>FE: {listing_report[], keyword_report[]}
    FE-->>U: 2 bảng editable để QA

    U->>FE: Chỉnh sửa (nếu cần) + Confirm
    FE->>API: POST /confirm<br/>{batch_id, listing_report[], keyword_report[]}
    API->>SVC: confirm_import()
    SVC->>DB: DELETE WHERE (listing_id, period, no_vm)
    SVC->>DB: INSERT listing_report + keyword_report
    SVC->>SNAP: Ghi snapshot JSON
    SVC->>FS: Xoá raw images
    SVC->>DB: UPDATE import_batch<br/>(status='confirmed', confirmed_at=now())
    API-->>FE: {imported: true, rows: {listing, keyword}}
    FE-->>U: "Import thành công"
```

## Sequence — discard / rollback

```mermaid
sequenceDiagram
    autonumber
    actor U as User
    participant FE as index.html (JS)
    participant API as /api/v1/internal
    participant SVC as internal_service
    participant FS as data/raw/internal/
    participant SNAP as snapshots/
    participant DB as Postgres

    alt Discard (chưa confirm)
        U->>FE: Click "Huỷ batch"
        FE->>API: POST /discard?batch_id=...
        API->>SVC: discard_batch()
        SVC->>FS: Xoá raw images
        SVC->>DB: UPDATE import_batch<br/>(status='discarded')
        API-->>FE: {ok: true}
    else Rollback (đã confirm)
        U->>FE: Click "Rollback" trên history
        FE->>API: POST /rollback?batch_id=...
        API->>SVC: rollback_batch()
        SVC->>SNAP: Load snapshot JSON
        SVC->>DB: DELETE listing_report<br/>WHERE (listing_id, period, no_vm)<br/>theo snapshot
        SVC->>DB: DELETE keyword_report ...
        SVC->>DB: UPDATE import_batch<br/>(status='rolled_back')
        API-->>FE: {ok: true}
    end
```

## Vision classification

```mermaid
flowchart LR
    Img[Ảnh screenshot] --> VIS[Claude/Gemini Vision]
    VIS --> CLS{Phân loại}
    CLS -->|Listing summary| A[Type A<br/>listing_id, title, price, stock,<br/>category, lifetime_*, metrics period]
    CLS -->|Keyword table| B[Type B<br/>keyword, roas, orders,<br/>spend, revenue, clicks,<br/>click_rate, views]
    A --> Merge[_merge_results<br/>aggregate by listing_id]
    B --> Merge
    Merge --> Preview[listing_report[] +<br/>keyword_report[]]
```

## Validation (stage upload)

| Check | Ngưỡng |
|---|---|
| MIME/magic bytes | `png` / `jpeg` / `webp` |
| File size | 10 KB ≤ size ≤ 20 MB |
| Kích thước ảnh | ≥ 200 × 200 px |
| VM code | không rỗng |
| Importer | không rỗng |

## Schema chạm tới

| Bảng | Vai trò |
|---|---|
| `import_batch` | state machine + progress |
| `listing_report` | ghi dữ liệu sau confirm, xoá khi rollback |
| `keyword_report` | ghi dữ liệu sau confirm, xoá khi rollback |

## File system paths

| Path | Lifecycle |
|---|---|
| `data/raw/internal/{batch_id}/*.png` | Tạo khi upload, xoá khi confirm/discard |
| `data/raw/internal/{batch_id}/extracted.json` | Tạo khi extract |
| `data/processed/snapshots/{batch_id}.json` | Tạo khi confirm (để rollback) |

## Khoá tự nhiên khi ghi DB

```
(listing_id, period, no_vm)  → xoá cũ, insert mới
```

Đảm bảo re-import cùng period không tạo bản ghi trùng.
