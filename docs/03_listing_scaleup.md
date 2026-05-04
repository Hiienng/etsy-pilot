# Flow 03 — Listing Scale Up

Feature: xác định listing đang "bay" (CTR / CR / ROAS đều đạt ngưỡng) để đề xuất nâng ngân sách hoặc nhân rộng.
UI pill: `perf-sub-scaleup`.

## Sequence flow

```mermaid
sequenceDiagram
    autonumber
    actor U as User
    participant FE as index.html (JS)
    participant CACHE as performance_dashboard.json
    participant API as /api/v1/performance/refresh
    participant SVC as performance_service
    participant DB as Postgres

    U->>FE: Chọn pill "Listing Scale Up"
    FE->>CACHE: GET performance_dashboard.json
    CACHE-->>FE: listings[]
    FE->>FE: Filter scale-up candidates<br/>(action='keep', ROAS ≥ 2.5,<br/>CR ≥ 4%, CTR ≥ 2%)
    FE->>FE: Build bubble chart<br/>(x=CR, y=ROAS, size=revenue)
    U->>FE: Chọn filter product / VM
    FE-->>U: Bubble chart + bảng candidates
    opt Cần rebuild
        U->>FE: Click "Tải lại"
        FE->>API: POST /performance/refresh
        API->>SVC: get_dashboard_listings()
        SVC->>DB: SELECT listing_report + JOIN
        DB-->>SVC: rows
        SVC->>CACHE: write_dashboard_json()
    end
```

## Logic chọn candidate

```mermaid
flowchart TD
    All[listings từ dashboard] --> F1{action = 'keep'?}
    F1 -->|no| Drop1[[Loại]]
    F1 -->|yes| F2{ROAS ≥ 2.5?}
    F2 -->|no| Drop2[[Loại]]
    F2 -->|yes| F3{CR ≥ 4% & CTR ≥ 2%?}
    F3 -->|no| Drop3[[Loại]]
    F3 -->|yes| F4{Doanh thu<br/>lifetime_revenue ≥ p50?}
    F4 -->|no| Watchlist[[Watchlist]]
    F4 -->|yes| Scale[[Scale candidate]]
    Scale --> Bubble[Bubble chart + bảng]
```

## Bubble chart mapping

| Trục | Giá trị | Giải thích |
|---|---|---|
| X | `cr` | Tỉ lệ convert |
| Y | `roas` | Lợi nhuận ads |
| Size | `revenue` (period gần nhất) | Độ lớn đóng góp doanh thu |
| Color | product group | Phân biệt baby romper / blanket / onesie |
| Label | `title` (rút gọn) + VM | Tooltip đầy đủ |

## Bảng candidates (cấu trúc cột)

| Cột | Nguồn |
|---|---|
| Listing | `listing_report.title` + link Etsy |
| Product | derived từ `category` |
| VM | `no_vm` |
| CTR / CR / ROAS | Numeric badge |
| Revenue (period) | `revenue` |
| Lifetime revenue | `lifetime_orders × price` hoặc `lifetime_revenue` |
| Reference market | `ref_title`, `ref_shop` từ LATERAL JOIN `market_listing` |
| Suggest action | "Tăng budget +30%" / "Nhân bản sang shop khác" |

## Schema chạm tới

- `listing_report` — metric gốc + lifetime
- `scenarios_rules` — chỉ lấy `action='keep'`
- `market_listing` — reference so sánh với top competitor

## Hành động sau khi chọn

```mermaid
flowchart LR
    Pick[User chọn candidate] --> A1[Note: tăng budget]
    Pick --> A2[Export list → Etsy Ads Manager]
    Pick --> A3[Đưa vào backlog<br/>nhân bản shop khác]
```
