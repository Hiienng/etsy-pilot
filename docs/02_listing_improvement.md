# Flow 02 — Listing Improvement

Feature: gợi ý hành động (`keep` / `improve` / `improve_or_off`) cho từng listing dựa trên ma trận `scenarios_rules`.
UI pill: `perf-sub-improvement`.

---

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

    U->>FE: Chọn pill "Listing Improvement"
    FE->>CACHE: GET performance_dashboard.json
    CACHE-->>FE: listings[] (đã embed scenario_action, fix_listing, fix_ads)

    U->>FE: Chọn filter<br/>(product / VM / CTR / CR / ROAS)
    FE->>FE: renderImprTable()<br/>lọc + sort theo action priority

    FE-->>U: Render bảng improvement

    opt Số liệu stale → Tải lại
        U->>FE: Click "Tải lại"
        FE->>API: POST /performance/refresh
        API->>SVC: get_dashboard_listings(db)
        SVC->>DB: SELECT listing_report<br/>JOIN scenarios_rules<br/>LATERAL JOIN market_listing
        DB-->>SVC: rows
        SVC->>CACHE: write_dashboard_json()
        API-->>FE: {ok: true}
        FE->>CACHE: GET performance_dashboard.json
        FE-->>U: Re-render bảng
    end
```

---

## Logic classification (band → scenario)

```mermaid
flowchart LR
    LR[listing_report row] --> ROAS{ROAS}
    ROAS -->|"≥ 2.0"| P["profitable"]
    ROAS -->|"1.0 – 1.99"| S["slight_loss"]
    ROAS -->|"0 < r < 1.0"| H["heavy_loss"]
    ROAS -->|"= 0"| N["no_sales"]

    LR --> CTR{CTR}
    CTR -->|"≥ 1.5%"| CH["high"]
    CTR -->|"< 1.5%"| CL["low"]

    LR --> CR{CR}
    CR -->|"≥ 3%"| RH["high"]
    CR -->|"< 3%"| RL["low / zero"]

    P --> J[("scenarios_rules\nJOIN")]
    S --> J
    H --> J
    N --> J
    CH --> J
    CL --> J
    RH --> J
    RL --> J

    J --> A1["keep 🟢"]
    J --> A2["improve 🟡"]
    J --> A3["improve_or_off 🔴"]
```

---

## Ma trận 14 kịch bản (seeded bởi `seed_scenarios()`)

| # | ROAS band | CR | CTR | Case | Action | Ưu tiên |
|---|---|---|---|---|---|---|
| 1 | profitable | high | high | Có sales và đang lời | **keep** | 3 |
| 2 | profitable | low | high | Có sales và đang lời | **improve** | 2 |
| 3 | profitable | high | low | Có sales và đang lời | **improve** | 2 |
| 4 | profitable | low | low | Có sales và đang lời | **improve** | 2 |
| 5 | slight_loss | high | high | Có sales, đang lỗ nhẹ | **improve** | 2 |
| 6 | slight_loss | high | low | Có sales, đang lỗ nhẹ | **improve** | 2 |
| 7 | slight_loss | low | high | Có sales, đang lỗ nhẹ | **improve** | 2 |
| 8 | slight_loss | low | low | Có sales, đang lỗ nhẹ | **improve** | 2 |
| 9 | heavy_loss | high | high | Có sales, lỗ nặng | **improve** | 2 |
| 10 | heavy_loss | low | high | Có sales, lỗ nặng | **improve** | 2 |
| 11 | heavy_loss | high | low | Có sales, lỗ nặng | **improve** | 2 |
| 12 | heavy_loss | low | low | Có sales, lỗ nặng | **improve_or_off** | 1 |
| 13 | no_sales | zero | high | Không có sale, có clicks | **improve_or_off** | 1 |
| 14 | no_sales | zero | low | Không có sale, có clicks | **improve_or_off** | 1 |

Ngưỡng: CTR ≥ **1.5%** = high · CR ≥ **3%** = high · ROAS break-even = **2.0**

---

## Gợi ý fix theo từng kịch bản đặc trưng

| Kịch bản | Cause tiêu biểu | Fix Listing | Fix Ads |
|---|---|---|---|
| `profitable` + CR low | Listing chưa đủ hấp dẫn, keywords ads chưa tối ưu | Kiểm tra giá, ship, review, hình chi tiết, options | Tắt keywords kém hiệu quả |
| `profitable` + CTR low | Hình main chưa đúng intent, giá mòi cao | Tối ưu keywords, hình main, alt, giá mòi | Tắt keywords không đúng intent |
| `slight_loss` bất kỳ | Views/clicks thấp hoặc AOV không cover ads spend | Sửa keywords long-tail, hình main, giá mòi | Tắt keywords rộng/cạnh tranh cao |
| `heavy_loss` + low/low | Listing chưa tối ưu; nếu đã tối ưu mà không cải thiện → tắt | Cùng như trên + kiểm tra review xấu | Tắt ads |
| `no_sales` + high clicks | Listing mới (theo dõi thêm) hoặc mất index | Đổi long-tail keywords, up ảnh chi tiết, xin reviews | Tắt keywords không liên quan |
| `no_sales` + low clicks | Listing mất index hoặc intent không khớp | Deactive → reactive lại listing + fix keywords | Tắt ads |

---

## Bảng improvement — cấu trúc cột UI

| Cột | Nguồn trường | Hiển thị |
|---|---|---|
| Listing title | `listing_report.title` | Link → Etsy URL |
| Product | derived từ `category` | Badge |
| VM | `listing_report.no_vm` | Text |
| CTR | Numeric | Badge xanh/vàng/đỏ |
| CR | Numeric | Badge xanh/vàng/đỏ |
| ROAS | Numeric | Badge theo band |
| Case | `scenarios_rules.case_name` | Text |
| Action | `scenarios_rules.action` | 🟢 keep / 🟡 improve / 🔴 improve_or_off |
| Cause | `scenarios_rules.cause` | Expand/tooltip |
| Fix Listing | `scenarios_rules.fix_listing` | Checklist expand |
| Fix Ads | `scenarios_rules.fix_ads` | Checklist expand |

---

## Sort & pagination

```mermaid
flowchart LR
    Raw["listings[] từ JSON"] --> F1["Filter client-side\n(product, VM, CTR/CR/ROAS levels)"]
    F1 --> S1["Sort priority:\n1 = improve_or_off\n2 = improve\n3 = keep\n→ roas ASC NULLS LAST"]
    S1 --> P1["Pagination 20 rows/trang\n+ Load More button"]
    P1 --> UI["Bảng improvement"]
```

---

## Schema chạm tới

| Bảng | Vai trò |
|---|---|
| `listing_report` | CTR / CR / ROAS thực tế + metadata |
| `scenarios_rules` | 14-row matrix → action + cause + fix guide |
| `listings` | (optional) link Etsy, title chuẩn hoá |
