# Performance Hub — Quy chuẩn thiết kế & phân tích

> Áp dụng cho mọi task liên quan đến tab **Performance Hub** trong `index.html`.  
> File này là nguồn sự thật duy nhất — đọc trước khi thực hiện bất kỳ thay đổi nào.

---

## 1. Mục tiêu

Performance Hub dùng để **nhìn lại kết quả đã thực hiện và xác định điểm cải thiện**, từ góc nhìn sản phẩm và listing cụ thể.

**3 chỉ số trọng tâm**: CTR · CR · ROAS  
Mọi section khác (portfolio, market gap) là ngữ cảnh phụ hỗ trợ ra quyết định.

---

## 2. Cấu trúc cố định (không thay đổi thứ tự)

```
01 — Tổng quan hiệu quả          (hero KPI cards: CTR · CR · ROAS)
02 — Listing tốt nhất & quick win (best performers + CR-good/CTR-bad)
03 — Chi tiết theo sản phẩm       (bar charts + stacked pass/fail)
04 — Bối cảnh thị trường          (gap chart + portfolio — secondary)
05 — Hành động ưu tiên            (bảng action, impact, lý do)
```

---

## 3. Ngưỡng chỉ số

| Chỉ số | Ngưỡng hiện tại | Ghi chú |
|---|---|---|
| CTR | ≥ 2% | Listing thu hút click trong feed |
| CR  | ≥ 4% | Listing thuyết phục buyer sau khi vào |
| ROAS | ≥ break-even | Break-even chưa xác định theo từng sản phẩm — dùng 2.0x tạm |

**Rule:** Khi user cung cấp break-even ROAS thực tế, cập nhật threshold line `perfROAS` và tất cả số liệu pass/fail liên quan.

---

## 4. Section 01 — Hero KPI cards

Mỗi chỉ số = 1 card gồm:
- Avg value (lớn, serif font)
- Pass count (N/128)
- Progress bar (tỷ lệ % listings đạt)
- 1 câu insight cụ thể nhất (ví dụ: sản phẩm nào có vấn đề gì)
- Border-left màu phân biệt: đỏ (#b53333) = critical fail, xanh (#4a8c60) = ok, stone = neutral

---

## 5. Section 02 — Listing tốt nhất & quick win

**Luôn hiển thị 2 bảng song song:**

### Bảng trái — Best performers
- Điều kiện: CTR ≥ 2 AND CR ≥ 4 AND ROAS ≥ 2
- Sắp xếp: composite score cao xuống thấp
- Dùng làm template để nhân bản sang listing khác

### Bảng phải — Quick win
- Điều kiện: CR ≥ 4 AND CTR < 2
- Lý do ưu tiên: buyer muốn mua (CR ok) nhưng không click vào (CTR fail) → chỉ cần fix thumbnail/title
- Sắp xếp: CR DESC, CTR ASC (CR cao nhất + CTR thấp nhất = dễ cải thiện nhất)

```sql
-- Best performers
SELECT listing_id, title, product, ctr, cr, roas
FROM internal_listing
WHERE ctr >= 2 AND cr >= 4 AND roas >= 2
ORDER BY (ctr/2 + cr/4 + roas/2) DESC LIMIT 8;

-- Quick win
SELECT listing_id, title, product, ctr, cr, roas
FROM internal_listing
WHERE cr >= 4 AND ctr < 2
ORDER BY cr DESC, ctr ASC LIMIT 8;
```

---

## 6. Section 03 — Charts chi tiết theo sản phẩm

- **Bar charts**: 1 bar per sản phẩm, có **threshold line dashed** (dùng mixed chart: bar + line)
- **Stacked bar**: xanh (#4a8c60) = pass, đỏ (#b53333) = fail — per sản phẩm
- Canvas IDs cố định: `perfCTR`, `perfCR`, `perfROAS`, `perfCTRStack`, `perfCRStack`
- Khởi tạo trong `initPerfCharts()` — chạy `requestAnimationFrame(initPerfCharts)` on load

---

## 7. Section 04 — Market context (secondary)

- Chỉ 2 chart nhỏ: gap chart (`perfChartGap`) + portfolio donut (`perfChartPortfolio`)
- Không có canvas `perfChartCount` — đã bị xóa, không thêm lại
- Insight tập trung vào under/over-indexed so với market hot rate

---

## 8. Section 05 — Bảng hành động

Cột cố định: `#` · `Hành động` · `Sản phẩm` · `Impact` · `Lý do`  
Sắp xếp: quick win trước, chiến lược sau, cần dữ liệu cuối

---

## 9. Nguồn dữ liệu

- Bảng: `internal_listing` (Neon PostgreSQL)
- Snapshot hiện tại: 128 listings, 3 sản phẩm, tất cả Closed
- CTR/CR/ROAS: đã populate trong DB — tham chiếu range thực từ screenshot Etsy Ads
- Ghi rõ ngày snapshot trong subtitle header section

---

## 10. Snapshot 2026-04-16

| Sản phẩm | n | CTR avg | CTR pass | CR avg | CR pass | ROAS avg | ROAS pass |
|---|---|---|---|---|---|---|---|
| baby blanket | 56 | 1.41% | 0/56 | 4.28% | 29/56 | 1.88x | 24/56 |
| baby onesie | 42 | 1.75% | 11/42 | 3.75% | 18/42 | 1.22x | 0/42 |
| baby romper | 30 | 1.71% | 9/30 | 4.53% | 18/30 | 1.64x | 8/30 |
| **TOTAL** | **128** | **1.59%** | **20/128** | **4.16%** | **65/128** | **1.61x** | **32/128** |

All 3 pass: **3 listings** (tất cả baby romper)  
All 3 fail: **41 listings**  
Quick win (CR ok, CTR bad): **54 listings**

---

*Cập nhật lần cuối: 2026-04-16*
