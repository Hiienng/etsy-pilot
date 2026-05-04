# Research Hub — Quy chuẩn phân tích & đề xuất sản phẩm

> Áp dụng cho mọi task liên quan đến tab **Research Hub** trong `index.html`.  
> File này là nguồn sự thật duy nhất — đọc trước khi thực hiện bất kỳ thay đổi nào.

---

## 1. Bối cảnh kinh doanh

**User là công ty gia công may thêu.**  
Năng lực cốt lõi: may, thêu, in vải — không sản xuất đồ gỗ, gốm sứ, điện tử, hay giấy in.

Toàn bộ phân tích và đề xuất trong Research Hub **phải phản ánh góc nhìn của một xưởng may**, không phải góc nhìn của nhà bán lẻ tổng quát.

---

## 2. Phân loại sản phẩm

### 2a. Sản phẩm ĐỦ ĐIỀU KIỆN (may/thêu được)

Sản phẩm được coi là phù hợp nếu chất liệu chính là **vải, sợi, len, denim, hoặc có thể thêu trang trí**.

| Nhóm | Ví dụ sản phẩm từ market_listing |
|---|---|
| May mặc trẻ em | baby romper, baby bodysuit, baby onesie, baby sweater, denim jacket, baby shoes |
| Chăn / khăn vải | baby blanket, milestone blanket, woven blanket |
| Album / sách bọc vải | photo album (vải), keepsake album, embroidered album |
| Gối / phụ kiện vải | name pillow |
| Giỏ / túi vải | diaper caddy basket, storage basket, handmade basket, baby hamper |
| Sản phẩm thêu chuyên biệt | embroidery hoop, embroidered birth announcement |
| Đồ chơi vải | plush toy, teddy bear |

### 2b. Sản phẩm KHÔNG PHÙ HỢP

Không đề xuất, không ưu tiên trong bảng recommendation:

| Lý do | Sản phẩm |
|---|---|
| Gốm / đất nung | jewelry dish, ring dish, trinket dish |
| Gỗ / acrylic | name sign, keepsake plaque, birth announcement sign, wooden puzzle, birth stats sign |
| Điện tử | night light, musical carousel, crystal ball lamp, ballerina night light |
| Giấy / in ấn | birth stats print, star map print, storybook |
| Kim loại | baptism pin, keychain |
| Hộp cứng | time capsule box, tooth fairy box |

> **Lưu ý:** Một số sản phẩm như `baby book`, `baby gift set`, `gift box` có thể có thành phần vải — chỉ đưa vào danh sách phù hợp nếu có bằng chứng từ title hoặc data cho thấy yếu tố vải chiếm chính.

---

## 3. Quy tắc phân tích trong Research Hub

### 3a. Bảng đề xuất sản phẩm (Section 05)
- **Chỉ hiển thị sản phẩm may/thêu được** trong bảng đề xuất chính.
- Mỗi row phải có cột hoặc indicator "Khả năng gia công" (thêu, may, in vải...).
- Sản phẩm không may được → loại khỏi bảng hoặc đánh dấu rõ là ngoài năng lực.

### 3b. Charts thị phần & hot rate
- Khi tô màu hoặc highlight chart, ưu tiên **làm nổi bật sản phẩm may/thêu được**.
- Sản phẩm không phù hợp (jewelry dish, ring dish...) vẫn hiển thị để context cạnh tranh, nhưng **không được đặt ở vị trí ưu tiên** trong narrative.

### 3c. Insight tự động / caption dưới chart
- Khi đề cập "sản phẩm đang hot" → luôn thêm filter ngầm: trong số các sản phẩm may được, cái nào hot nhất?
- Ví dụ đúng: *"Trong các sản phẩm may thêu, baby romper dẫn đầu với hot rate 71% và avg $58."*
- Ví dụ sai: *"Jewelry dish dẫn đầu với 9 badges."* (không phù hợp với năng lực gia công)

### 3d. Tín hiệu ưu tiên khi đề xuất sản phẩm mới
Xếp hạng ưu tiên khi chọn sản phẩm nên sản xuất:

1. **Sewable = true** — điều kiện bắt buộc
2. **Hot rate cao** (Popular now + Bestseller / tổng listing) — tín hiệu nhu cầu ngắn hạn
3. **Badge count tuyệt đối cao** — tín hiệu platform đang đẩy mạnh
4. **Avg price hợp lý** — $30–$130 là vùng tối ưu cho gia công may thêu
5. **Emerging** (rating cao, review thấp) — cơ hội gia nhập trước khi thị trường bão hoà
6. **Avg review đủ lớn** (>200) — xác nhận demand thật sự tồn tại

---

## 4. Danh sách sản phẩm may/thêu từ market_listing (tham chiếu nhanh)

Được xác định từ snapshot 2026-04-15, 180 listings:

| Sản phẩm | Listings | Hot rate | Avg price | Ưu tiên |
|---|---|---|---|---|
| baby romper | 7 | 71% | $58 | ⭐⭐⭐ |
| baby blanket | 6 | 50% | $92 | ⭐⭐⭐ |
| baby bodysuit | 3 | 67% | $62 | ⭐⭐⭐ |
| baby onesie | 2 | 100% | $62 | ⭐⭐⭐ |
| keepsake album | 2 | 100% | $110 | ⭐⭐⭐ |
| denim jacket | 1 | 100% | $66 | ⭐⭐ |
| name pillow | 1 | 100% | $62 | ⭐⭐ |
| diaper caddy basket | 2 | 100% | $45 | ⭐⭐ |
| baby sweater | 2 | 50% | $30 | ⭐⭐ |
| milestone blanket | 1 | 0% | $138 | ⭐⭐ (premium) |
| embroidered album | 1 | 0% | $133 | ⭐⭐ (premium) |
| storage basket | 2 | 50% | $18 | ⭐ (thấp giá) |
| plush toy | 2 | 100% | $67 | ⭐ (cần thiết kế) |
| photo album (vải) | 9 | 33% | $118 | ⭐⭐ (nếu vải) |

---

## 5. Cấu trúc tab Research Hub

### Sections cố định (không thay đổi thứ tự):
```
01 — Thị phần sản phẩm       (market share, listing count)
02 — Sản phẩm đang HOT       (badge ranking, hot rate)
03 — Định giá & cạnh tranh   (price range, discount strategy)
04 — Cơ hội sản phẩm         (emerging vs established)
05 — Tổng hợp & đề xuất      (bảng recommendation — CHỈ sản phẩm may/thêu)
```

### Nguồn dữ liệu:
- Bảng: `market_listing` (Neon PostgreSQL)
- Đơn vị giá: `price / 10000 = USD`
- Hot signal: `badge IN ('Popular now', 'Bestseller')`
- Hot rate: `hot_count / total_listings_of_that_type * 100`
- Emerging: `rating >= 4.8 AND review_count < 500`

---

## 6. Khi thêm dữ liệu mới (batch mới)

- Không hardcode data trong HTML — khi có batch mới, cập nhật JS data object từ DB query
- Ghi rõ ngày snapshot trong subtitle của section header
- Nếu product type mới xuất hiện, phân loại sewable/non-sewable theo Section 2 trước khi thêm vào chart
- Không tự ý thêm sản phẩm vào bảng đề xuất nếu không kiểm tra sewable status

---

*Cập nhật lần cuối: 2026-04-15*
