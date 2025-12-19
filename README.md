# 📚 Machine Learning Mathematics - Multi-Page Book System

Hệ thống sách điện tử hiện đại với **Right-Click Navigation** và **Multi-Page Architecture**.

## ✨ Tính năng nổi bật

### 🖱️ Right-Click Navigation Menu
- Nhấn chuột phải **ở bất kỳ đâu** để mở menu điều hướng
- Chuyển nhanh đến bất kỳ chương nào
- Xem được toàn bộ cấu trúc sách

### 📄 Multi-Page Design
- Mỗi chương là một trang riêng biệt
- Load nhanh, không bị lag
- Dễ đọc và tập trung vào từng chủ đề

### 🎯 Sidebar Navigation
- Luôn hiển thị ở bên trái
- Highlight chương đang đọc
- Click để chuyển chương nhanh chóng

### ⌨️ Keyboard Shortcuts
- `Ctrl/Cmd + H` - Về trang chủ
- `Ctrl/Cmd + ←` - Chương trước
- `Ctrl/Cmd + →` - Chương sau

### 📱 Responsive Design
- Tự động ẩn sidebar trên mobile
- Nút menu hamburger cho màn hình nhỏ

## 🚀 Cách sử dụng

### Bước 1: Mở sách

**Quan trọng:** Phải sử dụng HTTP server, không mở trực tiếp file HTML!

#### Option 1: VS Code Live Server (Khuyến nghị)
1. Cài extension "Live Server" trong VS Code
2. Right-click vào `index.html` → "Open with Live Server"
3. Tự động mở trong browser

#### Option 2: Python HTTP Server
```bash
cd c:\Users\admin\Downloads\learning
python -m http.server 8000
```
Sau đó mở: http://localhost:8000/index.html

#### Option 3: Node.js HTTP Server
```bash
npx http-server -p 8000
```
Sau đó mở: http://localhost:8000/index.html

### Bước 2: Điều hướng

- **Click vào sidebar** để chuyển chương
- **Nhấn chuột phải** để mở quick navigation menu
- **Dùng nút Previous/Next** ở cuối mỗi chương
- **Sử dụng keyboard shortcuts**

## 📁 Cấu trúc Files

```
learning/
├── index.html                    # Trang chủ
├── chapter1.html                 # Chapter pages (auto-generated)
├── chapter2.html
├── chapter3.html
├── chapter4.html
├── chapter5.html
│
├── config.json                   # Cấu hình sách (QUAN TRỌNG)
├── styles.css                    # Styles chung
├── navigation.js                 # Navigation system
│
├── 01_probability_statistics_for_ml.html    # Source files
├── 02_distributions_bayes_mle.html
├── 03_linear_algebra_for_ml.html
├── 04_eigendecomposition_pca_norms.html
├── 05_calculus_optimization_for_ml.html
│
├── generate-pages.js             # Script để generate pages
└── README.md
```

## ➕ Thêm chương mới

### Bước 1: Tạo file HTML nguồn
Tạo file mới như `06_new_topic.html`:

```html
<!DOCTYPE html>
<html lang="vi">
<head>
  <meta charset="UTF-8">
  <title>Your Chapter Title</title>
</head>
<body>

<h1>Your Chapter Title</h1>

<h2>Section 1</h2>
<p>Your content here...</p>

<h2>Section 2</h2>
<p>More content...</p>

</body>
</html>
```

### Bước 2: Cập nhật config.json
Thêm vào config.json:

```json
{
  "parts": [
    {
      "title": "Part III: Your Part Name",
      "chapters": [
        {
          "id": "chapter6",
          "title": "Your Chapter Title",
          "file": "06_new_topic.html",
          "chapterNumber": 6
        }
      ]
    }
  ]
}
```

### Bước 3: Generate chapter page
```bash
node generate-pages.js
```

### Bước 4: Refresh browser
Mở lại index.html và bạn sẽ thấy chương mới!

## 🎨 Customize

### Thay đổi màu sắc chủ đề
Sửa trong styles.css:

```css
/* Gradient header */
.header {
  background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
}

/* Màu chính */
h1 { color: #667eea; }
h2 { color: #764ba2; }
```

### Thay đổi tiêu đề sách
Sửa trong config.json:

```json
{
  "bookTitle": "Tên sách mới",
  "bookSubtitle": "Phụ đề mới"
}
```

## 🔧 Troubleshooting

### ❌ Menu chuột phải không hiện
- Kiểm tra xem có mở bằng HTTP server không (không phải file://)
- Check Console (F12) xem có lỗi JavaScript không
- Đảm bảo navigation.js và config.json cùng thư mục

### ❌ Sidebar trống
- Kiểm tra config.json có đúng format không
- Check Console để xem lỗi load config
- Đảm bảo mở bằng HTTP server

### ❌ Chapter không load
- Chạy lại `node generate-pages.js`
- Kiểm tra tên file trong config.json
- Xem Console để debug

### ❌ Styling bị lỗi
- Đảm bảo styles.css cùng thư mục với HTML files
- Clear browser cache (Ctrl + F5)

## 🎯 Demo Features

### 1. Right-Click Menu
- Nhấn chuột phải ở bất kỳ đâu trên trang
- Menu sẽ hiện với danh sách tất cả chapters
- Click vào chapter để chuyển ngay

### 2. Keyboard Navigation
- `Ctrl + →` để đến chapter tiếp theo
- `Ctrl + ←` để quay lại chapter trước
- `Ctrl + H` để về trang chủ

### 3. Responsive Mobile
- Resize browser xuống mobile size
- Sidebar tự động ẩn
- Nút hamburger menu xuất hiện
- Click để mở/đóng sidebar

## 📊 So sánh với hệ thống cũ

| Feature | Hệ thống cũ | Hệ thống mới |
|---------|-------------|--------------|
| Navigation | Scroll dài | Multi-page, nhanh |
| Load time | Chậm (load all) | Nhanh (1 page) |
| Right-click menu | ❌ | ✅ |
| Keyboard shortcuts | ❌ | ✅ |
| Mobile friendly | Limited | Full support |
| Thêm chapter | Sửa HTML chính | Chỉ cần run script |

## 🎓 Best Practices

1. **Luôn dùng HTTP server** - Không mở trực tiếp file HTML
2. **Chỉ sửa config.json** - Không sửa index.html hay chapter*.html
3. **Run generate-pages.js** - Sau khi thêm/sửa source files
4. **Commit cả source và generated files** - Để người khác có thể dùng ngay

## 📝 Notes

- File `chapter*.html` được tự động generate - không nên sửa trực tiếp
- Chỉ sửa file source (`01_*.html`, `02_*.html`, etc.)
- Sau khi sửa source, chạy lại `node generate-pages.js`
- config.json là file quan trọng nhất - control mọi thứ

## 🆘 Support

Nếu gặp vấn đề:
1. Mở Developer Console (F12) để xem lỗi
2. Kiểm tra xem có dùng HTTP server không
3. Verify tất cả files cùng thư mục
4. Thử clear cache và refresh (Ctrl + F5)

---

**Enjoy your ML Mathematics journey! 📚✨**

Right-click anywhere to start navigating!
