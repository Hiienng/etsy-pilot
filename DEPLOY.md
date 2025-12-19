# Hướng Dẫn Deploy lên nguyenphamdieuhien.online

## Bước 1: Đẩy Code lên GitHub

### 1.1. Tạo Repository trên GitHub
1. Truy cập https://github.com/new
2. Tên repository: `ml-mathematics-book` (hoặc tên bạn muốn)
3. Chọn **Public**
4. KHÔNG chọn "Add a README file" (vì đã có sẵn)
5. Click **Create repository**

### 1.2. Push Code lên GitHub
Chạy các lệnh sau trong terminal:

```bash
# Thêm remote repository
git remote add origin https://github.com/YOUR_USERNAME/ml-mathematics-book.git

# Đẩy code lên GitHub
git branch -M main
git push -u origin main
```

Thay `YOUR_USERNAME` bằng username GitHub của bạn.

---

## Bước 2: Deploy lên nguyenphamdieuhien.online

### Phương án 1: Deploy với GitHub Pages (MIỄN PHÍ - KHUYẾN NGHỊ)

#### 2.1.1. Cấu hình GitHub Pages
1. Vào repository trên GitHub
2. Click tab **Settings**
3. Click **Pages** ở sidebar bên trái
4. Trong phần **Source**, chọn:
   - Branch: `main`
   - Folder: `/ (root)`
5. Click **Save**
6. Đợi 2-3 phút, site sẽ được deploy tại: `https://YOUR_USERNAME.github.io/ml-mathematics-book/`

#### 2.1.2. Trỏ Domain nguyenphamdieuhien.online
1. Trong phần GitHub Pages Settings, thêm **Custom domain**: `nguyenphamdieuhien.online`
2. Click **Save**
3. Vào nhà cung cấp domain của bạn (GoDaddy, Namecheap, etc.)
4. Thêm DNS records:

**Nếu dùng nguyenphamdieuhien.online (apex domain):**
```
Type: A
Name: @
Value: 185.199.108.153

Type: A
Name: @
Value: 185.199.109.153

Type: A
Name: @
Value: 185.199.110.153

Type: A
Name: @
Value: 185.199.111.153
```

**Nếu dùng www.nguyenphamdieuhien.online:**
```
Type: CNAME
Name: www
Value: YOUR_USERNAME.github.io
```

5. Đợi 15-30 phút để DNS propagate
6. Enable **Enforce HTTPS** trong GitHub Pages settings

---

### Phương án 2: Deploy với Netlify (MIỄN PHÍ + DỄ HƠN)

#### 2.2.1. Deploy trên Netlify
1. Truy cập https://app.netlify.com/
2. Đăng nhập bằng GitHub
3. Click **Add new site** → **Import an existing project**
4. Chọn **GitHub** → Chọn repository `ml-mathematics-book`
5. Build settings:
   - Build command: (để trống)
   - Publish directory: `/`
6. Click **Deploy site**

#### 2.2.2. Trỏ Domain
1. Sau khi deploy xong, vào **Site settings** → **Domain management**
2. Click **Add custom domain**
3. Nhập: `nguyenphamdieuhien.online`
4. Netlify sẽ cung cấp DNS records, thêm vào nhà cung cấp domain:

```
Type: A
Name: @
Value: 75.2.60.5

Type: CNAME
Name: www
Value: YOUR-SITE-NAME.netlify.app
```

5. Enable **HTTPS** (tự động với Let's Encrypt)

---

### Phương án 3: Deploy với Vercel (MIỄN PHÍ)

#### 2.3.1. Deploy trên Vercel
1. Truy cập https://vercel.com/
2. Đăng nhập bằng GitHub
3. Click **Add New** → **Project**
4. Import repository `ml-mathematics-book`
5. Click **Deploy**

#### 2.3.2. Trỏ Domain
1. Vào **Settings** → **Domains**
2. Thêm domain: `nguyenphamdieuhien.online`
3. Thêm DNS records vào nhà cung cấp domain:

```
Type: A
Name: @
Value: 76.76.21.21

Type: CNAME
Name: www
Value: cname.vercel-dns.com
```

---

## Bước 3: Cập Nhật Nội Dung

Mỗi khi bạn cập nhật content:

```bash
# 1. Add changes
git add .

# 2. Commit với message mô tả
git commit -m "Update Chapter 1: Add more examples"

# 3. Push lên GitHub
git push

# 4. Site sẽ tự động rebuild (với GitHub Pages/Netlify/Vercel)
```

---

## So Sánh Các Phương Án

| Tính năng | GitHub Pages | Netlify | Vercel |
|-----------|-------------|---------|--------|
| **Miễn phí** | ✅ | ✅ | ✅ |
| **HTTPS** | ✅ | ✅ | ✅ |
| **Custom Domain** | ✅ | ✅ | ✅ |
| **Auto Deploy** | ✅ | ✅ | ✅ |
| **Tốc độ** | Tốt | Rất tốt | Rất tốt |
| **CDN Global** | ✅ | ✅ | ✅ |
| **Dễ setup** | Trung bình | Dễ nhất | Dễ |

**Khuyến nghị:** Dùng **Netlify** nếu muốn setup nhanh và dễ nhất.

---

## Kiểm Tra

Sau khi deploy xong, truy cập:
- https://nguyenphamdieuhien.online
- Kiểm tra:
  - ✅ Trang chủ hiển thị đúng
  - ✅ Navigation hoạt động
  - ✅ MathJax render công thức đúng
  - ✅ Responsive trên mobile
  - ✅ HTTPS hoạt động

---

## Troubleshooting

### Lỗi: 404 Not Found
- **GitHub Pages**: Đợi 5-10 phút sau khi push code
- **Netlify/Vercel**: Check build logs xem có lỗi không

### Lỗi: CSS/JS không load
- Kiểm tra đường dẫn file trong HTML (phải dùng relative paths)
- Clear browser cache (Ctrl+Shift+R)

### Domain không hoạt động
- Đợi 24-48 giờ để DNS propagate
- Kiểm tra DNS records: https://dnschecker.org/

---

## Support

Nếu cần hỗ trợ:
- GitHub Issues: https://github.com/YOUR_USERNAME/ml-mathematics-book/issues
- Netlify Support: https://answers.netlify.com/
- Vercel Support: https://vercel.com/support
