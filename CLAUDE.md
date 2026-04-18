# Project Rules — Etsy Listing Manager (nguyenphamdieuhien.online)

## Tổng quan

Tool quản lý Etsy listings tích hợp AI/ML để optimize title, tags, description.

**Stack:** Vanilla HTML/CSS/JS · FastAPI · PostgreSQL (Neon) · scikit-learn / HuggingFace / Claude API

---

## Cấu trúc thư mục — KHÔNG được thay đổi tùy tiện

```
nguyenphamdieuhien.online/
├── frontend/          ← Vanilla HTML/CSS/JS
├── backend/           ← FastAPI (Python)
├── model/             ← ML models (sklearn, HuggingFace, PyTorch)
├── data/
│   ├── raw/           ← File gốc, KHÔNG sửa
│   └── processed/     ← Output sau transform
├── docs/              ← Design system, specs
├── .env.example       ← Template env, KHÔNG chứa secret thật
└── .gitignore
```

---

## Rules theo từng layer

### FRONTEND (`frontend/`)

- Chỉ dùng **Vanilla HTML / CSS / JS** — không dùng React, Vue, hay bất kỳ JS framework nào
- `frontend/index.html` là file chính — **KHÔNG tạo lại, KHÔNG xóa**
- CSS dùng design tokens trong `:root` (xem `docs/DESIGN.md`)
- Màu sắc: dùng đúng tên biến CSS (`--terracotta`, `--parchment`...) — không hardcode hex
- Gọi API backend qua `fetch('/api/v1/...')` — không gọi thẳng Neon hay external service
- File JS đặt tại `frontend/js/`, CSS tại `frontend/css/`

### BACKEND (`backend/`)

- Framework: **FastAPI** — không dùng Flask, Django
- Mọi route đặt trong `backend/app/api/routes/` — prefix `/api/v1`
- Business logic đặt trong `backend/app/services/` — route chỉ validate + delegate
- DB models (SQLAlchemy ORM) → `backend/app/models/`
- Pydantic schemas (request/response) → `backend/app/schemas/`
- Config tập trung tại `backend/app/core/config.py` — dùng `get_settings()` (lru_cache)
- DB connection tại `backend/app/core/database.py` — dùng `get_db()` dependency
- **DATABASE_URL** đọc từ `.env` — không hardcode connection string trong code
- Claude API calls tập trung tại `backend/app/services/claude_service.py`

### MODEL (`model/`)

- Chỉ chứa **ML logic thuần** — không có FastAPI route, không kết nối DB trực tiếp
- Optimizer (TF-IDF, sklearn) → `model/src/optimizer/`
- Embedding / similarity search → `model/src/embeddings/`
- Model gọi từ backend service, không gọi trực tiếp từ route
- Checkpoint / weight file (`.pt`, `.pkl`, `.onnx`) → `model/checkpoints/` (gitignored)

### DATA (`data/`)

- `data/raw/` — file **gốc, read-only**, không sửa, không xóa
- `data/processed/` — output sau ETL/transform, có thể tái tạo → gitignored
- File nguồn chính: `data/raw/production_file_listing.csv`

### DOCS (`docs/`)

- `docs/DESIGN.md` — Design system (màu, font, component). Đọc trước khi làm FE

---

## Env vars

| Biến | Dùng cho | Nơi đọc |
|---|---|---|
| `DATABASE_URL` | Neon PostgreSQL connection string | `core/config.py` |
| `ANTHROPIC_API_KEY` | Claude API | `services/claude_service.py` |
| `CLAUDE_MODEL` | Model ID (default: `claude-sonnet-4-6`) | `core/config.py` |
| `APP_ENV` | `development` / `production` | `core/config.py` |
| `SECRET_KEY` | App secret | `core/config.py` |
| `ALLOWED_ORIGINS` | CORS (comma-separated) | `core/config.py` |

**Rule:** Secret thật chỉ trong `.env` (gitignored). Chỉ commit `.env.example`.

---

## Quy tắc chung cho mọi agent

1. **Đọc file trước khi sửa** — không sửa blind
2. **Không tạo file mới nếu đã có file phù hợp** để mở rộng
3. **Không đặt file sai layer** — logic ML không vào BE, route không vào service
4. **Không dùng `git add -A`** — stage từng file cụ thể
5. **Không commit `.env`** — chỉ commit `.env.example`
6. **Không xóa `data/raw/`** — đây là nguồn dữ liệu gốc
7. Khi thêm dependency: cập nhật đúng `requirements.txt` của layer tương ứng (`backend/` hoặc `model/`)
8. Mọi thay đổi DB schema: thêm migration, không `drop` table trực tiếp

---

## Cách chạy

```bash
# Backend
cd backend
pip install -r requirements.txt
uvicorn app.main:app --reload --port 8000

# Model (standalone)
cd model
pip install -r requirements.txt
```
