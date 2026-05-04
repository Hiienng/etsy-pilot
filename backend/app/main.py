from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from .core.config import get_settings
from .core.database import create_tables, AsyncSessionLocal
from .api.routes import listings, market, performance, internal, references
from .models import scenario  # noqa: F401 — registers scenarios_rules with Base
from .models import import_batch, listing_report, keyword_report  # noqa: F401 — register with Base
from .services import performance_service

settings = get_settings()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await create_tables()
    async with AsyncSessionLocal() as session:
        await performance_service.seed_scenarios(session)
    yield


app = FastAPI(
    title="Etsy Listing Manager API",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(listings.router, prefix="/api/v1")
app.include_router(market.router, prefix="/api/v1")
app.include_router(performance.router, prefix="/api/v1")
app.include_router(internal.router, prefix="/api/v1")
app.include_router(references.router, prefix="/api/v1")


@app.get("/health")
async def health():
    return {"status": "ok", "env": settings.APP_ENV}



# Serve processed data files (performance_dashboard.json, etc.)
_data_processed_dir = Path(__file__).resolve().parents[2] / "data" / "processed"
if _data_processed_dir.exists():
    app.mount("/data/processed", StaticFiles(directory=str(_data_processed_dir)), name="data_processed")

# Serve frontend static files (index.html, css/, js/) — must be AFTER API routes
_frontend_dir = Path(__file__).resolve().parents[2] / "frontend"
if (_frontend_dir / "index.html").exists():
    app.mount("/", StaticFiles(directory=str(_frontend_dir), html=True), name="frontend")
