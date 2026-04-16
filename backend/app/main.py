from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .core.config import get_settings
from .core.database import create_tables, AsyncSessionLocal
from .api.routes import listings, optimize, market, performance
from .models import scenario  # noqa: F401 — registers scenarios_rules with Base
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
app.include_router(optimize.router, prefix="/api/v1")
app.include_router(market.router, prefix="/api/v1")
app.include_router(performance.router, prefix="/api/v1")


@app.get("/health")
async def health():
    return {"status": "ok", "env": settings.APP_ENV}
