from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from prometheus_client import make_asgi_app

from src.config import load_config
from src.storage import StorageClient
from src.routers import spaces, materials, renovations, photos

_config = load_config()


@asynccontextmanager
async def lifespan(app: FastAPI):
    storage = StorageClient(_config.minio)
    storage.ensure_bucket()
    yield


app = FastAPI(
    title="House Reno Tracker API",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:80"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(spaces.router, prefix="/api/v1")
app.include_router(materials.router, prefix="/api/v1")
app.include_router(renovations.router, prefix="/api/v1")
app.include_router(photos.router, prefix="/api/v1")

metrics_app = make_asgi_app()
app.mount("/metrics", metrics_app)


@app.get("/healthz")
async def healthz() -> dict:
    return {"status": "ok"}
