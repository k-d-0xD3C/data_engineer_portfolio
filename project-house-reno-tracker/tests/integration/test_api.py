"""
Integration tests require a running Postgres and MinIO (docker-compose.base.yml).
Run with: pytest tests/integration/ -v
"""
import uuid

import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from src.main import app
from src.database import get_db
from src.models.orm import Base
from src.config import load_config

_cfg = load_config()
_test_engine = create_async_engine(_cfg.postgres.async_url, echo=False)
_TestSession = async_sessionmaker(_test_engine, expire_on_commit=False)


async def override_get_db():
    async with _TestSession() as session:
        yield session


app.dependency_overrides[get_db] = override_get_db


@pytest_asyncio.fixture(scope="session", autouse=True)
async def setup_schema():
    async with _test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield
    async with _test_engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
    await _test_engine.dispose()


@pytest_asyncio.fixture
async def client():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as c:
        yield c


@pytest.mark.asyncio
async def test_healthz(client):
    r = await client.get("/healthz")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_create_and_list_spaces(client):
    r = await client.post("/api/v1/spaces", json={"name": "Master Bath", "space_type": "bathroom"})
    assert r.status_code == 201
    data = r.json()
    assert data["name"] == "Master Bath"
    space_id = data["id"]

    r = await client.get("/api/v1/spaces")
    assert r.status_code == 200
    ids = [s["id"] for s in r.json()]
    assert space_id in ids


@pytest.mark.asyncio
async def test_update_space(client):
    r = await client.post("/api/v1/spaces", json={"name": "Kitchen", "space_type": "kitchen"})
    space_id = r.json()["id"]

    r = await client.put(f"/api/v1/spaces/{space_id}", json={"name": "New Kitchen"})
    assert r.status_code == 200
    assert r.json()["name"] == "New Kitchen"


@pytest.mark.asyncio
async def test_delete_space(client):
    r = await client.post("/api/v1/spaces", json={"name": "To Delete"})
    space_id = r.json()["id"]
    r = await client.delete(f"/api/v1/spaces/{space_id}")
    assert r.status_code == 204
    r = await client.get(f"/api/v1/spaces/{space_id}")
    assert r.status_code == 404


@pytest.mark.asyncio
async def test_create_renovation(client):
    r = await client.post("/api/v1/spaces", json={"name": "Living Room", "space_type": "living"})
    space_id = r.json()["id"]

    r = await client.post("/api/v1/renovations", json={
        "space_id": space_id,
        "title": "Hardwood Floors",
        "status": "in_progress",
    })
    assert r.status_code == 201
    assert r.json()["status"] == "in_progress"


@pytest.mark.asyncio
async def test_material_crud(client):
    r = await client.post("/api/v1/materials", json={
        "name": "Shaw Oak Plank",
        "category": "flooring",
        "brand": "Shaw",
        "color": "Natural",
        "unit": "sq_ft",
    })
    assert r.status_code == 201
    material_id = r.json()["id"]

    r = await client.get("/api/v1/materials")
    assert any(m["id"] == material_id for m in r.json())

    r = await client.put(f"/api/v1/materials/{material_id}", json={"color": "Honey"})
    assert r.json()["color"] == "Honey"

    r = await client.delete(f"/api/v1/materials/{material_id}")
    assert r.status_code == 204


@pytest.mark.asyncio
async def test_404_on_missing_space(client):
    r = await client.get(f"/api/v1/spaces/{uuid.uuid4()}")
    assert r.status_code == 404
