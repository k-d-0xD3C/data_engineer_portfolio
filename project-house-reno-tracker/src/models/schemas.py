import uuid
from datetime import datetime, date
from decimal import Decimal

from pydantic import BaseModel, ConfigDict

# ---------------------------------------------------------------------------
# Space schemas
# ---------------------------------------------------------------------------

SPACE_TYPES = ["bathroom", "kitchen", "bedroom", "living", "exterior", "other"]


class SpaceCreate(BaseModel):
    name: str
    space_type: str = "other"
    description: str | None = None
    address: str | None = None


class SpaceUpdate(BaseModel):
    name: str | None = None
    space_type: str | None = None
    description: str | None = None
    address: str | None = None


class SpaceOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    space_type: str
    description: str | None
    address: str | None
    created_at: datetime
    updated_at: datetime
    renovation_count: int = 0


# ---------------------------------------------------------------------------
# Material catalog schemas
# ---------------------------------------------------------------------------

MATERIAL_CATEGORIES = [
    "flooring", "paint", "tile", "cabinetry", "countertop",
    "fixtures", "lighting", "plumbing", "electrical", "trim", "other",
]


class MaterialCreate(BaseModel):
    name: str
    category: str
    brand: str | None = None
    sku: str | None = None
    color: str | None = None
    unit: str | None = None
    notes: str | None = None


class MaterialUpdate(BaseModel):
    name: str | None = None
    category: str | None = None
    brand: str | None = None
    sku: str | None = None
    color: str | None = None
    unit: str | None = None
    notes: str | None = None


class MaterialOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    category: str
    brand: str | None
    sku: str | None
    color: str | None
    unit: str | None
    notes: str | None
    created_at: datetime


# ---------------------------------------------------------------------------
# Renovation material usage schemas
# ---------------------------------------------------------------------------

class MaterialUsageCreate(BaseModel):
    material_id: uuid.UUID
    quantity: Decimal | None = None
    unit_cost: Decimal | None = None
    notes: str | None = None


class MaterialUsageUpdate(BaseModel):
    quantity: Decimal | None = None
    unit_cost: Decimal | None = None
    notes: str | None = None


class MaterialUsageOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    material_id: uuid.UUID
    quantity: Decimal | None
    unit_cost: Decimal | None
    notes: str | None
    material: MaterialOut


# ---------------------------------------------------------------------------
# Photo schemas
# ---------------------------------------------------------------------------

PHOTO_PHASES = ["before", "during", "after"]
ALLOWED_CONTENT_TYPES = {"image/jpeg", "image/png", "image/webp"}


class PhotoOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    renovation_id: uuid.UUID
    phase: str
    url: str
    original_name: str | None
    content_type: str | None
    file_size_bytes: int | None
    caption: str | None
    sort_order: int
    uploaded_at: datetime


class PhotoUpdate(BaseModel):
    caption: str | None = None
    sort_order: int | None = None


# ---------------------------------------------------------------------------
# Renovation schemas
# ---------------------------------------------------------------------------

RENOVATION_STATUSES = ["planning", "in_progress", "complete"]


class RenovationCreate(BaseModel):
    space_id: uuid.UUID
    title: str
    description: str | None = None
    status: str = "planning"
    start_date: date | None = None
    end_date: date | None = None
    estimated_cost: Decimal | None = None
    actual_cost: Decimal | None = None
    contractor: str | None = None
    notes: str | None = None


class RenovationUpdate(BaseModel):
    title: str | None = None
    description: str | None = None
    status: str | None = None
    start_date: date | None = None
    end_date: date | None = None
    estimated_cost: Decimal | None = None
    actual_cost: Decimal | None = None
    contractor: str | None = None
    notes: str | None = None


class RenovationOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    space_id: uuid.UUID
    title: str
    description: str | None
    status: str
    start_date: date | None
    end_date: date | None
    estimated_cost: Decimal | None
    actual_cost: Decimal | None
    contractor: str | None
    notes: str | None
    created_at: datetime
    updated_at: datetime


class RenovationDetail(RenovationOut):
    materials: list[MaterialUsageOut] = []
    photos: dict[str, list[PhotoOut]] = {"before": [], "during": [], "after": []}
