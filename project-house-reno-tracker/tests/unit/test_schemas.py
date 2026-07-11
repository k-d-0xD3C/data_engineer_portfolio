import uuid
from decimal import Decimal

import pytest

from src.models.schemas import (
    SpaceCreate, SpaceUpdate, RenovationCreate, MaterialCreate,
    PhotoUpdate, PHOTO_PHASES, ALLOWED_CONTENT_TYPES, SPACE_TYPES,
)


def test_space_create_defaults():
    s = SpaceCreate(name="Kitchen")
    assert s.space_type == "other"
    assert s.description is None


def test_space_update_all_optional():
    s = SpaceUpdate()
    assert s.model_dump(exclude_none=True) == {}


def test_renovation_create_status_default():
    r = RenovationCreate(space_id=uuid.uuid4(), title="New Floors")
    assert r.status == "planning"


def test_renovation_create_with_costs():
    r = RenovationCreate(
        space_id=uuid.uuid4(),
        title="Paint",
        estimated_cost=Decimal("1500.00"),
        actual_cost=Decimal("1420.50"),
    )
    assert r.estimated_cost == Decimal("1500.00")


def test_material_create_required_fields():
    m = MaterialCreate(name="Oak Hardwood", category="flooring")
    assert m.brand is None
    assert m.unit is None


def test_photo_phases_are_valid():
    assert set(PHOTO_PHASES) == {"before", "during", "after"}


def test_allowed_content_types():
    assert "image/jpeg" in ALLOWED_CONTENT_TYPES
    assert "image/png" in ALLOWED_CONTENT_TYPES
    assert "image/webp" in ALLOWED_CONTENT_TYPES
    assert "image/gif" not in ALLOWED_CONTENT_TYPES


def test_photo_update_partial():
    p = PhotoUpdate(caption="Before demo")
    assert p.sort_order is None
    assert p.caption == "Before demo"
