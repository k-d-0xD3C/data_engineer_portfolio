import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from src.database import get_db
from src.models.orm import Renovation, RenovationMaterial, MaterialCatalog
from src.models.schemas import (
    RenovationCreate, RenovationDetail, RenovationOut, RenovationUpdate,
    MaterialUsageCreate, MaterialUsageOut, PhotoOut, PHOTO_PHASES,
)
from src.storage import StorageClient
from src.config import load_config

router = APIRouter(prefix="/renovations", tags=["renovations"])

DB = Annotated[AsyncSession, Depends(get_db)]
_storage = StorageClient(load_config().minio)


def _attach_photo_urls(photos: list) -> list[PhotoOut]:
    out = []
    for p in photos:
        url = _storage.presigned_get_url(p.object_key)
        out.append(PhotoOut(
            id=p.id,
            renovation_id=p.renovation_id,
            phase=p.phase,
            url=url,
            original_name=p.original_name,
            content_type=p.content_type,
            file_size_bytes=p.file_size_bytes,
            caption=p.caption,
            sort_order=p.sort_order,
            uploaded_at=p.uploaded_at,
        ))
    return out


@router.get("", response_model=list[RenovationOut])
async def list_renovations(
    db: DB,
    space_id: uuid.UUID | None = Query(default=None),
    renovation_status: str | None = Query(default=None, alias="status"),
) -> list[RenovationOut]:
    q = select(Renovation).order_by(Renovation.created_at.desc())
    if space_id:
        q = q.where(Renovation.space_id == space_id)
    if renovation_status:
        q = q.where(Renovation.status == renovation_status)
    result = await db.execute(q)
    return [RenovationOut.model_validate(r) for r in result.scalars()]


@router.post("", response_model=RenovationOut, status_code=status.HTTP_201_CREATED)
async def create_renovation(body: RenovationCreate, db: DB) -> RenovationOut:
    reno = Renovation(**body.model_dump())
    db.add(reno)
    await db.commit()
    await db.refresh(reno)
    return RenovationOut.model_validate(reno)


@router.get("/{reno_id}", response_model=RenovationDetail)
async def get_renovation(reno_id: uuid.UUID, db: DB) -> RenovationDetail:
    result = await db.execute(
        select(Renovation)
        .where(Renovation.id == reno_id)
        .options(
            selectinload(Renovation.material_usages).selectinload(RenovationMaterial.material),
            selectinload(Renovation.photos),
        )
    )
    reno = result.scalar_one_or_none()
    if not reno:
        raise HTTPException(status_code=404, detail="Renovation not found")

    photos_by_phase: dict[str, list[PhotoOut]] = {phase: [] for phase in PHOTO_PHASES}
    for photo_out in _attach_photo_urls(reno.photos):
        photos_by_phase[photo_out.phase].append(photo_out)
    for phase in photos_by_phase:
        photos_by_phase[phase].sort(key=lambda p: p.sort_order)

    detail = RenovationDetail.model_validate(reno)
    detail = detail.model_copy(update={
        "materials": [MaterialUsageOut.model_validate(u) for u in reno.material_usages],
        "photos": photos_by_phase,
    })
    return detail


@router.put("/{reno_id}", response_model=RenovationOut)
async def update_renovation(reno_id: uuid.UUID, body: RenovationUpdate, db: DB) -> RenovationOut:
    reno = await db.get(Renovation, reno_id)
    if not reno:
        raise HTTPException(status_code=404, detail="Renovation not found")
    for field, value in body.model_dump(exclude_none=True).items():
        setattr(reno, field, value)
    await db.commit()
    await db.refresh(reno)
    return RenovationOut.model_validate(reno)


@router.delete("/{reno_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_renovation(reno_id: uuid.UUID, db: DB) -> None:
    reno = await db.get(Renovation, reno_id)
    if not reno:
        raise HTTPException(status_code=404, detail="Renovation not found")
    await db.delete(reno)
    await db.commit()


# ---------------------------------------------------------------------------
# Material ↔ Renovation sub-routes
# ---------------------------------------------------------------------------

@router.post("/{reno_id}/materials", response_model=MaterialUsageOut, status_code=status.HTTP_201_CREATED)
async def attach_material(reno_id: uuid.UUID, body: MaterialUsageCreate, db: DB) -> MaterialUsageOut:
    reno = await db.get(Renovation, reno_id)
    if not reno:
        raise HTTPException(status_code=404, detail="Renovation not found")
    material = await db.get(MaterialCatalog, body.material_id)
    if not material:
        raise HTTPException(status_code=404, detail="Material not found")

    usage = RenovationMaterial(
        renovation_id=reno_id,
        material_id=body.material_id,
        quantity=body.quantity,
        unit_cost=body.unit_cost,
        notes=body.notes,
    )
    db.add(usage)
    await db.commit()

    result = await db.execute(
        select(RenovationMaterial)
        .where(RenovationMaterial.id == usage.id)
        .options(selectinload(RenovationMaterial.material))
    )
    return MaterialUsageOut.model_validate(result.scalar_one())


@router.delete("/{reno_id}/materials/{material_id}", status_code=status.HTTP_204_NO_CONTENT)
async def detach_material(reno_id: uuid.UUID, material_id: uuid.UUID, db: DB) -> None:
    result = await db.execute(
        select(RenovationMaterial).where(
            RenovationMaterial.renovation_id == reno_id,
            RenovationMaterial.material_id == material_id,
        )
    )
    usage = result.scalar_one_or_none()
    if not usage:
        raise HTTPException(status_code=404, detail="Material usage not found")
    await db.delete(usage)
    await db.commit()
