import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.database import get_db
from src.models.orm import MaterialCatalog
from src.models.schemas import MaterialCreate, MaterialOut, MaterialUpdate

router = APIRouter(prefix="/materials", tags=["materials"])

DB = Annotated[AsyncSession, Depends(get_db)]


@router.get("", response_model=list[MaterialOut])
async def list_materials(
    db: DB,
    category: str | None = Query(default=None),
) -> list[MaterialOut]:
    q = select(MaterialCatalog).order_by(MaterialCatalog.category, MaterialCatalog.name)
    if category:
        q = q.where(MaterialCatalog.category == category)
    result = await db.execute(q)
    return [MaterialOut.model_validate(m) for m in result.scalars()]


@router.post("", response_model=MaterialOut, status_code=status.HTTP_201_CREATED)
async def create_material(body: MaterialCreate, db: DB) -> MaterialOut:
    material = MaterialCatalog(**body.model_dump())
    db.add(material)
    await db.commit()
    await db.refresh(material)
    return MaterialOut.model_validate(material)


@router.get("/{material_id}", response_model=MaterialOut)
async def get_material(material_id: uuid.UUID, db: DB) -> MaterialOut:
    material = await db.get(MaterialCatalog, material_id)
    if not material:
        raise HTTPException(status_code=404, detail="Material not found")
    return MaterialOut.model_validate(material)


@router.put("/{material_id}", response_model=MaterialOut)
async def update_material(material_id: uuid.UUID, body: MaterialUpdate, db: DB) -> MaterialOut:
    material = await db.get(MaterialCatalog, material_id)
    if not material:
        raise HTTPException(status_code=404, detail="Material not found")
    for field, value in body.model_dump(exclude_none=True).items():
        setattr(material, field, value)
    await db.commit()
    await db.refresh(material)
    return MaterialOut.model_validate(material)


@router.delete("/{material_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_material(material_id: uuid.UUID, db: DB) -> None:
    material = await db.get(MaterialCatalog, material_id)
    if not material:
        raise HTTPException(status_code=404, detail="Material not found")
    await db.delete(material)
    await db.commit()
