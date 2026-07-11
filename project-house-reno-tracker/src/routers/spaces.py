import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from src.database import get_db
from src.models.orm import Space, Renovation
from src.models.schemas import SpaceCreate, SpaceOut, SpaceUpdate, RenovationOut

router = APIRouter(prefix="/spaces", tags=["spaces"])

DB = Annotated[AsyncSession, Depends(get_db)]


@router.get("", response_model=list[SpaceOut])
async def list_spaces(db: DB) -> list[SpaceOut]:
    result = await db.execute(
        select(Space, func.count(Renovation.id).label("renovation_count"))
        .outerjoin(Renovation, Renovation.space_id == Space.id)
        .group_by(Space.id)
        .order_by(Space.created_at.desc())
    )
    rows = result.all()
    out = []
    for space, count in rows:
        item = SpaceOut.model_validate(space)
        item = item.model_copy(update={"renovation_count": count})
        out.append(item)
    return out


@router.post("", response_model=SpaceOut, status_code=status.HTTP_201_CREATED)
async def create_space(body: SpaceCreate, db: DB) -> SpaceOut:
    space = Space(**body.model_dump())
    db.add(space)
    await db.commit()
    await db.refresh(space)
    return SpaceOut.model_validate(space)


@router.get("/{space_id}", response_model=SpaceOut)
async def get_space(space_id: uuid.UUID, db: DB) -> SpaceOut:
    result = await db.execute(
        select(Space, func.count(Renovation.id).label("renovation_count"))
        .outerjoin(Renovation, Renovation.space_id == Space.id)
        .where(Space.id == space_id)
        .group_by(Space.id)
    )
    row = result.first()
    if not row:
        raise HTTPException(status_code=404, detail="Space not found")
    space, count = row
    item = SpaceOut.model_validate(space)
    return item.model_copy(update={"renovation_count": count})


@router.put("/{space_id}", response_model=SpaceOut)
async def update_space(space_id: uuid.UUID, body: SpaceUpdate, db: DB) -> SpaceOut:
    space = await db.get(Space, space_id)
    if not space:
        raise HTTPException(status_code=404, detail="Space not found")
    for field, value in body.model_dump(exclude_none=True).items():
        setattr(space, field, value)
    await db.commit()
    await db.refresh(space)
    return SpaceOut.model_validate(space)


@router.delete("/{space_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_space(space_id: uuid.UUID, db: DB) -> None:
    space = await db.get(Space, space_id)
    if not space:
        raise HTTPException(status_code=404, detail="Space not found")
    await db.delete(space)
    await db.commit()


@router.get("/{space_id}/renovations", response_model=list[RenovationOut])
async def list_space_renovations(space_id: uuid.UUID, db: DB) -> list[RenovationOut]:
    space = await db.get(Space, space_id)
    if not space:
        raise HTTPException(status_code=404, detail="Space not found")
    result = await db.execute(
        select(Renovation)
        .where(Renovation.space_id == space_id)
        .order_by(Renovation.created_at.desc())
    )
    return [RenovationOut.model_validate(r) for r in result.scalars()]
