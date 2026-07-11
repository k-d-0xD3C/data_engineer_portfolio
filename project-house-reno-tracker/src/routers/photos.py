import uuid
from typing import Annotated

from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.database import get_db
from src.models.orm import Photo, Renovation
from src.models.schemas import PhotoOut, PhotoUpdate, ALLOWED_CONTENT_TYPES, PHOTO_PHASES
from src.storage import StorageClient
from src.config import load_config

router = APIRouter(tags=["photos"])

DB = Annotated[AsyncSession, Depends(get_db)]
_storage = StorageClient(load_config().minio)


def _to_out(photo: Photo) -> PhotoOut:
    return PhotoOut(
        id=photo.id,
        renovation_id=photo.renovation_id,
        phase=photo.phase,
        url=_storage.presigned_get_url(photo.object_key),
        original_name=photo.original_name,
        content_type=photo.content_type,
        file_size_bytes=photo.file_size_bytes,
        caption=photo.caption,
        sort_order=photo.sort_order,
        uploaded_at=photo.uploaded_at,
    )


@router.post("/renovations/{reno_id}/photos", response_model=PhotoOut, status_code=status.HTTP_201_CREATED)
async def upload_photo(
    reno_id: uuid.UUID,
    db: DB,
    file: UploadFile = File(...),
    phase: str = Form(...),
    caption: str | None = Form(default=None),
) -> PhotoOut:
    reno = await db.get(Renovation, reno_id)
    if not reno:
        raise HTTPException(status_code=404, detail="Renovation not found")

    if phase not in PHOTO_PHASES:
        raise HTTPException(status_code=400, detail=f"phase must be one of: {PHOTO_PHASES}")

    content_type = file.content_type or ""
    if content_type not in ALLOWED_CONTENT_TYPES:
        raise HTTPException(status_code=400, detail=f"File type not allowed. Accepted: {ALLOWED_CONTENT_TYPES}")

    data = await file.read()
    safe_name = (file.filename or "upload").replace(" ", "_")
    object_key = f"reno/{reno_id}/{phase}/{uuid.uuid4().hex}_{safe_name}"

    _storage.upload_object(object_key, data, content_type)

    photo = Photo(
        renovation_id=reno_id,
        phase=phase,
        object_key=object_key,
        original_name=file.filename,
        content_type=content_type,
        file_size_bytes=len(data),
        caption=caption,
    )
    db.add(photo)
    await db.commit()
    await db.refresh(photo)
    return _to_out(photo)


@router.get("/renovations/{reno_id}/photos", response_model=list[PhotoOut])
async def list_photos(reno_id: uuid.UUID, db: DB) -> list[PhotoOut]:
    reno = await db.get(Renovation, reno_id)
    if not reno:
        raise HTTPException(status_code=404, detail="Renovation not found")
    result = await db.execute(
        select(Photo)
        .where(Photo.renovation_id == reno_id)
        .order_by(Photo.phase, Photo.sort_order, Photo.uploaded_at)
    )
    return [_to_out(p) for p in result.scalars()]


@router.delete("/photos/{photo_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_photo(photo_id: uuid.UUID, db: DB) -> None:
    photo = await db.get(Photo, photo_id)
    if not photo:
        raise HTTPException(status_code=404, detail="Photo not found")
    _storage.delete_object(photo.object_key)
    await db.delete(photo)
    await db.commit()


@router.patch("/photos/{photo_id}", response_model=PhotoOut)
async def update_photo(photo_id: uuid.UUID, body: PhotoUpdate, db: DB) -> PhotoOut:
    photo = await db.get(Photo, photo_id)
    if not photo:
        raise HTTPException(status_code=404, detail="Photo not found")
    for field, value in body.model_dump(exclude_none=True).items():
        setattr(photo, field, value)
    await db.commit()
    await db.refresh(photo)
    return _to_out(photo)
