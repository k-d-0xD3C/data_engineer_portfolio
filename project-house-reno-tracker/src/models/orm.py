import uuid
from datetime import datetime, date
from decimal import Decimal

from sqlalchemy import (
    UUID, String, Text, Integer, Numeric, BigInteger,
    Date, TIMESTAMP, ForeignKey, UniqueConstraint, Index, func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class Space(Base):
    __tablename__ = "spaces"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    space_type: Mapped[str] = mapped_column(String(50), nullable=False, default="other")
    description: Mapped[str | None] = mapped_column(Text)
    address: Mapped[str | None] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())

    renovations: Mapped[list["Renovation"]] = relationship("Renovation", back_populates="space", cascade="all, delete-orphan")


class Renovation(Base):
    __tablename__ = "renovations"
    __table_args__ = (
        Index("idx_renovations_space_id", "space_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    space_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("spaces.id", ondelete="CASCADE"), nullable=False)
    title: Mapped[str] = mapped_column(String(200), nullable=False)
    description: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default="planning")
    start_date: Mapped[date | None] = mapped_column(Date)
    end_date: Mapped[date | None] = mapped_column(Date)
    estimated_cost: Mapped[Decimal | None] = mapped_column(Numeric(12, 2))
    actual_cost: Mapped[Decimal | None] = mapped_column(Numeric(12, 2))
    contractor: Mapped[str | None] = mapped_column(String(150))
    notes: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), server_default=func.now(), onupdate=func.now())

    space: Mapped["Space"] = relationship("Space", back_populates="renovations")
    photos: Mapped[list["Photo"]] = relationship("Photo", back_populates="renovation", cascade="all, delete-orphan")
    material_usages: Mapped[list["RenovationMaterial"]] = relationship("RenovationMaterial", back_populates="renovation", cascade="all, delete-orphan")


class MaterialCatalog(Base):
    __tablename__ = "material_catalog"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(150), nullable=False)
    category: Mapped[str] = mapped_column(String(50), nullable=False)
    brand: Mapped[str | None] = mapped_column(String(100))
    sku: Mapped[str | None] = mapped_column(String(100))
    color: Mapped[str | None] = mapped_column(String(80))
    unit: Mapped[str | None] = mapped_column(String(30))
    notes: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), server_default=func.now())

    usages: Mapped[list["RenovationMaterial"]] = relationship("RenovationMaterial", back_populates="material", cascade="all, delete-orphan")


class RenovationMaterial(Base):
    __tablename__ = "renovation_materials"
    __table_args__ = (
        UniqueConstraint("renovation_id", "material_id", name="uq_reno_material"),
        Index("idx_reno_materials_renovation_id", "renovation_id"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    renovation_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("renovations.id", ondelete="CASCADE"), nullable=False)
    material_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("material_catalog.id"), nullable=False)
    quantity: Mapped[Decimal | None] = mapped_column(Numeric(10, 2))
    unit_cost: Mapped[Decimal | None] = mapped_column(Numeric(10, 2))
    notes: Mapped[str | None] = mapped_column(Text)

    renovation: Mapped["Renovation"] = relationship("Renovation", back_populates="material_usages")
    material: Mapped["MaterialCatalog"] = relationship("MaterialCatalog", back_populates="usages")


class Photo(Base):
    __tablename__ = "photos"
    __table_args__ = (
        Index("idx_photos_renovation_id", "renovation_id"),
        Index("idx_photos_phase", "phase"),
    )

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    renovation_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), ForeignKey("renovations.id", ondelete="CASCADE"), nullable=False)
    phase: Mapped[str] = mapped_column(String(20), nullable=False)
    object_key: Mapped[str] = mapped_column(String(500), nullable=False)
    original_name: Mapped[str | None] = mapped_column(String(255))
    content_type: Mapped[str | None] = mapped_column(String(80))
    file_size_bytes: Mapped[int | None] = mapped_column(BigInteger)
    caption: Mapped[str | None] = mapped_column(Text)
    sort_order: Mapped[int] = mapped_column(Integer, default=0)
    uploaded_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), server_default=func.now())

    renovation: Mapped["Renovation"] = relationship("Renovation", back_populates="photos")
