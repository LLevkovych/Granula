import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import JSON, DateTime, ForeignKey, Integer, String, Text
from sqlalchemy.dialects.sqlite import JSON as SQLITE_JSON
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.session import Base


class File(Base):
	__tablename__ = "files"

	id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
	filename: Mapped[str] = mapped_column(String(255), nullable=False)
	path: Mapped[str] = mapped_column(Text, nullable=False)
	status: Mapped[str] = mapped_column(String(32), nullable=False, default="queued")
	total_chunks: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
	processed_chunks: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
	failed_chunks: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
	error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
	created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
	updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)

	chunks: Mapped[list["Chunk"]] = relationship("Chunk", back_populates="file", cascade="all, delete-orphan", lazy="raise")
	records: Mapped[list["ProcessedRecord"]] = relationship("ProcessedRecord", back_populates="file", cascade="all, delete-orphan", lazy="raise")


class Chunk(Base):
	__tablename__ = "chunks"

	id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
	file_id: Mapped[str] = mapped_column(String(36), ForeignKey("files.id", ondelete="CASCADE"), index=True)
	index: Mapped[int] = mapped_column(Integer, nullable=False)
	status: Mapped[str] = mapped_column(String(32), nullable=False, default="queued")
	attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
	result_meta: Mapped[Optional[dict]] = mapped_column(SQLITE_JSON().with_variant(JSON, "postgresql"), nullable=True)
	error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
	created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)
	updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc), nullable=False)

	file: Mapped["File"] = relationship("File", back_populates="chunks", lazy="raise")


class ProcessedRecord(Base):
	__tablename__ = "processed_records"

	id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid.uuid4()))
	file_id: Mapped[str] = mapped_column(String(36), ForeignKey("files.id", ondelete="CASCADE"), index=True)
	chunk_index: Mapped[int] = mapped_column(Integer, nullable=False)
	data: Mapped[dict] = mapped_column(SQLITE_JSON().with_variant(JSON, "postgresql"), nullable=False)
	created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc), nullable=False)

	file: Mapped["File"] = relationship("File", back_populates="records", lazy="raise") 