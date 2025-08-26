from fastapi import APIRouter, Depends, HTTPException, status, UploadFile, Query
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func
from typing import List
import asyncio
import os

from app.db.session import get_session
from app.db.models import File, Chunk, ProcessedRecord
from app.schemas import UploadResponse, FileStatus, ProcessedRecordResponse, PaginatedResults
from app.services.storage import save_upload
from app.services.processing import get_processing_manager
from app.core.config import settings

router = APIRouter()


@router.post("/upload", response_model=UploadResponse, status_code=status.HTTP_201_CREATED)
async def upload(
	file: UploadFile = UploadFile(...),
	priority: int = Query(0, ge=0, le=10, description="Processing priority (0-10, higher = more priority)"),
	session: AsyncSession = Depends(get_session)
) -> UploadResponse:
	import asyncio # Moved to top of function
	# Validate file type
	if not file.content_type or file.content_type != "text/csv":
		raise HTTPException(status_code=400, detail=f"Only CSV files are allowed, got: {file.content_type}")

	# Retry logic for database operations
	max_retries = 3
	for attempt in range(max_retries):
		try:
			file_id, path, original_name = await save_upload(file)
			# Validate saved file size against configured limit
			max_bytes = settings.MAX_UPLOAD_MB * 1024 * 1024
			try:
				actual_size = os.path.getsize(path)
			except Exception:
				actual_size = 0
			if actual_size > max_bytes:
				# Delete oversized file and reject
				try:
					os.remove(path)
				except Exception:
					pass
				raise HTTPException(status_code=400, detail="File size exceeds configured limit")

			entity = File(id=file_id, filename=original_name, path=path, status="queued")
			session.add(entity)
			await session.commit()
			break
		except HTTPException:
			raise
		except Exception as e:
			if attempt < max_retries - 1:
				await session.rollback()
				await asyncio.sleep(0.1 * (2 ** attempt))  # Exponential backoff
			else:
				raise HTTPException(status_code=400, detail=f"Upload failed: {e}")

	# Start processing in background with a fresh session
	async def _start_processing(fid: str, prio: int, engine=None) -> None:
		if engine is None:
			# Use the same session dependency that the route uses
			async for s in get_session():
				f = await s.get(File, fid)
				if f is None:
					return
				processing_manager = get_processing_manager()
				await processing_manager.start()
				await processing_manager.enqueue_file(s, f, priority=prio)
				break  # Only use the first session
		else:
			# Use the provided engine (for testing)
			from sqlalchemy.orm import sessionmaker
			from sqlalchemy.ext.asyncio import AsyncSession
			TestingSessionLocal = sessionmaker(
				engine, class_=AsyncSession, expire_on_commit=False
			)
			async with TestingSessionLocal() as s:
				f = await s.get(File, fid)
				if f is None:
					return
				processing_manager = get_processing_manager()
				await processing_manager.start()
				await processing_manager.enqueue_file(s, f, priority=prio)

	# Skip background if disabled via settings
	if not settings.DISABLE_BACKGROUND:
		asyncio.create_task(_start_processing(file_id, priority))
	return UploadResponse(file_id=file_id)


@router.get("/status/{file_id}", response_model=FileStatus)
async def get_file_status(
	file_id: str,
	session: AsyncSession = Depends(get_session)
) -> FileStatus:
	file = await session.get(File, file_id)
	if not file:
		raise HTTPException(status_code=404, detail="File not found")
	
	# Derive processed/failed from chunks live
	completed_q = select(func.count(Chunk.id)).where(Chunk.file_id == file_id, Chunk.status == "completed")
	failed_q = select(func.count(Chunk.id)).where(Chunk.file_id == file_id, Chunk.status == "failed")
	completed = (await session.execute(completed_q)).scalar() or 0
	failed = (await session.execute(failed_q)).scalar() or 0
	
	# Prefer live counts for progress; fall back to file fields for totals
	total = file.total_chunks or 0
	status_value = file.status
	if total > 0:
		if completed + failed >= total:
			status_value = "completed" if failed == 0 else ("failed" if completed == 0 else "completed_with_errors")
		elif completed > 0 and status_value == "processing":
			status_value = "processing"
	
	return FileStatus(
		id=file.id,
		filename=file.filename,
		status=status_value,
		total_chunks=total,
		processed_chunks=completed,
		failed_chunks=failed,
		error_message=file.error_message
	)


@router.get("/results/{file_id}", response_model=PaginatedResults)
async def get_file_results(
	file_id: str,
	page: int = Query(1, ge=1, description="Page number"),
	size: int = Query(10, ge=1, le=100, description="Page size"),
	session: AsyncSession = Depends(get_session)
) -> PaginatedResults:
	# Check if file exists
	file = await session.get(File, file_id)
	if not file:
		raise HTTPException(status_code=404, detail="File not found")
	
	# Get total count
	total_query = select(func.count(ProcessedRecord.id)).where(ProcessedRecord.file_id == file_id)
	total_result = await session.execute(total_query)
	total = total_result.scalar()
	
	if total == 0:
		return PaginatedResults(
			results=[],
			total=0,
			page=page,
			size=size,
			pages=0
		)
	
	# Calculate pagination
	offset = (page - 1) * size
	pages = (total + size - 1) // size
	
	# Get paginated results
	results_query = select(ProcessedRecord).where(
		ProcessedRecord.file_id == file_id
	).offset(offset).limit(size)
	
	results_result = await session.execute(results_query)
	records = results_result.scalars().all()
	
	return PaginatedResults(
		results=[ProcessedRecordResponse.from_orm(record) for record in records],
		total=total,
		page=page,
		size=size,
		pages=pages
	) 