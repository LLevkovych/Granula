from fastapi import APIRouter, Depends, File as UploadFileParam, HTTPException, UploadFile, status
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func

from app.db.session import get_session
from app.db.models import File, ProcessedRecord
from app.schemas.files import UploadResponse, FileStatusResponse, ResultsResponse, ResultRecord
from app.services.storage import save_upload
from app.services.processing import processing_manager


router = APIRouter()


@router.get("/health")
async def health() -> dict:
	return {"status": "ok"}


@router.post("/upload", response_model=UploadResponse, status_code=status.HTTP_201_CREATED)
async def upload(file: UploadFile = UploadFileParam(...), session: AsyncSession = Depends(get_session)) -> UploadResponse:
	file_id, path, original_name = await save_upload(file)
	entity = File(id=file_id, filename=original_name, path=path, status="queued")
	session.add(entity)
	await session.commit()

	await processing_manager.start()
	await processing_manager.enqueue_file(session, entity)

	return UploadResponse(file_id=file_id)


@router.get("/status/{file_id}", response_model=FileStatusResponse)
async def status_endpoint(file_id: str, session: AsyncSession = Depends(get_session)) -> FileStatusResponse:
	file = await session.get(File, file_id)
	if not file:
		raise HTTPException(status_code=404, detail="File not found")
	progress = 0.0
	if file.total_chunks:
		progress = (file.processed_chunks + file.failed_chunks) / file.total_chunks * 100.0
	return FileStatusResponse(
		file_id=file.id,
		status=file.status,
		total_chunks=file.total_chunks,
		processed_chunks=file.processed_chunks,
		failed_chunks=file.failed_chunks,
		progress_percent=round(progress, 2),
	)


@router.get("/results/{file_id}", response_model=ResultsResponse)
async def results_endpoint(file_id: str, limit: int = 100, offset: int = 0, session: AsyncSession = Depends(get_session)) -> ResultsResponse:
	file = await session.get(File, file_id)
	if not file:
		raise HTTPException(status_code=404, detail="File not found")

	count = await session.scalar(select(func.count(ProcessedRecord.id)).where(ProcessedRecord.file_id == file_id))
	res = await session.execute(
		select(ProcessedRecord).where(ProcessedRecord.file_id == file_id).order_by(ProcessedRecord.created_at).limit(limit).offset(offset)
	)
	items = [ResultRecord(id=r.id, chunk_index=r.chunk_index, data=r.data) for r in res.scalars().all()]
	return ResultsResponse(file_id=file_id, items=items, total=count or 0, limit=limit, offset=offset) 