from .files import UploadResponse, FileStatusResponse, ResultRecord, ResultsResponse
from pydantic import BaseModel
from typing import Any, List, Optional


class FileStatus(BaseModel):
	id: str
	filename: str
	status: str
	total_chunks: int
	processed_chunks: int
	failed_chunks: int
	error_message: Optional[str] = None


class ProcessedRecordResponse(BaseModel):
	id: str
	file_id: str
	chunk_index: int
	data: Any

	class Config:
		from_attributes = True


class PaginatedResults(BaseModel):
	results: List[ProcessedRecordResponse]
	total: int
	page: int
	size: int
	pages: int 