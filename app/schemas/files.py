from pydantic import BaseModel
from typing import Any, List, Optional


class UploadResponse(BaseModel):
	file_id: str


class FileStatusResponse(BaseModel):
	file_id: str
	status: str
	total_chunks: int
	processed_chunks: int
	failed_chunks: int
	progress_percent: float


class ResultRecord(BaseModel):
	id: str
	chunk_index: int
	data: Any


class ResultsResponse(BaseModel):
	file_id: str
	items: List[ResultRecord]
	total: int
	limit: int
	offset: int 