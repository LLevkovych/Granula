import asyncio
import csv
import uuid
import math
import logging
import io
import os
from dataclasses import dataclass
from typing import Optional, List
from datetime import datetime, timezone

from sqlalchemy import select, func, insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.exc import OperationalError, DisconnectionError

from app.core.config import settings
from app.db.models import Chunk, File, ProcessedRecord

logger = logging.getLogger(__name__)


@dataclass
class ChunkTask:
	file_id: str
	chunk_index: int
	start_cookie: int
	num_rows: int
	attempts: int = 0
	priority: int = 0
	
	def __lt__(self, other):
		# For PriorityQueue sorting: higher priority first, then by chunk_index
		if self.priority != other.priority:
			return self.priority > other.priority  # Higher priority first
		return self.chunk_index < other.chunk_index  # Lower chunk_index first


class ProcessingManager:
	def __init__(self) -> None:
		self.queue = None  # Will be created lazily
		self.concurrency = settings.MAX_CONCURRENCY
		if settings.DATABASE_URL.startswith("sqlite"):
			self.concurrency = 1  # SQLite single-writer limitation
		self.semaphore = None  # Will be created lazily
		self.workers: list[asyncio.Task] = []
		self._started = False

	def _ensure_initialized(self):
		"""Ensure queue and semaphore are initialized in the current event loop."""
		if self.queue is None:
			self.queue = asyncio.PriorityQueue()
		if self.semaphore is None:
			self.semaphore = asyncio.Semaphore(self.concurrency)

	async def start(self) -> None:
		if self._started:
			return
		self._ensure_initialized()
		self._started = True
		for _ in range(self.concurrency):
			self.workers.append(asyncio.create_task(self._worker_loop()))

	async def stop(self) -> None:
		for w in self.workers:
			w.cancel()
		self.workers.clear()
		self._started = False

	async def _validate_csv_structure(self, file_path: str) -> tuple[bool, str, int]:
		"""Validate CSV file structure and count rows."""
		try:
			with open(file_path, 'r', encoding='utf-8', newline='') as f:
				reader = csv.reader(f)
				header = next(reader, None)
				if not header:
					return False, "Empty file", 0
				
				row_count = 1  # Header row
				for row in reader:
					row_count += 1
					if len(row) != len(header):
						return False, f"Row {row_count} has {len(row)} columns, expected {len(header)}", row_count
				
				return True, f"Valid CSV with {len(header)} columns", row_count
		except Exception as e:
			return False, f"CSV validation error: {str(e)}", 0

	async def enqueue_file(self, session: AsyncSession, file: File, priority: int = 0) -> None:
		# Validate file exists
		if not file.path or not os.path.exists(file.path):
			file.status = "failed"
			file.error_message = "file not found on disk"
			session.add(file)
			await session.commit()
			return
		
		# Validate CSV structure
		is_valid, message, total_rows = await self._validate_csv_structure(file.path)
		if not is_valid:
			file.status = "failed"
			file.error_message = f"CSV validation failed: {message}"
			session.add(file)
			await session.commit()
			return
		
		chunk_size = settings.CHUNK_SIZE
		chunk_index = 0

		# Update file status
		file.status = "processing"
		file.total_chunks = math.ceil((total_rows - 1) / chunk_size)  # Exclude header
		session.add(file)
		await session.commit()

		# Create chunks based on CSV structure
		with open(file.path, "rb") as fb:
			text = io.TextIOWrapper(fb, encoding="utf-8", newline="")
			reader = csv.reader(text)
			next(reader)  # Skip header
			
			current_chunk_rows = 0
			current_chunk_start_cookie = fb.tell()
			
			while True:
				cookie_before = fb.tell()
				try:
					row = next(reader)
				except StopIteration:
					# Process final chunk
					if current_chunk_rows > 0:
						await self._create_chunk(session, file, chunk_index, current_chunk_start_cookie, current_chunk_rows)
						await self.queue.put((priority, chunk_index, ChunkTask(file.id, chunk_index, current_chunk_start_cookie, current_chunk_rows, priority=priority)))
						logger.info("enqueued final chunk", extra={"file_id": file.id, "chunk_index": chunk_index, "rows": current_chunk_rows})
						chunk_index += 1
					break

				# Start of a new chunk
				if current_chunk_rows == 0:
					current_chunk_start_cookie = cookie_before
				current_chunk_rows += 1

				if current_chunk_rows >= chunk_size:
					await self._create_chunk(session, file, chunk_index, current_chunk_start_cookie, current_chunk_rows)
					await self.queue.put((priority, chunk_index, ChunkTask(file.id, chunk_index, current_chunk_start_cookie, current_chunk_rows, priority=priority)))
					logger.info("enqueued chunk", extra={"file_id": file.id, "chunk_index": chunk_index, "rows": current_chunk_rows})
					chunk_index += 1
					current_chunk_rows = 0

		logger.info(f"File {file.id} processed into {chunk_index} chunks", extra={"file_id": file.id, "total_chunks": chunk_index})

	async def _create_chunk(self, session: AsyncSession, file: File, index: int, start_cookie: int, num_rows: int) -> None:
		chunk = Chunk(
			id=str(uuid.uuid4()),
			file_id=file.id,
			index=index,
			status="queued",
			attempts=0,
			result_meta={"start_cookie": start_cookie, "num_rows": num_rows}
		)
		session.add(chunk)
		
		# Retry logic for database connection issues
		max_retries = 5
		for attempt in range(max_retries):
			try:
				await session.commit()
				break
			except (OperationalError, DisconnectionError) as e:
				if attempt < max_retries - 1:
					await session.rollback()
					# Re-add entities to session
					session.add(chunk)
					await asyncio.sleep(0.1 * (2 ** attempt))
					logger.warning(f"Database connection issue, retrying {attempt + 1}/{max_retries}: {e}")
				else:
					raise e
			except Exception as e:
				await session.rollback()
				raise e

	async def _worker_loop(self) -> None:
		while True:
			try:
				_, _, task = await self.queue.get()
				async with self.semaphore:
					await self._process_task(task)
				self.queue.task_done()
			except asyncio.CancelledError:
				break
			except Exception as e:
				logger.exception("worker error", extra={"error": str(e)})

	async def _process_task(self, task: ChunkTask) -> None:
		from app.db.session import AsyncSessionLocal
		async with AsyncSessionLocal() as session:
			# Find chunk by file_id and chunk_index
			chunk = await session.execute(
				select(Chunk).where(Chunk.file_id == task.file_id, Chunk.index == task.chunk_index)
			)
			chunk = chunk.scalar_one_or_none()
			if not chunk:
				logger.error(f"Chunk not found for file {task.file_id}, chunk {task.chunk_index}")
				return
			
			logger.info(f"Processing chunk {task.chunk_index} for file {task.file_id}")
			
			chunk.status = "processing"
			session.add(chunk)
			await session.commit()
			
			try:
				# Read chunk rows
				rows = await self._read_rows_in_thread(task.file_id, task.start_cookie, task.num_rows)
				logger.info(f"Read {len(rows)} rows for chunk {task.chunk_index}")
				
				# Process and store results
				records = []
				for row in rows:
					record = ProcessedRecord(
						id=str(uuid.uuid4()),
						file_id=task.file_id,
						chunk_index=task.chunk_index,
						data={"row": row}
					)
					records.append(record)
				
				logger.info(f"Created {len(records)} ProcessedRecord objects")
				
				# Add records to session and commit
				for record in records:
					session.add(record)
				
				# Update chunk and file status
				chunk.status = "completed"
				chunk.result_meta = {"processed_rows": len(rows)}
				session.add(chunk)
				
				file = await session.get(File, task.file_id)
				if file:
					file.processed_chunks += 1
					session.add(file)
				
				await session.commit()
				logger.info("chunk completed", extra={"file_id": task.file_id, "chunk_index": task.chunk_index, "rows": len(rows)})
			except Exception as e:
				logger.error(f"Error processing chunk {task.chunk_index}: {e}")
				await session.rollback()
				task.attempts += 1
				chunk.attempts = task.attempts
				chunk.status = "queued" if task.attempts < settings.MAX_RETRIES else "failed"
				chunk.error_message = str(e)
				session.add(chunk)

				file: Optional[File] = await session.get(File, task.file_id)
				if file and task.attempts >= settings.MAX_RETRIES:
					file.failed_chunks += 1
					session.add(file)

				await session.commit()
				logger.exception("chunk failed", extra={"file_id": task.file_id, "chunk_index": task.chunk_index, "attempts": task.attempts})

				if task.attempts < settings.MAX_RETRIES:
					backoff = min(settings.MAX_BACKOFF, settings.BASE_BACKOFF * (2 ** (task.attempts - 1)))
					await asyncio.sleep(backoff)
					await self.queue.put((task.priority, task.chunk_index, task))
			finally:
				await self._maybe_finalize_file(task.file_id)

	async def _read_rows_in_thread(self, file_id: str, start_cookie: int, num_rows: int) -> List[List[str]]:
		from app.db.session import AsyncSessionLocal
		async with AsyncSessionLocal() as session:
			file = await session.get(File, file_id)
			if not file or not file.path:
				return []
		
		def _read_rows() -> List[List[str]]:
			rows = []
			with open(file.path, "rb") as fb:
				fb.seek(start_cookie)
				text = io.TextIOWrapper(fb, encoding="utf-8", newline="")
				reader = csv.reader(text)
				for _ in range(num_rows):
					try:
						row = next(reader)
						rows.append(row)
					except StopIteration:
						break
			return rows
		
		return await asyncio.to_thread(_read_rows)

	async def _maybe_finalize_file(self, file_id: str) -> None:
		from app.db.session import AsyncSessionLocal
		async with AsyncSessionLocal() as session:
			file: Optional[File] = await session.get(File, file_id)
			if not file:
				return
			if file.total_chunks == 0:
				return
			if file.processed_chunks + file.failed_chunks >= file.total_chunks:
				# Determine final status
				if file.failed_chunks == 0:
					file.status = "completed"
				elif file.processed_chunks == 0:
					file.status = "failed"
				else:
					file.status = "completed_with_errors"
				
				session.add(file)
				await session.commit()
				
				# Clean up file if configured
				if settings.DELETE_FILE_ON_COMPLETE and file.path and os.path.exists(file.path):
					try:
						os.remove(file.path)
						logger.info("deleted file after processing", extra={"file_id": file_id, "path": file.path})
					except Exception as e:
						logger.warning("failed to delete file", extra={"file_id": file_id, "path": file.path, "error": str(e)})


# Create a global instance that will be initialized lazily
_processing_manager = None

def get_processing_manager() -> ProcessingManager:
	"""Get the global processing manager instance, creating it if necessary."""
	global _processing_manager
	if _processing_manager is None:
		_processing_manager = ProcessingManager()
	return _processing_manager

# For backward compatibility
processing_manager = get_processing_manager() 