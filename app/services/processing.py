import asyncio
import csv
import uuid
import math
import logging
import io
from dataclasses import dataclass
from typing import Optional
from datetime import datetime, timezone

from sqlalchemy import select, func, insert
from sqlalchemy.ext.asyncio import AsyncSession

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


class ProcessingManager:
	def __init__(self) -> None:
		self.queue: "asyncio.PriorityQueue[tuple[int, int, ChunkTask]]" = asyncio.PriorityQueue()
		# Reduce concurrency for SQLite to avoid writer lock contention
		concurrency = settings.MAX_CONCURRENCY
		if settings.DATABASE_URL.startswith("sqlite"):
			concurrency = 1
		self.semaphore = asyncio.Semaphore(concurrency)
		self.workers: list[asyncio.Task] = []
		self._started = False

	async def start(self) -> None:
		if self._started:
			return
		self._started = True
		for _ in range(settings.MAX_CONCURRENCY):
			self.workers.append(asyncio.create_task(self._worker_loop()))

	async def stop(self) -> None:
		for w in self.workers:
			w.cancel()
		self.workers.clear()
		self._started = False

	async def _count_lines_in_thread(self, path: str) -> int:
		def _count() -> int:
			cnt = 0
			with open(path, "r", encoding="utf-8", newline="") as f:
				for _ in f:
					cnt += 1
			return cnt
		return await asyncio.to_thread(_count)

	async def _estimate_total_chunks_bg(self, file_id: str, path: str, chunk_size: int) -> None:
		from app.db.session import AsyncSessionLocal
		try:
			lines = await self._count_lines_in_thread(path)
			estimate = math.ceil(lines / chunk_size) if lines > 0 else 0
			async with AsyncSessionLocal() as s:
				file = await s.get(File, file_id)
				if not file:
					return
				file.total_chunks = max(file.total_chunks, estimate)
				s.add(file)
				await s.commit()
		except Exception:
			return

	async def enqueue_file(self, session: AsyncSession, file: File, priority: int = 0) -> None:
		# Initialize by scanning the file once and creating chunk tasks based on CSV row boundaries
		import os
		if not file.path or not os.path.exists(file.path):
			file.status = "failed"
			file.error_message = "file not found on disk"
			session.add(file)
			await session.commit()
			return
		chunk_size = settings.CHUNK_SIZE
		chunk_index = 0

		# Update file status
		file.status = "processing"
		session.add(file)
		await session.commit()

		# Kick off a non-blocking estimation of total_chunks
		asyncio.create_task(self._estimate_total_chunks_bg(file.id, file.path, chunk_size))

		with open(file.path, "rb") as fb:
			text = io.TextIOWrapper(fb, encoding="utf-8", newline="")
			reader = csv.reader(text)
			current_chunk_rows = 0
			current_chunk_start_cookie = fb.tell()
			while True:
				cookie_before = fb.tell()
				try:
					row = next(reader)
				except StopIteration:
					# tail chunk
					if current_chunk_rows > 0:
						await self._create_chunk(session, file, chunk_index, current_chunk_start_cookie, current_chunk_rows)
						await self.queue.put((priority, chunk_index, ChunkTask(file.id, chunk_index, current_chunk_start_cookie, current_chunk_rows, priority=priority)))
						logger.info("enqueued tail chunk", extra={"file_id": file.id, "chunk_index": chunk_index, "rows": current_chunk_rows})
						chunk_index += 1
					break

				# start of a new chunk
				if current_chunk_rows == 0:
					current_chunk_start_cookie = cookie_before
				current_chunk_rows += 1

				if current_chunk_rows >= chunk_size:
					await self._create_chunk(session, file, chunk_index, current_chunk_start_cookie, current_chunk_rows)
					await self.queue.put((priority, chunk_index, ChunkTask(file.id, chunk_index, current_chunk_start_cookie, current_chunk_rows, priority=priority)))
					logger.info("enqueued chunk", extra={"file_id": file.id, "chunk_index": chunk_index, "rows": current_chunk_rows})
					chunk_index += 1
					current_chunk_rows = 0
					# next chunk will start from the next row; continue

		# total_chunks already updated progressively in _create_chunk

	async def _create_chunk(self, session: AsyncSession, file: File, index: int, start_cookie: int, num_rows: int) -> None:
		chunk = Chunk(
			file_id=file.id,
			index=index,
			status="queued",
			attempts=0,
			result_meta={"start_cookie": start_cookie, "num_rows": num_rows},
		)
		session.add(chunk)
		# increment total chunks progressively for better status reporting
		file.total_chunks = max(file.total_chunks, index + 1)
		session.add(file)
		await session.commit()

	async def _worker_loop(self) -> None:
		while True:
			priority, _, task = await self.queue.get()
			try:
				async with self.semaphore:
					await self._process_task(task)
			except asyncio.CancelledError:
				raise
			except Exception:
				await asyncio.sleep(0)
			finally:
				self.queue.task_done()

	async def _read_rows_in_thread(self, path: str, start_cookie: int, num_rows: int) -> list[list[str]]:
		def _read() -> list[list[str]]:
			with open(path, "rb") as fb:
				fb.seek(start_cookie)
				text = io.TextIOWrapper(fb, encoding="utf-8", newline="")
				reader = csv.reader(text)
				rows: list[list[str]] = []
				for _ in range(num_rows):
					try:
						rows.append(next(reader))
					except StopIteration:
						break
				return rows
		return await asyncio.to_thread(_read)

	async def _process_task(self, task: ChunkTask) -> None:
		from app.db.session import AsyncSessionLocal

		async with AsyncSessionLocal() as session:
			chunk: Optional[Chunk] = await session.scalar(
				select(Chunk).where(Chunk.file_id == task.file_id, Chunk.index == task.chunk_index)
			)
			if not chunk:
				return

			chunk.status = "processing"
			session.add(chunk)
			await session.commit()

			try:
				file_obj = await session.get(File, task.file_id)
				if not file_obj or not file_obj.path:
					raise FileNotFoundError("File path not found for processing")
				rows = await self._read_rows_in_thread(
					path=file_obj.path,
					start_cookie=task.start_cookie,
					num_rows=task.num_rows,
				)

				# Batch insert
				now = datetime.now(timezone.utc)
				payload = [
					{
						"id": str(uuid.uuid4()),
						"file_id": task.file_id,
						"chunk_index": task.chunk_index,
						"data": {"row": r},
						"created_at": now,
					}
					for r in rows
				]
				if payload:
					await session.execute(insert(ProcessedRecord), payload)
				await session.commit()

				chunk.status = "completed"
				chunk.attempts = task.attempts
				session.add(chunk)

				file: Optional[File] = await session.get(File, task.file_id)
				if file:
					file.processed_chunks += 1
					session.add(file)

				await session.commit()
				logger.info("chunk completed", extra={"file_id": task.file_id, "chunk_index": task.chunk_index, "rows": len(rows)})
			except Exception as e:
				await session.rollback()
				task.attempts += 1
				chunk.attempts = task.attempts
				chunk.status = "queued" if task.attempts < settings.MAX_RETRIES else "failed"
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
				else:
					pass
			finally:
				await self._maybe_finalize_file(task.file_id)

	async def _maybe_finalize_file(self, file_id: str) -> None:
		from app.db.session import AsyncSessionLocal

		async with AsyncSessionLocal() as session:
			file: Optional[File] = await session.get(File, file_id)
			if not file:
				return
			if file.total_chunks == 0:
				return
			if file.processed_chunks + file.failed_chunks >= file.total_chunks:
				file.status = "completed" if file.failed_chunks == 0 else "completed_with_errors"
				session.add(file)
				await session.commit()


processing_manager = ProcessingManager() 