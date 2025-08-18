import asyncio
import csv
from dataclasses import dataclass
from typing import Any, List, Optional

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.db.models import Chunk, File, ProcessedRecord


@dataclass
class ChunkTask:
	file_id: str
	chunk_index: int
	rows: List[list[str]]
	attempts: int = 0
	priority: int = 0


class ProcessingManager:
	def __init__(self) -> None:
		self.queue: "asyncio.PriorityQueue[tuple[int, ChunkTask]]" = asyncio.PriorityQueue()
		self.semaphore = asyncio.Semaphore(settings.MAX_CONCURRENCY)
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

	async def enqueue_file(self, session: AsyncSession, file: File) -> None:
		# Initialize by scanning the file and creating chunk tasks
		chunk_size = settings.CHUNK_SIZE
		chunk_index = 0
		
		# Update file status
		file.status = "processing"
		session.add(file)
		await session.commit()

		with open(file.path, "r", newline="", encoding="utf-8") as f:
			reader = csv.reader(f)
			buffer: list[list[str]] = []
			for row in reader:
				buffer.append(row)
				if len(buffer) >= chunk_size:
					await self._create_chunk(session, file, chunk_index, buffer)
					await self.queue.put((0, ChunkTask(file.id, chunk_index, buffer)))
					chunk_index += 1
					buffer = []
			# Tail
			if buffer:
				await self._create_chunk(session, file, chunk_index, buffer)
				await self.queue.put((0, ChunkTask(file.id, chunk_index, buffer)))
				chunk_index += 1

		file.total_chunks = chunk_index
		session.add(file)
		await session.commit()

	async def _create_chunk(self, session: AsyncSession, file: File, index: int, rows: list[list[str]]) -> None:
		chunk = Chunk(file_id=file.id, index=index, status="queued", attempts=0)
		session.add(chunk)
		await session.commit()

	async def _worker_loop(self) -> None:
		while True:
			priority, task = await self.queue.get()
			try:
				async with self.semaphore:
					await self._process_task(task)
			except asyncio.CancelledError:
				raise
			except Exception as e:
				await asyncio.sleep(0)
			finally:
				self.queue.task_done()

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
				# Business logic placeholder: convert rows to simple records
				items = [ProcessedRecord(file_id=task.file_id, chunk_index=task.chunk_index, data={"row": r}) for r in task.rows]
				session.add_all(items)
				await session.commit()

				chunk.status = "completed"
				chunk.attempts = task.attempts
				session.add(chunk)

				file: Optional[File] = await session.get(File, task.file_id)
				if file:
					file.processed_chunks += 1
					session.add(file)

				await session.commit()
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

				if task.attempts < settings.MAX_RETRIES:
					backoff = min(settings.MAX_BACKOFF, settings.BASE_BACKOFF * (2 ** (task.attempts - 1)))
					await asyncio.sleep(backoff)
					await self.queue.put((task.priority, task))
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