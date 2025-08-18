from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import router as api_router
from app.db.session import Base, engine, AsyncSessionLocal
from app.services.processing import processing_manager
from app.core.config import settings
from sqlalchemy import update, select
from app.db.models import Chunk, File


async def create_db_and_tables() -> None:
	async with engine.begin() as conn:
		await conn.run_sync(Base.metadata.create_all)


async def recover_and_resume() -> None:
	# Move stuck processing -> queued and resume processing
	async with AsyncSessionLocal() as s:
		await s.execute(update(Chunk).where(Chunk.status == "processing").values(status="queued"))
		await s.commit()
		# Restart processing for files in processing/queued
		res = await s.execute(select(File).where(File.status.in_(["processing", "queued"])) )
		files = res.scalars().all()
		for f in files:
			await processing_manager.enqueue_file(s, f)


@asynccontextmanager
async def lifespan(app: FastAPI):
	await create_db_and_tables()
	await processing_manager.start()
	await recover_and_resume()
	yield


app = FastAPI(title="Granula Async Processor", lifespan=lifespan)

# CORS
if settings.CORS_ORIGINS == ["*"]:
	app.add_middleware(
		CORSMiddleware,
		allow_origins=["*"],
		allow_credentials=True,
		allow_methods=["*"],
		allow_headers=["*"],
	)
else:
	app.add_middleware(
		CORSMiddleware,
		allow_origins=settings.CORS_ORIGINS,
		allow_credentials=True,
		allow_methods=["*"],
		allow_headers=["*"],
	)

app.include_router(api_router)
