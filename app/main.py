from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import router as api_router
from app.db.session import Base, engine
from app.services.processing import processing_manager
from app.core.config import settings


async def create_db_and_tables() -> None:
	async with engine.begin() as conn:
		await conn.run_sync(Base.metadata.create_all)


@asynccontextmanager
async def lifespan(app: FastAPI):
	await create_db_and_tables()
	await processing_manager.start()
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
