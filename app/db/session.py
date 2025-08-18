from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy import event

from app.core.config import settings


connect_args = {}
if settings.DATABASE_URL.startswith("sqlite"):
	connect_args = {"timeout": 30}

engine = create_async_engine(
	settings.DATABASE_URL,
	echo=False,
	pool_pre_ping=True,
	future=True,
	connect_args=connect_args,
)

# Apply SQLite pragmas to improve concurrency
if settings.DATABASE_URL.startswith("sqlite"):
	@event.listens_for(engine.sync_engine, "connect")
	def _set_sqlite_pragma(dbapi_connection, connection_record):
		cursor = dbapi_connection.cursor()
		try:
			cursor.execute("PRAGMA journal_mode=WAL;")
			cursor.execute("PRAGMA synchronous=NORMAL;")
			cursor.execute("PRAGMA busy_timeout=5000;")
		finally:
			cursor.close()

AsyncSessionLocal = async_sessionmaker(
	engine,
	expire_on_commit=False,
	class_=AsyncSession,
)

Base = declarative_base()


async def get_session() -> AsyncSession:
	async with AsyncSessionLocal() as session:
		yield session
