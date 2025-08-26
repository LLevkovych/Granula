import os
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from app.core.config import settings

# Create declarative base
Base = declarative_base()

# Configure engine with appropriate settings for the database type
if settings.DATABASE_URL.startswith("sqlite"):
	# SQLite-specific settings for better concurrency
	engine = create_async_engine(
		settings.DATABASE_URL,
		echo=settings.DB_ECHO,
		pool_size=1,  # Single connection for SQLite
		max_overflow=0,  # No overflow for SQLite
		pool_timeout=30.0,  # Longer timeout for SQLite
		connect_args={
			"timeout": 30.0,  # SQLite timeout
			"check_same_thread": False,  # Allow async access
		}
	)
else:
	# PostgreSQL settings with configurable pool
	engine = create_async_engine(
		settings.DATABASE_URL,
		echo=settings.DB_ECHO,
		pool_size=min(settings.DB_POOL_SIZE, 4),  # cap small for Windows stability
		max_overflow=0,  # avoid overflow on Windows
		pool_timeout=max(30.0, settings.DB_POOL_TIMEOUT),
		pool_pre_ping=True,  # Verify connections before use
		connect_args={
			"server_settings": {
				"application_name": "granula_app",
				"statement_timeout": "60000"
			},
			"command_timeout": 60,  # Command timeout in seconds
			"timeout": 10,  # asyncpg TCP connect timeout
			"statement_cache_size": 0,  # disable statement cache for stability
			"ssl": "disable",
		}
	)

AsyncSessionLocal = sessionmaker(
	engine, class_=AsyncSession, expire_on_commit=False
)


async def get_session() -> AsyncSession:
	async with AsyncSessionLocal() as session:
		try:
			yield session
		finally:
			await session.close()
