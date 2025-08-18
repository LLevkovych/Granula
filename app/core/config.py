import os
from urllib.parse import urlparse


def _normalize_async_database_url(raw_url: str) -> str:
	if not raw_url:
		return raw_url
	parsed = urlparse(raw_url)
	scheme = parsed.scheme
	# Handle postgres short alias
	if scheme == "postgres":
		raw_url = raw_url.replace("postgres://", "postgresql://", 1)
		parsed = urlparse(raw_url)
		scheme = parsed.scheme
	# Convert to async drivers if missing
	if scheme == "sqlite" and "+aiosqlite" not in scheme:
		return raw_url.replace("sqlite://", "sqlite+aiosqlite://", 1)
	if scheme == "postgresql" and "+asyncpg" not in scheme:
		return raw_url.replace("postgresql://", "postgresql+asyncpg://", 1)
	return raw_url


class Settings:
	def __init__(self) -> None:
		raw_db = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./storage/app.db")
		self.DATABASE_URL = _normalize_async_database_url(raw_db)
		self.MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", "10"))
		self.CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "10000"))
		self.MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
		self.BASE_BACKOFF = float(os.getenv("BASE_BACKOFF", "1.0"))
		self.MAX_BACKOFF = float(os.getenv("MAX_BACKOFF", "30.0"))
		self.DELETE_FILE_ON_COMPLETE = os.getenv("DELETE_FILE_ON_COMPLETE", "false").lower() == "true"


settings = Settings() 