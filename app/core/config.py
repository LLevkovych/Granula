import os
import logging
from typing import List

# Load .env if python-dotenv is available
try:
	from dotenv import load_dotenv  # type: ignore
	load_dotenv()
except Exception:
	# If python-dotenv is not installed, ignore silently
	pass


def _normalize_async_database_url(raw_url: str) -> str:
	"""Convert synchronous database URLs to async ones and normalize host."""
	url = raw_url
	if url.startswith("sqlite://"):
		url = url.replace("sqlite://", "sqlite+aiosqlite://", 1)
	elif url.startswith("postgresql://"):
		url = url.replace("postgresql://", "postgresql+asyncpg://", 1)
	# Normalize localhost -> 127.0.0.1 for asyncpg on Windows
	if url.startswith("postgresql+asyncpg://") and "@localhost:" in url:
		url = url.replace("@localhost:", "@127.0.0.1:")
	return url



def setup_logging() -> None:
	"""Setup logging configuration."""
	logging.basicConfig(
		level=logging.INFO,
		format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
		handlers=[
			logging.StreamHandler(),
		]
	)
	
	# Set SQLAlchemy logging level
	logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)
	logging.getLogger('sqlalchemy.pool').setLevel(logging.WARNING)


class Settings:
	def __init__(self) -> None:
		# Default to PostgreSQL for production
		raw_db = os.getenv("DATABASE_URL", "postgresql://granula_user:granula_pass@localhost:5432/granula")
		self.DATABASE_URL = _normalize_async_database_url(raw_db)
		
		# Optimize concurrency based on database type
		default_concurrency = 10 if "postgresql" in self.DATABASE_URL else 1
		self.MAX_CONCURRENCY = int(os.getenv("MAX_CONCURRENCY", str(default_concurrency)))
		
		self.CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "10000"))
		self.MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
		self.BASE_BACKOFF = float(os.getenv("BASE_BACKOFF", "1.0"))
		self.MAX_BACKOFF = float(os.getenv("MAX_BACKOFF", "30.0"))
		self.DELETE_FILE_ON_COMPLETE = os.getenv("DELETE_FILE_ON_COMPLETE", "false").lower() == "true"
		
		# Upload constraints
		self.MAX_UPLOAD_MB = int(os.getenv("MAX_UPLOAD_MB", "500"))
		self.ALLOWED_CONTENT_TYPES = [
			ct.strip() for ct in os.getenv("ALLOWED_CONTENT_TYPES", "text/csv,application/csv").split(",") if ct.strip()
		]
		
		# Testing / runtime flags
		self.DISABLE_BACKGROUND = os.getenv("DISABLE_BACKGROUND", "0") == "1"
		
		# Database connection settings
		self.DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "5"))
		self.DB_MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", "10"))
		self.DB_POOL_TIMEOUT = float(os.getenv("DB_POOL_TIMEOUT", "30.0"))
		self.DB_ECHO = os.getenv("DB_ECHO", "false").lower() == "true"


# Setup logging when module is imported
setup_logging()
settings = Settings() 