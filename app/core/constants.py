"""
Constants for Granula Async File Processor.
"""

# File processing constants
DEFAULT_CHUNK_SIZE = 10000
DEFAULT_MAX_CONCURRENCY = 10
DEFAULT_MAX_RETRIES = 3
DEFAULT_BASE_BACKOFF = 1.0
DEFAULT_MAX_BACKOFF = 30.0

# File upload constants
DEFAULT_MAX_UPLOAD_MB = 500
DEFAULT_ALLOWED_CONTENT_TYPES = ["text/csv", "application/csv"]

# Database constants
DEFAULT_DB_POOL_SIZE = 5
DEFAULT_DB_MAX_OVERFLOW = 10
DEFAULT_DB_POOL_TIMEOUT = 30.0

# Status constants
STATUS_QUEUED = "queued"
STATUS_PROCESSING = "processing"
STATUS_COMPLETED = "completed"
STATUS_COMPLETED_WITH_ERRORS = "completed_with_errors"
STATUS_FAILED = "failed"

# Priority constants
MIN_PRIORITY = 0
MAX_PRIORITY = 10
DEFAULT_PRIORITY = 0

# Storage constants
STORAGE_DIR = "./storage"
UPLOADS_DIR = "./storage/uploads"

# Validation constants
MAX_FILENAME_LENGTH = 255
MIN_FILE_SIZE_BYTES = 1 