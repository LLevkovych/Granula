# Granula Async File Processor

A high-performance asynchronous FastAPI service for processing large data files with chunk-based processing, task queuing, and retry mechanisms.

## 🏗️ Architecture

```
┌─────────────┐    ┌──────────────┐    ┌─────────────┐
│   Client    │───▶│   FastAPI    │───▶│ PostgreSQL │
│  (Browser)  │    │   Service    │    │   Database  │
└─────────────┘    └──────────────┘    └─────────────┘
                          │
                          ▼
                   ┌──────────────┐
                   │ Task Queue   │
                   │ (Priority)   │
                   └──────────────┘
                          │
                          ▼
                   ┌──────────────┐
                   │ Worker Pool  │
                   │ (Semaphore)  │
                   └──────────────┘
```

## 🛠️ Tech Stack

- **Backend**: FastAPI + Python 3.11+
- **Async Runtime**: asyncio
- **Database**: PostgreSQL (with asyncpg) + SQLite (for testing)
- **ORM**: SQLAlchemy 2.0 (async)
- **Migrations**: Alembic
- **Testing**: pytest + pytest-asyncio
- **Containerization**: Docker Compose

## 📦 Installation

### Prerequisites
- Python 3.11+
- Docker & Docker Compose (for PostgreSQL)

### Quick Start

1. **Clone and Setup**
   ```bash
   git clone <repository-url>
   python -m venv .venv
   source .venv/bin/activate  # Linux/Mac
   # or
   .venv\Scripts\Activate.ps1  # Windows
   
   pip install -r requirements.txt
   ```

2. **Database Setup (Optional - for PostgreSQL)**
   ```bash
   # Start PostgreSQL
   docker-compose up -d postgres
   
   # Wait for database to be ready
   docker-compose ps postgres
   ```

3. **Environment Configuration**
   ```bash
   # For PostgreSQL (default)
   export DATABASE_URL='postgresql+asyncpg://granula_user:granula_pass@localhost:5432/granula'
   
   # For SQLite (fallback)
   export DATABASE_URL='sqlite+aiosqlite:///./storage/granula.db'
   
   # Other settings
   export MAX_CONCURRENCY='10'
   export CHUNK_SIZE='10000'
   ```

4. **Start Service**
   ```bash
   # With PostgreSQL
   uvicorn app.main:app --reload --port 8000
   
   # With SQLite
   export DATABASE_URL="sqlite+aiosqlite:///./storage/granula.db"
   uvicorn app.main:app --reload --port 8000
   ```

5. **Access API**
   - API Documentation: http://localhost:8000/docs
   - API Endpoints: http://localhost:8000/api/v1/

## 🔧 Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `DATABASE_URL` | `postgresql+asyncpg://granula_user:granula_pass@localhost:5432/granula` | Database connection string |
| `MAX_CONCURRENCY` | `10` (PostgreSQL) / `1` (SQLite) | Maximum concurrent processing tasks |
| `CHUNK_SIZE` | `10000` | Number of rows per processing chunk |
| `MAX_RETRIES` | `3` | Maximum retry attempts for failed chunks |
| `BASE_BACKOFF` | `1.0` | Base delay for exponential backoff (seconds) |
| `MAX_BACKOFF` | `30.0` | Maximum delay between retries (seconds) |
| `MAX_UPLOAD_MB` | `500` | Maximum file upload size in MB |
| `ALLOWED_CONTENT_TYPES` | `text/csv,application/csv` | Allowed file MIME types |
| `DELETE_FILE_ON_COMPLETE` | `false` | Delete files after processing |

## 📡 API Endpoints

### File Upload
```http
POST /api/v1/upload?priority=5
Content-Type: multipart/form-data

file: <CSV file>
priority: 0-10 (optional, default: 0)
```

### Status Check
```http
GET /api/v1/status/{file_id}
```

Response:
```json
{
  "file_id": "uuid",
  "status": "queued|processing|completed|completed_with_errors|failed",
  "total_chunks": 100,
  "processed_chunks": 45,
  "failed_chunks": 0,
  "progress_percent": 45.0
}
```

### Results Retrieval
```http
GET /api/v1/results/{file_id}?limit=100&offset=0
```

Response:
```json
{
  "file_id": "uuid",
  "items": [
    {
      "id": "uuid",
      "chunk_index": 0,
      "data": {"row": ["col1", "col2", "col3"]}
    }
  ],
  "total": 1000,
  "limit": 100,
  "offset": 0
}
```

## 🧪 Testing

```bash
# Run all tests
python -m pytest

# Run with coverage
python -m pytest --cov=app

# Run specific test file
python -m pytest app/tests/test_api.py

# Run tests with SQLite (isolated)
export DATABASE_URL="sqlite+aiosqlite:///./storage/test.db"
python -m pytest
```

## 📊 Performance

- **Processing Speed**: ~10,000 rows per second per worker
- **Concurrency**: Configurable worker pool (10 for PostgreSQL, 1 for SQLite)
- **Memory Usage**: Efficient chunk-based processing
- **Scalability**: Horizontal scaling ready with external task queues

## Generate sample CSV files

Use the helper script to create valid CSV files (headers: `id,name,value`).

- Python script (recommended):

```bash
python scripts/generate_csv.py --rows 10000 --out sample.csv
```

Options:
- `--rows, -n` number of rows (default: 1000)
- `--out, -o` output path (default: sample.csv)
- `--min` minimal value for `value` column (default: 1)
- `--max` maximal value for `value` column (default: 1000)
- `--names` custom list of names (space-separated)
- `--seed` random seed for reproducibility

Examples:
```bash
python scripts/generate_csv.py -n 5000 -o data/sample_5k.csv
python scripts/generate_csv.py -n 100000 -o data/big.csv --min 10 --max 9999 --seed 42
python scripts/generate_csv.py -n 2000 --names Alice Bob Carol Dave
```

On PowerShell:
```powershell
python .\scripts\generate_csv.py -n 10000 -o .\sample.csv
```

Then upload via API:
```bash
curl -F "file=@sample.csv;type=text/csv" "http://localhost:8000/api/v1/upload?priority=5"
```

### Notes on DATABASE_URL
- Local host run: `postgresql+asyncpg://<user>:<pass>@localhost:5432/<db>`
- Docker Compose: use the service hostname: `postgresql+asyncpg://postgres@postgres:5432/postgres`

### Status endpoint progress
`GET /api/v1/status/{file_id}` now computes `processed_chunks` and `failed_chunks` live from `chunks` (`status='completed'|'failed'`). This reflects in-flight progress more accurately than the `files` counters.
