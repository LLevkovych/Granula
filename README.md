Granula Async Processor

Run (Windows PowerShell):

1. Create venv and install deps:
   
   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   pip install --upgrade pip setuptools wheel
   pip install -r requirements.txt
   ```

2. Start PostgreSQL via Docker Compose (optional but recommended):
   
   ```powershell
   docker compose up -d db
   # Adminer UI (optional): docker compose up -d adminer
   # Adminer is available at http://localhost:8080 (server: db, user: granula_user, pass: granula_pass, db: granula)
   ```

3. Set up environment variables:
   
   ```powershell
   # Option A: Copy env.example to .env and edit
   copy env.example .env
   # Edit .env file with your settings
   
   # Option B: Set environment variables directly
   $env:DATABASE_URL='postgresql+asyncpg://granula_user:granula_pass@localhost:5432/granula'
   $env:MAX_CONCURRENCY='10'
   $env:CHUNK_SIZE='10000'
   ```

4. Run migrations:
   
   ```powershell
   alembic upgrade head
   ```

5. Start app:
   
   ```powershell
   uvicorn app.main:app --reload --port 8000
   ```

6. Open docs: `http://localhost:8000/docs` 