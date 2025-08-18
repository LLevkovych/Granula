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

3. Set environment and run migrations:
   
   ```powershell
   $env:DATABASE_URL='postgresql+asyncpg://granula_user:granula_pass@localhost:5432/granula'
   alembic upgrade head
   ```

4. Start app:
   
   ```powershell
   uvicorn app.main:app --reload --port 8000
   ```

5. Open docs: `http://localhost:8000/docs` 