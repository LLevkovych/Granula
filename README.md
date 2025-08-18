Granula Async Processor

Run (Windows PowerShell):

1. Create venv and install deps:
   
   ```powershell
   python -m venv .venv
   .\.venv\Scripts\Activate.ps1
   pip install --upgrade pip setuptools wheel
   pip install -r requirements.txt
   ```

2. Start app:
   
   ```powershell
   uvicorn app.main:app --reload --port 8000
   ```

3. Open docs: `http://localhost:8000/docs` 