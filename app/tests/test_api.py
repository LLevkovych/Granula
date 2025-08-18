import os
import shutil
import tempfile
import asyncio
import io

import pytest
import httpx

from app.main import app


@pytest.fixture(autouse=True)
def _isolate_storage(monkeypatch):
	tmpdir = tempfile.mkdtemp(prefix="granula-test-")
	uploads = os.path.join(tmpdir, "uploads")
	os.makedirs(uploads, exist_ok=True)
	monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{os.path.join(tmpdir, 'app.db')}")
	monkeypatch.setenv("CORS_ORIGINS", "*")
	monkeypatch.setenv("MAX_CONCURRENCY", "1")
	monkeypatch.setenv("CHUNK_SIZE", "1000")
	yield
	shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.mark.asyncio
async def test_health():
	async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
		resp = await client.get("/health")
		assert resp.status_code == 200
		assert resp.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_upload_status_results_flow():
	# prepare small CSV in-memory
	csv_data = "id,name\n1,Alice\n2,Bob\n3,Carol\n"
	files = {"file": ("small.csv", io.BytesIO(csv_data.encode("utf-8")), "text/csv")}

	async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
		# upload
		r = await client.post("/upload", files=files)
		assert r.status_code == 201
		file_id = r.json()["file_id"]

		# poll status until processed or timeout
		for _ in range(50):
			st = await client.get(f"/status/{file_id}")
			assert st.status_code == 200
			data = st.json()
			if data["processed_chunks"] + data["failed_chunks"] >= max(1, data["total_chunks"]):
				break
			await asyncio.sleep(0.1)

		# results
		res = await client.get(f"/results/{file_id}?limit=100")
		assert res.status_code == 200
		body = res.json()
		assert body["file_id"] == file_id
		assert body["total"] >= 3
		assert len(body["items"]) >= 3 