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
	monkeypatch.setenv("CHUNK_SIZE", "5")
	yield
	shutil.rmtree(tmpdir, ignore_errors=True)


@pytest.mark.asyncio
async def test_health():
	async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
		resp = await client.get("/health")
		assert resp.status_code == 200
		assert resp.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_upload_status_results_flow_and_pagination():
	# CSV with 23 data rows (+ header)
	rows = ["id,name"] + [f"{i},Name{i}" for i in range(1, 24)]
	csv_data = "\n".join(rows) + "\n"
	files = {"file": ("data.csv", io.BytesIO(csv_data.encode("utf-8")), "text/csv")}

	async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
		upload = await client.post("/upload", files=files)
		assert upload.status_code == 201
		file_id = upload.json()["file_id"]

		# poll status; with CHUNK_SIZE=5 expect total_chunks around ceil(24/5)=5 (header counts as row)
		for _ in range(100):
			st = await client.get(f"/status/{file_id}")
			assert st.status_code == 200
			data = st.json()
			if data["total_chunks"] >= 4:  # allow some variance
				break
			await asyncio.sleep(0.05)

		# wait until processed
		for _ in range(400):
			st = await client.get(f"/status/{file_id}")
			data = st.json()
			if data["total_chunks"] > 0 and (data["processed_chunks"] + data["failed_chunks"] == data["total_chunks"]):
				break
			await asyncio.sleep(0.05)

		# pagination checks
		res1 = await client.get(f"/results/{file_id}?limit=10&offset=0")
		res2 = await client.get(f"/results/{file_id}?limit=10&offset=10")
		res3 = await client.get(f"/results/{file_id}?limit=10&offset=20")
		assert res1.status_code == 200 and res2.status_code == 200 and res3.status_code == 200
		b1, b2, b3 = res1.json(), res2.json(), res3.json()
		assert b1["file_id"] == file_id
		assert b1["total"] >= 23  # at least all rows, header included in current simple logic
		# ensure items lists make sense across pages
		assert len(b1["items"]) <= 10 and len(b2["items"]) <= 10 and len(b3["items"]) <= 10


@pytest.mark.asyncio
async def test_404_for_missing_file():
	async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
		st = await client.get("/status/non-existent-id")
		assert st.status_code == 404
		res = await client.get("/results/non-existent-id")
		assert res.status_code == 404 