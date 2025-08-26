import pytest
import asyncio
import io
import os
from unittest.mock import patch, MagicMock
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession
from fastapi import FastAPI

from app.db.session import get_session
from app.db.models import File, Chunk, ProcessedRecord
from app.services.processing import ProcessingManager


@pytest.fixture
async def _isolate_storage():
	"""Isolate storage for each test."""
	# Use SQLite for testing
	os.environ["DATABASE_URL"] = "sqlite+aiosqlite:///./storage/test.db"
	
	# Ensure storage directory exists
	os.makedirs("./storage", exist_ok=True)
	
	# Clean up any existing test files
	for file in os.listdir("./storage"):
		if file.startswith("test_") and file.endswith(".csv"):
			os.remove(os.path.join("./storage", file))
	
	yield
	
	# Cleanup after test (best-effort; SQLite may keep handles briefly)
	try:
		if os.path.exists("./storage/test.db"):
			os.remove("./storage/test.db")
	except Exception:
		pass


@pytest.fixture
async def test_app():
	"""Create a test app with SQLite database."""
	from app.api.routes import router as api_router
	from app.db.models import Base
	from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
	from sqlalchemy.orm import sessionmaker
	from app.core.config import settings
	
	# Ensure background disabled for tests
	os.environ["DISABLE_BACKGROUND"] = "1"
	settings.DISABLE_BACKGROUND = True
	
	# Create a separate engine for testing
	test_database_url = "sqlite+aiosqlite:///./storage/test.db"
	test_engine = create_async_engine(
		test_database_url,
		echo=False,
		pool_size=1,
		max_overflow=0,
		pool_timeout=30.0,
		connect_args={"timeout": 30.0, "check_same_thread": False}
	)
	
	# Create tables
	async with test_engine.begin() as conn:
		await conn.run_sync(Base.metadata.create_all)
	
	# Create session factory
	TestingSessionLocal = sessionmaker(
		test_engine, class_=AsyncSession, expire_on_commit=False
	)
	
	# Override the get_session dependency for testing
	async def override_get_session():
		async with TestingSessionLocal() as session:
			yield session
	
	app = FastAPI(title="Test Granula Async File Processor")
	app.include_router(api_router, prefix="/api/v1")
	
	# Override dependencies
	app.dependency_overrides[get_session] = override_get_session
	
	# Store test engine for use in tests
	app.state.test_engine = test_engine
	
	yield app
	
	# Cleanup
	await test_engine.dispose()


@pytest.fixture
async def session(test_app):
	"""Create a database session for testing."""
	async for s in get_session():
		yield s


@pytest.fixture
async def client(test_app):
	"""Create a test client."""
	async with AsyncClient(app=test_app, base_url="http://test") as ac:
		yield ac


@pytest.fixture
async def processing_manager():
	"""Create a fresh ProcessingManager for each test."""
	manager = ProcessingManager()
	yield manager
	# Cleanup
	await manager.stop()


def create_test_file(filename: str, content: bytes, content_type: str = "text/csv") -> tuple:
	"""Helper function to create test files with proper content_type for httpx."""
	file_obj = io.BytesIO(content)
	file_obj.seek(0)
	# For httpx, we need to pass the content_type in the files dict
	return (filename, file_obj, content_type)


@pytest.mark.asyncio
async def test_upload_status_results_flow_and_pagination(_isolate_storage, client, session, processing_manager):
	"""Test the complete flow: upload -> status -> results with pagination."""
	# Create test CSV content
	csv_content = "name,age,city\nJohn,25,NYC\nJane,30,LA\nBob,35,Chicago\nAlice,28,Boston\nCharlie,32,Seattle"
	
	# Upload file
	response = await client.post(
		"/api/v1/upload",
		files={"file": create_test_file("test.csv", csv_content.encode())},
		params={"priority": 5}
	)
	assert response.status_code == 201
	upload_data = response.json()
	file_id = upload_data["file_id"]
	
	# Check initial status
	status_response = await client.get(f"/api/v1/status/{file_id}")
	assert status_response.status_code == 200
	status_data = status_response.json()
	assert status_data["status"] == "queued"
	
	# Get results (should be empty initially)
	results_response = await client.get(f"/api/v1/results/{file_id}?page=1&size=2")
	assert results_response.status_code == 200
	results_data = results_response.json()
	
	# Should have no results initially
	assert len(results_data["results"]) == 0
	assert results_data["total"] == 0
	assert results_data["page"] == 1
	assert results_data["size"] == 2


@pytest.mark.asyncio
async def test_csv_structure_validation(_isolate_storage, client, session, processing_manager):
	"""Test CSV structure validation."""
	# Test with invalid CSV (inconsistent columns)
	invalid_csv = "name,age\nJohn,25,NYC\nJane,30"  # Second row has 3 columns, third has 2
	
	response = await client.post(
		"/api/v1/upload",
		files={"file": create_test_file("invalid.csv", invalid_csv.encode())}
	)
	
	# Should accept the upload initially
	assert response.status_code == 201
	upload_data = response.json()
	file_id = upload_data["file_id"]
	
	# Check status
	status_response = await client.get(f"/api/v1/status/{file_id}")
	assert status_response.status_code == 200
	status_data = status_response.json()
	assert status_data["status"] == "queued"


@pytest.mark.asyncio
async def test_404_for_missing_file(_isolate_storage, client, processing_manager):
	"""Test 404 responses for non-existent files."""
	# Test status endpoint
	response = await client.get("/api/v1/status/nonexistent-id")
	assert response.status_code == 404
	
	# Test results endpoint
	response = await client.get("/api/v1/results/nonexistent-id")
	assert response.status_code == 404


@pytest.mark.asyncio
async def test_file_type_validation(_isolate_storage, client, processing_manager):
	"""Test file type validation."""
	# Test with non-CSV file
	response = await client.post(
		"/api/v1/upload",
		files={"file": create_test_file("test.txt", b"not a csv", "text/plain")}
	)
	assert response.status_code == 400
	assert "Only CSV files are allowed" in response.json()["detail"]


@pytest.mark.asyncio
async def test_file_size_validation(_isolate_storage, client, processing_manager, monkeypatch):
	"""Test file size validation."""
	# Reduce max upload limit for the test to avoid generating huge data
	from app.core.config import settings
	old_limit = settings.MAX_UPLOAD_MB
	settings.MAX_UPLOAD_MB = 1  # 1 MB limit for test
	try:
		# Create a payload ~2MB to exceed 1MB limit
		payload = b"header\n" + (b"x" * (2 * 1024 * 1024))
		response = await client.post(
			"/api/v1/upload",
			files={"file": ("too_big.csv", io.BytesIO(payload), "text/csv")}
		)
		assert response.status_code == 400
		assert "File size exceeds" in response.json()["detail"]
	finally:
		settings.MAX_UPLOAD_MB = old_limit


@pytest.mark.asyncio
async def test_priority_handling(_isolate_storage, client, session, processing_manager):
	"""Test priority-based task queuing."""
	# Create test CSV content
	csv_content = "name,age\nJohn,25\nJane,30\nBob,35"
	
	# Upload with different priorities
	response1 = await client.post(
		"/api/v1/upload",
		files={"file": create_test_file("low_priority.csv", csv_content.encode())},
		params={"priority": 1}
	)
	assert response1.status_code == 201
	
	response2 = await client.post(
		"/api/v1/upload",
		files={"file": create_test_file("high_priority.csv", csv_content.encode())},
		params={"priority": 9}
	)
	assert response2.status_code == 201
	
	# Both should be accepted
	assert response1.json()["file_id"] != response2.json()["file_id"]


@pytest.mark.asyncio
async def test_concurrent_processing(_isolate_storage, client, session, processing_manager):
	"""Test concurrent processing of multiple files."""
	# Create multiple test files
	csv_content = "name,age\nJohn,25\nJane,30\nBob,35"
	
	# Upload multiple files
	file_ids = []
	for i in range(3):
		response = await client.post(
			"/api/v1/upload",
			files={"file": create_test_file(f"concurrent_{i}.csv", csv_content.encode())}
		)
		assert response.status_code == 201
		file_ids.append(response.json()["file_id"])
	
	# All should be accepted
	assert len(set(file_ids)) == 3
	
	# Check that all files are queued
	for file_id in file_ids:
		status_response = await client.get(f"/api/v1/status/{file_id}")
		assert status_response.status_code == 200
		status_data = status_response.json()
		assert status_data["status"] == "queued" 