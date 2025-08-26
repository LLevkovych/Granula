"""
Main FastAPI application for Granula Async File Processor.
"""
import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.db.session import engine, Base
from app.api import router as api_router
from app.middleware import LoggingMiddleware
from sqlalchemy import text

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    # Startup
    logger.info("Starting Granula Async File Processor...")
    
    # Create database tables
    await create_db_and_tables()
    
    # Start processing manager
    from app.services import get_processing_manager
    processing_manager = get_processing_manager()
    await processing_manager.start()
    
    logger.info("Application startup completed successfully!")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Granula Async File Processor...")
    
    # Stop processing manager
    await processing_manager.stop()
    
    # Close database connections
    await engine.dispose()
    
    logger.info("Application shutdown completed successfully!")


async def create_db_and_tables():
    """Create database tables if they don't exist and validate connectivity."""
    max_retries = 10
    base_delay = 0.5
    
    for attempt in range(max_retries):
        try:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
                # Simple connectivity validation to stabilize asyncpg on Windows
                await conn.execute(text("SELECT 1"))
            logger.info("Database tables created and connectivity verified!")
            return
        except Exception as e:
            if attempt < max_retries - 1:
                delay = min(3.0, base_delay * (2 ** attempt))
                logger.warning(f"Failed to create tables/connect (attempt {attempt + 1}/{max_retries}): {e}. Retrying in {delay:.1f}s...")
                await asyncio.sleep(delay)
            else:
                logger.error(f"Failed to initialize database after {max_retries} attempts: {e}")
                raise HTTPException(
                    status_code=500,
                    detail="Failed to initialize database"
                )


# Create FastAPI application
app = FastAPI(
    title="Granula Async File Processor",
    description="High-performance asynchronous file processing service",
    version="1.0.0",
    lifespan=lifespan
)

# Add middleware
app.add_middleware(LoggingMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include API router
app.include_router(api_router, prefix="/api/v1")


@app.get("/")
async def root():
    """Root endpoint."""
    return {
        "message": "Granula Async File Processor",
        "version": "1.0.0",
        "docs": "/docs",
        "api": "/api/v1"
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}
