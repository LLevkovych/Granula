"""
CORS middleware for FastAPI.
"""
from fastapi.middleware.cors import CORSMiddleware as FastAPICORSMiddleware


class CORSMiddleware(FastAPICORSMiddleware):
    """CORS middleware with default configuration."""
    
    def __init__(self):
        super().__init__(
            allow_origins=["*"],  # Configure as needed for production
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        ) 