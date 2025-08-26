"""
File handling utilities.
"""
import os
from pathlib import Path
from typing import Optional


def get_file_size_mb(file_path: str) -> float:
    """
    Get file size in megabytes.
    
    Args:
        file_path: Path to file
        
    Returns:
        File size in MB
    """
    try:
        size_bytes = os.path.getsize(file_path)
        return size_bytes / (1024 * 1024)
    except OSError:
        return 0.0


def ensure_storage_dir(directory: str = "./storage") -> str:
    """
    Ensure storage directory exists.
    
    Args:
        directory: Directory path
        
    Returns:
        Absolute path to directory
    """
    path = Path(directory)
    path.mkdir(parents=True, exist_ok=True)
    return str(path.absolute())


def get_safe_filename(filename: str) -> str:
    """
    Get safe filename by removing potentially dangerous characters.
    
    Args:
        filename: Original filename
        
    Returns:
        Safe filename
    """
    # Remove or replace dangerous characters
    dangerous_chars = ['<', '>', ':', '"', '|', '?', '*', '\\', '/']
    safe_name = filename
    
    for char in dangerous_chars:
        safe_name = safe_name.replace(char, '_')
    
    return safe_name


def cleanup_old_files(directory: str, max_age_hours: int = 24) -> int:
    """
    Clean up old files in directory.
    
    Args:
        directory: Directory to clean
        max_age_hours: Maximum age of files in hours
        
    Returns:
        Number of files removed
    """
    import time
    
    current_time = time.time()
    max_age_seconds = max_age_hours * 3600
    removed_count = 0
    
    try:
        for filename in os.listdir(directory):
            file_path = os.path.join(directory, filename)
            
            if os.path.isfile(file_path):
                file_age = current_time - os.path.getmtime(file_path)
                
                if file_age > max_age_seconds:
                    os.remove(file_path)
                    removed_count += 1
                    
    except OSError:
        pass
    
    return removed_count 