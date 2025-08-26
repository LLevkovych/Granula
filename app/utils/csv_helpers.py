"""
CSV processing utilities.
"""
import csv
import io
from typing import List, Dict, Any, Iterator
from fastapi import HTTPException, status


def validate_csv_structure(file_content: bytes) -> tuple[List[str], int]:
    """
    Validate CSV file structure and return headers and row count.
    
    Args:
        file_content: Raw file content as bytes
        
    Returns:
        Tuple of (headers, row_count)
        
    Raises:
        HTTPException: If CSV structure is invalid
    """
    try:
        # Decode content
        content = file_content.decode('utf-8')
        lines = content.splitlines()
        
        if not lines:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="CSV validation failed: Empty file"
            )
        
        # Parse headers
        reader = csv.reader(io.StringIO(content))
        headers = next(reader)
        
        if not headers:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="CSV validation failed: No headers found"
            )
        
        # Count rows and validate structure
        row_count = 0
        expected_columns = len(headers)
        
        for row_num, row in enumerate(reader, start=2):
            if len(row) != expected_columns:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"CSV validation failed: Row {row_num} has {len(row)} columns, expected {expected_columns}"
                )
            row_count += 1
        
        if row_count == 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="CSV validation failed: No data rows found"
            )
        
        return headers, row_count
        
    except UnicodeDecodeError:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="CSV validation failed: Invalid encoding (use UTF-8)"
        )
    except csv.Error as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"CSV validation failed: {str(e)}"
        )


def chunk_file(file_path: str, chunk_size: int = 10000) -> Iterator[List[Dict[str, Any]]]:
    """
    Read CSV file in chunks.
    
    Args:
        file_path: Path to CSV file
        chunk_size: Number of rows per chunk
        
    Yields:
        List of dictionaries representing rows in the chunk
    """
    with open(file_path, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        chunk = []
        
        for row in reader:
            chunk.append(row)
            
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
        
        # Yield remaining rows
        if chunk:
            yield chunk 