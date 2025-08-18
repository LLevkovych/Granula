import os
import uuid
from typing import Tuple

from fastapi import UploadFile


UPLOAD_DIR = os.path.join("storage", "uploads")


def ensure_directories() -> None:
	os.makedirs(UPLOAD_DIR, exist_ok=True)


def generate_file_destination(original_filename: str) -> Tuple[str, str]:
	file_id = str(uuid.uuid4())
	name, ext = os.path.splitext(os.path.basename(original_filename))
	dst_filename = f"{file_id}{ext or '.dat'}"
	dst_path = os.path.join(UPLOAD_DIR, dst_filename)
	return file_id, dst_path


async def save_upload(file: UploadFile) -> tuple[str, str, str]:
	ensure_directories()
	file_id, dst_path = generate_file_destination(file.filename)

	# Streaming save to disk
	with open(dst_path, "wb") as out:
		while True:
			chunk = await file.read(1024 * 1024)
			if not chunk:
				break
			out.write(chunk)

	return file_id, dst_path, os.path.basename(file.filename)
