from __future__ import annotations

"""
S3 JSON Lines storage utilities for Reddit ingestion.

This module provides a writer class compatible with JsonlWriter, but which
persists records directly to Amazon S3. It is intended to be used from
serverless environments such as AWS Lambda.
"""

import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Any

import boto3
from botocore.exceptions import ClientError


S3_CONTENT_TYPE_JSONL = "application/x-ndjson"
DEFAULT_ROTATE_MAX_LINES = 100_000
DEFAULT_BUFFER_SIZE = 1_000


@dataclass
class S3JsonlWriter:
    """
    Write JSON Lines records directly to S3 with optional rotation and buffering.

    The writer accumulates records in memory and periodically flushes them to
    S3 as a single JSONL object. When rotate_max_lines is set and the total
    number of written records exceeds the threshold, the writer will start a
    new S3 object with an incremented part suffix.
    """

    bucket_name: str
    base_prefix: str
    filename: str
    rotate_max_lines: int = DEFAULT_ROTATE_MAX_LINES
    buffer_size: int = DEFAULT_BUFFER_SIZE
    _buffer: List[Dict[str, Any]] = field(default_factory=list, init=False)
    _lines_written_in_part: int = field(default=0, init=False)
    _part: int = field(default=0, init=False)

    def __post_init__(self) -> None:
        if not self.bucket_name:
            raise ValueError("bucket_name must be provided")
        if not self.filename:
            raise ValueError("filename must be provided")

        self.base_prefix = self._normalize_prefix(self.base_prefix)
        self.rotate_max_lines = max(0, int(self.rotate_max_lines))
        self.buffer_size = max(1, int(self.buffer_size))
        self._client = boto3.client("s3")

    @staticmethod
    def _normalize_prefix(prefix: str) -> str:
        cleaned = (prefix or "").lstrip("/")
        if cleaned and not cleaned.endswith("/"):
            return f"{cleaned}/"
        return cleaned

    def _current_key(self) -> str:
        if self.rotate_max_lines > 0 and self._part > 0:
            if "." in self.filename:
                name, ext = self.filename.rsplit(".", 1)
                return f"{self.base_prefix}{name}.part{self._part}.{ext}"
            return f"{self.base_prefix}{self.filename}.part{self._part}"
        return f"{self.base_prefix}{self.filename}"

    def _flush_buffer(self) -> None:
        if not self._buffer:
            return

        key = self._current_key()
        body = "\n".join(json.dumps(obj, ensure_ascii=False) for obj in self._buffer)

        try:
            self._client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=body.encode("utf-8"),
                ContentType=S3_CONTENT_TYPE_JSONL,
            )
        except ClientError as exc:
            # Surface a clear error to the caller; Lambda logs will capture context.
            raise RuntimeError(f"Failed to write JSONL batch to s3://{self.bucket_name}/{key}") from exc

        self._buffer.clear()

    def _maybe_rotate(self) -> None:
        if self.rotate_max_lines and self._lines_written_in_part >= self.rotate_max_lines:
            self._flush_buffer()
            self._part += 1
            self._lines_written_in_part = 0

    def write(self, obj: Dict[str, Any]) -> None:
        """
        Append a single JSON-serializable object to the current JSONL stream.
        """
        self._buffer.append(obj)
        self._lines_written_in_part += 1

        if len(self._buffer) >= self.buffer_size:
            self._flush_buffer()

        self._maybe_rotate()

    def flush(self) -> None:
        """
        Flush any buffered records to S3.
        """
        self._flush_buffer()

    def close(self) -> None:
        """
        Flush remaining records; included for API compatibility with JsonlWriter.
        """
        self.flush()

    def __enter__(self) -> "S3JsonlWriter":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


