from __future__ import annotations

import io
import json
import os
from datetime import datetime
from typing import Optional


def create_timestamped_dir(base_dir: Optional[str]) -> str:
    base = base_dir or "data"
    ts = datetime.utcnow().strftime("%Y-%m-%d_%H%M%S")
    out_dir = os.path.join(base, ts)
    os.makedirs(out_dir, exist_ok=True)
    return out_dir


class JsonlWriter:
    """JSON Lines writer with optional rotation by max_lines.

    A new file is opened when the current file reaches `rotate_max_lines` lines.
    If rotate_max_lines is 0 or None, rotation is disabled.
    """

    def __init__(
        self,
        base_path: str,
        filename: str,
        rotate_max_lines: int = 100_000,
    ) -> None:
        self.base_path = base_path
        self.filename = filename
        self.rotate_max_lines = max(0, int(rotate_max_lines))
        self._file: Optional[io.TextIOWrapper] = None
        self._lines_written = 0
        self._part = 0
        os.makedirs(base_path, exist_ok=True)
        self._open_new_file()

    def _current_path(self) -> str:
        if self.rotate_max_lines > 0 and self._part > 0:
            name, ext = os.path.splitext(self.filename)
            return os.path.join(self.base_path, f"{name}.part{self._part}{ext}")
        return os.path.join(self.base_path, self.filename)

    def _open_new_file(self) -> None:
        if self._file:
            self._file.close()
        path = self._current_path()
        self._file = open(path, mode="w", encoding="utf-8", newline="\n")
        self._lines_written = 0

    def _maybe_rotate(self) -> None:
        if self.rotate_max_lines and self._lines_written >= self.rotate_max_lines:
            self._part += 1
            self._open_new_file()

    def write(self, obj: dict) -> None:
        if not self._file:
            self._open_new_file()
        line = json.dumps(obj, ensure_ascii=False)
        self._file.write(line + "\n")
        self._lines_written += 1
        self._maybe_rotate()

    def flush(self) -> None:
        if self._file:
            self._file.flush()

    def close(self) -> None:
        if self._file:
            try:
                self._file.flush()
            finally:
                self._file.close()
                self._file = None

    def __enter__(self) -> "JsonlWriter":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


