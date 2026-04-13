"""General file system utilities for the FMS workflow."""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Iterator


def ensure_dir(path: str | Path) -> Path:
    """Create *path* (and parents) if it does not exist; return it as a Path."""
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def glob_files(folder: str | Path, pattern: str) -> list[Path]:
    """Return sorted list of files matching *pattern* under *folder*."""
    return sorted(Path(folder).glob(pattern))


def file_age_minutes(path: str | Path) -> float:
    """Return the age of *path* in minutes based on its modification time."""
    import time

    mtime = Path(path).stat().st_mtime
    return (time.time() - mtime) / 60


def md5(path: str | Path, chunk_size: int = 65536) -> str:
    """Compute the MD5 hex digest of a file."""
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            h.update(chunk)
    return h.hexdigest()


def safe_remove(path: str | Path) -> bool:
    """
    Delete a file without raising if it does not exist.

    Returns True if the file was deleted, False if it was not found.
    """
    try:
        Path(path).unlink()
        return True
    except FileNotFoundError:
        return False


def walk_files(folder: str | Path, extension: str = "") -> Iterator[Path]:
    """
    Recursively yield files under *folder*, optionally filtered by *extension*.

    Parameters
    ----------
    folder : str | Path
        Root directory to walk.
    extension : str
        File extension filter including the leading dot (e.g. ``".snp"``).
        Pass empty string to yield all files.
    """
    for root, _, files in os.walk(folder):
        for name in files:
            if not extension or name.endswith(extension):
                yield Path(root) / name
