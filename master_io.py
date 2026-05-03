"""
Safe I/O helpers for the master bet-tracker spreadsheet.

Three protections layered together so a single bad save can't destroy the file:

1. ``master_lock(path)`` — advisory ``fcntl`` flock on a sibling lock file,
   serialising writers across processes (hourly bookie_grabber vs. 4×/day
   results_updater / sm_odds_updater).

2. ``safe_save_workbook(wb, path)`` — atomic save: writes to ``<path>.tmp``
   then ``os.replace()`` over the live file. APFS guarantees the rename is
   atomic, so a crash, kill, or disk-full during ``wb.save`` leaves the live
   file untouched.

3. Free-space pre-flight inside ``safe_save_workbook`` — if the destination
   volume has less than ``MIN_FREE_BYTES`` available, raises
   ``InsufficientSpaceError`` *before* touching anything.
"""

from __future__ import annotations

import fcntl
import logging
import os
import shutil
import time
from contextlib import contextmanager
from pathlib import Path

logger = logging.getLogger(__name__)

MIN_FREE_BYTES = 200 * 1024 * 1024      # 200 MB headroom for the save
LOCK_TIMEOUT_SECONDS = 120.0            # wait up to 2 min for another writer


class InsufficientSpaceError(OSError):
    """Raised when the destination volume has too little free space to save."""


def _lock_path(master_path: Path) -> Path:
    return master_path.parent / ("." + master_path.name + ".lock")


@contextmanager
def master_lock(master_path: Path, timeout_s: float = LOCK_TIMEOUT_SECONDS):
    """Acquire an exclusive advisory lock on the master file.

    Multiple writers (bet_tracker_updater, results_updater, sm_odds_updater)
    can collide on hours like 7:00/13:00/19:00. The lock makes them queue
    instead of stomping on each other's in-memory openpyxl copies.
    """
    lp = _lock_path(Path(master_path))
    lp.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(str(lp), os.O_CREAT | os.O_RDWR, 0o644)
    deadline = time.monotonic() + timeout_s
    waited = False
    try:
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                waited = True
                if time.monotonic() >= deadline:
                    raise TimeoutError(
                        f"master_lock: could not acquire {lp} within {timeout_s:.0f}s"
                    )
                time.sleep(0.5)
        if waited:
            logger.info(f"master_lock: acquired after waiting (lock={lp})")
        yield
    finally:
        try:
            fcntl.flock(fd, fcntl.LOCK_UN)
        finally:
            os.close(fd)


def check_free_space(path: Path, min_free_bytes: int = MIN_FREE_BYTES) -> int:
    """Return free bytes on the volume containing ``path``; raise if too low."""
    free = shutil.disk_usage(Path(path).parent).free
    if free < min_free_bytes:
        raise InsufficientSpaceError(
            f"Only {free / 1e6:.0f} MB free on {path.parent}, "
            f"need >= {min_free_bytes / 1e6:.0f} MB"
        )
    return free


def safe_save_workbook(wb, path, min_free_bytes: int = MIN_FREE_BYTES) -> None:
    """Save an openpyxl workbook atomically, after a free-space pre-flight.

    On success the live file is replaced atomically.
    On any failure (disk full, kill, exception in ``wb.save``) the live file
    is untouched and any partial tmp file is cleaned up.
    """
    path = Path(path)
    check_free_space(path, min_free_bytes)

    tmp = path.with_name(path.name + ".tmp")
    try:
        wb.save(str(tmp))
        # os.replace is atomic on POSIX when src and dst are on the same volume,
        # which they always are here (same parent directory).
        os.replace(str(tmp), str(path))
    except Exception:
        try:
            if tmp.exists():
                tmp.unlink()
        except Exception:
            logger.exception(f"safe_save_workbook: failed to remove partial tmp {tmp}")
        raise
