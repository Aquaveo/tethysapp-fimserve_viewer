"""Run one flood-map generation job to completion, wherever it is executed.

These functions are worker safe: they hold no web process state and reach the
shared jobs_db and S3 through the app, so a Dask worker and a local thread run
them identically. The job store is cached per process and reused.
"""

import os
import socket
import threading
from contextlib import suppress
from typing import Callable, Optional

from .job_store import JobStore
from .model import JobStatus

JobRunner = Callable[[dict, Callable[[str, str], None]], str]

HEARTBEAT_SECONDS = 30

_store: Optional[JobStore] = None
_store_lock = threading.Lock()


def process_store() -> JobStore:
    """Return this process's job store, created once on first use."""
    global _store
    if _store is None:
        with _store_lock:
            if _store is None:
                _store = JobStore()
    return _store


def worker_identity() -> str:
    """Return the host and process identifier recorded on a claimed job."""
    return f"{socket.gethostname()}:{os.getpid()}"


def _beat(store: JobStore, owner: str, stop: threading.Event) -> None:
    """Refresh the owner's active job heartbeats until stop is set."""
    while not stop.wait(HEARTBEAT_SECONDS):
        with suppress(Exception):
            store.touch_owned(owner)


def execute_job(job_id: str, runner: JobRunner) -> None:
    """Claim a job, run its pipeline with progress updates, and record the outcome."""
    store = process_store()
    owner = worker_identity()
    if not store.claim(job_id, owner):
        return
    job = store.get(job_id)
    if job is None:
        return
    stop = threading.Event()
    threading.Thread(target=_beat, args=(store, owner, stop), daemon=True).start()
    try:
        result_file = runner(job, lambda status, message="": store.update_progress(job_id, status, message))
        store.finish(job_id, JobStatus.SUCCESS, "Flood map generated successfully.", result_file)
    except Exception as exc:
        store.finish(job_id, JobStatus.ERROR, str(exc))
    finally:
        stop.set()
