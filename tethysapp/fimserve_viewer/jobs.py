"""Background job manager for flood-map generation.

Runs one generation at a time in a worker thread. Deduplication and the
right to execute a job are both enforced in the database, so any number of
portal replicas can run a manager safely.
"""

import os
import socket
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Callable, Optional, Tuple

from .job_store import JobStore
from .model import JobStatus

JobRunner = Callable[[dict, Callable[[str, str], None]], str]

HEARTBEAT_SECONDS = 30
STALE_TIMEOUT_SECONDS = 180


def worker_identity() -> str:
    """Identity of this replica and process, recorded on claimed jobs."""
    return f"{socket.gethostname()}:{os.getpid()}"


class Heartbeat:
    """Periodically refreshes a job's heartbeat while its pipeline runs."""

    def __init__(self, store: JobStore, job_id: str, interval: int = HEARTBEAT_SECONDS):
        self.store = store
        self.job_id = job_id
        self.interval = interval
        self.stopped = threading.Event()
        self.thread = threading.Thread(target=self.run, daemon=True)

    def run(self):
        while not self.stopped.wait(self.interval):
            self.store.touch(self.job_id)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *exc_info):
        self.stopped.set()


class JobManager:
    """Submits, executes, and tracks background generation jobs."""

    def __init__(self, store: Optional[JobStore] = None, max_workers: int = 1):
        self.store = store or JobStore()
        self.executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="fimjob")

    def submit(self, kind: str, huc8: str, key: str, params: dict, runner: JobRunner) -> Tuple[dict, bool]:
        """Queue a job, or return the already-active job with the same key.

        Returns ``(job, created)`` where ``created`` is False when an active
        duplicate was found.
        """
        self.store.mark_stale_interrupted(STALE_TIMEOUT_SECONDS)
        job, created = self.store.create_or_get_active(kind=kind, huc8=huc8, key=key, params=params)
        if created:
            self.executor.submit(self.execute, job["id"], runner)
        return job, created

    def execute(self, job_id: str, runner: JobRunner) -> None:
        """Run one claimed job to completion, recording progress and outcome."""
        if not self.store.claim(job_id, worker_identity()):
            return
        job = self.store.get(job_id)
        if job is None:
            return

        def progress(status: str, message: str = "") -> None:
            self.store.update_progress(job_id, status, message)

        with Heartbeat(self.store, job_id):
            try:
                result_file = runner(job, progress)
                self.store.finish(job_id, JobStatus.SUCCESS, "Flood map generated successfully.", result_file)
            except Exception as exc:
                self.store.finish(job_id, JobStatus.ERROR, str(exc))

    def get(self, job_id: str) -> Optional[dict]:
        return self.store.get(job_id)

    def active_for_huc(self, huc8: str) -> Optional[dict]:
        self.store.mark_stale_interrupted(STALE_TIMEOUT_SECONDS)
        return self.store.find_active_for_huc(huc8)


manager_instance: Optional[JobManager] = None
manager_lock = threading.Lock()


def get_job_manager() -> JobManager:
    """Return the process-wide :class:`JobManager`, creating it on first use."""
    global manager_instance
    with manager_lock:
        if manager_instance is None:
            manager_instance = JobManager()
        return manager_instance
