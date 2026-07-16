"""Background job manager for flood-map generation.

Runs one generation at a time in a worker thread, deduplicates identical
requests by job key, and records progress through :class:`JobStore` so the
UI can poll status and reattach after a page refresh.
"""

import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Callable, Optional, Tuple

from .job_store import Job, JobStatus, JobStore

JobRunner = Callable[[Job, Callable[[str, str], None]], str]


class JobManager:
    """Submits, executes, and tracks background generation jobs."""

    def __init__(self, store: JobStore, max_workers: int = 1):
        self.store = store
        self.executor = ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="fimjob")
        self.submit_lock = threading.Lock()
        self.store.mark_interrupted_jobs()

    def submit(self, kind: str, huc8: str, key: str, params: dict, runner: JobRunner) -> Tuple[Job, bool]:
        """Queue a job, or return the already-active job with the same key.

        Returns ``(job, created)`` where ``created`` is False when an active
        duplicate was found.
        """
        with self.submit_lock:
            existing = self.store.find_active(key)
            if existing is not None:
                return existing, False
            job = self.store.create(kind=kind, huc8=huc8, key=key, params=params)
        self.executor.submit(self.execute, job.id, runner)
        return job, True

    def execute(self, job_id: str, runner: JobRunner) -> None:
        job = self.store.get(job_id)
        if job is None:
            return

        def progress(status: str, message: str = "") -> None:
            job.status = status
            job.message = message
            self.store.save(job)

        try:
            job.result_file = runner(job, progress)
            job.status = JobStatus.SUCCESS
            job.message = "Flood map generated successfully."
        except Exception as exc:
            job.status = JobStatus.ERROR
            job.message = str(exc)
        self.store.save(job)

    def get(self, job_id: str) -> Optional[Job]:
        return self.store.get(job_id)

    def active_for_huc(self, huc8: str) -> Optional[Job]:
        return self.store.find_active_for_huc(huc8)


manager_instance: Optional[JobManager] = None
manager_lock = threading.Lock()


def jobs_directory() -> Path:
    from .fim_logic import _ensure_fimserv_root_env

    return Path(_ensure_fimserv_root_env()) / "jobs"


def get_job_manager() -> JobManager:
    """Return the process-wide :class:`JobManager`, creating it on first use."""
    global manager_instance
    with manager_lock:
        if manager_instance is None:
            manager_instance = JobManager(JobStore(jobs_directory()))
        return manager_instance
