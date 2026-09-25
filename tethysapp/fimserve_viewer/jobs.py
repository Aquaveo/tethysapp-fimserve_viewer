"""Submit flood-map generation jobs and read their status.

Deduplication and ownership live in the jobs_db persistent store. A submitter
decides where a job runs: a Dask worker when a scheduler is configured,
otherwise a local thread. Execution itself lives in :mod:`execution`.
"""

from typing import Optional, Tuple

from .execution import JobRunner, process_store
from .model import JobStatus
from .submitters import get_submitter

STALE_TIMEOUT_SECONDS = 180


def submit_job(kind: str, huc8: str, key: str, params: dict, runner: JobRunner) -> Tuple[dict, bool]:
    """Queue a job, or return the active job that already has the same key.

    Returns ``(job, created)``; ``created`` is False when an active duplicate
    was found. A dispatch failure marks the new job errored before raising, so
    it never lingers as a phantom queued row.
    """
    store = process_store()
    store.mark_stale_interrupted(STALE_TIMEOUT_SECONDS)
    job, created = store.create_or_get_active(kind=kind, huc8=huc8, key=key, params=params)
    if created:
        try:
            get_submitter().submit(job["id"], runner)
        except Exception as exc:
            store.finish(job["id"], JobStatus.ERROR, f"Could not start generation: {exc}")
            raise
    return job, created


def active_for_huc(huc8: str) -> Optional[dict]:
    """Return the active job for a HUC8, or None."""
    store = process_store()
    store.mark_stale_interrupted(STALE_TIMEOUT_SECONDS)
    return store.find_active_for_huc(huc8)


def get_job(job_id: str) -> Optional[dict]:
    """Return one job by id, or None."""
    return process_store().get(job_id)
