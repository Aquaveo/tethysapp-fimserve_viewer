"""Submit flood-map generation jobs and read their status.

Deduplication and ownership live in the jobs_db persistent store. A submitter
decides where a job runs: a Dask worker when a scheduler is configured,
otherwise a local thread. Execution itself lives in :mod:`execution`.
"""

from typing import Optional, Tuple

from .execution import JobRunner, process_store
from .submitters import get_submitter

STALE_TIMEOUT_SECONDS = 180


def submit_job(kind: str, huc8: str, key: str, params: dict, runner: JobRunner) -> Tuple[dict, bool]:
    """Queue a job, or return the active job that already has the same key.

    Returns ``(job, created)``; ``created`` is False when an active duplicate
    was found. A dispatch failure errors the new job only while it is still
    unclaimed, so it neither lingers as a phantom queued row nor stomps a job a
    worker already picked up, and the caller gets the errored job rather than a
    server error.
    """
    store = process_store()
    store.mark_stale_interrupted(STALE_TIMEOUT_SECONDS)
    job, created = store.create_or_get_active(kind=kind, huc8=huc8, key=key, params=params)
    if created:
        try:
            get_submitter().submit(job["id"], runner)
        except Exception as exc:
            store.fail_if_unclaimed(job["id"], f"Could not start generation: {exc}")
            job = store.get(job["id"]) or job
    return job, created


def active_for_huc(huc8: str) -> Optional[dict]:
    """Return the active job for a HUC8, or None."""
    store = process_store()
    store.mark_stale_interrupted(STALE_TIMEOUT_SECONDS)
    return store.find_active_for_huc(huc8)


def get_job(job_id: str) -> Optional[dict]:
    """Return one job by id, sweeping stale jobs so a dead worker's job resolves.

    The status endpoint is the only thing a client polls, so the sweep must run
    here or a worker that died mid-run would leave the job non-terminal forever.
    """
    store = process_store()
    store.mark_stale_interrupted(STALE_TIMEOUT_SECONDS)
    return store.get(job_id)
