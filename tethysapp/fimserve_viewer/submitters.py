"""Choose where a job runs: a local thread for development, a Dask worker in production."""

from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Protocol

from .execution import JobRunner, execute_job
from .scheduler import dask_client


class JobSubmitter(Protocol):
    """Dispatches a claimed job for execution."""

    def submit(self, job_id: str, runner: JobRunner) -> None:
        """Start running the job without blocking the caller."""


class ThreadSubmitter:
    """Runs jobs one at a time in a single background thread."""

    def __init__(self):
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="fimjob")

    def submit(self, job_id: str, runner: JobRunner) -> None:
        """Queue the job behind any job already running in this process."""
        self._executor.submit(execute_job, job_id, runner)


class DaskSubmitter:
    """Sends each job to the Dask cluster for a worker to run."""

    def __init__(self, client):
        self._client = client

    def submit(self, job_id: str, runner: JobRunner) -> None:
        """Fire the job onto a Dask worker and return immediately."""
        from dask.distributed import fire_and_forget

        fire_and_forget(self._client.submit(execute_job, job_id, runner, pure=False))


_thread_submitter: Optional[ThreadSubmitter] = None


def get_submitter() -> JobSubmitter:
    """Return a Dask submitter when a scheduler is configured, else a shared thread submitter."""
    client = dask_client()
    if client is not None:
        return DaskSubmitter(client)
    global _thread_submitter
    if _thread_submitter is None:
        _thread_submitter = ThreadSubmitter()
    return _thread_submitter
