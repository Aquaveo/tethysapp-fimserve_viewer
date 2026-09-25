"""Access the app's Dask scheduler for offloading jobs to workers."""

import logging

log = logging.getLogger(__name__)

_client = None


def _scheduler():
    """Return the configured dask_primary scheduler, or None when unavailable.

    A None return means the scheduler is simply unassigned (development); a
    failure to look it up is logged so a broken production scheduler is visible
    rather than silently degrading to in-process execution.
    """
    from tethys_apps.exceptions import TethysAppSettingNotAssigned

    from .app import App, DASK_SCHEDULER_NAME

    try:
        return App.get_scheduler(DASK_SCHEDULER_NAME)
    except TethysAppSettingNotAssigned:
        return None
    except Exception:
        log.warning(
            "Dask scheduler '%s' could not be resolved; running generation in-process.",
            DASK_SCHEDULER_NAME,
            exc_info=True,
        )
        return None


def dask_client():
    """Return the cached Dask client for the configured scheduler, or None.

    The client is resolved once and reused, so a job submission does not pay a
    fresh scheduler handshake each time.
    """
    global _client
    if _client is None:
        scheduler = _scheduler()
        if scheduler is not None:
            _client = scheduler.client
    return _client
