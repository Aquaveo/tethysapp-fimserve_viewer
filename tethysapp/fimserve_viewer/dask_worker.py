"""Bootstrap Django on a Dask worker so offloaded jobs can reach the app database and storage.

A bare ``dask worker`` process never runs the image entrypoint, so Django is
unconfigured and any ``tethysapp`` import that touches the ORM (``jobs_db``) or
``default_storage`` (S3 COGs) fails. Dask imports this module through
``--preload`` and calls :func:`dask_setup` once per worker process before any
task runs.
"""

import os


def dask_setup(worker):
    """Configure Django once for this worker process, before the first task.

    Dask runs preloads from the worker's async event loop, and Tethys's app
    ``ready()`` issues a synchronous ORM query (cookie sync). ``DJANGO_ALLOW_ASYNC_UNSAFE``
    lets that one-time setup query run; real task ORM access happens in worker
    threads, where the guard does not apply.
    """
    from django.apps import apps

    if apps.ready:
        return
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tethys_portal.settings")
    os.environ.setdefault("DJANGO_ALLOW_ASYNC_UNSAFE", "true")
    import django

    django.setup()
