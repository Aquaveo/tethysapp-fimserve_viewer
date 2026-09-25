"""Bootstrap Django on a Dask worker so offloaded jobs can reach the app database and storage.

A bare ``dask worker`` process never runs the image entrypoint, so Django is
unconfigured and any ``tethysapp`` import that touches the ORM (``jobs_db``) or
``default_storage`` (S3 COGs) fails. Dask imports this module through
``--preload`` and calls :func:`dask_setup` once per worker process before any
task runs.
"""

import os


def dask_setup(worker):
    """Configure Django once for this worker process, before the first task."""
    from django.apps import apps

    if apps.ready:
        return
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "tethys_portal.settings")
    import django

    django.setup()
