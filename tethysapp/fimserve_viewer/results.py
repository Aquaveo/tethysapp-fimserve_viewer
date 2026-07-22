"""Storage and naming of generated flood-map results.

Results are published to Django's default file storage, so they live on S3
when the portal configures it and on the local filesystem otherwise. Every
replica sees the same results either way.
"""

import fnmatch
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Optional

from django.core.files import File
from django.core.files.base import ContentFile
from django.core.files.storage import default_storage
from django.http import FileResponse


def sanitize_discharge(discharge_val: float) -> str:
    """Discharge value as the token used in result file names."""
    return str(discharge_val).replace(".", "_").replace("-", "m")


def nwm_pattern(huc8: str, date_str: str) -> str:
    """Filename pattern of an NWM result for a date or exact timestamp."""
    if len(date_str) == 10:
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        return f"NWM_{date_obj.strftime('%Y%m%d')}*_{huc8}_inundation.tif"
    date_obj = datetime.strptime(date_str, "%Y-%m-%d-%H-%M-%S")
    return f"NWM_{date_obj.strftime('%Y%m%d%H%M%S')}_{huc8}_inundation.tif"


def custom_pattern(huc8: str, discharge_val: float) -> str:
    """Filename pattern of a custom-discharge result."""
    return f"CustomQ_{sanitize_discharge(discharge_val)}_{huc8}_inundation.tif"


def labels_name_for_tif(tif_name: str) -> str:
    """Streamflow-labels filename co-named with a result tif."""
    return tif_name.replace("_inundation.tif", "_qlabels.geojson")


def nwm_labels_pattern(huc8: str, date_str: str) -> str:
    """Filename pattern of the stored streamflow-labels GeoJSON."""
    return labels_name_for_tif(nwm_pattern(huc8, date_str))


class ResultStorage:
    """Publishes and retrieves result tifs through default storage."""

    prefix = "fimserve_viewer/results"

    def key_for(self, huc8: str, filename: str) -> str:
        """Storage key of one result file."""
        return f"{self.prefix}/{huc8}/{filename}"

    def store(self, map_file: Path, huc8: str) -> str:
        """Publish a generated tif, remove the local copy, and return its key."""
        map_file = Path(map_file)
        key = self.key_for(huc8, map_file.name)
        if default_storage.exists(key):
            default_storage.delete(key)
        with map_file.open("rb") as handle:
            default_storage.save(key, File(handle))
        map_file.unlink()
        return key

    def store_text(self, text: str, huc8: str, filename: str) -> str:
        """Publish a text artifact (e.g. a labels GeoJSON) and return its key."""
        key = self.key_for(huc8, filename)
        if default_storage.exists(key):
            default_storage.delete(key)
        default_storage.save(key, ContentFile(text.encode("utf-8")))
        return key

    def text(self, key: str) -> Optional[str]:
        """Return a stored text artifact, or None if it is absent."""
        if not default_storage.exists(key):
            return None
        with default_storage.open(key, "rb") as handle:
            return handle.read().decode("utf-8")

    def find(self, huc8: str, pattern: str) -> Optional[str]:
        """Key of the newest stored result matching a filename pattern."""
        directory = f"{self.prefix}/{huc8}"
        try:
            names = default_storage.listdir(directory)[1]
        except (FileNotFoundError, NotADirectoryError, OSError):
            return None
        matches = sorted(name for name in names if fnmatch.fnmatch(name, pattern))
        if not matches:
            return None
        return f"{directory}/{matches[-1]}"

    def response(self, key: str, download_name: str = "") -> FileResponse:
        """Stream one stored result tif."""
        handle = default_storage.open(key, "rb")
        if download_name:
            return FileResponse(
                handle, content_type="image/tiff", as_attachment=True, filename=download_name
            )
        return FileResponse(handle, content_type="image/tiff")

    @contextmanager
    def local(self, key: str):
        """Yield a temporary local copy of a stored result for raster work."""
        with default_storage.open(key, "rb") as source:
            with NamedTemporaryFile(suffix=".tif", delete=False) as target:
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    target.write(chunk)
                path = Path(target.name)
        try:
            yield path
        finally:
            path.unlink(missing_ok=True)


results = ResultStorage()
