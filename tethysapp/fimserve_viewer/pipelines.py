"""Flood-map generation pipelines executed by the background job manager.

Each runner receives the job record and a ``progress(status, message)``
callback, performs the full generation, and returns the produced tif path.
"""

from typing import Callable

from . import fim_logic
from .model import JobKind, JobStatus
from .results import results

Progress = Callable[[str, str], None]


def nwm_job_key(huc8: str, datetime_str: str) -> str:
    return f"{JobKind.NWM}:{huc8}:{datetime_str}"


def custom_job_key(huc8: str, discharge: float) -> str:
    return f"{JobKind.CUSTOM}:{huc8}:{discharge}"


def run_nwm_pipeline(job: dict, progress: Progress) -> str:
    huc8 = job["huc8"]
    datetime_str = job["params"]["datetime_str"]
    progress(JobStatus.STEP1, "Step 1/3: Downloading HUC8 hydrofabric...")
    fim_logic._run_flood_step1_download_huc8(huc8)
    progress(JobStatus.STEP2, "Step 2/3: Fetching NWM streamflow data...")
    fim_logic._run_flood_step2_nwm_streamflow(huc8, datetime_str)
    progress(JobStatus.STEP3, "Step 3/3: Computing flood inundation...")
    fim_logic._run_flood_step3_hand_inundation(huc8, datetime_str)
    map_file, missing_message = fim_logic._locate_generated_inundation_tif(huc8, datetime_str)
    if map_file is None:
        raise RuntimeError(missing_message)
    return results.store(map_file, huc8)


def run_custom_pipeline(job: dict, progress: Progress) -> str:
    progress(JobStatus.STEP3, "Computing flood inundation for custom discharge...")
    map_file = fim_logic.run_custom_discharge_flood_map(job["huc8"], float(job["params"]["discharge"]))
    return results.store(map_file, job["huc8"])
