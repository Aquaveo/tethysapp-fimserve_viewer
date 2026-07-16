"""Job records and file-backed persistence for background flood-map jobs.

Each job is stored as one JSON file under ``<FIMSERV_ROOT>/jobs/`` so state
survives portal restarts without requiring a database table.
"""

import json
import threading
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional


class JobKind:
    NWM = "nwm"
    CUSTOM = "custom"


class JobStatus:
    QUEUED = "queued"
    STEP1 = "step1"
    STEP2 = "step2"
    STEP3 = "step3"
    SUCCESS = "success"
    ERROR = "error"
    INTERRUPTED = "interrupted"

    ACTIVE = frozenset({QUEUED, STEP1, STEP2, STEP3})


def utcnow() -> str:
    """Current UTC time as an ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


@dataclass
class Job:
    """A single background generation request and its lifecycle state."""

    id: str
    key: str
    kind: str
    huc8: str
    params: dict
    status: str = JobStatus.QUEUED
    message: str = ""
    result_file: str = ""
    created_at: str = field(default_factory=utcnow)
    updated_at: str = field(default_factory=utcnow)

    def is_active(self) -> bool:
        return self.status in JobStatus.ACTIVE

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Job":
        return cls(**data)

    @classmethod
    def new(cls, kind: str, huc8: str, key: str, params: dict) -> "Job":
        return cls(id=uuid.uuid4().hex[:12], key=key, kind=kind, huc8=huc8, params=params)


class JobStore:
    """Reads and writes :class:`Job` records as JSON files in a directory."""

    def __init__(self, directory: Path):
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self.lock = threading.Lock()

    def path_for(self, job_id: str) -> Path:
        return self.directory / f"{job_id}.json"

    def save(self, job: Job) -> None:
        """Persist atomically (temp file + rename) so readers never see a partial file."""
        job.updated_at = utcnow()
        path = self.path_for(job.id)
        temp_path = path.with_name(path.name + ".tmp")
        with self.lock:
            temp_path.write_text(json.dumps(job.to_dict(), indent=2))
            temp_path.replace(path)

    def create(self, kind: str, huc8: str, key: str, params: dict) -> Job:
        job = Job.new(kind=kind, huc8=huc8, key=key, params=params)
        self.save(job)
        return job

    def get(self, job_id: str) -> Optional[Job]:
        path = self.path_for(job_id)
        if not path.is_file():
            return None
        return self.read_file(path)

    def read_file(self, path: Path) -> Optional[Job]:
        try:
            return Job.from_dict(json.loads(path.read_text()))
        except (ValueError, TypeError, OSError):
            return None

    def all_jobs(self) -> List[Job]:
        jobs = [self.read_file(p) for p in sorted(self.directory.glob("*.json"))]
        return [job for job in jobs if job is not None]

    def find_active(self, key: str) -> Optional[Job]:
        return next((j for j in self.all_jobs() if j.key == key and j.is_active()), None)

    def find_active_for_huc(self, huc8: str) -> Optional[Job]:
        return next((j for j in self.all_jobs() if j.huc8 == huc8 and j.is_active()), None)

    def mark_interrupted_jobs(self) -> None:
        """Flag jobs left active by a previous process as interrupted.

        Call once at manager startup, before any worker runs.
        """
        for job in self.all_jobs():
            if job.is_active():
                job.status = JobStatus.INTERRUPTED
                job.message = "The portal restarted while this job was running."
                self.save(job)
