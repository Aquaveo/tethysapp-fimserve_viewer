"""Database model for background flood-map jobs."""

import uuid
from datetime import datetime, timezone

from sqlalchemy import JSON, Column, DateTime, Index, String, Text, text
from sqlalchemy.orm import declarative_base

Base = declarative_base()


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

    ACTIVE = (QUEUED, STEP1, STEP2, STEP3)


def utcnow() -> datetime:
    """Current timezone-aware UTC time."""
    return datetime.now(timezone.utc)


def new_job_id() -> str:
    """Short unique identifier for a job."""
    return uuid.uuid4().hex[:12]


class Job(Base):
    """A single background generation request and its lifecycle state."""

    __tablename__ = "jobs"

    id = Column(String(12), primary_key=True, default=new_job_id)
    key = Column(String(255), nullable=False)
    kind = Column(String(16), nullable=False)
    huc8 = Column(String(8), nullable=False)
    params = Column(JSON, nullable=False, default=dict)
    status = Column(String(16), nullable=False, default=JobStatus.QUEUED)
    message = Column(Text, nullable=False, default="")
    result_file = Column(Text, nullable=False, default="")
    claimed_by = Column(String(255), nullable=False, default="")
    heartbeat_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    created_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)
    updated_at = Column(DateTime(timezone=True), nullable=False, default=utcnow)

    __table_args__ = (
        Index(
            "uq_jobs_active_key",
            "key",
            unique=True,
            postgresql_where=text("status IN ('queued', 'step1', 'step2', 'step3')"),
            sqlite_where=text("status IN ('queued', 'step1', 'step2', 'step3')"),
        ),
    )

    def is_active(self) -> bool:
        return self.status in JobStatus.ACTIVE

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "key": self.key,
            "kind": self.kind,
            "huc8": self.huc8,
            "params": self.params,
            "status": self.status,
            "message": self.message,
            "result_file": self.result_file,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
        }


def init_jobs_db(engine, first_time):
    """Create the job tables in the app persistent store."""
    Base.metadata.create_all(engine)
