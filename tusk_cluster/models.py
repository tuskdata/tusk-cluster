"""Models for cluster mode"""

from datetime import datetime
from typing import Literal, Any
import msgspec

# Type aliases
JobStatus = Literal["pending", "running", "completed", "failed", "cancelled"]
WorkerStatus = Literal["idle", "busy", "offline"]


class Job(msgspec.Struct):
    """A distributed query job"""
    id: str
    sql: str
    status: JobStatus = "pending"
    created_at: datetime = msgspec.field(default_factory=datetime.now)
    started_at: datetime | None = None
    completed_at: datetime | None = None
    progress: float = 0.0
    stages_total: int = 1
    stages_completed: int = 0
    rows_processed: int = 0
    bytes_processed: int = 0
    error: str | None = None
    worker_id: str | None = None
    result: Any = None
    retry_count: int = 0
    max_retries: int = 3
    next_retry_at: datetime | None = None


class WorkerInfo(msgspec.Struct):
    """Information about a worker node"""
    id: str
    address: str
    port: int
    status: WorkerStatus = "idle"
    cpu_percent: float = 0.0
    memory_mb: float = 0.0
    memory_percent: float = 0.0
    last_heartbeat: datetime = msgspec.field(default_factory=datetime.now)
    jobs_completed: int = 0
    bytes_processed: int = 0
    active_jobs: int = 0


class ClusterStatus(msgspec.Struct):
    """Overall cluster status"""
    scheduler_address: str
    scheduler_port: int
    workers_online: int
    workers_total: int
    active_jobs: int
    completed_jobs: int
    total_bytes_processed: int = 0
    uptime_seconds: float = 0.0


class JobSubmission(msgspec.Struct):
    """Request to submit a new job"""
    sql: str
    source_files: list[str] | None = None


class JobResult(msgspec.Struct):
    """Result of a completed job"""
    job_id: str
    columns: list[dict]
    rows: list[tuple]
    row_count: int
    execution_time_ms: float
