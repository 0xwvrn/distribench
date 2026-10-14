from enum import Enum, IntEnum
from typing import Any, Dict, List, Optional
from datetime import datetime, timezone
import uuid
from pydantic import BaseModel, Field

class TaskStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    TIMEOUT = "TIMEOUT"
    CANCELLED = "CANCELLED"

class TaskPriority(IntEnum):
    LOW = 1
    NORMAL = 5
    HIGH = 8
    CRITICAL = 10

class WorkerStatus(str, Enum):
    IDLE = "IDLE"
    BUSY = "BUSY"
    OFFLINE = "OFFLINE"

class Task(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    job_id: Optional[str] = None
    task_type: str = Field(..., description="e.g. 'eval', 'data_transform', 'sim_failure'")
    payload: Dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=TaskPriority.NORMAL, ge=1, le=10)
    timeout_seconds: int = Field(default=60, ge=1)
    max_retries: int = Field(default=2, ge=0)
    retry_count: int = Field(default=0)
    status: TaskStatus = Field(default=TaskStatus.PENDING)
    assigned_worker: Optional[str] = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    log_file: Optional[str] = None

class Job(BaseModel):
    id: str = Field(default_factory=lambda: f"job-{uuid.uuid4().hex[:6]}")
    name: str = "batch-job"
    task_ids: List[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    status: TaskStatus = Field(default=TaskStatus.PENDING)

class WorkerHeartbeat(BaseModel):
    worker_id: str
    hostname: str
    status: WorkerStatus = WorkerStatus.IDLE
    current_task_id: Optional[str] = None
    cpu_percent: float = 0.0
    memory_percent: float = 0.0
    last_seen: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

class FailureCategory(str, Enum):
    RESOURCE_EXHAUSTION = "RESOURCE_EXHAUSTION"
    TIMEOUT = "TIMEOUT"                          
    LOGIC_ASSERTION = "LOGIC_ASSERTION"          
    DEPENDENCY_ERROR = "DEPENDENCY_ERROR"      
    UNKNOWN = "UNKNOWN"

class TriageReport(BaseModel):
    task_id: str
    category: FailureCategory
    summary: str = Field(..., description="Brief one-line summary of what went wrong")
    root_cause: str = Field(..., description="Detailed explanation of the failure")
    suggested_fix: str = Field(..., description="Actionable recommendation to resolve the issue")
    confidence: float = Field(default=1.0, ge=0.0, le=1.0)
    triaged_by: str = Field(default="gemini-2.0-flash")