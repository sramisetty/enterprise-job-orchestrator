"""
Job-related data models for Enterprise Job Orchestrator

Defines the core data structures for jobs, job types, priorities, and processing strategies.
"""

from enum import Enum
from datetime import datetime
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field


class JobStatus(Enum):
    """Job execution status enumeration."""
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    TIMEOUT = "timeout"
    RETRYING = "retrying"


class JobType(Enum):
    """Job type enumeration."""
    # Data Processing Types
    DATA_PROCESSING = "data_processing"
    BATCH_PROCESSING = "batch_processing"
    STREAM_PROCESSING = "stream_processing"

    # Analytics and ML Types
    MACHINE_LEARNING = "machine_learning"
    DATA_ANALYSIS = "data_analysis"
    MODEL_TRAINING = "model_training"
    MODEL_INFERENCE = "model_inference"

    # Data Quality Types
    DATA_VALIDATION = "data_validation"
    DATA_QUALITY = "data_quality"
    DATA_CLEANING = "data_cleaning"
    DEDUPLICATION = "deduplication"

    # Identity and Matching Types
    IDENTITY_MATCHING = "identity_matching"
    ENTITY_RESOLUTION = "entity_resolution"
    RECORD_LINKAGE = "record_linkage"
    HOUSEHOLD_DETECTION = "household_detection"

    # ETL and Integration Types
    ETL_PIPELINE = "etl_pipeline"
    DATA_MIGRATION = "data_migration"
    DATA_SYNCHRONIZATION = "data_synchronization"
    BULK_EXPORT = "bulk_export"
    BULK_IMPORT = "bulk_import"

    # Workflow Types
    CUSTOM_WORKFLOW = "custom_workflow"
    SCHEDULED_TASK = "scheduled_task"
    NOTIFICATION_JOB = "notification_job"

    # System Types
    SYSTEM_MAINTENANCE = "system_maintenance"
    BACKUP_JOB = "backup_job"
    CLEANUP_JOB = "cleanup_job"


class JobPriority(Enum):
    """Job priority enumeration."""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    URGENT = "urgent"
    CRITICAL = "critical"


class ProcessingStrategy(Enum):
    """Processing strategy enumeration."""
    SEQUENTIAL = "sequential"
    PARALLEL_CHUNKS = "parallel_chunks"
    STREAMING = "streaming"
    HYBRID = "hybrid"
    MAP_REDUCE = "map_reduce"
    CUSTOM = "custom"


@dataclass
class Job:
    """Core job data model."""

    # Primary identification
    job_id: str
    job_name: str
    job_type: JobType

    # Job configuration
    priority: JobPriority = JobPriority.NORMAL
    processing_strategy: ProcessingStrategy = ProcessingStrategy.PARALLEL_CHUNKS
    config: Dict[str, Any] = field(default_factory=dict)

    # Status tracking
    status: JobStatus = JobStatus.PENDING

    # Data source configuration
    input_config: Optional[Dict[str, Any]] = None
    output_config: Optional[Dict[str, Any]] = None

    # Metadata
    created_by: str = "system"
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)

    # Job scheduling
    scheduled_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    # Dependencies
    depends_on: List[str] = field(default_factory=list)

    # Resource requirements
    resource_requirements: Dict[str, Any] = field(default_factory=dict)

    # Tags for organization
    tags: List[str] = field(default_factory=list)

    # Progress tracking
    progress_percent: float = 0.0
    records_processed: int = 0
    records_total: int = 0

    # Error handling
    retry_count: int = 0
    max_retries: int = 3
    error_message: Optional[str] = None

    # Soft delete
    deleted_at: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert job to dictionary for serialization."""
        return {
            "job_id": self.job_id,
            "job_name": self.job_name,
            "job_type": self.job_type.value,
            "priority": self.priority.value,
            "processing_strategy": self.processing_strategy.value,
            "config": self.config,
            "status": self.status.value,
            "input_config": self.input_config,
            "output_config": self.output_config,
            "created_by": self.created_by,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "scheduled_at": self.scheduled_at.isoformat() if self.scheduled_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "depends_on": self.depends_on,
            "resource_requirements": self.resource_requirements,
            "tags": self.tags,
            "progress_percent": self.progress_percent,
            "records_processed": self.records_processed,
            "records_total": self.records_total,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "error_message": self.error_message,
            "deleted_at": self.deleted_at.isoformat() if self.deleted_at else None
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "Job":
        """Create job from dictionary."""
        # Parse datetime fields
        for field_name in ["created_at", "updated_at", "scheduled_at", "started_at", "completed_at", "deleted_at"]:
            if data.get(field_name):
                data[field_name] = datetime.fromisoformat(data[field_name])

        # Parse enum fields
        data["job_type"] = JobType(data["job_type"])
        data["priority"] = JobPriority(data["priority"])
        data["processing_strategy"] = ProcessingStrategy(data["processing_strategy"])
        if "status" in data:
            data["status"] = JobStatus(data["status"])

        return cls(**data)

    def is_active(self) -> bool:
        """Check if job is in an active state."""
        return self.deleted_at is None

    def can_be_cancelled(self) -> bool:
        """Check if job can be cancelled."""
        return self.deleted_at is None and self.status not in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]

    def get_duration(self) -> Optional[float]:
        """Get job duration in seconds if completed."""
        if self.started_at and self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None

    def estimate_resources(self) -> Dict[str, Any]:
        """Estimate resource requirements based on job configuration."""
        base_resources = {
            "memory_mb": 4096,
            "cpu_cores": 4,
            "disk_gb": 100,
            "network_mbps": 100
        }

        # Merge with specified requirements
        resources = base_resources.copy()
        resources.update(self.resource_requirements)

        # Adjust based on processing strategy
        if self.processing_strategy == ProcessingStrategy.PARALLEL_CHUNKS:
            resources["memory_mb"] = int(resources["memory_mb"] * 2.0)
            resources["cpu_cores"] = int(resources["cpu_cores"] * 2.0)
        elif self.processing_strategy == ProcessingStrategy.HYBRID:
            resources["memory_mb"] = int(resources["memory_mb"] * 3.0)
            resources["cpu_cores"] = int(resources["cpu_cores"] * 3.0)
        elif self.processing_strategy == ProcessingStrategy.STREAMING:
            resources["memory_mb"] = int(resources["memory_mb"] * 0.75)

        return resources

    def update_progress(self, processed: int, total: Optional[int] = None):
        """Update job progress."""
        self.records_processed = processed
        if total is not None:
            self.records_total = total

        if self.records_total > 0:
            self.progress_percent = (self.records_processed / self.records_total) * 100

        self.updated_at = datetime.utcnow()


@dataclass
class JobTemplate:
    """Job template for creating predefined job configurations."""

    template_id: str
    template_name: str
    job_type: JobType
    processing_strategy: ProcessingStrategy = ProcessingStrategy.PARALLEL_CHUNKS
    default_config: Dict[str, Any] = field(default_factory=dict)
    description: Optional[str] = None
    version: str = "1.0.0"
    is_active: bool = True
    usage_count: int = 0
    last_used_at: Optional[datetime] = None
    created_by: str = "system"
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    tags: List[str] = field(default_factory=list)
    category: Optional[str] = None

    def create_job(self, job_name: str, created_by: str, **overrides) -> Job:
        """Create a job from this template."""
        job_config = self.default_config.copy()
        job_config.update(overrides.get("config", {}))

        return Job(
            job_id=overrides.get("job_id", f"job_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"),
            job_name=job_name,
            job_type=self.job_type,
            processing_strategy=overrides.get("processing_strategy", self.processing_strategy),
            config=job_config,
            created_by=created_by,
            input_config=overrides.get("input_config"),
            output_config=overrides.get("output_config"),
            resource_requirements=overrides.get("resource_requirements", {}),
            tags=overrides.get("tags", self.tags.copy()),
            priority=overrides.get("priority", JobPriority.NORMAL)
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert template to dictionary."""
        return {
            "template_id": self.template_id,
            "template_name": self.template_name,
            "job_type": self.job_type.value,
            "processing_strategy": self.processing_strategy.value,
            "default_config": self.default_config,
            "description": self.description,
            "version": self.version,
            "is_active": self.is_active,
            "usage_count": self.usage_count,
            "last_used_at": self.last_used_at.isoformat() if self.last_used_at else None,
            "created_by": self.created_by,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "tags": self.tags,
            "category": self.category
        }


@dataclass
class JobMetrics:
    """Job performance metrics."""

    job_id: str
    metric_name: str
    metric_value: float
    metric_unit: Optional[str] = None
    metric_type: str = "gauge"
    timestamp: datetime = field(default_factory=datetime.utcnow)
    tags: Dict[str, str] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary."""
        return {
            "job_id": self.job_id,
            "metric_name": self.metric_name,
            "metric_value": self.metric_value,
            "metric_unit": self.metric_unit,
            "metric_type": self.metric_type,
            "timestamp": self.timestamp.isoformat(),
            "tags": self.tags,
            "metadata": self.metadata
        }


@dataclass
class JobAlert:
    """Job alert model for monitoring and notifications."""

    alert_id: str
    job_id: Optional[str]
    alert_type: str
    severity: str  # info, warning, error, critical
    title: str
    message: str
    component: Optional[str] = None
    worker_id: Optional[str] = None
    is_active: bool = True
    acknowledged_at: Optional[datetime] = None
    acknowledged_by: Optional[str] = None
    resolved_at: Optional[datetime] = None
    resolved_by: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    alert_data: Dict[str, Any] = field(default_factory=dict)

    def acknowledge(self, user_id: str):
        """Acknowledge the alert."""
        self.acknowledged_at = datetime.utcnow()
        self.acknowledged_by = user_id

    def resolve(self, user_id: str):
        """Resolve the alert."""
        self.resolved_at = datetime.utcnow()
        self.resolved_by = user_id
        self.is_active = False

    def to_dict(self) -> Dict[str, Any]:
        """Convert alert to dictionary."""
        return {
            "alert_id": self.alert_id,
            "job_id": self.job_id,
            "alert_type": self.alert_type,
            "severity": self.severity,
            "title": self.title,
            "message": self.message,
            "component": self.component,
            "worker_id": self.worker_id,
            "is_active": self.is_active,
            "acknowledged_at": self.acknowledged_at.isoformat() if self.acknowledged_at else None,
            "acknowledged_by": self.acknowledged_by,
            "resolved_at": self.resolved_at.isoformat() if self.resolved_at else None,
            "resolved_by": self.resolved_by,
            "created_at": self.created_at.isoformat(),
            "alert_data": self.alert_data
        }


@dataclass
class JobAuditLog:
    """Job audit log entry for compliance and tracking."""

    log_id: str
    job_id: Optional[str]
    event_type: str
    event_action: str
    event_description: Optional[str] = None
    user_id: Optional[str] = None
    worker_id: Optional[str] = None
    ip_address: Optional[str] = None
    old_values: Optional[Dict[str, Any]] = None
    new_values: Optional[Dict[str, Any]] = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert audit log to dictionary."""
        return {
            "log_id": self.log_id,
            "job_id": self.job_id,
            "event_type": self.event_type,
            "event_action": self.event_action,
            "event_description": self.event_description,
            "user_id": self.user_id,
            "worker_id": self.worker_id,
            "ip_address": self.ip_address,
            "old_values": self.old_values,
            "new_values": self.new_values,
            "timestamp": self.timestamp.isoformat(),
            "metadata": self.metadata
        }


# Job status transition rules
JOB_STATUS_TRANSITIONS = {
    JobStatus.PENDING: [JobStatus.QUEUED, JobStatus.CANCELLED],
    JobStatus.QUEUED: [JobStatus.RUNNING, JobStatus.CANCELLED, JobStatus.PAUSED],
    JobStatus.RUNNING: [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED, JobStatus.PAUSED, JobStatus.TIMEOUT],
    JobStatus.PAUSED: [JobStatus.QUEUED, JobStatus.CANCELLED],
    JobStatus.COMPLETED: [],  # Terminal state
    JobStatus.FAILED: [JobStatus.RETRYING, JobStatus.CANCELLED],
    JobStatus.CANCELLED: [],  # Terminal state
    JobStatus.TIMEOUT: [JobStatus.RETRYING, JobStatus.CANCELLED],
    JobStatus.RETRYING: [JobStatus.QUEUED, JobStatus.FAILED, JobStatus.CANCELLED]
}


def can_transition_to(current_status: JobStatus, target_status: JobStatus) -> bool:
    """Check if a job can transition from current status to target status."""
    return target_status in JOB_STATUS_TRANSITIONS.get(current_status, [])


def get_valid_transitions(current_status: JobStatus) -> List[JobStatus]:
    """Get list of valid status transitions from current status."""
    return JOB_STATUS_TRANSITIONS.get(current_status, [])