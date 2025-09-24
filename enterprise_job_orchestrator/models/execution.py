"""
Job execution and chunk processing models for Enterprise Job Orchestrator

Defines data structures for tracking job execution state, chunk processing,
and checkpoints for fault tolerance.
"""

from enum import Enum
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field
from decimal import Decimal

from .job import JobStatus


class ChunkStatus(Enum):
    """Chunk processing status enumeration."""
    PENDING = "pending"
    ASSIGNED = "assigned"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"


@dataclass
class JobExecution:
    """Job execution state tracking."""

    # Primary identification
    execution_id: str
    job_id: str

    # Execution state
    status: JobStatus = JobStatus.PENDING
    progress_percent: Decimal = Decimal("0.0")

    # Record processing metrics
    total_records: int = 0
    processed_records: int = 0
    successful_records: int = 0
    failed_records: int = 0
    skipped_records: int = 0

    # Performance metrics
    records_per_second: Decimal = Decimal("0.0")
    estimated_completion_time: Optional[datetime] = None

    # Resource usage
    cpu_usage_percent: Optional[Decimal] = None
    memory_usage_mb: Optional[int] = None
    disk_usage_gb: Optional[Decimal] = None
    network_io_mb: Optional[Decimal] = None

    # Error tracking
    error_count: int = 0
    last_error_message: Optional[str] = None
    last_error_time: Optional[datetime] = None

    # Worker assignment
    assigned_worker_id: Optional[str] = None
    worker_node_info: Optional[Dict[str, Any]] = None

    # Timing
    started_at: Optional[datetime] = None
    last_heartbeat: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    # Metadata
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)

    def update_progress(self, processed: int, successful: int, failed: int, skipped: int = 0):
        """Update processing progress."""
        self.processed_records = processed
        self.successful_records = successful
        self.failed_records = failed
        self.skipped_records = skipped
        self.updated_at = datetime.utcnow()

        # Calculate progress percentage
        if self.total_records > 0:
            self.progress_percent = Decimal(str((processed / self.total_records) * 100))
        else:
            self.progress_percent = Decimal("0.0")

        # Update processing rate
        if self.started_at:
            elapsed_seconds = (datetime.utcnow() - self.started_at).total_seconds()
            if elapsed_seconds > 0:
                self.records_per_second = Decimal(str(processed / elapsed_seconds))

        # Update heartbeat
        self.last_heartbeat = datetime.utcnow()

    def record_error(self, error_message: str):
        """Record an error during execution."""
        self.error_count += 1
        self.last_error_message = error_message
        self.last_error_time = datetime.utcnow()
        self.updated_at = datetime.utcnow()

    def estimate_completion(self):
        """Estimate completion time based on current progress."""
        if self.started_at and self.records_per_second > 0:
            remaining_records = self.total_records - self.processed_records
            remaining_seconds = float(remaining_records) / float(self.records_per_second)
            self.estimated_completion_time = datetime.utcnow() + timedelta(seconds=remaining_seconds)

    def get_duration(self) -> Optional[timedelta]:
        """Get execution duration."""
        if self.started_at:
            end_time = self.completed_at or datetime.utcnow()
            return end_time - self.started_at
        return None

    def is_healthy(self) -> bool:
        """Check if execution is healthy (recent heartbeat)."""
        if not self.last_heartbeat:
            return False

        # Consider execution unhealthy if no heartbeat for 5 minutes
        return (datetime.utcnow() - self.last_heartbeat).total_seconds() < 300

    def to_dict(self) -> Dict[str, Any]:
        """Convert execution to dictionary."""
        return {
            "execution_id": self.execution_id,
            "job_id": self.job_id,
            "status": self.status.value,
            "progress_percent": float(self.progress_percent),
            "total_records": self.total_records,
            "processed_records": self.processed_records,
            "successful_records": self.successful_records,
            "failed_records": self.failed_records,
            "skipped_records": self.skipped_records,
            "records_per_second": float(self.records_per_second),
            "estimated_completion_time": self.estimated_completion_time.isoformat() if self.estimated_completion_time else None,
            "cpu_usage_percent": float(self.cpu_usage_percent) if self.cpu_usage_percent else None,
            "memory_usage_mb": self.memory_usage_mb,
            "disk_usage_gb": float(self.disk_usage_gb) if self.disk_usage_gb else None,
            "network_io_mb": float(self.network_io_mb) if self.network_io_mb else None,
            "error_count": self.error_count,
            "last_error_message": self.last_error_message,
            "last_error_time": self.last_error_time.isoformat() if self.last_error_time else None,
            "assigned_worker_id": self.assigned_worker_id,
            "worker_node_info": self.worker_node_info,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "last_heartbeat": self.last_heartbeat.isoformat() if self.last_heartbeat else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }


@dataclass
class JobChunk:
    """Individual chunk for parallel processing."""

    # Primary identification
    chunk_id: str
    job_id: str

    # Chunk definition
    chunk_number: int
    start_record: int
    end_record: int

    # Chunk state
    status: ChunkStatus = ChunkStatus.PENDING
    priority: int = 0

    # Processing metrics
    processed_records: int = 0
    successful_records: int = 0
    failed_records: int = 0

    # Worker assignment
    assigned_worker_id: Optional[str] = None
    assigned_at: Optional[datetime] = None

    # Timing
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    processing_duration: Optional[timedelta] = None

    # Retry management
    retry_count: int = 0
    max_retries: int = 3
    next_retry_at: Optional[datetime] = None

    # Error tracking
    error_message: Optional[str] = None
    error_details: Optional[Dict[str, Any]] = None

    # Results
    output_location: Optional[str] = None
    output_size_bytes: Optional[int] = None

    # Metadata
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)

    @property
    def record_count(self) -> int:
        """Calculate number of records in this chunk."""
        return self.end_record - self.start_record

    def assign_to_worker(self, worker_id: str):
        """Assign chunk to a worker."""
        self.assigned_worker_id = worker_id
        self.assigned_at = datetime.utcnow()
        self.status = ChunkStatus.ASSIGNED
        self.updated_at = datetime.utcnow()

    def start_processing(self):
        """Mark chunk as started processing."""
        self.status = ChunkStatus.PROCESSING
        self.started_at = datetime.utcnow()
        self.updated_at = datetime.utcnow()

    def complete_processing(self, successful: int, failed: int):
        """Mark chunk as completed."""
        self.status = ChunkStatus.COMPLETED
        self.completed_at = datetime.utcnow()
        self.successful_records = successful
        self.failed_records = failed
        self.processed_records = successful + failed

        if self.started_at:
            self.processing_duration = self.completed_at - self.started_at

        self.updated_at = datetime.utcnow()

    def fail_processing(self, error_message: str, error_details: Optional[Dict[str, Any]] = None):
        """Mark chunk as failed."""
        self.status = ChunkStatus.FAILED
        self.error_message = error_message
        self.error_details = error_details or {}
        self.completed_at = datetime.utcnow()

        if self.started_at:
            self.processing_duration = self.completed_at - self.started_at

        self.updated_at = datetime.utcnow()

        # Schedule retry if attempts remaining
        if self.retry_count < self.max_retries:
            self.schedule_retry()

    def schedule_retry(self):
        """Schedule chunk for retry with exponential backoff."""
        if self.retry_count < self.max_retries:
            self.retry_count += 1
            self.status = ChunkStatus.RETRYING

            # Exponential backoff: 2^retry_count minutes
            backoff_minutes = 2 ** self.retry_count
            self.next_retry_at = datetime.utcnow() + timedelta(minutes=backoff_minutes)

            # Reset worker assignment
            self.assigned_worker_id = None
            self.assigned_at = None

            self.updated_at = datetime.utcnow()

    def can_retry(self) -> bool:
        """Check if chunk can be retried."""
        return (self.retry_count < self.max_retries and
                self.status in [ChunkStatus.FAILED, ChunkStatus.RETRYING] and
                (self.next_retry_at is None or datetime.utcnow() >= self.next_retry_at))

    def get_progress_percent(self) -> float:
        """Get processing progress as percentage."""
        if self.record_count > 0:
            return (self.processed_records / self.record_count) * 100
        return 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert chunk to dictionary."""
        return {
            "chunk_id": self.chunk_id,
            "job_id": self.job_id,
            "chunk_number": self.chunk_number,
            "start_record": self.start_record,
            "end_record": self.end_record,
            "record_count": self.record_count,
            "status": self.status.value,
            "priority": self.priority,
            "processed_records": self.processed_records,
            "successful_records": self.successful_records,
            "failed_records": self.failed_records,
            "assigned_worker_id": self.assigned_worker_id,
            "assigned_at": self.assigned_at.isoformat() if self.assigned_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "processing_duration": str(self.processing_duration) if self.processing_duration else None,
            "retry_count": self.retry_count,
            "max_retries": self.max_retries,
            "next_retry_at": self.next_retry_at.isoformat() if self.next_retry_at else None,
            "error_message": self.error_message,
            "error_details": self.error_details,
            "output_location": self.output_location,
            "output_size_bytes": self.output_size_bytes,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat()
        }


@dataclass
class JobCheckpoint:
    """Job checkpoint for fault tolerance."""

    # Primary identification
    checkpoint_id: str
    job_id: str

    # Checkpoint data
    checkpoint_name: str
    checkpoint_type: str = "progress"
    sequence_number: int = 0

    # State information
    state_data: Dict[str, Any] = field(default_factory=dict)
    records_processed: int = 0
    progress_percent: Decimal = Decimal("0.0")

    # File references
    checkpoint_files: List[str] = field(default_factory=list)
    data_location: Optional[str] = None

    # Timing
    created_at: datetime = field(default_factory=datetime.utcnow)

    # Validation
    checksum: Optional[str] = None
    is_valid: bool = True

    def validate_integrity(self) -> bool:
        """Validate checkpoint integrity."""
        # Implement checksum validation logic
        # For now, assume valid if checksum exists
        return self.checksum is not None

    def get_age(self) -> timedelta:
        """Get checkpoint age."""
        return datetime.utcnow() - self.created_at

    def is_recent(self, max_age_hours: int = 24) -> bool:
        """Check if checkpoint is recent enough to be useful."""
        return self.get_age().total_seconds() < (max_age_hours * 3600)

    def to_dict(self) -> Dict[str, Any]:
        """Convert checkpoint to dictionary."""
        return {
            "checkpoint_id": self.checkpoint_id,
            "job_id": self.job_id,
            "checkpoint_name": self.checkpoint_name,
            "checkpoint_type": self.checkpoint_type,
            "sequence_number": self.sequence_number,
            "state_data": self.state_data,
            "records_processed": self.records_processed,
            "progress_percent": float(self.progress_percent),
            "checkpoint_files": self.checkpoint_files,
            "data_location": self.data_location,
            "created_at": self.created_at.isoformat(),
            "checksum": self.checksum,
            "is_valid": self.is_valid
        }


@dataclass
class ExecutionSummary:
    """Summary of job execution for reporting."""

    job_id: str
    execution_id: str
    job_name: str
    status: JobStatus

    # Timing
    total_duration: Optional[timedelta] = None
    queue_time: Optional[timedelta] = None
    execution_time: Optional[timedelta] = None

    # Processing statistics
    total_records: int = 0
    successful_records: int = 0
    failed_records: int = 0
    skipped_records: int = 0

    # Performance metrics
    average_records_per_second: Decimal = Decimal("0.0")
    peak_records_per_second: Decimal = Decimal("0.0")

    # Resource usage
    peak_cpu_usage: Optional[Decimal] = None
    peak_memory_usage_mb: Optional[int] = None
    total_disk_usage_gb: Optional[Decimal] = None

    # Chunk statistics (for parallel processing)
    total_chunks: int = 0
    successful_chunks: int = 0
    failed_chunks: int = 0
    retried_chunks: int = 0

    # Error statistics
    total_errors: int = 0
    unique_error_types: int = 0
    most_common_error: Optional[str] = None

    # Cost and efficiency
    estimated_cost: Optional[Decimal] = None
    efficiency_score: Optional[Decimal] = None

    def calculate_success_rate(self) -> float:
        """Calculate overall success rate."""
        if self.total_records > 0:
            return (self.successful_records / self.total_records) * 100
        return 0.0

    def calculate_throughput(self) -> float:
        """Calculate average throughput in records per hour."""
        if self.execution_time and self.execution_time.total_seconds() > 0:
            hours = self.execution_time.total_seconds() / 3600
            return self.successful_records / hours
        return 0.0

    def to_dict(self) -> Dict[str, Any]:
        """Convert summary to dictionary."""
        return {
            "job_id": self.job_id,
            "execution_id": self.execution_id,
            "job_name": self.job_name,
            "status": self.status.value,
            "total_duration": str(self.total_duration) if self.total_duration else None,
            "queue_time": str(self.queue_time) if self.queue_time else None,
            "execution_time": str(self.execution_time) if self.execution_time else None,
            "total_records": self.total_records,
            "successful_records": self.successful_records,
            "failed_records": self.failed_records,
            "skipped_records": self.skipped_records,
            "success_rate_percent": self.calculate_success_rate(),
            "average_records_per_second": float(self.average_records_per_second),
            "peak_records_per_second": float(self.peak_records_per_second),
            "throughput_per_hour": self.calculate_throughput(),
            "peak_cpu_usage": float(self.peak_cpu_usage) if self.peak_cpu_usage else None,
            "peak_memory_usage_mb": self.peak_memory_usage_mb,
            "total_disk_usage_gb": float(self.total_disk_usage_gb) if self.total_disk_usage_gb else None,
            "total_chunks": self.total_chunks,
            "successful_chunks": self.successful_chunks,
            "failed_chunks": self.failed_chunks,
            "retried_chunks": self.retried_chunks,
            "chunk_success_rate": (self.successful_chunks / self.total_chunks * 100) if self.total_chunks > 0 else 0,
            "total_errors": self.total_errors,
            "unique_error_types": self.unique_error_types,
            "most_common_error": self.most_common_error,
            "estimated_cost": float(self.estimated_cost) if self.estimated_cost else None,
            "efficiency_score": float(self.efficiency_score) if self.efficiency_score else None
        }