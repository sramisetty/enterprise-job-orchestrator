"""
Worker node models for Enterprise Job Orchestrator

Defines data structures for worker node management, capabilities,
health monitoring, and job assignments.
"""

from enum import Enum
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field
from decimal import Decimal


class WorkerStatus(Enum):
    """Worker node status enumeration."""
    OFFLINE = "offline"
    ONLINE = "online"
    BUSY = "busy"
    MAINTENANCE = "maintenance"
    ERROR = "error"
    DRAINING = "draining"  # Not accepting new jobs, finishing current ones


@dataclass
class WorkerNode:
    """Worker node data model."""

    # Primary identification
    worker_id: str
    node_name: str

    # Node information
    host_name: Optional[str] = None
    ip_address: Optional[str] = None
    port: int = 8080

    # Capabilities
    capabilities: List[str] = field(default_factory=list)
    max_concurrent_jobs: int = 1
    available_resources: Dict[str, Any] = field(default_factory=dict)

    # Status
    status: WorkerStatus = WorkerStatus.OFFLINE
    is_active: bool = True

    # Health monitoring
    last_heartbeat: Optional[datetime] = None
    health_check_url: Optional[str] = None
    health_status: str = "unknown"

    # Performance metrics
    cpu_cores: Optional[int] = None
    memory_gb: Optional[int] = None
    disk_gb: Optional[int] = None
    network_speed_mbps: Optional[int] = None

    # Current load
    current_jobs: int = 0
    cpu_usage_percent: Optional[Decimal] = None
    memory_usage_percent: Optional[Decimal] = None

    # Metadata
    worker_version: Optional[str] = None
    worker_metadata: Dict[str, Any] = field(default_factory=dict)

    # Timing
    registered_at: datetime = field(default_factory=datetime.utcnow)
    last_seen_at: datetime = field(default_factory=datetime.utcnow)

    def update_heartbeat(self, health_data: Optional[Dict[str, Any]] = None):
        """Update worker heartbeat and health information."""
        self.last_heartbeat = datetime.utcnow()
        self.last_seen_at = datetime.utcnow()

        if health_data:
            self.cpu_usage_percent = health_data.get("cpu_usage_percent")
            self.memory_usage_percent = health_data.get("memory_usage_percent")
            self.current_jobs = health_data.get("current_jobs", self.current_jobs)
            self.health_status = health_data.get("health_status", "healthy")

            # Update status based on health
            if self.health_status == "healthy":
                if self.current_jobs >= self.max_concurrent_jobs:
                    self.status = WorkerStatus.BUSY
                else:
                    self.status = WorkerStatus.ONLINE
            else:
                self.status = WorkerStatus.ERROR

    def is_healthy(self, timeout_seconds: int = 300) -> bool:
        """Check if worker is healthy based on recent heartbeat."""
        if not self.last_heartbeat:
            return False

        age = datetime.utcnow() - self.last_heartbeat
        return age.total_seconds() < timeout_seconds

    def can_accept_job(self, required_capabilities: Optional[List[str]] = None) -> bool:
        """Check if worker can accept a new job."""
        if not self.is_active or self.status not in [WorkerStatus.ONLINE, WorkerStatus.BUSY]:
            return False

        if self.current_jobs >= self.max_concurrent_jobs:
            return False

        if required_capabilities:
            return all(capability in self.capabilities for capability in required_capabilities)

        return True

    def get_utilization_percent(self) -> float:
        """Get current utilization as percentage."""
        if self.max_concurrent_jobs > 0:
            return (self.current_jobs / self.max_concurrent_jobs) * 100
        return 0.0

    def get_available_capacity(self) -> int:
        """Get number of additional jobs this worker can handle."""
        return max(0, self.max_concurrent_jobs - self.current_jobs)

    def assign_job(self, job_id: str):
        """Assign a job to this worker."""
        if self.can_accept_job():
            self.current_jobs += 1
            if self.current_jobs >= self.max_concurrent_jobs:
                self.status = WorkerStatus.BUSY

    def complete_job(self, job_id: str):
        """Mark a job as completed on this worker."""
        if self.current_jobs > 0:
            self.current_jobs -= 1
            if self.status == WorkerStatus.BUSY and self.current_jobs < self.max_concurrent_jobs:
                self.status = WorkerStatus.ONLINE

    def get_score(self, required_capabilities: Optional[List[str]] = None) -> float:
        """Calculate worker score for job assignment (higher is better)."""
        if not self.can_accept_job(required_capabilities):
            return 0.0

        # Base score from availability
        availability_score = (self.max_concurrent_jobs - self.current_jobs) / self.max_concurrent_jobs

        # Health score
        health_score = 1.0 if self.is_healthy() else 0.0

        # Resource score (lower CPU usage is better)
        resource_score = 1.0
        if self.cpu_usage_percent:
            resource_score = max(0.0, (100.0 - float(self.cpu_usage_percent)) / 100.0)

        # Capability match score
        capability_score = 1.0
        if required_capabilities:
            matching_capabilities = sum(1 for cap in required_capabilities if cap in self.capabilities)
            capability_score = matching_capabilities / len(required_capabilities)

        # Combine scores with weights
        total_score = (
            availability_score * 0.4 +
            health_score * 0.3 +
            resource_score * 0.2 +
            capability_score * 0.1
        )

        return total_score

    def to_dict(self) -> Dict[str, Any]:
        """Convert worker to dictionary."""
        return {
            "worker_id": self.worker_id,
            "node_name": self.node_name,
            "host_name": self.host_name,
            "ip_address": self.ip_address,
            "port": self.port,
            "capabilities": self.capabilities,
            "max_concurrent_jobs": self.max_concurrent_jobs,
            "available_resources": self.available_resources,
            "status": self.status.value,
            "is_active": self.is_active,
            "last_heartbeat": self.last_heartbeat.isoformat() if self.last_heartbeat else None,
            "health_check_url": self.health_check_url,
            "health_status": self.health_status,
            "cpu_cores": self.cpu_cores,
            "memory_gb": self.memory_gb,
            "disk_gb": self.disk_gb,
            "network_speed_mbps": self.network_speed_mbps,
            "current_jobs": self.current_jobs,
            "cpu_usage_percent": float(self.cpu_usage_percent) if self.cpu_usage_percent else None,
            "memory_usage_percent": float(self.memory_usage_percent) if self.memory_usage_percent else None,
            "worker_version": self.worker_version,
            "worker_metadata": self.worker_metadata,
            "registered_at": self.registered_at.isoformat(),
            "last_seen_at": self.last_seen_at.isoformat(),
            "utilization_percent": self.get_utilization_percent(),
            "available_capacity": self.get_available_capacity(),
            "is_healthy": self.is_healthy()
        }


@dataclass
class WorkerAssignment:
    """Worker job assignment tracking."""

    # Primary identification
    assignment_id: str
    worker_id: str
    job_id: str

    # Assignment details
    chunk_ids: List[str] = field(default_factory=list)
    assigned_at: datetime = field(default_factory=datetime.utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    # Status
    assignment_status: str = "assigned"

    # Resource allocation
    allocated_cpu_cores: Optional[int] = None
    allocated_memory_mb: Optional[int] = None
    allocated_disk_gb: Optional[int] = None

    # Performance tracking
    actual_cpu_usage: Optional[Decimal] = None
    actual_memory_usage: Optional[Decimal] = None
    actual_duration: Optional[timedelta] = None

    def start_execution(self):
        """Mark assignment as started."""
        self.started_at = datetime.utcnow()
        self.assignment_status = "running"

    def complete_execution(self):
        """Mark assignment as completed."""
        self.completed_at = datetime.utcnow()
        self.assignment_status = "completed"

        if self.started_at:
            self.actual_duration = self.completed_at - self.started_at

    def fail_execution(self, error_message: str):
        """Mark assignment as failed."""
        self.completed_at = datetime.utcnow()
        self.assignment_status = "failed"

        if self.started_at:
            self.actual_duration = self.completed_at - self.started_at

    def get_duration(self) -> Optional[timedelta]:
        """Get assignment duration."""
        if self.started_at:
            end_time = self.completed_at or datetime.utcnow()
            return end_time - self.started_at
        return None

    def update_resource_usage(self, cpu_usage: float, memory_usage: float):
        """Update actual resource usage."""
        self.actual_cpu_usage = Decimal(str(cpu_usage))
        self.actual_memory_usage = Decimal(str(memory_usage))

    def to_dict(self) -> Dict[str, Any]:
        """Convert assignment to dictionary."""
        return {
            "assignment_id": self.assignment_id,
            "worker_id": self.worker_id,
            "job_id": self.job_id,
            "chunk_ids": self.chunk_ids,
            "assigned_at": self.assigned_at.isoformat(),
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "assignment_status": self.assignment_status,
            "allocated_cpu_cores": self.allocated_cpu_cores,
            "allocated_memory_mb": self.allocated_memory_mb,
            "allocated_disk_gb": self.allocated_disk_gb,
            "actual_cpu_usage": float(self.actual_cpu_usage) if self.actual_cpu_usage else None,
            "actual_memory_usage": float(self.actual_memory_usage) if self.actual_memory_usage else None,
            "actual_duration": str(self.actual_duration) if self.actual_duration else None
        }


@dataclass
class WorkerPool:
    """Collection of workers with management capabilities."""

    pool_name: str
    workers: Dict[str, WorkerNode] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)

    def add_worker(self, worker: WorkerNode):
        """Add a worker to the pool."""
        self.workers[worker.worker_id] = worker

    def remove_worker(self, worker_id: str) -> Optional[WorkerNode]:
        """Remove a worker from the pool."""
        return self.workers.pop(worker_id, None)

    def get_worker(self, worker_id: str) -> Optional[WorkerNode]:
        """Get a specific worker."""
        return self.workers.get(worker_id)

    def get_available_workers(self, required_capabilities: Optional[List[str]] = None) -> List[WorkerNode]:
        """Get list of available workers."""
        available = []
        for worker in self.workers.values():
            if worker.can_accept_job(required_capabilities):
                available.append(worker)

        # Sort by score (best workers first)
        available.sort(key=lambda w: w.get_score(required_capabilities), reverse=True)
        return available

    def get_best_worker(self, required_capabilities: Optional[List[str]] = None) -> Optional[WorkerNode]:
        """Get the best available worker for a job."""
        available = self.get_available_workers(required_capabilities)
        return available[0] if available else None

    def get_healthy_workers(self) -> List[WorkerNode]:
        """Get list of healthy workers."""
        return [worker for worker in self.workers.values() if worker.is_healthy()]

    def get_total_capacity(self) -> int:
        """Get total capacity across all workers."""
        return sum(worker.max_concurrent_jobs for worker in self.workers.values() if worker.is_active)

    def get_available_capacity(self) -> int:
        """Get available capacity across all workers."""
        return sum(worker.get_available_capacity() for worker in self.workers.values())

    def get_utilization_percent(self) -> float:
        """Get overall pool utilization."""
        total_capacity = self.get_total_capacity()
        if total_capacity == 0:
            return 0.0

        used_capacity = sum(worker.current_jobs for worker in self.workers.values() if worker.is_active)
        return (used_capacity / total_capacity) * 100

    def get_pool_statistics(self) -> Dict[str, Any]:
        """Get comprehensive pool statistics."""
        workers = list(self.workers.values())
        active_workers = [w for w in workers if w.is_active]
        healthy_workers = [w for w in workers if w.is_healthy()]
        online_workers = [w for w in workers if w.status == WorkerStatus.ONLINE]
        busy_workers = [w for w in workers if w.status == WorkerStatus.BUSY]

        return {
            "pool_name": self.pool_name,
            "total_workers": len(workers),
            "active_workers": len(active_workers),
            "healthy_workers": len(healthy_workers),
            "online_workers": len(online_workers),
            "busy_workers": len(busy_workers),
            "total_capacity": self.get_total_capacity(),
            "available_capacity": self.get_available_capacity(),
            "utilization_percent": self.get_utilization_percent(),
            "created_at": self.created_at.isoformat()
        }

    def to_dict(self) -> Dict[str, Any]:
        """Convert pool to dictionary."""
        return {
            "pool_name": self.pool_name,
            "workers": {worker_id: worker.to_dict() for worker_id, worker in self.workers.items()},
            "statistics": self.get_pool_statistics(),
            "created_at": self.created_at.isoformat()
        }