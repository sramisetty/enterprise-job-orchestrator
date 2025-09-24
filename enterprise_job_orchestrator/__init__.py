"""
Enterprise Job Orchestrator

A comprehensive, enterprise-grade job orchestration system designed for processing
massive datasets (80+ million records) with distributed workers, fault tolerance,
and comprehensive monitoring.

This package provides a pluggable, standalone job orchestration solution that can
be integrated into any Python application or system.

Key Features:
- Distributed job processing with automatic load balancing
- Database-backed state management with fault tolerance
- Real-time monitoring and alerting
- Configurable retry logic and circuit breaker patterns
- Enterprise-grade logging and observability
- Comprehensive CLI interface
- Plugin architecture for easy integration

Usage:
    from enterprise_job_orchestrator import JobOrchestrator, Job, JobType, JobPriority
    from enterprise_job_orchestrator.utils import DatabaseManager

    # Initialize database and orchestrator
    db_manager = DatabaseManager("postgresql://localhost/orchestrator")
    await db_manager.initialize()

    orchestrator = JobOrchestrator(db_manager)
    await orchestrator.start()

    # Create and submit a job
    job = Job(
        job_id="example_job_001",
        job_name="Data Processing Task",
        job_type=JobType.DATA_PROCESSING,
        priority=JobPriority.HIGH,
        config={
            "input_file": "/path/to/data.csv",
            "chunk_size": 10000,
            "output_directory": "/path/to/output/"
        }
    )

    job_id = await orchestrator.submit_job(job)
    print(f"Job submitted with ID: {job_id}")

    # Monitor job progress
    status = await orchestrator.get_job_status(job_id)
    print(f"Job status: {status}")
"""

__version__ = "1.0.0"
__author__ = "Enterprise Job Orchestrator Team"
__license__ = "MIT"

# Core orchestrator
from .core.orchestrator import JobOrchestrator

# Data models
from .models.job import Job, JobStatus, JobType, JobPriority, ProcessingStrategy
from .models.worker import WorkerNode, WorkerStatus, WorkerPool
from .models.execution import JobExecution, JobChunk, ChunkStatus

# Services (for advanced usage)
from .services.job_manager import JobManager
from .services.queue_manager import QueueManager
from .services.worker_manager import WorkerManager
from .services.monitoring_service import MonitoringService

# Utilities
from .utils.database import DatabaseManager
from .utils.logger import setup_logger, get_logger

# Exceptions
from .core.exceptions import (
    JobOrchestratorError,
    JobNotFoundError,
    JobSubmissionError,
    JobExecutionError,
    WorkerNotFoundError,
    WorkerRegistrationError,
    NoAvailableWorkersError,
    ConfigurationError,
    DatabaseError,
    MonitoringError
)

__all__ = [
    # Core
    "JobOrchestrator",

    # Models
    "Job",
    "JobStatus",
    "JobType",
    "JobPriority",
    "ProcessingStrategy",
    "WorkerNode",
    "WorkerStatus",
    "WorkerPool",
    "JobExecution",
    "JobChunk",
    "ChunkStatus",

    # Services (for advanced usage)
    "JobManager",
    "QueueManager",
    "WorkerManager",
    "MonitoringService",

    # Utilities
    "DatabaseManager",
    "setup_logger",
    "get_logger",

    # Exceptions
    "JobOrchestratorError",
    "JobNotFoundError",
    "JobSubmissionError",
    "JobExecutionError",
    "WorkerNotFoundError",
    "WorkerRegistrationError",
    "NoAvailableWorkersError",
    "ConfigurationError",
    "DatabaseError",
    "MonitoringError",

    # Package metadata
    "__version__",
    "__author__",
    "__license__"
]

# Package-level configuration
import logging
logging.getLogger(__name__).addHandler(logging.NullHandler())

# Quick start helper
def quick_start(database_url: str = "postgresql://localhost/job_orchestrator") -> JobOrchestrator:
    """
    Quick start helper for simple use cases.

    Args:
        database_url: PostgreSQL connection URL

    Returns:
        Configured and ready-to-use JobOrchestrator instance

    Example:
        orchestrator = quick_start("postgresql://user:pass@host/db")
        await orchestrator.start()

        job = Job(
            job_id="quick_job",
            job_name="Quick Test Job",
            job_type=JobType.DATA_PROCESSING
        )

        job_id = await orchestrator.submit_job(job)
    """
    db_manager = DatabaseManager(database_url)
    return JobOrchestrator(db_manager)