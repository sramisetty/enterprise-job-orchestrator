"""
Main JobOrchestrator class that coordinates all services

Provides the primary interface for job submission, worker management,
and system orchestration.
"""

import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Any

from ..models.job import Job, JobStatus
from ..models.worker import WorkerNode
from ..services.job_manager import JobManager
from ..services.queue_manager import QueueManager
from ..services.worker_manager import WorkerManager
from ..services.monitoring_service import MonitoringService
from ..utils.database import DatabaseManager
from ..utils.logger import get_logger, set_log_context
from ..core.exceptions import OrchestratorError, JobSubmissionError


class JobOrchestrator:
    """
    Main orchestrator class that coordinates all services.

    Provides a unified interface for:
    - Job submission and management
    - Worker registration and coordination
    - System monitoring and health checks
    - Performance reporting and metrics
    """

    def __init__(self, database_manager: DatabaseManager):
        """
        Initialize the JobOrchestrator.

        Args:
            database_manager: Database connection manager
        """
        self.db = database_manager

        # Initialize service managers
        self.job_manager = JobManager(database_manager)
        self.queue_manager = QueueManager(database_manager)
        self.worker_manager = WorkerManager(database_manager)
        self.monitoring_service = MonitoringService(database_manager)

        # State tracking
        self._is_running = False
        self._shutdown_event = asyncio.Event()

        # Logger
        self.logger = get_logger(__name__)
        set_log_context(self.logger, component="orchestrator")

    async def start(self):
        """Start the orchestrator and all services."""
        self.logger.info("Starting JobOrchestrator")

        try:
            # Start all services
            await self.db.initialize()
            await self.job_manager.start()
            await self.queue_manager.start()
            await self.worker_manager.start()
            await self.monitoring_service.start()

            self._is_running = True

            self.logger.info("JobOrchestrator started successfully")

        except Exception as e:
            self.logger.error("Failed to start JobOrchestrator", exc_info=True)
            await self.stop()
            raise OrchestratorError(f"Failed to start orchestrator: {str(e)}")

    async def stop(self):
        """Stop the orchestrator and all services."""
        self.logger.info("Stopping JobOrchestrator")

        self._shutdown_event.set()

        # Stop all services
        services = [
            self.monitoring_service,
            self.worker_manager,
            self.queue_manager,
            self.job_manager
        ]

        for service in services:
            try:
                await service.stop()
            except Exception as e:
                self.logger.error(f"Error stopping service {service.__class__.__name__}", exc_info=True)

        self._is_running = False
        self.logger.info("JobOrchestrator stopped")

    # Job Management Interface
    async def submit_job(self, job: Job) -> str:
        """
        Submit a new job for processing.

        Args:
            job: Job instance to submit

        Returns:
            Job ID of submitted job

        Raises:
            JobSubmissionError: If job submission fails
        """
        if not self._is_running:
            raise OrchestratorError("Orchestrator is not running")

        try:
            self.logger.info("Submitting job", extra={
                "job_id": job.job_id,
                "job_name": job.job_name,
                "job_type": job.job_type.value
            })

            # Submit to job manager
            job_id = await self.job_manager.submit_job(job)

            # Add to queue
            await self.queue_manager.enqueue_job(job)

            self.logger.info("Job submitted successfully", extra={"job_id": job_id})
            return job_id

        except Exception as e:
            self.logger.error("Failed to submit job", exc_info=True)
            raise JobSubmissionError(f"Failed to submit job: {str(e)}")

    async def cancel_job(self, job_id: str, force: bool = False) -> bool:
        """
        Cancel a running job.

        Args:
            job_id: ID of job to cancel
            force: Whether to force cancellation

        Returns:
            True if cancellation successful
        """
        try:
            self.logger.info("Cancelling job", extra={
                "job_id": job_id,
                "force": force
            })

            success = await self.job_manager.cancel_job(job_id, force)

            if success:
                self.logger.info("Job cancelled successfully", extra={"job_id": job_id})
            else:
                self.logger.warning("Job cancellation failed", extra={"job_id": job_id})

            return success

        except Exception as e:
            self.logger.error("Error cancelling job", extra={
                "job_id": job_id,
                "error": str(e)
            })
            return False

    async def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed status of a job.

        Args:
            job_id: ID of job to query

        Returns:
            Job status dictionary or None if not found
        """
        try:
            return await self.job_manager.get_job_status(job_id)
        except Exception as e:
            self.logger.error("Error getting job status", extra={
                "job_id": job_id,
                "error": str(e)
            })
            return None

    async def list_jobs(self, status_filter: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """
        List jobs with optional filtering.

        Args:
            status_filter: Optional status to filter by
            limit: Maximum number of jobs to return

        Returns:
            List of job dictionaries
        """
        try:
            return await self.job_manager.list_jobs(status_filter, limit)
        except Exception as e:
            self.logger.error("Error listing jobs", exc_info=True)
            return []

    # Worker Management Interface
    async def register_worker(self, worker: WorkerNode) -> bool:
        """
        Register a new worker node.

        Args:
            worker: Worker node to register

        Returns:
            True if registration successful
        """
        try:
            self.logger.info("Registering worker", extra={
                "worker_id": worker.worker_id,
                "node_name": worker.node_name
            })

            success = await self.worker_manager.register_worker(worker)

            if success:
                self.logger.info("Worker registered successfully", extra={
                    "worker_id": worker.worker_id
                })

            return success

        except Exception as e:
            self.logger.error("Error registering worker", extra={
                "worker_id": worker.worker_id,
                "error": str(e)
            })
            return False

    async def deregister_worker(self, worker_id: str, graceful: bool = True) -> bool:
        """
        Deregister a worker node.

        Args:
            worker_id: ID of worker to deregister
            graceful: Whether to wait for current jobs to complete

        Returns:
            True if deregistration successful
        """
        try:
            return await self.worker_manager.deregister_worker(worker_id, graceful)
        except Exception as e:
            self.logger.error("Error deregistering worker", extra={
                "worker_id": worker_id,
                "error": str(e)
            })
            return False

    async def get_workers(self, status_filter: Optional[str] = None, pool_filter: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Get list of workers with optional filtering.

        Args:
            status_filter: Optional status to filter by
            pool_filter: Optional pool to filter by

        Returns:
            List of worker dictionaries
        """
        try:
            workers = await self.worker_manager.get_available_workers()

            # Apply filters
            if status_filter:
                workers = [w for w in workers if w.status.value == status_filter]

            if pool_filter:
                workers = [w for w in workers if w.worker_metadata.get('pool') == pool_filter]

            return [w.to_dict() for w in workers]

        except Exception as e:
            self.logger.error("Error getting workers", exc_info=True)
            return []

    # Monitoring Interface
    async def get_system_health(self) -> Dict[str, Any]:
        """
        Get current system health status.

        Returns:
            System health dictionary
        """
        try:
            health = await self.monitoring_service.get_system_health()
            return {
                "overall_status": health.overall_status,
                "total_jobs": health.total_jobs,
                "running_jobs": health.running_jobs,
                "failed_jobs": health.failed_jobs,
                "total_workers": health.total_workers,
                "healthy_workers": health.healthy_workers,
                "average_cpu_usage": health.average_cpu_usage,
                "average_memory_usage": health.average_memory_usage,
                "queue_size": health.queue_size,
                "processing_rate": health.processing_rate,
                "error_rate": health.error_rate,
                "uptime_seconds": health.uptime_seconds,
                "timestamp": health.timestamp.isoformat()
            }

        except Exception as e:
            self.logger.error("Error getting system health", exc_info=True)
            return {
                "overall_status": "unknown",
                "error": str(e)
            }

    async def get_performance_report(self, hours: int = 24) -> Dict[str, Any]:
        """
        Get performance report for specified time period.

        Args:
            hours: Number of hours to include in report

        Returns:
            Performance report dictionary
        """
        try:
            return await self.monitoring_service.get_performance_report(hours)
        except Exception as e:
            self.logger.error("Error getting performance report", exc_info=True)
            return {"error": str(e)}

    # Queue Management Interface
    async def get_queue_statistics(self) -> Dict[str, Any]:
        """
        Get queue statistics.

        Returns:
            Queue statistics dictionary
        """
        try:
            return await self.queue_manager.get_queue_statistics()
        except Exception as e:
            self.logger.error("Error getting queue statistics", exc_info=True)
            return {}

    async def pause_queue(self) -> bool:
        """
        Pause job queue processing.

        Returns:
            True if successful
        """
        try:
            await self.queue_manager.pause_processing()
            self.logger.info("Queue processing paused")
            return True
        except Exception as e:
            self.logger.error("Error pausing queue", exc_info=True)
            return False

    async def resume_queue(self) -> bool:
        """
        Resume job queue processing.

        Returns:
            True if successful
        """
        try:
            await self.queue_manager.resume_processing()
            self.logger.info("Queue processing resumed")
            return True
        except Exception as e:
            self.logger.error("Error resuming queue", exc_info=True)
            return False

    # Utility Methods
    def is_running(self) -> bool:
        """Check if orchestrator is running."""
        return self._is_running

    async def wait_for_shutdown(self):
        """Wait for shutdown signal."""
        await self._shutdown_event.wait()

    async def health_check(self) -> bool:
        """
        Perform a basic health check.

        Returns:
            True if system is healthy
        """
        try:
            # Check if all services are running
            if not self._is_running:
                return False

            # Check database connectivity
            if not await self.db.is_healthy():
                return False

            # Check if we can get system health
            health = await self.get_system_health()
            return health.get("overall_status") not in ["critical", "unknown"]

        except Exception:
            return False