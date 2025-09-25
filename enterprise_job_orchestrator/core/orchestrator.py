"""
Main JobOrchestrator class that coordinates all services

Provides the primary interface for job submission, worker management,
and system orchestration. Enhanced with Spark and Airflow integration
while maintaining complete backward compatibility.
"""

import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Any, TYPE_CHECKING

from ..models.job import Job, JobStatus
from ..models.worker import WorkerNode
from ..services.job_manager import JobManager
from ..services.queue_manager import QueueManager
from ..services.worker_manager import WorkerManager
from ..services.monitoring_service import MonitoringService
from ..utils.database import DatabaseManager
from ..utils.logger import get_logger, set_log_context
from ..core.exceptions import OrchestratorError, JobSubmissionError

# Conditional imports for new features
if TYPE_CHECKING:
    from ..services.engine_manager import EngineManager
    from ..services.fault_tolerance import FaultToleranceService


class JobOrchestrator:
    """
    Main orchestrator class that coordinates all services.

    Provides a unified interface for:
    - Job submission and management
    - Worker registration and coordination
    - System monitoring and health checks
    - Performance reporting and metrics
    - Spark and Airflow integration (when configured)
    - Advanced fault tolerance and recovery
    """

    def __init__(
        self,
        database_manager: DatabaseManager,
        engine_config: Optional[Dict[str, Any]] = None,
        enable_fault_tolerance: bool = False,
        enable_legacy_workers: bool = True
    ):
        """
        Initialize the JobOrchestrator.

        Args:
            database_manager: Database connection manager
            engine_config: Optional configuration for execution engines (Spark, local)
            enable_fault_tolerance: Enable fault tolerance features
            enable_legacy_workers: Enable traditional worker management
        """
        self.db = database_manager

        # Initialize enhanced services if configured
        self.engine_manager: Optional['EngineManager'] = None
        self.fault_tolerance: Optional['FaultToleranceService'] = None

        if engine_config:
            try:
                from ..services.engine_manager import EngineManager
                self.engine_manager = EngineManager(engine_config)
            except ImportError:
                self.logger.warning("Engine manager dependencies not available")

        if enable_fault_tolerance:
            try:
                from ..services.fault_tolerance import FaultToleranceService
                fault_config = engine_config.get("fault_tolerance", {}) if engine_config else {}
                self.fault_tolerance = FaultToleranceService(fault_config)
            except ImportError:
                self.logger.warning("Fault tolerance dependencies not available")

        # Initialize service managers with enhanced features
        self.job_manager = JobManager(
            database_manager,
            engine_manager=self.engine_manager,
            fault_tolerance_service=self.fault_tolerance
        )
        self.queue_manager = QueueManager(database_manager)
        self.monitoring_service = MonitoringService(database_manager)

        # Initialize worker manager if enabled
        self.worker_manager = None
        if enable_legacy_workers:
            self.worker_manager = WorkerManager(database_manager)

        # State tracking
        self._is_running = False
        self._shutdown_event = asyncio.Event()
        self.enable_legacy_workers = enable_legacy_workers

        # Logger
        self.logger = get_logger(__name__)
        set_log_context(self.logger, component="orchestrator")

    async def start(self):
        """Start the orchestrator and all services."""
        self.logger.info("Starting JobOrchestrator", extra={
            "engines_enabled": self.engine_manager is not None,
            "fault_tolerance_enabled": self.fault_tolerance is not None,
            "legacy_workers_enabled": self.enable_legacy_workers
        })

        try:
            # Start database
            await self.db.initialize()

            # Start core services
            await self.job_manager.start()
            await self.queue_manager.start()
            await self.monitoring_service.start()

            # Start worker manager if enabled
            if self.enable_legacy_workers and self.worker_manager:
                await self.worker_manager.start()

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
            self.queue_manager,
            self.job_manager
        ]

        # Add worker manager if enabled
        if self.enable_legacy_workers and self.worker_manager:
            services.append(self.worker_manager)

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

        Automatically determines whether to use Spark/local engines or traditional queue based on
        job type and configuration. Maintains complete backward compatibility.

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
                "job_type": job.job_type.value,
                "engine_manager_available": self.engine_manager is not None
            })

            # Submit to enhanced job manager (handles both engine and traditional execution)
            job_id = await self.job_manager.submit_job(job)

            # If engines are not available or job doesn't specify engine, use traditional queue
            if not self.engine_manager or not getattr(job, 'execution_engine', None):
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
        Get list of workers including both traditional workers and Spark executors.

        Args:
            status_filter: Optional status to filter by
            pool_filter: Optional pool to filter by

        Returns:
            List of worker dictionaries
        """
        workers = []

        try:
            # Get traditional workers if available
            if self.enable_legacy_workers and self.worker_manager:
                legacy_workers = await self.worker_manager.get_available_workers()
                for worker in legacy_workers:
                    worker_dict = worker.to_dict()
                    worker_dict["type"] = "legacy"
                    workers.append(worker_dict)

            # Get Spark executors if available
            if self.engine_manager:
                try:
                    engine_health = await self.engine_manager.get_engine_health()
                    if "spark" in engine_health and engine_health["spark"].get("status") == "healthy":
                        spark_info = engine_health["spark"]
                        resource_usage = spark_info.get("resource_usage", {})
                        executors = resource_usage.get("executors", 0)

                        for i in range(executors):
                            workers.append({
                                "worker_id": f"spark_executor_{i}",
                                "type": "spark",
                                "status": "healthy",
                                "node_name": f"spark-executor-{i}",
                                "worker_metadata": {
                                    "engine": "spark",
                                    "cores": resource_usage.get("total_cores", 0) // max(executors, 1),
                                    "memory": resource_usage.get("total_memory_mb", 0) // max(executors, 1)
                                }
                            })
                except Exception as e:
                    self.logger.warning(f"Failed to get Spark executor info: {str(e)}")

            # Apply filters
            if status_filter:
                workers = [w for w in workers if w.get("status") == status_filter]

            if pool_filter:
                workers = [w for w in workers if w.get("worker_metadata", {}).get("pool") == pool_filter]

            return workers

        except Exception as e:
            self.logger.error("Error getting workers", exc_info=True)
            return []

    # Monitoring Interface
    async def get_system_health(self) -> Dict[str, Any]:
        """
        Get comprehensive system health including engines.

        Returns:
            Enhanced system health dictionary
        """
        try:
            # Get base health from monitoring service
            health = await self.monitoring_service.get_system_health()

            health_dict = {
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

            # Add engine health if available
            if self.engine_manager:
                try:
                    engine_health = await self.engine_manager.get_engine_health()
                    health_dict["engines"] = engine_health
                except Exception as e:
                    health_dict["engines"] = {"error": str(e)}

            # Add fault tolerance status if available
            if self.fault_tolerance:
                try:
                    dlq = await self.fault_tolerance.get_dead_letter_queue()
                    circuit_breakers = await self.fault_tolerance.get_circuit_breaker_status()
                    health_dict["fault_tolerance"] = {
                        "dead_letter_queue_size": len(dlq),
                        "circuit_breakers": circuit_breakers
                    }
                except Exception as e:
                    health_dict["fault_tolerance"] = {"error": str(e)}

            return health_dict

        except Exception as e:
            self.logger.error("Error getting system health", exc_info=True)
            return {
                "overall_status": "unknown",
                "error": str(e)
            }

    async def get_performance_report(self, hours: int = 24) -> Dict[str, Any]:
        """
        Get enhanced performance report including engine statistics.

        Args:
            hours: Number of hours to include in report

        Returns:
            Enhanced performance report dictionary
        """
        try:
            # Get base report
            report = await self.monitoring_service.get_performance_report(hours)

            # Add job metrics from enhanced job manager
            try:
                job_metrics = await self.job_manager.get_job_metrics(hours)
                report["job_metrics"] = job_metrics
            except Exception as e:
                report["job_metrics"] = {"error": str(e)}

            # Add engine statistics if available
            if self.engine_manager:
                try:
                    engine_stats = await self.engine_manager.get_engine_statistics()
                    report["engine_statistics"] = engine_stats
                except Exception as e:
                    report["engine_statistics"] = {"error": str(e)}

            # Add fault tolerance metrics if available
            if self.fault_tolerance:
                try:
                    dlq = await self.fault_tolerance.get_dead_letter_queue()
                    report["fault_tolerance_metrics"] = {
                        "dead_letter_jobs": len(dlq),
                        "recent_failures": [
                            {
                                "job_id": entry["job_id"],
                                "error": entry["error_message"],
                                "attempts": entry["attempts"]
                            }
                            for entry in dlq[:10]  # Last 10 failures
                        ]
                    }
                except Exception as e:
                    report["fault_tolerance_metrics"] = {"error": str(e)}

            return report

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
        Perform a comprehensive health check including engines.

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

            # Check engine health if available
            if self.engine_manager:
                if not await self.engine_manager.health_check():
                    self.logger.warning("Engine manager health check failed")

            # Check if we can get system health
            health = await self.get_system_health()
            return health.get("overall_status") not in ["critical", "unknown"]

        except Exception:
            return False

    # Enhanced methods (available when engines/fault tolerance are configured)
    async def retry_job(self, job_id: str) -> Optional[str]:
        """Retry a failed job (requires enhanced mode)."""
        return await self.job_manager.retry_job(job_id)

    async def get_job_metrics(self, hours: int = 24) -> Dict[str, Any]:
        """Get detailed job execution metrics."""
        return await self.job_manager.get_job_metrics(hours)

    async def get_dead_letter_queue(self) -> List[Dict[str, Any]]:
        """Get jobs in dead letter queue (requires fault tolerance)."""
        return await self.job_manager.get_dead_letter_queue()

    async def reprocess_dead_letter_job(self, job_id: str) -> bool:
        """Reprocess a job from dead letter queue (requires fault tolerance)."""
        return await self.job_manager.reprocess_dead_letter_job(job_id)

    async def cleanup_old_jobs(self, days: int = 30) -> int:
        """Clean up old job records."""
        return await self.job_manager.cleanup_old_jobs(days)

    def get_engine_status(self) -> Dict[str, Any]:
        """Get status of execution engines."""
        return {
            "engines_available": self.engine_manager is not None,
            "fault_tolerance_available": self.fault_tolerance is not None,
            "legacy_workers_enabled": self.enable_legacy_workers,
            "enhanced_mode": self.job_manager.enhanced_mode
        }