"""
JobManager service for Enterprise Job Orchestrator

Manages job lifecycle, execution tracking, and job-related operations.
Enhanced with Spark and Airflow integration while maintaining backward compatibility.
"""

import asyncio
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, TYPE_CHECKING
from uuid import uuid4

from ..models.job import Job, JobStatus, JobType, JobPriority
from ..models.execution import JobExecution, ExecutionResult
from ..utils.database import DatabaseManager
from ..utils.logger import get_logger, set_log_context
from ..core.exceptions import JobNotFoundError, JobExecutionError

# Conditional imports to avoid circular dependencies
if TYPE_CHECKING:
    from ..services.engine_manager import EngineManager
    from ..services.fault_tolerance import FaultToleranceService


class JobManager:
    """
    Manages job lifecycle and execution state.

    Provides capabilities for:
    - Job creation and persistence
    - Job status management
    - Progress tracking
    - Job history and metrics
    - Spark and Airflow integration
    - Fault tolerance and recovery
    """

    def __init__(
        self,
        database_manager: DatabaseManager,
        engine_manager: Optional['EngineManager'] = None,
        fault_tolerance_service: Optional['FaultToleranceService'] = None
    ):
        """
        Initialize JobManager.

        Args:
            database_manager: Database connection manager
            engine_manager: Optional engine manager for Spark/local execution
            fault_tolerance_service: Optional fault tolerance service
        """
        self.db = database_manager
        self.engine_manager = engine_manager
        self.fault_tolerance = fault_tolerance_service

        # Tracking for enhanced features
        self.active_jobs: Dict[str, Job] = {}
        self.job_executions: Dict[str, JobExecution] = {}
        self.job_results: Dict[str, ExecutionResult] = {}

        # Check if enhanced features are available
        self.enhanced_mode = engine_manager is not None

        self.logger = get_logger(__name__)
        set_log_context(self.logger, component="job_manager")

    async def start(self):
        """Start the job manager."""
        self.logger.info("Starting JobManager", extra={
            "enhanced_mode": self.enhanced_mode,
            "engine_manager_available": self.engine_manager is not None,
            "fault_tolerance_available": self.fault_tolerance is not None
        })

        # Initialize engine manager if available
        if self.engine_manager and self.enhanced_mode:
            await self.engine_manager.initialize()
            self.logger.info("Engine manager initialized")

    async def stop(self):
        """Stop the job manager."""
        self.logger.info("Stopping JobManager")

        # Cancel all active jobs
        for job_id in list(self.active_jobs.keys()):
            await self.cancel_job(job_id, force=True)

        # Shutdown engine manager
        if self.engine_manager and self.enhanced_mode:
            await self.engine_manager.shutdown()

    async def submit_job(self, job: Job) -> str:
        """
        Submit a new job with automatic engine selection and fault tolerance.

        Args:
            job: Job to submit

        Returns:
            Job ID
        """
        self.logger.info("Submitting job", extra={
            "job_id": job.job_id,
            "job_name": job.job_name,
            "job_type": job.job_type.value,
            "execution_engine": job.execution_engine,
            "enhanced_mode": self.enhanced_mode
        })

        try:
            # Auto-assign execution engine if not specified
            if not job.execution_engine and self.enhanced_mode:
                job.execution_engine = self._determine_execution_engine(job)

            # Store job in database
            await self.db.upsert_job(job)

            # Track active job for enhanced features
            if self.enhanced_mode:
                self.active_jobs[job.job_id] = job

                # Create job execution record
                execution = JobExecution(
                    job_id=job.job_id,
                    start_time=datetime.utcnow(),
                    status="pending"
                )
                self.job_executions[job.job_id] = execution

                # Start execution with engine if available
                if self.engine_manager and job.execution_engine:
                    asyncio.create_task(self._execute_job_with_engine(job))

            return job.job_id

        except Exception as e:
            self.logger.error("Failed to submit job", extra={
                "job_id": job.job_id,
                "error": str(e)
            })
            raise JobExecutionError(f"Failed to submit job: {str(e)}")

    def _determine_execution_engine(self, job: Job) -> str:
        """Determine the best execution engine for a job."""
        if job.job_type in [JobType.DATA_PROCESSING, JobType.ETL, JobType.ML_TRAINING, JobType.ANALYTICS]:
            return "spark"
        else:
            return "local"

    async def _execute_job_with_engine(self, job: Job):
        """Execute job using the engine manager."""
        job_id = job.job_id

        try:
            self.logger.info(f"Starting job execution with engine: {job_id}")

            # Update job status
            job.status = JobStatus.RUNNING
            job.started_at = datetime.utcnow()
            await self.db.upsert_job(job)

            # Execute with fault tolerance if available
            if self.fault_tolerance:
                result = await self.fault_tolerance.execute_with_retry(
                    func=self.engine_manager.execute_job,
                    job=job,
                    retry_policy_name=self._get_retry_policy(job)
                )
            else:
                result = await self.engine_manager.execute_job(job)

            # Store result
            self.job_results[job_id] = result

            # Update job status
            job.status = JobStatus.COMPLETED
            job.completed_at = datetime.utcnow()
            job.result = result.result
            await self.db.upsert_job(job)

            self.logger.info(f"Job completed successfully: {job_id}")

        except Exception as e:
            self.logger.error(f"Job execution failed: {job_id}", exc_info=True)

            # Update job status
            job.status = JobStatus.FAILED
            job.error_message = str(e)
            job.completed_at = datetime.utcnow()
            await self.db.upsert_job(job)

            # Send to dead letter queue if fault tolerance is enabled
            if self.fault_tolerance:
                await self.fault_tolerance._send_to_dead_letter_queue(
                    job, str(e), getattr(job, 'retry_count', 0)
                )

        finally:
            # Cleanup
            self.active_jobs.pop(job_id, None)

    def _get_retry_policy(self, job: Job) -> str:
        """Determine retry policy based on job characteristics."""
        if hasattr(job.priority, 'value') and job.priority.value == "critical":
            return "critical_job"
        elif job.job_type in [JobType.DATA_PROCESSING, JobType.ETL]:
            return "network_error"
        else:
            return "default"

    async def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get enhanced job status including engine information."""
        try:
            # Get job from database
            job = await self.db.get_job(job_id)
            if not job:
                return None

            status = job.to_dict()

            # Add enhanced information if available
            if self.enhanced_mode:
                # Add execution information if available
                if job_id in self.job_executions:
                    execution = self.job_executions[job_id]
                    status["execution"] = {
                        "start_time": execution.start_time.isoformat() if execution.start_time else None,
                        "status": execution.status
                    }

                # Add result information if available
                if job_id in self.job_results:
                    result = self.job_results[job_id]
                    status["result"] = {
                        "status": result.status.value,
                        "execution_time_seconds": result.execution_time_seconds,
                        "resource_usage": result.resource_usage,
                        "metadata": result.metadata
                    }

                # Get engine-specific status if available
                if self.engine_manager:
                    engine_status = await self.engine_manager.get_job_status(job_id)
                    if engine_status:
                        status["engine_status"] = engine_status

            return status

        except Exception as e:
            self.logger.error(f"Error getting job status", extra={
                "job_id": job_id,
                "error": str(e)
            })
            return None

    async def list_jobs(
        self,
        status_filter: Optional[str] = None,
        engine_filter: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """List jobs with enhanced filtering options."""
        try:
            jobs = await self.db.get_all_jobs()

            # Apply filters
            if status_filter:
                jobs = [j for j in jobs if j.status.value == status_filter]

            if engine_filter and self.enhanced_mode:
                jobs = [j for j in jobs if j.execution_engine == engine_filter]

            # Convert to dictionaries and enhance with additional info
            job_list = []
            for job in jobs[:limit]:
                job_dict = job.to_dict()

                # Add enhanced information if available
                if self.enhanced_mode:
                    # Add active status
                    job_dict["is_active"] = job.job_id in self.active_jobs

                    # Add result if available
                    if job.job_id in self.job_results:
                        result = self.job_results[job.job_id]
                        job_dict["has_result"] = True
                        job_dict["execution_time"] = result.execution_time_seconds

                job_list.append(job_dict)

            return job_list

        except Exception as e:
            self.logger.error("Error listing jobs", exc_info=True)
            return []

    async def cancel_job(self, job_id: str, force: bool = False) -> bool:
        """Cancel a job with enhanced engine support."""
        try:
            self.logger.info("Cancelling job", extra={
                "job_id": job_id,
                "force": force
            })

            # Get job from database
            job = await self.db.get_job(job_id)
            if not job:
                raise JobNotFoundError(f"Job {job_id} not found")

            # Cancel in engine manager if running there
            if self.enhanced_mode and self.engine_manager:
                engine_cancelled = await self.engine_manager.cancel_job(job_id, force)
                if engine_cancelled:
                    self.logger.info(f"Job cancelled in engine: {job_id}")

            # Update job status
            job.status = JobStatus.CANCELLED
            job.updated_at = datetime.utcnow()
            job.completed_at = datetime.utcnow()
            await self.db.upsert_job(job)

            # Cleanup tracking
            if self.enhanced_mode:
                self.active_jobs.pop(job_id, None)
                self.job_executions.pop(job_id, None)

            self.logger.info("Job cancelled successfully", extra={"job_id": job_id})
            return True

        except Exception as e:
            self.logger.error("Failed to cancel job", extra={
                "job_id": job_id,
                "error": str(e)
            })
            return False

    # Enhanced methods (available only in enhanced mode)
    async def retry_job(self, job_id: str) -> Optional[str]:
        """Retry a failed job (enhanced mode only)."""
        if not self.enhanced_mode:
            self.logger.warning("retry_job requires enhanced mode")
            return None

        try:
            # Get original job
            original_job = await self.db.get_job(job_id)
            if not original_job:
                raise JobNotFoundError(f"Job {job_id} not found")

            # Create new job as retry
            retry_job = Job(
                job_name=f"{original_job.job_name}_retry",
                job_type=original_job.job_type,
                job_data=original_job.job_data,
                execution_engine=original_job.execution_engine,
                priority=original_job.priority,
                job_metadata={
                    **getattr(original_job, 'job_metadata', {}),
                    "original_job_id": job_id,
                    "is_retry": True,
                    "retry_attempt": getattr(original_job, 'retry_count', 0) + 1
                },
                parent_job_id=job_id,
                retry_count=getattr(original_job, 'retry_count', 0) + 1
            )

            # Submit retry job
            new_job_id = await self.submit_job(retry_job)

            self.logger.info(f"Job retry submitted", extra={
                "original_job_id": job_id,
                "retry_job_id": new_job_id,
                "retry_attempt": retry_job.retry_count
            })

            return new_job_id

        except Exception as e:
            self.logger.error(f"Failed to retry job", extra={
                "job_id": job_id,
                "error": str(e)
            })
            return None

    async def get_job_metrics(self, hours: int = 24) -> Dict[str, Any]:
        """Get job execution metrics."""
        try:
            since = datetime.utcnow() - timedelta(hours=hours)
            jobs = await self.db.get_all_jobs()

            # Filter jobs by time
            recent_jobs = [j for j in jobs if j.created_at >= since]

            # Calculate metrics
            metrics = {
                "total_jobs": len(recent_jobs),
                "active_jobs": len(self.active_jobs) if self.enhanced_mode else 0,
                "jobs_by_status": {},
                "jobs_by_type": {},
                "average_execution_time": 0,
                "success_rate": 0
            }

            # Count by status
            for job in recent_jobs:
                status = job.status.value
                metrics["jobs_by_status"][status] = metrics["jobs_by_status"].get(status, 0) + 1

                # Count by type
                job_type = job.job_type.value
                metrics["jobs_by_type"][job_type] = metrics["jobs_by_type"].get(job_type, 0) + 1

            # Add engine metrics if in enhanced mode
            if self.enhanced_mode:
                metrics["jobs_by_engine"] = {}
                for job in recent_jobs:
                    engine = getattr(job, 'execution_engine', None) or "legacy"
                    metrics["jobs_by_engine"][engine] = metrics["jobs_by_engine"].get(engine, 0) + 1

                # Add engine health if available
                if self.engine_manager:
                    metrics["engine_health"] = await self.engine_manager.get_engine_health()

            # Calculate success rate
            completed = metrics["jobs_by_status"].get("completed", 0)
            failed = metrics["jobs_by_status"].get("failed", 0)
            if completed + failed > 0:
                metrics["success_rate"] = completed / (completed + failed)

            return metrics

        except Exception as e:
            self.logger.error(f"Error calculating job metrics", exc_info=True)
            return {}

    async def get_dead_letter_queue(self) -> List[Dict[str, Any]]:
        """Get dead letter queue entries (enhanced mode only)."""
        if not self.enhanced_mode or not self.fault_tolerance:
            return []
        return await self.fault_tolerance.get_dead_letter_queue()

    async def reprocess_dead_letter_job(self, job_id: str) -> bool:
        """Reprocess a job from dead letter queue (enhanced mode only)."""
        if not self.enhanced_mode or not self.fault_tolerance:
            return False
        return await self.fault_tolerance.reprocess_dead_letter_job(job_id)

    async def cleanup_old_jobs(self, days: int = 30) -> int:
        """Clean up old job records."""
        try:
            cutoff = datetime.utcnow() - timedelta(days=days)
            jobs = await self.db.get_all_jobs()

            cleaned = 0
            for job in jobs:
                if (hasattr(job, 'completed_at') and job.completed_at and job.completed_at < cutoff and
                    job.status in [JobStatus.COMPLETED, JobStatus.FAILED, JobStatus.CANCELLED]):

                    if self.enhanced_mode:
                        # Remove from tracking
                        self.job_results.pop(job.job_id, None)
                        self.job_executions.pop(job.job_id, None)

                    cleaned += 1

            self.logger.info(f"Cleaned up {cleaned} old jobs")
            return cleaned

        except Exception as e:
            self.logger.error(f"Error cleaning up old jobs", exc_info=True)
            return 0