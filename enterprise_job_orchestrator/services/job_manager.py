"""
JobManager service for Enterprise Job Orchestrator

Manages job lifecycle, execution tracking, and job-related operations.
"""

import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Any
from uuid import uuid4

from ..models.job import Job, JobStatus
from ..models.execution import JobExecution
from ..utils.database import DatabaseManager
from ..utils.logger import get_logger, set_log_context
from ..core.exceptions import JobNotFoundError, JobExecutionError


class JobManager:
    """
    Manages job lifecycle and execution state.

    Provides capabilities for:
    - Job creation and persistence
    - Job status management
    - Progress tracking
    - Job history and metrics
    """

    def __init__(self, database_manager: DatabaseManager):
        """
        Initialize JobManager.

        Args:
            database_manager: Database connection manager
        """
        self.db = database_manager
        self.logger = get_logger(__name__)
        set_log_context(self.logger, component="job_manager")

    async def start(self):
        """Start the job manager."""
        self.logger.info("Starting JobManager")

    async def stop(self):
        """Stop the job manager."""
        self.logger.info("Stopping JobManager")

    async def submit_job(self, job: Job) -> str:
        """
        Submit a new job.

        Args:
            job: Job to submit

        Returns:
            Job ID
        """
        self.logger.info("Submitting job", extra={
            "job_id": job.job_id,
            "job_name": job.job_name
        })

        await self.db.upsert_job(job)
        return job.job_id

    async def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get job status."""
        job = await self.db.get_job(job_id)
        if job:
            return job.to_dict()
        return None

    async def list_jobs(self, status_filter: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
        """List jobs with optional filtering."""
        jobs = await self.db.get_all_jobs()

        if status_filter:
            jobs = [j for j in jobs if j.status.value == status_filter]

        return [job.to_dict() for job in jobs[:limit]]

    async def cancel_job(self, job_id: str, force: bool = False) -> bool:
        """Cancel a job."""
        try:
            job = await self.db.get_job(job_id)
            if not job:
                raise JobNotFoundError(f"Job {job_id} not found")

            job.status = JobStatus.CANCELLED
            job.updated_at = datetime.utcnow()

            await self.db.upsert_job(job)

            self.logger.info("Job cancelled", extra={"job_id": job_id})
            return True

        except Exception as e:
            self.logger.error("Failed to cancel job", extra={
                "job_id": job_id,
                "error": str(e)
            })
            return False