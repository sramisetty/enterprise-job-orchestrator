"""
Airflow hooks for Enterprise Job Orchestrator integration.

Provides connection and interaction capabilities between Airflow and the orchestrator.
"""

import asyncio
from typing import Dict, Any, Optional, List
from airflow.hooks.base import BaseHook
from airflow.utils.context import Context

from ..core.orchestrator import JobOrchestrator
from ..models.job import Job
from ..utils.database import DatabaseManager
from ..utils.logger import get_logger


class EnterpriseJobHook(BaseHook):
    """
    Hook for interacting with Enterprise Job Orchestrator.

    Provides methods to submit, monitor, and manage jobs through Airflow.
    """

    conn_name_attr = "enterprise_job_conn_id"
    default_conn_name = "enterprise_job_default"
    conn_type = "enterprise_job_orchestrator"
    hook_name = "Enterprise Job Orchestrator"

    def __init__(self, enterprise_job_conn_id: str = default_conn_name):
        """
        Initialize the hook.

        Args:
            enterprise_job_conn_id: Airflow connection ID for the orchestrator
        """
        super().__init__()
        self.enterprise_job_conn_id = enterprise_job_conn_id
        self.orchestrator: Optional[JobOrchestrator] = None
        self.logger = get_logger(__name__)

    def get_connection(self):
        """Get the connection object."""
        return self.get_connection(self.enterprise_job_conn_id)

    async def get_orchestrator(self) -> JobOrchestrator:
        """Get or create JobOrchestrator instance."""
        if self.orchestrator is None:
            # Get connection details from Airflow connection
            conn = self.get_connection(self.enterprise_job_conn_id)

            # Initialize database manager with connection details
            db_config = {
                "host": conn.host or "localhost",
                "port": conn.port or 5432,
                "database": conn.schema or "enterprise_jobs",
                "user": conn.login,
                "password": conn.password,
                "extra": conn.extra_dejson
            }

            db_manager = DatabaseManager(db_config)
            self.orchestrator = JobOrchestrator(db_manager)

            # Start orchestrator if not already running
            if not self.orchestrator.is_running():
                await self.orchestrator.start()

        return self.orchestrator

    async def submit_job(self, job: Job) -> str:
        """
        Submit a job to the orchestrator.

        Args:
            job: Job instance to submit

        Returns:
            Job ID of submitted job
        """
        orchestrator = await self.get_orchestrator()
        return await orchestrator.submit_job(job)

    async def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get status of a job.

        Args:
            job_id: ID of job to query

        Returns:
            Job status dictionary or None if not found
        """
        orchestrator = await self.get_orchestrator()
        return await orchestrator.get_job_status(job_id)

    async def wait_for_job_completion(
        self,
        job_id: str,
        timeout: Optional[int] = None,
        check_interval: int = 30
    ) -> Dict[str, Any]:
        """
        Wait for job completion with polling.

        Args:
            job_id: ID of job to wait for
            timeout: Maximum time to wait in seconds
            check_interval: Polling interval in seconds

        Returns:
            Final job status dictionary
        """
        import time
        start_time = time.time()

        while True:
            status = await self.get_job_status(job_id)
            if not status:
                raise Exception(f"Job {job_id} not found")

            job_status = status.get("status")
            if job_status in ["completed", "failed", "cancelled"]:
                return status

            if timeout and (time.time() - start_time) > timeout:
                raise Exception(f"Job {job_id} timed out after {timeout} seconds")

            await asyncio.sleep(check_interval)

    async def cancel_job(self, job_id: str, force: bool = False) -> bool:
        """
        Cancel a running job.

        Args:
            job_id: ID of job to cancel
            force: Whether to force cancellation

        Returns:
            True if cancellation successful
        """
        orchestrator = await self.get_orchestrator()
        return await orchestrator.cancel_job(job_id, force)

    async def get_system_health(self) -> Dict[str, Any]:
        """
        Get system health status.

        Returns:
            System health dictionary
        """
        orchestrator = await self.get_orchestrator()
        return await orchestrator.get_system_health()

    async def list_jobs(
        self,
        status_filter: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        List jobs with optional filtering.

        Args:
            status_filter: Optional status to filter by
            limit: Maximum number of jobs to return

        Returns:
            List of job dictionaries
        """
        orchestrator = await self.get_orchestrator()
        return await orchestrator.list_jobs(status_filter, limit)

    def test_connection(self) -> bool:
        """
        Test connection to the orchestrator.

        Returns:
            True if connection successful
        """
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            orchestrator = loop.run_until_complete(self.get_orchestrator())
            health_check = loop.run_until_complete(orchestrator.health_check())
            loop.close()
            return health_check
        except Exception as e:
            self.logger.error(f"Connection test failed: {str(e)}")
            return False

    async def close_connection(self):
        """Close the orchestrator connection."""
        if self.orchestrator and self.orchestrator.is_running():
            await self.orchestrator.stop()
            self.orchestrator = None