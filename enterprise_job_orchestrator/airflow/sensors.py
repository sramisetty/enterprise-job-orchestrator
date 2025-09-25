"""
Airflow sensors for Enterprise Job Orchestrator integration.

Sensors that monitor job states and system conditions.
"""

import asyncio
from typing import Dict, Any, Optional
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context

from .hooks import EnterpriseJobHook
from ..utils.logger import get_logger


class EnterpriseJobSensor(BaseSensorOperator):
    """
    Sensor that monitors job completion status.

    Waits for a specific job to reach a target status before allowing
    downstream tasks to proceed.
    """

    template_fields = ('job_id', 'target_status')

    def __init__(
        self,
        job_id: str,
        target_status: str = "completed",
        enterprise_job_conn_id: str = "enterprise_job_default",
        **kwargs
    ):
        """
        Initialize the job sensor.

        Args:
            job_id: ID of job to monitor
            target_status: Status to wait for (completed, failed, cancelled)
            enterprise_job_conn_id: Airflow connection ID for orchestrator
        """
        super().__init__(**kwargs)
        self.job_id = job_id
        self.target_status = target_status
        self.enterprise_job_conn_id = enterprise_job_conn_id
        self.logger = get_logger(__name__)

    def poke(self, context: Context) -> bool:
        """
        Check if job has reached target status.

        Args:
            context: Airflow task context

        Returns:
            True if job has reached target status
        """
        # Create event loop for async operations
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            return loop.run_until_complete(self._async_poke(context))
        finally:
            loop.close()

    async def _async_poke(self, context: Context) -> bool:
        """Async implementation of status checking."""
        hook = EnterpriseJobHook(self.enterprise_job_conn_id)

        try:
            status = await hook.get_job_status(self.job_id)

            if not status:
                self.logger.warning(f"Job {self.job_id} not found")
                return False

            current_status = status.get("status")
            self.logger.info(f"Job {self.job_id} current status: {current_status}")

            return current_status == self.target_status

        except Exception as e:
            self.logger.error(f"Error checking job status: {str(e)}")
            return False
        finally:
            await hook.close_connection()


class EnterpriseSystemHealthSensor(BaseSensorOperator):
    """
    Sensor that monitors system health status.

    Waits for the orchestrator system to be in a healthy state
    before proceeding with job execution.
    """

    def __init__(
        self,
        min_healthy_workers: int = 1,
        max_error_rate: float = 0.1,
        max_queue_size: Optional[int] = None,
        enterprise_job_conn_id: str = "enterprise_job_default",
        **kwargs
    ):
        """
        Initialize the system health sensor.

        Args:
            min_healthy_workers: Minimum number of healthy workers required
            max_error_rate: Maximum acceptable error rate (0.0-1.0)
            max_queue_size: Maximum acceptable queue size
            enterprise_job_conn_id: Airflow connection ID for orchestrator
        """
        super().__init__(**kwargs)
        self.min_healthy_workers = min_healthy_workers
        self.max_error_rate = max_error_rate
        self.max_queue_size = max_queue_size
        self.enterprise_job_conn_id = enterprise_job_conn_id
        self.logger = get_logger(__name__)

    def poke(self, context: Context) -> bool:
        """
        Check if system health meets requirements.

        Args:
            context: Airflow task context

        Returns:
            True if system is healthy
        """
        # Create event loop for async operations
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            return loop.run_until_complete(self._async_poke(context))
        finally:
            loop.close()

    async def _async_poke(self, context: Context) -> bool:
        """Async implementation of health checking."""
        hook = EnterpriseJobHook(self.enterprise_job_conn_id)

        try:
            health = await hook.get_system_health()

            if not health or health.get("overall_status") == "unknown":
                self.logger.warning("Could not retrieve system health")
                return False

            # Check overall status
            overall_status = health.get("overall_status")
            if overall_status in ["critical", "unknown"]:
                self.logger.info(f"System status is {overall_status}")
                return False

            # Check healthy workers
            healthy_workers = health.get("healthy_workers", 0)
            if healthy_workers < self.min_healthy_workers:
                self.logger.info(
                    f"Insufficient healthy workers: {healthy_workers} < {self.min_healthy_workers}"
                )
                return False

            # Check error rate
            error_rate = health.get("error_rate", 0)
            if error_rate > self.max_error_rate:
                self.logger.info(f"Error rate too high: {error_rate} > {self.max_error_rate}")
                return False

            # Check queue size if specified
            if self.max_queue_size is not None:
                queue_size = health.get("queue_size", 0)
                if queue_size > self.max_queue_size:
                    self.logger.info(f"Queue size too large: {queue_size} > {self.max_queue_size}")
                    return False

            self.logger.info("System health check passed")
            return True

        except Exception as e:
            self.logger.error(f"Error checking system health: {str(e)}")
            return False
        finally:
            await hook.close_connection()


class EnterpriseResourceSensor(BaseSensorOperator):
    """
    Sensor that monitors resource availability.

    Waits for sufficient resources to be available before
    allowing resource-intensive jobs to proceed.
    """

    def __init__(
        self,
        min_cpu_percent: float = 20.0,
        min_memory_percent: float = 20.0,
        min_available_workers: int = 1,
        enterprise_job_conn_id: str = "enterprise_job_default",
        **kwargs
    ):
        """
        Initialize the resource sensor.

        Args:
            min_cpu_percent: Minimum available CPU percentage required
            min_memory_percent: Minimum available memory percentage required
            min_available_workers: Minimum number of available workers required
            enterprise_job_conn_id: Airflow connection ID for orchestrator
        """
        super().__init__(**kwargs)
        self.min_cpu_percent = min_cpu_percent
        self.min_memory_percent = min_memory_percent
        self.min_available_workers = min_available_workers
        self.enterprise_job_conn_id = enterprise_job_conn_id
        self.logger = get_logger(__name__)

    def poke(self, context: Context) -> bool:
        """
        Check if sufficient resources are available.

        Args:
            context: Airflow task context

        Returns:
            True if resources are available
        """
        # Create event loop for async operations
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            return loop.run_until_complete(self._async_poke(context))
        finally:
            loop.close()

    async def _async_poke(self, context: Context) -> bool:
        """Async implementation of resource checking."""
        hook = EnterpriseJobHook(self.enterprise_job_conn_id)

        try:
            health = await hook.get_system_health()

            if not health:
                self.logger.warning("Could not retrieve system health for resource check")
                return False

            # Check CPU availability (assuming lower usage means more available)
            avg_cpu = health.get("average_cpu_usage", 100)
            available_cpu = 100 - avg_cpu
            if available_cpu < self.min_cpu_percent:
                self.logger.info(f"Insufficient CPU available: {available_cpu}% < {self.min_cpu_percent}%")
                return False

            # Check memory availability
            avg_memory = health.get("average_memory_usage", 100)
            available_memory = 100 - avg_memory
            if available_memory < self.min_memory_percent:
                self.logger.info(
                    f"Insufficient memory available: {available_memory}% < {self.min_memory_percent}%"
                )
                return False

            # Check available workers
            healthy_workers = health.get("healthy_workers", 0)
            running_jobs = health.get("running_jobs", 0)
            # Assuming each worker can handle one job at a time for simplicity
            available_workers = max(0, healthy_workers - running_jobs)

            if available_workers < self.min_available_workers:
                self.logger.info(
                    f"Insufficient workers available: {available_workers} < {self.min_available_workers}"
                )
                return False

            self.logger.info("Resource availability check passed")
            return True

        except Exception as e:
            self.logger.error(f"Error checking resource availability: {str(e)}")
            return False
        finally:
            await hook.close_connection()