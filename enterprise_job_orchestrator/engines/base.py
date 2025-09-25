"""
Base execution engine interface.

Defines the common interface that all execution engines must implement.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from ..models.job import Job
from ..models.execution import ExecutionResult


class BaseExecutionEngine(ABC):
    """
    Abstract base class for all execution engines.

    This interface ensures consistent behavior across different execution
    frameworks (Spark, local, etc.).
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the execution engine.

        Args:
            config: Engine-specific configuration
        """
        self.config = config
        self._is_initialized = False

    @abstractmethod
    async def initialize(self) -> bool:
        """
        Initialize the execution engine.

        Returns:
            True if initialization successful
        """
        pass

    @abstractmethod
    async def shutdown(self) -> bool:
        """
        Shutdown the execution engine and clean up resources.

        Returns:
            True if shutdown successful
        """
        pass

    @abstractmethod
    async def execute_job(self, job: Job) -> ExecutionResult:
        """
        Execute a job using this engine.

        Args:
            job: Job to execute

        Returns:
            ExecutionResult with job outcome
        """
        pass

    @abstractmethod
    async def cancel_job(self, job_id: str, force: bool = False) -> bool:
        """
        Cancel a running job.

        Args:
            job_id: ID of job to cancel
            force: Whether to force cancellation

        Returns:
            True if cancellation successful
        """
        pass

    @abstractmethod
    async def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get status of a running job.

        Args:
            job_id: ID of job to query

        Returns:
            Job status dictionary or None if not found
        """
        pass

    @abstractmethod
    def supports_job_type(self, job_type: str) -> bool:
        """
        Check if this engine supports the given job type.

        Args:
            job_type: Type of job to check

        Returns:
            True if job type is supported
        """
        pass

    @abstractmethod
    async def get_resource_usage(self) -> Dict[str, Any]:
        """
        Get current resource usage of the engine.

        Returns:
            Resource usage statistics
        """
        pass

    @property
    def is_initialized(self) -> bool:
        """Check if engine is initialized."""
        return self._is_initialized

    @property
    def engine_name(self) -> str:
        """Get the name of this engine."""
        return self.__class__.__name__