"""
Exception classes for Enterprise Job Orchestrator

Provides a comprehensive hierarchy of exceptions for different error conditions
that can occur during job orchestration and processing.
"""

from typing import Optional, Dict, Any


class JobOrchestratorError(Exception):
    """Base exception for all job orchestrator errors."""

    def __init__(self, message: str, error_code: Optional[str] = None, details: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.message = message
        self.error_code = error_code
        self.details = details or {}

    def to_dict(self) -> Dict[str, Any]:
        """Convert exception to dictionary for JSON serialization."""
        return {
            "error": self.__class__.__name__,
            "message": self.message,
            "error_code": self.error_code,
            "details": self.details
        }


class JobNotFoundError(JobOrchestratorError):
    """Raised when a requested job cannot be found."""

    def __init__(self, job_id: str):
        super().__init__(
            f"Job {job_id} not found",
            error_code="JOB_NOT_FOUND",
            details={"job_id": job_id}
        )


class JobSubmissionError(JobOrchestratorError):
    """Raised when job submission fails."""

    def __init__(self, message: str, job_config: Optional[Dict[str, Any]] = None):
        super().__init__(
            f"Job submission failed: {message}",
            error_code="JOB_SUBMISSION_ERROR",
            details={"job_config": job_config}
        )


class JobExecutionError(JobOrchestratorError):
    """Raised when job execution encounters an error."""

    def __init__(self, job_id: str, message: str, stage: Optional[str] = None):
        super().__init__(
            f"Job {job_id} execution failed: {message}",
            error_code="JOB_EXECUTION_ERROR",
            details={"job_id": job_id, "stage": stage}
        )


class WorkerNotFoundError(JobOrchestratorError):
    """Raised when a requested worker cannot be found."""

    def __init__(self, worker_id: str):
        super().__init__(
            f"Worker {worker_id} not found",
            error_code="WORKER_NOT_FOUND",
            details={"worker_id": worker_id}
        )


class WorkerRegistrationError(JobOrchestratorError):
    """Raised when worker registration fails."""

    def __init__(self, message: str, worker_id: Optional[str] = None):
        super().__init__(
            f"Worker registration failed: {message}",
            error_code="WORKER_REGISTRATION_ERROR",
            details={"worker_id": worker_id}
        )


class WorkerAssignmentError(JobOrchestratorError):
    """Raised when worker job assignment fails."""

    def __init__(self, message: str, worker_id: Optional[str] = None, job_id: Optional[str] = None):
        super().__init__(
            f"Worker assignment failed: {message}",
            error_code="WORKER_ASSIGNMENT_ERROR",
            details={"worker_id": worker_id, "job_id": job_id}
        )


class NoAvailableWorkersError(JobOrchestratorError):
    """Raised when no workers are available to process a job."""

    def __init__(self, message: str, required_capabilities: Optional[list] = None):
        super().__init__(
            message,
            error_code="NO_AVAILABLE_WORKERS",
            details={"required_capabilities": required_capabilities}
        )


class ConfigurationError(JobOrchestratorError):
    """Raised when there's an error in configuration."""

    def __init__(self, config_key: str, message: str):
        super().__init__(
            f"Configuration error for {config_key}: {message}",
            error_code="CONFIGURATION_ERROR",
            details={"config_key": config_key}
        )


class DatabaseError(JobOrchestratorError):
    """Raised when database operations fail."""

    def __init__(self, operation: str, message: str, table: Optional[str] = None):
        super().__init__(
            f"Database operation '{operation}' failed: {message}",
            error_code="DATABASE_ERROR",
            details={"operation": operation, "table": table}
        )


class QueueError(JobOrchestratorError):
    """Raised when queue operations fail."""

    def __init__(self, operation: str, message: str):
        super().__init__(
            f"Queue operation '{operation}' failed: {message}",
            error_code="QUEUE_ERROR",
            details={"operation": operation}
        )


class MonitoringError(JobOrchestratorError):
    """Raised when monitoring operations fail."""

    def __init__(self, component: str, message: str):
        super().__init__(
            f"Monitoring error in {component}: {message}",
            error_code="MONITORING_ERROR",
            details={"component": component}
        )


class OrchestratorError(JobOrchestratorError):
    """Raised when orchestrator-level operations fail."""

    def __init__(self, message: str):
        super().__init__(
            f"Orchestrator error: {message}",
            error_code="ORCHESTRATOR_ERROR"
        )


class ValidationError(JobOrchestratorError):
    """Raised when input validation fails."""

    def __init__(self, field: str, message: str, value: Any = None):
        super().__init__(
            f"Validation error for {field}: {message}",
            error_code="VALIDATION_ERROR",
            details={"field": field, "value": str(value) if value is not None else None}
        )


class ResourceExhaustedError(JobOrchestratorError):
    """Raised when system resources are exhausted."""

    def __init__(self, resource_type: str, current_usage: Optional[int] = None, limit: Optional[int] = None):
        message = f"Resource exhausted: {resource_type}"
        if current_usage and limit:
            message += f" ({current_usage}/{limit})"

        super().__init__(
            message,
            error_code="RESOURCE_EXHAUSTED",
            details={"resource_type": resource_type, "current_usage": current_usage, "limit": limit}
        )


class TimeoutError(JobOrchestratorError):
    """Raised when operations timeout."""

    def __init__(self, operation: str, timeout_seconds: int):
        super().__init__(
            f"Operation '{operation}' timed out after {timeout_seconds} seconds",
            error_code="TIMEOUT_ERROR",
            details={"operation": operation, "timeout_seconds": timeout_seconds}
        )


# Global error registry for tracking patterns
class ErrorRegistry:
    """Registry for tracking and analyzing errors."""

    def __init__(self):
        self.error_counts = {}

    def record_error(self, error: JobOrchestratorError):
        """Record an error for analysis."""
        error_type = error.__class__.__name__
        self.error_counts[error_type] = self.error_counts.get(error_type, 0) + 1

    def get_error_statistics(self) -> Dict[str, Any]:
        """Get error statistics for monitoring."""
        return {
            "total_errors": sum(self.error_counts.values()),
            "error_counts": self.error_counts,
            "most_common_error": max(self.error_counts.items(), key=lambda x: x[1])[0] if self.error_counts else None
        }


# Global error registry instance
error_registry = ErrorRegistry()