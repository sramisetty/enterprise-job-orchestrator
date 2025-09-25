"""
Fault tolerance and recovery mechanisms.

Provides comprehensive fault tolerance including:
- Job retry with exponential backoff
- Dead letter queues for failed jobs
- Circuit breaker patterns for external dependencies
- Health checks and automatic recovery
- Checkpoint and recovery for long-running jobs
"""

import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from enum import Enum
from dataclasses import dataclass

from ..models.job import Job, JobStatus
from ..models.execution import ExecutionResult, ExecutionStatus
from ..utils.logger import get_logger
from ..core.exceptions import RetryableError, NonRetryableError


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


@dataclass
class RetryPolicy:
    """Configuration for retry behavior."""
    max_attempts: int = 3
    initial_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""
    failure_threshold: int = 5
    success_threshold: int = 3
    timeout_duration: float = 60.0


class CircuitBreaker:
    """
    Circuit breaker implementation for fault tolerance.

    Prevents cascading failures by temporarily disabling calls to
    failing services and allowing them time to recover.
    """

    def __init__(self, config: CircuitBreakerConfig):
        """Initialize circuit breaker."""
        self.config = config
        self.state = CircuitState.CLOSED
        self.failure_count = 0
        self.success_count = 0
        self.next_attempt_time = None
        self.logger = get_logger(__name__)

    async def call(self, func: Callable, *args, **kwargs):
        """Execute function with circuit breaker protection."""
        if self.state == CircuitState.OPEN:
            if datetime.utcnow() < self.next_attempt_time:
                raise Exception("Circuit breaker is OPEN")
            else:
                self.state = CircuitState.HALF_OPEN
                self.success_count = 0

        try:
            result = await func(*args, **kwargs)
            await self._on_success()
            return result
        except Exception as e:
            await self._on_failure()
            raise e

    async def _on_success(self):
        """Handle successful call."""
        if self.state == CircuitState.HALF_OPEN:
            self.success_count += 1
            if self.success_count >= self.config.success_threshold:
                self.state = CircuitState.CLOSED
                self.failure_count = 0
        elif self.state == CircuitState.CLOSED:
            self.failure_count = max(0, self.failure_count - 1)

    async def _on_failure(self):
        """Handle failed call."""
        self.failure_count += 1

        if self.failure_count >= self.config.failure_threshold:
            self.state = CircuitState.OPEN
            self.next_attempt_time = datetime.utcnow() + timedelta(seconds=self.config.timeout_duration)
            self.logger.warning(f"Circuit breaker opened due to {self.failure_count} failures")


class FaultToleranceService:
    """
    Comprehensive fault tolerance service.

    Provides retry mechanisms, circuit breakers, checkpointing,
    and recovery capabilities for job execution.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize fault tolerance service.

        Args:
            config: Fault tolerance configuration
        """
        self.config = config
        self.retry_policies: Dict[str, RetryPolicy] = {}
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.job_checkpoints: Dict[str, Dict[str, Any]] = {}
        self.dead_letter_queue: List[Dict[str, Any]] = []
        self.logger = get_logger(__name__)

        # Initialize default retry policies
        self._initialize_retry_policies()

        # Initialize circuit breakers
        self._initialize_circuit_breakers()

    def _initialize_retry_policies(self):
        """Initialize retry policies for different job types and errors."""
        default_policy = RetryPolicy(
            max_attempts=self.config.get("default_max_attempts", 3),
            initial_delay=self.config.get("default_initial_delay", 1.0),
            max_delay=self.config.get("default_max_delay", 60.0)
        )

        self.retry_policies = {
            "default": default_policy,
            "network_error": RetryPolicy(max_attempts=5, initial_delay=2.0, max_delay=120.0),
            "resource_exhausted": RetryPolicy(max_attempts=3, initial_delay=30.0, max_delay=300.0),
            "temporary_failure": RetryPolicy(max_attempts=2, initial_delay=5.0, max_delay=30.0),
            "critical_job": RetryPolicy(max_attempts=5, initial_delay=1.0, max_delay=30.0)
        }

    def _initialize_circuit_breakers(self):
        """Initialize circuit breakers for external dependencies."""
        default_config = CircuitBreakerConfig(
            failure_threshold=self.config.get("circuit_failure_threshold", 5),
            success_threshold=self.config.get("circuit_success_threshold", 3),
            timeout_duration=self.config.get("circuit_timeout_duration", 60.0)
        )

        # Circuit breakers for different services
        self.circuit_breakers = {
            "database": CircuitBreaker(default_config),
            "spark_cluster": CircuitBreaker(default_config),
            "external_api": CircuitBreaker(default_config)
        }

    async def execute_with_retry(
        self,
        func: Callable,
        job: Job,
        retry_policy_name: str = "default",
        *args,
        **kwargs
    ) -> Any:
        """
        Execute function with retry logic.

        Args:
            func: Function to execute
            job: Job being executed
            retry_policy_name: Name of retry policy to use

        Returns:
            Function result
        """
        policy = self.retry_policies.get(retry_policy_name, self.retry_policies["default"])
        attempt = 0
        last_exception = None

        while attempt < policy.max_attempts:
            try:
                self.logger.info(f"Executing job {job.job_id}, attempt {attempt + 1}/{policy.max_attempts}")

                result = await func(*args, **kwargs)

                if attempt > 0:
                    self.logger.info(f"Job {job.job_id} succeeded on attempt {attempt + 1}")

                return result

            except NonRetryableError as e:
                self.logger.error(f"Non-retryable error for job {job.job_id}: {str(e)}")
                await self._send_to_dead_letter_queue(job, str(e), attempt + 1)
                raise e

            except Exception as e:
                attempt += 1
                last_exception = e

                self.logger.warning(f"Job {job.job_id} failed on attempt {attempt}: {str(e)}")

                if attempt >= policy.max_attempts:
                    self.logger.error(f"Job {job.job_id} failed after {attempt} attempts")
                    await self._send_to_dead_letter_queue(job, str(e), attempt)
                    raise e

                # Calculate delay with exponential backoff and jitter
                delay = min(
                    policy.initial_delay * (policy.exponential_base ** (attempt - 1)),
                    policy.max_delay
                )

                if policy.jitter:
                    import random
                    delay *= (0.5 + random.random() * 0.5)  # Add jitter: 50%-100% of calculated delay

                self.logger.info(f"Retrying job {job.job_id} in {delay:.2f} seconds")
                await asyncio.sleep(delay)

        raise last_exception

    async def execute_with_circuit_breaker(
        self,
        func: Callable,
        circuit_breaker_name: str,
        *args,
        **kwargs
    ) -> Any:
        """
        Execute function with circuit breaker protection.

        Args:
            func: Function to execute
            circuit_breaker_name: Name of circuit breaker to use

        Returns:
            Function result
        """
        circuit_breaker = self.circuit_breakers.get(circuit_breaker_name)
        if not circuit_breaker:
            # If no circuit breaker found, execute directly
            return await func(*args, **kwargs)

        return await circuit_breaker.call(func, *args, **kwargs)

    async def create_checkpoint(self, job_id: str, checkpoint_data: Dict[str, Any]):
        """
        Create a checkpoint for a job.

        Args:
            job_id: ID of the job
            checkpoint_data: Data to checkpoint
        """
        try:
            self.job_checkpoints[job_id] = {
                "timestamp": datetime.utcnow().isoformat(),
                "data": checkpoint_data
            }

            # Optionally persist to storage
            if self.config.get("persist_checkpoints", True):
                await self._persist_checkpoint(job_id, checkpoint_data)

            self.logger.info(f"Created checkpoint for job {job_id}")

        except Exception as e:
            self.logger.error(f"Failed to create checkpoint for job {job_id}: {str(e)}")

    async def restore_from_checkpoint(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Restore job from checkpoint.

        Args:
            job_id: ID of the job

        Returns:
            Checkpoint data if found
        """
        try:
            # First try in-memory checkpoints
            if job_id in self.job_checkpoints:
                checkpoint = self.job_checkpoints[job_id]
                self.logger.info(f"Restored job {job_id} from in-memory checkpoint")
                return checkpoint["data"]

            # Try to restore from persistent storage
            if self.config.get("persist_checkpoints", True):
                checkpoint_data = await self._restore_checkpoint_from_storage(job_id)
                if checkpoint_data:
                    self.logger.info(f"Restored job {job_id} from persistent checkpoint")
                    return checkpoint_data

            return None

        except Exception as e:
            self.logger.error(f"Failed to restore checkpoint for job {job_id}: {str(e)}")
            return None

    async def _persist_checkpoint(self, job_id: str, checkpoint_data: Dict[str, Any]):
        """Persist checkpoint to storage (implementation depends on storage backend)."""
        # This would integrate with your chosen storage backend
        # For now, we'll simulate persistence
        checkpoint_file = f"/tmp/checkpoints/{job_id}.json"

        try:
            import os
            os.makedirs(os.path.dirname(checkpoint_file), exist_ok=True)

            with open(checkpoint_file, 'w') as f:
                json.dump({
                    "timestamp": datetime.utcnow().isoformat(),
                    "data": checkpoint_data
                }, f)

        except Exception as e:
            self.logger.warning(f"Failed to persist checkpoint to file: {str(e)}")

    async def _restore_checkpoint_from_storage(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Restore checkpoint from storage."""
        checkpoint_file = f"/tmp/checkpoints/{job_id}.json"

        try:
            with open(checkpoint_file, 'r') as f:
                checkpoint = json.load(f)
                return checkpoint.get("data")

        except FileNotFoundError:
            return None
        except Exception as e:
            self.logger.warning(f"Failed to restore checkpoint from file: {str(e)}")
            return None

    async def _send_to_dead_letter_queue(self, job: Job, error_message: str, attempts: int):
        """Send failed job to dead letter queue."""
        dead_letter_entry = {
            "job_id": job.job_id,
            "job_name": job.job_name,
            "job_type": job.job_type.value,
            "job_data": job.job_data,
            "error_message": error_message,
            "attempts": attempts,
            "timestamp": datetime.utcnow().isoformat(),
            "original_job": job.to_dict()
        }

        self.dead_letter_queue.append(dead_letter_entry)

        # Limit dead letter queue size
        max_dlq_size = self.config.get("max_dead_letter_queue_size", 1000)
        if len(self.dead_letter_queue) > max_dlq_size:
            self.dead_letter_queue.pop(0)  # Remove oldest entry

        self.logger.error(f"Job {job.job_id} sent to dead letter queue after {attempts} attempts")

        # Optionally persist dead letter queue
        if self.config.get("persist_dead_letter_queue", True):
            await self._persist_dead_letter_entry(dead_letter_entry)

    async def _persist_dead_letter_entry(self, entry: Dict[str, Any]):
        """Persist dead letter queue entry to storage."""
        # This would integrate with your chosen storage backend
        try:
            dlq_file = f"/tmp/dead_letter_queue/{entry['job_id']}.json"
            import os
            os.makedirs(os.path.dirname(dlq_file), exist_ok=True)

            with open(dlq_file, 'w') as f:
                json.dump(entry, f, indent=2)

        except Exception as e:
            self.logger.warning(f"Failed to persist dead letter entry: {str(e)}")

    async def get_dead_letter_queue(self) -> List[Dict[str, Any]]:
        """Get current dead letter queue entries."""
        return list(self.dead_letter_queue)

    async def reprocess_dead_letter_job(self, job_id: str) -> bool:
        """
        Reprocess a job from the dead letter queue.

        Args:
            job_id: ID of the job to reprocess

        Returns:
            True if job was found and requeued
        """
        for i, entry in enumerate(self.dead_letter_queue):
            if entry["job_id"] == job_id:
                # Remove from dead letter queue
                dead_letter_entry = self.dead_letter_queue.pop(i)

                # This would typically requeue the job for execution
                self.logger.info(f"Reprocessing dead letter job {job_id}")

                return True

        return False

    async def clear_dead_letter_queue(self) -> int:
        """Clear all entries from dead letter queue."""
        count = len(self.dead_letter_queue)
        self.dead_letter_queue.clear()
        self.logger.info(f"Cleared {count} entries from dead letter queue")
        return count

    async def get_circuit_breaker_status(self) -> Dict[str, Dict[str, Any]]:
        """Get status of all circuit breakers."""
        status = {}

        for name, breaker in self.circuit_breakers.items():
            status[name] = {
                "state": breaker.state.value,
                "failure_count": breaker.failure_count,
                "success_count": breaker.success_count,
                "next_attempt_time": breaker.next_attempt_time.isoformat() if breaker.next_attempt_time else None
            }

        return status

    async def reset_circuit_breaker(self, breaker_name: str) -> bool:
        """Reset a circuit breaker to closed state."""
        if breaker_name in self.circuit_breakers:
            breaker = self.circuit_breakers[breaker_name]
            breaker.state = CircuitState.CLOSED
            breaker.failure_count = 0
            breaker.success_count = 0
            breaker.next_attempt_time = None

            self.logger.info(f"Reset circuit breaker {breaker_name}")
            return True

        return False

    def cleanup_checkpoints(self, max_age_hours: int = 24):
        """Clean up old checkpoints."""
        cutoff_time = datetime.utcnow() - timedelta(hours=max_age_hours)

        to_remove = []
        for job_id, checkpoint in self.job_checkpoints.items():
            checkpoint_time = datetime.fromisoformat(checkpoint["timestamp"])
            if checkpoint_time < cutoff_time:
                to_remove.append(job_id)

        for job_id in to_remove:
            del self.job_checkpoints[job_id]
            self.logger.info(f"Cleaned up checkpoint for job {job_id}")

        return len(to_remove)