"""
WorkerManager service for Enterprise Job Orchestrator

Manages worker node registration, health monitoring, job assignment,
and cluster coordination.
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple, Any
from uuid import uuid4

from ..models.worker import WorkerNode, WorkerStatus, WorkerPool, WorkerAssignment
from ..models.job import Job, JobStatus
from ..utils.database import DatabaseManager
from ..utils.logger import get_logger, set_log_context, LoggerContext
from ..core.exceptions import (
    WorkerNotFoundError,
    WorkerRegistrationError,
    NoAvailableWorkersError,
    WorkerAssignmentError,
    DatabaseError
)


class WorkerManager:
    """
    Manages worker node lifecycle, health monitoring, and job assignments.

    Provides capabilities for:
    - Worker registration and deregistration
    - Health monitoring and heartbeat processing
    - Job assignment and load balancing
    - Worker pool management
    - Resource allocation and tracking
    """

    def __init__(self,
                 database_manager: DatabaseManager,
                 health_check_interval: int = 60,
                 worker_timeout: int = 300,
                 max_assignment_retries: int = 3):
        """
        Initialize WorkerManager.

        Args:
            database_manager: Database connection manager
            health_check_interval: Seconds between health checks
            worker_timeout: Seconds before considering worker unhealthy
            max_assignment_retries: Maximum retries for job assignment
        """
        self.db = database_manager
        self.health_check_interval = health_check_interval
        self.worker_timeout = worker_timeout
        self.max_assignment_retries = max_assignment_retries

        # In-memory worker tracking
        self.workers: Dict[str, WorkerNode] = {}
        self.worker_pools: Dict[str, WorkerPool] = {}
        self.active_assignments: Dict[str, WorkerAssignment] = {}

        # Health monitoring
        self._health_check_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()

        # Logger
        self.logger = get_logger(__name__)
        set_log_context(self.logger, component="worker_manager")

    async def start(self):
        """Start the worker manager and health monitoring."""
        self.logger.info("Starting WorkerManager")

        try:
            # Load existing workers from database
            await self._load_workers_from_database()

            # Start health monitoring task
            self._health_check_task = asyncio.create_task(self._health_monitor_loop())

            self.logger.info("WorkerManager started successfully", extra={
                "loaded_workers": len(self.workers),
                "worker_pools": len(self.worker_pools)
            })

        except Exception as e:
            self.logger.error("Failed to start WorkerManager", exc_info=True)
            raise WorkerRegistrationError(f"Failed to start WorkerManager: {str(e)}")

    async def stop(self):
        """Stop the worker manager and cleanup resources."""
        self.logger.info("Stopping WorkerManager")

        # Signal shutdown
        self._shutdown_event.set()

        # Cancel health check task
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass

        # Update all workers to offline status
        for worker in self.workers.values():
            worker.status = WorkerStatus.OFFLINE
            await self._update_worker_in_database(worker)

        self.logger.info("WorkerManager stopped")

    async def register_worker(self, worker: WorkerNode) -> bool:
        """
        Register a new worker node.

        Args:
            worker: Worker node to register

        Returns:
            True if registration successful

        Raises:
            WorkerRegistrationError: If registration fails
        """
        with LoggerContext(self.logger, worker_id=worker.worker_id):
            self.logger.info("Registering worker", extra={
                "node_name": worker.node_name,
                "capabilities": worker.capabilities,
                "max_concurrent_jobs": worker.max_concurrent_jobs
            })

            try:
                # Check if worker already exists
                existing_worker = await self.db.get_worker(worker.worker_id)
                if existing_worker:
                    self.logger.warning("Worker already registered, updating")

                # Update registration timestamp
                worker.registered_at = datetime.utcnow()
                worker.last_seen_at = datetime.utcnow()
                worker.status = WorkerStatus.ONLINE

                # Store in database
                await self.db.upsert_worker(worker)

                # Add to in-memory tracking
                self.workers[worker.worker_id] = worker

                # Add to appropriate pool
                pool_name = worker.worker_metadata.get("pool", "default")
                await self._add_worker_to_pool(worker, pool_name)

                self.logger.info("Worker registered successfully")
                return True

            except Exception as e:
                self.logger.error("Failed to register worker", exc_info=True)
                raise WorkerRegistrationError(f"Failed to register worker {worker.worker_id}: {str(e)}")

    async def deregister_worker(self, worker_id: str, graceful: bool = True) -> bool:
        """
        Deregister a worker node.

        Args:
            worker_id: ID of worker to deregister
            graceful: Whether to wait for current jobs to complete

        Returns:
            True if deregistration successful
        """
        with LoggerContext(self.logger, worker_id=worker_id):
            self.logger.info("Deregistering worker", extra={"graceful": graceful})

            try:
                worker = self.workers.get(worker_id)
                if not worker:
                    raise WorkerNotFoundError(f"Worker {worker_id} not found")

                if graceful and worker.current_jobs > 0:
                    # Set to draining mode
                    worker.status = WorkerStatus.DRAINING
                    await self._update_worker_in_database(worker)

                    self.logger.info("Worker set to draining mode", extra={
                        "current_jobs": worker.current_jobs
                    })

                    # Wait for jobs to complete (with timeout)
                    timeout = 300  # 5 minutes
                    start_time = datetime.utcnow()

                    while worker.current_jobs > 0:
                        if (datetime.utcnow() - start_time).total_seconds() > timeout:
                            self.logger.warning("Timeout waiting for jobs to complete, forcing deregistration")
                            break

                        await asyncio.sleep(5)
                        # Refresh worker state
                        updated_worker = await self.db.get_worker(worker_id)
                        if updated_worker:
                            worker.current_jobs = updated_worker.current_jobs

                # Mark as offline
                worker.status = WorkerStatus.OFFLINE
                worker.is_active = False
                await self._update_worker_in_database(worker)

                # Remove from in-memory tracking
                self.workers.pop(worker_id, None)

                # Remove from pools
                for pool in self.worker_pools.values():
                    pool.remove_worker(worker_id)

                self.logger.info("Worker deregistered successfully")
                return True

            except Exception as e:
                self.logger.error("Failed to deregister worker", exc_info=True)
                raise WorkerRegistrationError(f"Failed to deregister worker {worker_id}: {str(e)}")

    async def update_worker_heartbeat(self, worker_id: str, health_data: Dict[str, Any]) -> bool:
        """
        Update worker heartbeat and health information.

        Args:
            worker_id: ID of worker
            health_data: Health metrics from worker

        Returns:
            True if update successful
        """
        try:
            worker = self.workers.get(worker_id)
            if not worker:
                # Try to load from database
                worker = await self.db.get_worker(worker_id)
                if worker:
                    self.workers[worker_id] = worker
                else:
                    raise WorkerNotFoundError(f"Worker {worker_id} not found")

            # Update heartbeat and health data
            worker.update_heartbeat(health_data)

            # Update in database
            await self._update_worker_in_database(worker)

            return True

        except Exception as e:
            self.logger.error("Failed to update worker heartbeat", extra={
                "worker_id": worker_id,
                "error": str(e)
            })
            return False

    async def assign_job_to_worker(self, job: Job, required_capabilities: Optional[List[str]] = None) -> Optional[WorkerAssignment]:
        """
        Assign a job to the best available worker.

        Args:
            job: Job to assign
            required_capabilities: Required worker capabilities

        Returns:
            WorkerAssignment if successful, None otherwise

        Raises:
            NoAvailableWorkersError: If no suitable workers available
        """
        with LoggerContext(self.logger, job_id=job.job_id):
            self.logger.info("Assigning job to worker", extra={
                "job_name": job.job_name,
                "required_capabilities": required_capabilities
            })

            # Find best available worker
            best_worker = await self._find_best_worker(required_capabilities)
            if not best_worker:
                raise NoAvailableWorkersError(
                    "No available workers for job assignment",
                    details={"required_capabilities": required_capabilities}
                )

            try:
                # Create assignment
                assignment = WorkerAssignment(
                    assignment_id=str(uuid4()),
                    worker_id=best_worker.worker_id,
                    job_id=job.job_id,
                    assigned_at=datetime.utcnow()
                )

                # Update worker state
                best_worker.assign_job(job.job_id)
                await self._update_worker_in_database(best_worker)

                # Store assignment
                await self.db.upsert_worker_assignment(assignment)
                self.active_assignments[assignment.assignment_id] = assignment

                self.logger.info("Job assigned successfully", extra={
                    "worker_id": best_worker.worker_id,
                    "assignment_id": assignment.assignment_id
                })

                return assignment

            except Exception as e:
                self.logger.error("Failed to assign job", exc_info=True)
                raise WorkerAssignmentError(f"Failed to assign job to worker: {str(e)}")

    async def complete_job_assignment(self, assignment_id: str) -> bool:
        """
        Mark a job assignment as completed.

        Args:
            assignment_id: ID of assignment to complete

        Returns:
            True if completion successful
        """
        try:
            assignment = self.active_assignments.get(assignment_id)
            if not assignment:
                assignment = await self.db.get_worker_assignment(assignment_id)
                if not assignment:
                    raise WorkerAssignmentError(f"Assignment {assignment_id} not found")

            with LoggerContext(self.logger,
                             job_id=assignment.job_id,
                             worker_id=assignment.worker_id):

                # Mark assignment as completed
                assignment.complete_execution()
                await self.db.upsert_worker_assignment(assignment)

                # Update worker state
                worker = self.workers.get(assignment.worker_id)
                if worker:
                    worker.complete_job(assignment.job_id)
                    await self._update_worker_in_database(worker)

                # Remove from active assignments
                self.active_assignments.pop(assignment_id, None)

                self.logger.info("Job assignment completed")
                return True

        except Exception as e:
            self.logger.error("Failed to complete job assignment", extra={
                "assignment_id": assignment_id,
                "error": str(e)
            })
            return False

    async def get_worker_pool_statistics(self, pool_name: Optional[str] = None) -> Dict[str, Any]:
        """
        Get worker pool statistics.

        Args:
            pool_name: Specific pool name, or None for all pools

        Returns:
            Pool statistics dictionary
        """
        if pool_name:
            pool = self.worker_pools.get(pool_name)
            if pool:
                return pool.get_pool_statistics()
            else:
                return {}
        else:
            # Return statistics for all pools
            stats = {}
            for name, pool in self.worker_pools.items():
                stats[name] = pool.get_pool_statistics()
            return stats

    async def get_available_workers(self, required_capabilities: Optional[List[str]] = None) -> List[WorkerNode]:
        """
        Get list of available workers.

        Args:
            required_capabilities: Required capabilities filter

        Returns:
            List of available workers
        """
        available_workers = []

        for worker in self.workers.values():
            if worker.can_accept_job(required_capabilities):
                available_workers.append(worker)

        # Sort by score (best workers first)
        available_workers.sort(key=lambda w: w.get_score(required_capabilities), reverse=True)

        return available_workers

    async def _load_workers_from_database(self):
        """Load existing workers from database."""
        try:
            workers = await self.db.get_all_workers()
            for worker in workers:
                self.workers[worker.worker_id] = worker

                # Add to appropriate pool
                pool_name = worker.worker_metadata.get("pool", "default")
                await self._add_worker_to_pool(worker, pool_name)

        except Exception as e:
            self.logger.error("Failed to load workers from database", exc_info=True)
            raise DatabaseError(f"Failed to load workers: {str(e)}")

    async def _add_worker_to_pool(self, worker: WorkerNode, pool_name: str):
        """Add worker to specified pool."""
        if pool_name not in self.worker_pools:
            self.worker_pools[pool_name] = WorkerPool(pool_name=pool_name)

        self.worker_pools[pool_name].add_worker(worker)

    async def _find_best_worker(self, required_capabilities: Optional[List[str]] = None) -> Optional[WorkerNode]:
        """Find the best available worker for job assignment."""
        available_workers = await self.get_available_workers(required_capabilities)
        return available_workers[0] if available_workers else None

    async def _update_worker_in_database(self, worker: WorkerNode):
        """Update worker in database."""
        try:
            await self.db.upsert_worker(worker)
        except Exception as e:
            self.logger.error("Failed to update worker in database", extra={
                "worker_id": worker.worker_id,
                "error": str(e)
            })

    async def _health_monitor_loop(self):
        """Main health monitoring loop."""
        self.logger.info("Starting health monitor loop")

        while not self._shutdown_event.is_set():
            try:
                await self._perform_health_checks()
                await asyncio.sleep(self.health_check_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error("Error in health monitor loop", exc_info=True)
                await asyncio.sleep(self.health_check_interval)

    async def _perform_health_checks(self):
        """Perform health checks on all workers."""
        current_time = datetime.utcnow()
        unhealthy_workers = []

        for worker_id, worker in self.workers.items():
            if not worker.is_healthy(self.worker_timeout):
                unhealthy_workers.append(worker_id)

                # Mark worker as offline if timeout exceeded
                if worker.status != WorkerStatus.OFFLINE:
                    self.logger.warning("Worker health check failed, marking offline", extra={
                        "worker_id": worker_id,
                        "last_heartbeat": worker.last_heartbeat.isoformat() if worker.last_heartbeat else None
                    })

                    worker.status = WorkerStatus.ERROR
                    await self._update_worker_in_database(worker)

        if unhealthy_workers:
            self.logger.info("Health check summary", extra={
                "unhealthy_workers": len(unhealthy_workers),
                "total_workers": len(self.workers)
            })