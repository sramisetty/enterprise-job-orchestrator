"""
QueueManager service for Enterprise Job Orchestrator

Manages job queue operations, priority scheduling, and job distribution.
"""

import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Any
import heapq

from ..models.job import Job, JobPriority
from ..utils.database import DatabaseManager
from ..utils.logger import get_logger, set_log_context
from ..core.exceptions import QueueError


class QueueManager:
    """
    Manages job queue and scheduling operations.

    Provides capabilities for:
    - Priority-based job queuing
    - Job scheduling and distribution
    - Queue statistics and monitoring
    - Load balancing across workers
    """

    def __init__(self, database_manager: DatabaseManager):
        """
        Initialize QueueManager.

        Args:
            database_manager: Database connection manager
        """
        self.db = database_manager
        self._job_queue: List[tuple] = []  # Priority queue
        self._processing_jobs: Dict[str, datetime] = {}
        self._is_paused = False

        self.logger = get_logger(__name__)
        set_log_context(self.logger, component="queue_manager")

    async def start(self):
        """Start the queue manager."""
        self.logger.info("Starting QueueManager")
        await self._load_pending_jobs()

    async def stop(self):
        """Stop the queue manager."""
        self.logger.info("Stopping QueueManager")

    async def enqueue_job(self, job: Job):
        """
        Add a job to the queue.

        Args:
            job: Job to enqueue
        """
        priority_value = self._get_priority_value(job.priority)
        queue_item = (priority_value, datetime.utcnow(), job.job_id)

        heapq.heappush(self._job_queue, queue_item)

        self.logger.info("Job enqueued", extra={
            "job_id": job.job_id,
            "priority": job.priority.value,
            "queue_size": len(self._job_queue)
        })

    async def dequeue_job(self) -> Optional[str]:
        """
        Get the next job from the queue.

        Returns:
            Job ID or None if queue is empty
        """
        if self._is_paused or not self._job_queue:
            return None

        _, _, job_id = heapq.heappop(self._job_queue)
        self._processing_jobs[job_id] = datetime.utcnow()

        self.logger.info("Job dequeued", extra={
            "job_id": job_id,
            "remaining_queue_size": len(self._job_queue)
        })

        return job_id

    async def get_queue_statistics(self) -> Dict[str, Any]:
        """Get queue statistics."""
        return {
            "queue_size": len(self._job_queue),
            "processing_jobs": len(self._processing_jobs),
            "is_paused": self._is_paused
        }

    async def pause_processing(self):
        """Pause queue processing."""
        self._is_paused = True
        self.logger.info("Queue processing paused")

    async def resume_processing(self):
        """Resume queue processing."""
        self._is_paused = False
        self.logger.info("Queue processing resumed")

    def _get_priority_value(self, priority: JobPriority) -> int:
        """Convert priority to numeric value for queue ordering."""
        priority_map = {
            JobPriority.URGENT: 1,
            JobPriority.HIGH: 2,
            JobPriority.NORMAL: 3,
            JobPriority.LOW: 4
        }
        return priority_map.get(priority, 3)

    async def _load_pending_jobs(self):
        """Load pending jobs from database into queue."""
        try:
            jobs = await self.db.get_all_jobs()
            pending_jobs = [j for j in jobs if j.status.value == "pending"]

            for job in pending_jobs:
                await self.enqueue_job(job)

            self.logger.info("Loaded pending jobs", extra={
                "loaded_jobs": len(pending_jobs)
            })

        except Exception as e:
            self.logger.error("Failed to load pending jobs", exc_info=True)