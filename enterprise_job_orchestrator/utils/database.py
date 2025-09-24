"""
Database utilities for Enterprise Job Orchestrator

Provides database connection management, query execution, and state persistence
for the job orchestration system.
"""

import asyncio
import asyncpg
from typing import Dict, List, Optional, Any, Union
from datetime import datetime
from contextlib import asynccontextmanager

from ..models.job import Job, JobStatus
from ..models.worker import WorkerNode, WorkerAssignment
from ..models.execution import JobExecution, JobChunk
from ..core.exceptions import DatabaseError


class DatabaseManager:
    """
    Manages database connections and operations for the job orchestrator.

    Provides high-level methods for job, worker, and execution state management
    with connection pooling and transaction support.
    """

    def __init__(self, connection_string: str, pool_size: int = 10, max_overflow: int = 20):
        """
        Initialize database manager.

        Args:
            connection_string: PostgreSQL connection string
            pool_size: Base connection pool size
            max_overflow: Maximum additional connections
        """
        self.connection_string = connection_string
        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.pool: Optional[asyncpg.Pool] = None

    async def initialize(self) -> None:
        """Initialize database connection pool."""
        try:
            self.pool = await asyncpg.create_pool(
                self.connection_string,
                min_size=5,
                max_size=self.pool_size + self.max_overflow,
                command_timeout=60
            )
        except Exception as e:
            raise DatabaseError("initialization", f"Failed to create connection pool: {str(e)}")

    async def close(self) -> None:
        """Close database connection pool."""
        if self.pool:
            await self.pool.close()

    async def is_healthy(self) -> bool:
        """Check database connectivity."""
        try:
            async with self.pool.acquire() as connection:
                await connection.execute("SELECT 1")
                return True
        except Exception:
            return False

    @asynccontextmanager
    async def get_connection(self):
        """Get a database connection from the pool."""
        if not self.pool:
            raise DatabaseError("connection", "Database pool not initialized")

        async with self.pool.acquire() as connection:
            yield connection

    # Job Management Methods
    async def upsert_job(self, job: Job) -> bool:
        """Insert or update a job."""
        try:
            async with self.get_connection() as conn:
                await conn.execute("""
                    INSERT INTO jobs (
                        job_id, job_name, job_type, priority, processing_strategy,
                        status, config, input_config, output_config, created_by,
                        created_at, updated_at, progress_percent, records_processed,
                        records_total, retry_count, max_retries, error_message
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
                    ON CONFLICT (job_id) DO UPDATE SET
                        job_name = EXCLUDED.job_name,
                        status = EXCLUDED.status,
                        config = EXCLUDED.config,
                        updated_at = EXCLUDED.updated_at,
                        progress_percent = EXCLUDED.progress_percent,
                        records_processed = EXCLUDED.records_processed,
                        records_total = EXCLUDED.records_total,
                        retry_count = EXCLUDED.retry_count,
                        error_message = EXCLUDED.error_message
                """,
                job.job_id, job.job_name, job.job_type.value, job.priority.value,
                job.processing_strategy.value, job.status.value, job.config,
                job.input_config, job.output_config, job.created_by,
                job.created_at, job.updated_at, job.progress_percent,
                job.records_processed, job.records_total, job.retry_count,
                job.max_retries, job.error_message)
            return True
        except Exception as e:
            raise DatabaseError("upsert_job", str(e))

    async def get_job(self, job_id: str) -> Optional[Job]:
        """Get a job by ID."""
        try:
            async with self.get_connection() as conn:
                row = await conn.fetchrow(
                    "SELECT * FROM jobs WHERE job_id = $1", job_id
                )
                if row:
                    return Job.from_dict(dict(row))
                return None
        except Exception as e:
            raise DatabaseError("get_job", str(e))

    async def get_all_jobs(self) -> List[Job]:
        """Get all jobs."""
        try:
            async with self.get_connection() as conn:
                rows = await conn.fetch("SELECT * FROM jobs ORDER BY created_at DESC")
                return [Job.from_dict(dict(row)) for row in rows]
        except Exception as e:
            raise DatabaseError("get_all_jobs", str(e))

    # Worker Management Methods
    async def upsert_worker(self, worker: WorkerNode) -> bool:
        """Insert or update a worker."""
        try:
            async with self.get_connection() as conn:
                await conn.execute("""
                    INSERT INTO workers (
                        worker_id, node_name, host_name, ip_address, port,
                        capabilities, max_concurrent_jobs, status, is_active,
                        last_heartbeat, health_status, cpu_cores, memory_gb,
                        current_jobs, cpu_usage_percent, memory_usage_percent,
                        worker_metadata, registered_at, last_seen_at
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
                    ON CONFLICT (worker_id) DO UPDATE SET
                        node_name = EXCLUDED.node_name,
                        host_name = EXCLUDED.host_name,
                        status = EXCLUDED.status,
                        is_active = EXCLUDED.is_active,
                        last_heartbeat = EXCLUDED.last_heartbeat,
                        health_status = EXCLUDED.health_status,
                        current_jobs = EXCLUDED.current_jobs,
                        cpu_usage_percent = EXCLUDED.cpu_usage_percent,
                        memory_usage_percent = EXCLUDED.memory_usage_percent,
                        last_seen_at = EXCLUDED.last_seen_at
                """,
                worker.worker_id, worker.node_name, worker.host_name,
                worker.ip_address, worker.port, worker.capabilities,
                worker.max_concurrent_jobs, worker.status.value, worker.is_active,
                worker.last_heartbeat, worker.health_status, worker.cpu_cores,
                worker.memory_gb, worker.current_jobs,
                float(worker.cpu_usage_percent) if worker.cpu_usage_percent else None,
                float(worker.memory_usage_percent) if worker.memory_usage_percent else None,
                worker.worker_metadata, worker.registered_at, worker.last_seen_at)
            return True
        except Exception as e:
            raise DatabaseError("upsert_worker", str(e))

    async def get_worker(self, worker_id: str) -> Optional[WorkerNode]:
        """Get a worker by ID."""
        try:
            async with self.get_connection() as conn:
                row = await conn.fetchrow(
                    "SELECT * FROM workers WHERE worker_id = $1", worker_id
                )
                if row:
                    # Convert row to WorkerNode (simplified)
                    return WorkerNode(
                        worker_id=row['worker_id'],
                        node_name=row['node_name'],
                        host_name=row['host_name'],
                        ip_address=row['ip_address'],
                        port=row['port'],
                        capabilities=row['capabilities'] or [],
                        max_concurrent_jobs=row['max_concurrent_jobs'],
                        is_active=row['is_active'],
                        last_heartbeat=row['last_heartbeat'],
                        health_status=row['health_status'] or "unknown",
                        cpu_cores=row['cpu_cores'],
                        memory_gb=row['memory_gb'],
                        current_jobs=row['current_jobs'] or 0,
                        worker_metadata=row['worker_metadata'] or {},
                        registered_at=row['registered_at'],
                        last_seen_at=row['last_seen_at']
                    )
                return None
        except Exception as e:
            raise DatabaseError("get_worker", str(e))

    async def get_all_workers(self) -> List[WorkerNode]:
        """Get all workers."""
        try:
            async with self.get_connection() as conn:
                rows = await conn.fetch("SELECT * FROM workers ORDER BY registered_at DESC")
                workers = []
                for row in rows:
                    worker = WorkerNode(
                        worker_id=row['worker_id'],
                        node_name=row['node_name'],
                        host_name=row['host_name'],
                        ip_address=row['ip_address'],
                        port=row['port'],
                        capabilities=row['capabilities'] or [],
                        max_concurrent_jobs=row['max_concurrent_jobs'],
                        is_active=row['is_active'],
                        last_heartbeat=row['last_heartbeat'],
                        health_status=row['health_status'] or "unknown",
                        cpu_cores=row['cpu_cores'],
                        memory_gb=row['memory_gb'],
                        current_jobs=row['current_jobs'] or 0,
                        worker_metadata=row['worker_metadata'] or {},
                        registered_at=row['registered_at'],
                        last_seen_at=row['last_seen_at']
                    )
                    workers.append(worker)
                return workers
        except Exception as e:
            raise DatabaseError("get_all_workers", str(e))

    # Worker Assignment Methods
    async def upsert_worker_assignment(self, assignment: WorkerAssignment) -> bool:
        """Insert or update a worker assignment."""
        try:
            async with self.get_connection() as conn:
                await conn.execute("""
                    INSERT INTO worker_assignments (
                        assignment_id, worker_id, job_id, assigned_at,
                        started_at, completed_at, assignment_status
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (assignment_id) DO UPDATE SET
                        started_at = EXCLUDED.started_at,
                        completed_at = EXCLUDED.completed_at,
                        assignment_status = EXCLUDED.assignment_status
                """,
                assignment.assignment_id, assignment.worker_id, assignment.job_id,
                assignment.assigned_at, assignment.started_at, assignment.completed_at,
                assignment.assignment_status)
            return True
        except Exception as e:
            raise DatabaseError("upsert_worker_assignment", str(e))

    async def get_worker_assignment(self, assignment_id: str) -> Optional[WorkerAssignment]:
        """Get a worker assignment by ID."""
        try:
            async with self.get_connection() as conn:
                row = await conn.fetchrow(
                    "SELECT * FROM worker_assignments WHERE assignment_id = $1", assignment_id
                )
                if row:
                    return WorkerAssignment(
                        assignment_id=row['assignment_id'],
                        worker_id=row['worker_id'],
                        job_id=row['job_id'],
                        assigned_at=row['assigned_at'],
                        started_at=row['started_at'],
                        completed_at=row['completed_at'],
                        assignment_status=row['assignment_status']
                    )
                return None
        except Exception as e:
            raise DatabaseError("get_worker_assignment", str(e))

    # Statistics and Monitoring Methods
    async def get_job_statistics(self) -> Dict[str, int]:
        """Get job statistics."""
        try:
            async with self.get_connection() as conn:
                rows = await conn.fetch("""
                    SELECT status, COUNT(*) as count
                    FROM jobs
                    GROUP BY status
                """)
                stats = {"total": 0}
                for row in rows:
                    stats[row['status']] = row['count']
                    stats["total"] += row['count']
                return stats
        except Exception as e:
            raise DatabaseError("get_job_statistics", str(e))

    async def get_worker_statistics(self) -> Dict[str, int]:
        """Get worker statistics."""
        try:
            async with self.get_connection() as conn:
                total_workers = await conn.fetchval("SELECT COUNT(*) FROM workers")
                healthy_workers = await conn.fetchval("""
                    SELECT COUNT(*) FROM workers
                    WHERE health_status = 'healthy' AND is_active = true
                """)
                return {
                    "total": total_workers or 0,
                    "healthy": healthy_workers or 0
                }
        except Exception as e:
            raise DatabaseError("get_worker_statistics", str(e))

    async def get_pending_jobs_count(self) -> int:
        """Get count of pending jobs."""
        try:
            async with self.get_connection() as conn:
                count = await conn.fetchval(
                    "SELECT COUNT(*) FROM jobs WHERE status = $1", JobStatus.PENDING.value
                )
                return count or 0
        except Exception as e:
            raise DatabaseError("get_pending_jobs_count", str(e))