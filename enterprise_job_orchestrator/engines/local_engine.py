"""
Local execution engine for smaller jobs.

Provides local execution capabilities for jobs that don't require distributed processing.
"""

import asyncio
import subprocess
import json
from datetime import datetime
from typing import Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

from .base import BaseExecutionEngine
from ..models.job import Job, JobType
from ..models.execution import ExecutionResult, ExecutionStatus
from ..utils.logger import get_logger
from ..core.exceptions import JobExecutionError


class LocalExecutionEngine(BaseExecutionEngine):
    """
    Local execution engine for smaller jobs.

    Suitable for:
    - Small data processing tasks
    - Testing and development
    - Jobs that don't require distributed processing
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize local execution engine.

        Args:
            config: Local execution configuration
        """
        super().__init__(config)
        self.max_workers = config.get("max_workers", 4)
        self.thread_executor = None
        self.process_executor = None
        self.active_jobs: Dict[str, Any] = {}
        self.logger = get_logger(__name__)

    async def initialize(self) -> bool:
        """Initialize thread and process executors."""
        try:
            self.thread_executor = ThreadPoolExecutor(max_workers=self.max_workers)
            self.process_executor = ProcessPoolExecutor(max_workers=self.max_workers)
            self._is_initialized = True
            self.logger.info("Local execution engine initialized successfully")
            return True
        except Exception as e:
            self.logger.error(f"Failed to initialize local engine: {str(e)}")
            return False

    async def shutdown(self) -> bool:
        """Shutdown executors and clean up resources."""
        try:
            # Cancel all active jobs
            for job_id in list(self.active_jobs.keys()):
                await self.cancel_job(job_id, force=True)

            # Shutdown executors
            if self.thread_executor:
                self.thread_executor.shutdown(wait=True)
            if self.process_executor:
                self.process_executor.shutdown(wait=True)

            self._is_initialized = False
            self.logger.info("Local execution engine shut down successfully")
            return True
        except Exception as e:
            self.logger.error(f"Error shutting down local engine: {str(e)}")
            return False

    async def execute_job(self, job: Job) -> ExecutionResult:
        """Execute a job locally."""
        if not self._is_initialized:
            raise JobExecutionError("Local engine not initialized")

        job_id = job.job_id
        start_time = datetime.utcnow()

        try:
            self.logger.info(f"Starting local job execution: {job_id}")

            # Register job as active
            self.active_jobs[job_id] = {
                "job": job,
                "start_time": start_time,
                "status": "running"
            }

            # Execute based on job type
            result = await self._execute_by_type(job)

            # Update job status
            if job_id in self.active_jobs:
                self.active_jobs[job_id]["status"] = "completed"
                del self.active_jobs[job_id]

            end_time = datetime.utcnow()
            execution_time = (end_time - start_time).total_seconds()

            self.logger.info(f"Local job completed: {job_id}, execution_time: {execution_time}s")

            return ExecutionResult(
                job_id=job_id,
                status=ExecutionStatus.COMPLETED,
                result=result,
                error_message=None,
                start_time=start_time,
                end_time=end_time,
                execution_time_seconds=execution_time,
                resource_usage=await self._get_job_resource_usage(),
                metadata={"engine": "local"}
            )

        except Exception as e:
            end_time = datetime.utcnow()
            execution_time = (end_time - start_time).total_seconds()

            # Update job status
            if job_id in self.active_jobs:
                self.active_jobs[job_id]["status"] = "failed"
                del self.active_jobs[job_id]

            self.logger.error(f"Local job failed: {job_id}, error: {str(e)}", exc_info=True)

            return ExecutionResult(
                job_id=job_id,
                status=ExecutionStatus.FAILED,
                result=None,
                error_message=str(e),
                start_time=start_time,
                end_time=end_time,
                execution_time_seconds=execution_time,
                resource_usage=None,
                metadata={"engine": "local"}
            )

    async def _execute_by_type(self, job: Job) -> Dict[str, Any]:
        """Execute job based on its type."""
        job_type = job.job_type

        if job_type == JobType.SYSTEM:
            return await self._execute_system_job(job)
        elif job_type == JobType.SCRIPT:
            return await self._execute_script_job(job)
        elif job_type == JobType.CUSTOM:
            return await self._execute_custom_job(job)
        else:
            # For other types, execute as generic processing
            return await self._execute_generic_job(job)

    async def _execute_system_job(self, job: Job) -> Dict[str, Any]:
        """Execute system command job."""
        job_data = job.job_data
        command = job_data.get("command")
        working_dir = job_data.get("working_dir", ".")
        timeout = job_data.get("timeout", 300)  # 5 minutes default

        if not command:
            raise JobExecutionError("System job requires command in job_data")

        loop = asyncio.get_event_loop()

        def run_command():
            try:
                result = subprocess.run(
                    command,
                    shell=True,
                    capture_output=True,
                    text=True,
                    timeout=timeout,
                    cwd=working_dir
                )
                return {
                    "return_code": result.returncode,
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                    "command": command
                }
            except subprocess.TimeoutExpired:
                raise JobExecutionError(f"Command timed out after {timeout} seconds")
            except Exception as e:
                raise JobExecutionError(f"Command execution failed: {str(e)}")

        return await loop.run_in_executor(self.process_executor, run_command)

    async def _execute_script_job(self, job: Job) -> Dict[str, Any]:
        """Execute script job."""
        job_data = job.job_data
        script_path = job_data.get("script_path")
        script_args = job_data.get("args", [])
        interpreter = job_data.get("interpreter", "python")
        timeout = job_data.get("timeout", 600)  # 10 minutes default

        if not script_path:
            raise JobExecutionError("Script job requires script_path in job_data")

        command = [interpreter, script_path] + script_args

        loop = asyncio.get_event_loop()

        def run_script():
            try:
                result = subprocess.run(
                    command,
                    capture_output=True,
                    text=True,
                    timeout=timeout
                )
                return {
                    "return_code": result.returncode,
                    "stdout": result.stdout,
                    "stderr": result.stderr,
                    "script_path": script_path,
                    "interpreter": interpreter
                }
            except subprocess.TimeoutExpired:
                raise JobExecutionError(f"Script timed out after {timeout} seconds")
            except Exception as e:
                raise JobExecutionError(f"Script execution failed: {str(e)}")

        return await loop.run_in_executor(self.process_executor, run_script)

    async def _execute_custom_job(self, job: Job) -> Dict[str, Any]:
        """Execute custom job with user-defined logic."""
        job_data = job.job_data
        custom_function = job_data.get("function")
        function_args = job_data.get("args", {})

        if not custom_function:
            raise JobExecutionError("Custom job requires function in job_data")

        # This would need proper sandboxing in production
        # For now, return a placeholder result
        return {
            "custom_execution": True,
            "function": custom_function,
            "args": function_args,
            "result": "Custom job executed successfully"
        }

    async def _execute_generic_job(self, job: Job) -> Dict[str, Any]:
        """Execute generic processing job."""
        job_data = job.job_data

        # Simulate processing
        await asyncio.sleep(1)

        return {
            "job_type": job.job_type.value,
            "processing_time": 1.0,
            "records_processed": job_data.get("record_count", 0),
            "result": "Generic job executed successfully"
        }

    async def cancel_job(self, job_id: str, force: bool = False) -> bool:
        """Cancel a running local job."""
        try:
            if job_id not in self.active_jobs:
                return False

            job_info = self.active_jobs[job_id]
            job_info["status"] = "cancelled"
            del self.active_jobs[job_id]

            # Note: In a full implementation, we would need to track
            # and terminate the actual subprocess/thread

            self.logger.info(f"Cancelled local job: {job_id}")
            return True

        except Exception as e:
            self.logger.error(f"Error cancelling local job {job_id}: {str(e)}")
            return False

    async def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a running job."""
        if job_id not in self.active_jobs:
            return None

        job_info = self.active_jobs[job_id]
        current_time = datetime.utcnow()
        runtime = (current_time - job_info["start_time"]).total_seconds()

        return {
            "job_id": job_id,
            "status": job_info["status"],
            "runtime_seconds": runtime,
            "start_time": job_info["start_time"].isoformat(),
            "execution_method": "local"
        }

    def supports_job_type(self, job_type: str) -> bool:
        """Check if local engine supports the job type."""
        # Local engine supports most job types except large-scale distributed ones
        return job_type not in [JobType.DATA_PROCESSING.value, JobType.ETL.value]

    async def get_resource_usage(self) -> Dict[str, Any]:
        """Get current local resource usage."""
        import psutil

        try:
            return {
                "cpu_percent": psutil.cpu_percent(),
                "memory_percent": psutil.virtual_memory().percent,
                "disk_usage_percent": psutil.disk_usage("/").percent if psutil.disk_usage("/") else 0,
                "active_jobs": len(self.active_jobs),
                "max_workers": self.max_workers
            }
        except Exception as e:
            self.logger.error(f"Error getting local resource usage: {str(e)}")
            return {}

    async def _get_job_resource_usage(self) -> Dict[str, Any]:
        """Get resource usage for job execution."""
        import psutil

        try:
            return {
                "cpu_percent": psutil.cpu_percent(),
                "memory_mb": psutil.virtual_memory().used // 1024 // 1024,
                "execution_method": "local"
            }
        except Exception:
            return {"execution_method": "local"}