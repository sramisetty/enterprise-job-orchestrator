"""
Engine management service for coordinating different execution engines.

Manages Spark, local, and other execution engines, routing jobs to appropriate engines
based on job requirements and resource availability.
"""

import asyncio
from datetime import datetime
from typing import Dict, List, Optional, Any
from enum import Enum

from ..engines.base import BaseExecutionEngine
from ..engines.spark_engine import SparkExecutionEngine
from ..engines.local_engine import LocalExecutionEngine
from ..models.job import Job, JobType
from ..models.execution import ExecutionResult
from ..utils.logger import get_logger
from ..core.exceptions import ExecutionEngineError, JobExecutionError


class EngineType(Enum):
    """Supported execution engine types."""
    SPARK = "spark"
    LOCAL = "local"


class EngineManager:
    """
    Manages multiple execution engines and routes jobs appropriately.

    Provides:
    - Engine lifecycle management
    - Job routing based on type and requirements
    - Resource monitoring and load balancing
    - Fault tolerance and fallback mechanisms
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the engine manager.

        Args:
            config: Engine configuration containing settings for each engine type
        """
        self.config = config
        self.engines: Dict[str, BaseExecutionEngine] = {}
        self.engine_stats: Dict[str, Dict[str, Any]] = {}
        self.job_engine_mapping: Dict[str, str] = {}
        self.logger = get_logger(__name__)

        # Default engine routing rules
        self.engine_routing_rules = {
            JobType.DATA_PROCESSING: EngineType.SPARK,
            JobType.ETL: EngineType.SPARK,
            JobType.ML_TRAINING: EngineType.SPARK,
            JobType.ANALYTICS: EngineType.SPARK,
            JobType.SYSTEM: EngineType.LOCAL,
            JobType.SCRIPT: EngineType.LOCAL,
            JobType.CUSTOM: EngineType.SPARK,  # Default to Spark for custom jobs
        }

    async def initialize(self) -> bool:
        """Initialize all configured engines."""
        try:
            self.logger.info("Initializing execution engines")

            # Initialize Spark engine if configured
            if self.config.get("spark", {}).get("enabled", True):
                spark_config = self.config.get("spark", {})
                spark_engine = SparkExecutionEngine(spark_config)
                if await spark_engine.initialize():
                    self.engines[EngineType.SPARK.value] = spark_engine
                    self.engine_stats[EngineType.SPARK.value] = {
                        "jobs_executed": 0,
                        "total_execution_time": 0,
                        "last_used": None,
                        "status": "healthy"
                    }
                    self.logger.info("Spark engine initialized successfully")
                else:
                    self.logger.warning("Failed to initialize Spark engine")

            # Initialize local engine if configured
            if self.config.get("local", {}).get("enabled", True):
                local_config = self.config.get("local", {})
                local_engine = LocalExecutionEngine(local_config)
                if await local_engine.initialize():
                    self.engines[EngineType.LOCAL.value] = local_engine
                    self.engine_stats[EngineType.LOCAL.value] = {
                        "jobs_executed": 0,
                        "total_execution_time": 0,
                        "last_used": None,
                        "status": "healthy"
                    }
                    self.logger.info("Local engine initialized successfully")
                else:
                    self.logger.warning("Failed to initialize local engine")

            if not self.engines:
                raise ExecutionEngineError("No execution engines could be initialized")

            self.logger.info(f"Engine manager initialized with {len(self.engines)} engines")
            return True

        except Exception as e:
            self.logger.error(f"Failed to initialize engine manager: {str(e)}", exc_info=True)
            return False

    async def shutdown(self) -> bool:
        """Shutdown all engines."""
        try:
            self.logger.info("Shutting down execution engines")

            shutdown_tasks = []
            for engine_name, engine in self.engines.items():
                shutdown_tasks.append(self._shutdown_engine(engine_name, engine))

            await asyncio.gather(*shutdown_tasks, return_exceptions=True)

            self.engines.clear()
            self.engine_stats.clear()
            self.job_engine_mapping.clear()

            self.logger.info("All execution engines shut down")
            return True

        except Exception as e:
            self.logger.error(f"Error shutting down engines: {str(e)}", exc_info=True)
            return False

    async def _shutdown_engine(self, engine_name: str, engine: BaseExecutionEngine):
        """Shutdown a single engine."""
        try:
            await engine.shutdown()
            self.logger.info(f"Engine {engine_name} shut down successfully")
        except Exception as e:
            self.logger.error(f"Error shutting down engine {engine_name}: {str(e)}")

    async def execute_job(self, job: Job) -> ExecutionResult:
        """
        Execute a job using the most appropriate engine.

        Args:
            job: Job to execute

        Returns:
            ExecutionResult from job execution
        """
        # Determine the best engine for this job
        engine_name = await self._select_engine(job)

        if not engine_name or engine_name not in self.engines:
            raise JobExecutionError(f"No suitable engine found for job {job.job_id}")

        engine = self.engines[engine_name]
        self.job_engine_mapping[job.job_id] = engine_name

        try:
            self.logger.info(f"Executing job {job.job_id} on {engine_name} engine")

            start_time = datetime.utcnow()
            result = await engine.execute_job(job)
            end_time = datetime.utcnow()

            # Update engine statistics
            execution_time = (end_time - start_time).total_seconds()
            self._update_engine_stats(engine_name, execution_time, True)

            return result

        except Exception as e:
            self.logger.error(f"Job execution failed on {engine_name}: {str(e)}")

            # Update engine statistics for failure
            self._update_engine_stats(engine_name, 0, False)

            # Try fallback engine if available and appropriate
            fallback_result = await self._try_fallback_execution(job, engine_name, e)
            if fallback_result:
                return fallback_result

            raise e
        finally:
            # Clean up job mapping
            self.job_engine_mapping.pop(job.job_id, None)

    async def _select_engine(self, job: Job) -> Optional[str]:
        """Select the most appropriate engine for a job."""
        # Check if job specifies a preferred engine
        preferred_engine = job.execution_engine
        if preferred_engine and preferred_engine in self.engines:
            engine = self.engines[preferred_engine]
            if engine.supports_job_type(job.job_type.value):
                return preferred_engine

        # Use routing rules based on job type
        default_engine_type = self.engine_routing_rules.get(job.job_type, EngineType.LOCAL)
        engine_name = default_engine_type.value

        if engine_name in self.engines:
            engine = self.engines[engine_name]
            if engine.supports_job_type(job.job_type.value):
                return engine_name

        # Find any engine that supports this job type
        for engine_name, engine in self.engines.items():
            if engine.supports_job_type(job.job_type.value):
                return engine_name

        return None

    async def _try_fallback_execution(
        self,
        job: Job,
        failed_engine: str,
        original_error: Exception
    ) -> Optional[ExecutionResult]:
        """Try to execute job on a fallback engine."""
        # Define fallback logic
        fallback_engines = []

        if failed_engine == EngineType.SPARK.value and EngineType.LOCAL.value in self.engines:
            # If Spark fails and job can run locally, try local engine
            local_engine = self.engines[EngineType.LOCAL.value]
            if local_engine.supports_job_type(job.job_type.value):
                fallback_engines.append(EngineType.LOCAL.value)

        for fallback_engine_name in fallback_engines:
            try:
                self.logger.info(f"Attempting fallback execution of job {job.job_id} on {fallback_engine_name}")

                fallback_engine = self.engines[fallback_engine_name]
                result = await fallback_engine.execute_job(job)

                self.logger.info(f"Fallback execution successful for job {job.job_id}")
                self._update_engine_stats(fallback_engine_name, result.execution_time_seconds, True)

                # Add fallback information to result metadata
                if result.metadata:
                    result.metadata["fallback_from"] = failed_engine
                    result.metadata["original_error"] = str(original_error)

                return result

            except Exception as e:
                self.logger.warning(f"Fallback execution failed on {fallback_engine_name}: {str(e)}")
                continue

        return None

    def _update_engine_stats(self, engine_name: str, execution_time: float, success: bool):
        """Update statistics for an engine."""
        if engine_name in self.engine_stats:
            stats = self.engine_stats[engine_name]
            stats["jobs_executed"] += 1
            stats["total_execution_time"] += execution_time
            stats["last_used"] = datetime.utcnow()

            if not success:
                stats["failed_jobs"] = stats.get("failed_jobs", 0) + 1

    async def cancel_job(self, job_id: str, force: bool = False) -> bool:
        """
        Cancel a running job.

        Args:
            job_id: ID of job to cancel
            force: Whether to force cancellation

        Returns:
            True if cancellation successful
        """
        engine_name = self.job_engine_mapping.get(job_id)
        if not engine_name or engine_name not in self.engines:
            return False

        try:
            engine = self.engines[engine_name]
            return await engine.cancel_job(job_id, force)
        except Exception as e:
            self.logger.error(f"Error cancelling job {job_id}: {str(e)}")
            return False

    async def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get status of a running job."""
        engine_name = self.job_engine_mapping.get(job_id)
        if not engine_name or engine_name not in self.engines:
            return None

        try:
            engine = self.engines[engine_name]
            status = await engine.get_job_status(job_id)
            if status:
                status["execution_engine"] = engine_name
            return status
        except Exception as e:
            self.logger.error(f"Error getting job status {job_id}: {str(e)}")
            return None

    async def get_engine_health(self) -> Dict[str, Any]:
        """Get health status of all engines."""
        health_info = {}

        for engine_name, engine in self.engines.items():
            try:
                resource_usage = await engine.get_resource_usage()
                stats = self.engine_stats.get(engine_name, {})

                health_info[engine_name] = {
                    "status": "healthy" if engine.is_initialized else "unhealthy",
                    "resource_usage": resource_usage,
                    "statistics": stats,
                    "engine_type": engine.engine_name
                }
            except Exception as e:
                health_info[engine_name] = {
                    "status": "error",
                    "error": str(e),
                    "engine_type": engine.engine_name if hasattr(engine, 'engine_name') else "unknown"
                }

        return health_info

    async def get_engine_statistics(self) -> Dict[str, Any]:
        """Get detailed statistics for all engines."""
        return {
            "engines": dict(self.engine_stats),
            "active_jobs": len(self.job_engine_mapping),
            "job_mappings": dict(self.job_engine_mapping)
        }

    def get_supported_job_types(self) -> Dict[str, List[str]]:
        """Get job types supported by each engine."""
        supported_types = {}

        for engine_name, engine in self.engines.items():
            supported_types[engine_name] = []
            for job_type in JobType:
                if engine.supports_job_type(job_type.value):
                    supported_types[engine_name].append(job_type.value)

        return supported_types

    async def health_check(self) -> bool:
        """Perform health check on all engines."""
        try:
            health_info = await self.get_engine_health()

            # Check if at least one engine is healthy
            healthy_engines = sum(1 for info in health_info.values() if info.get("status") == "healthy")

            return healthy_engines > 0
        except Exception:
            return False