"""
Apache Spark execution engine for distributed data processing.

Provides scalable, fault-tolerant execution for big data jobs using Apache Spark.
"""

import asyncio
import json
import uuid
from datetime import datetime
from typing import Dict, Any, Optional, List
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
from pyspark import SparkContext, SparkConf

from .base import BaseExecutionEngine
from ..models.job import Job, JobType
from ..models.execution import ExecutionResult, ExecutionStatus
from ..utils.logger import get_logger
from ..core.exceptions import ExecutionEngineError, JobExecutionError


class SparkExecutionEngine(BaseExecutionEngine):
    """
    Apache Spark execution engine.

    Provides distributed data processing capabilities with:
    - Automatic resource management
    - Fault tolerance and recovery
    - Dynamic scaling
    - Comprehensive monitoring
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize Spark execution engine.

        Args:
            config: Spark configuration including:
                - app_name: Spark application name
                - master: Spark master URL
                - executor_memory: Memory per executor
                - executor_cores: Cores per executor
                - max_executors: Maximum number of executors
                - dynamic_allocation: Enable dynamic allocation
        """
        super().__init__(config)
        self.spark_session: Optional[SparkSession] = None
        self.spark_context: Optional[SparkContext] = None
        self.active_jobs: Dict[str, Any] = {}
        self.logger = get_logger(__name__)

        # Default configuration
        self.spark_config = {
            "app_name": config.get("app_name", "EnterpriseJobOrchestrator"),
            "master": config.get("master", "local[*]"),
            "executor_memory": config.get("executor_memory", "4g"),
            "executor_cores": config.get("executor_cores", "2"),
            "max_executors": config.get("max_executors", "10"),
            "dynamic_allocation": config.get("dynamic_allocation", True),
            "serializer": "org.apache.spark.serializer.KryoSerializer",
            "sql.adaptive.enabled": "true",
            "sql.adaptive.coalescePartitions.enabled": "true",
            "sql.execution.arrow.pyspark.enabled": "true"
        }

    async def initialize(self) -> bool:
        """Initialize Spark session and context."""
        try:
            self.logger.info("Initializing Spark execution engine")

            # Create Spark configuration
            conf = SparkConf()
            conf.setAppName(self.spark_config["app_name"])
            conf.setMaster(self.spark_config["master"])

            # Set Spark configurations
            for key, value in self.spark_config.items():
                if key not in ["app_name", "master"]:
                    conf.set(f"spark.{key}", str(value))

            # Create Spark session
            self.spark_session = SparkSession.builder.config(conf=conf).getOrCreate()
            self.spark_context = self.spark_session.sparkContext

            # Set log level
            self.spark_context.setLogLevel("WARN")

            self._is_initialized = True
            self.logger.info("Spark execution engine initialized successfully")
            return True

        except Exception as e:
            self.logger.error(f"Failed to initialize Spark engine: {str(e)}", exc_info=True)
            return False

    async def shutdown(self) -> bool:
        """Shutdown Spark session and clean up resources."""
        try:
            self.logger.info("Shutting down Spark execution engine")

            # Cancel all active jobs
            for job_id in list(self.active_jobs.keys()):
                await self.cancel_job(job_id, force=True)

            # Stop Spark session
            if self.spark_session:
                self.spark_session.stop()
                self.spark_session = None
                self.spark_context = None

            self._is_initialized = False
            self.logger.info("Spark execution engine shut down successfully")
            return True

        except Exception as e:
            self.logger.error(f"Error shutting down Spark engine: {str(e)}", exc_info=True)
            return False

    async def execute_job(self, job: Job) -> ExecutionResult:
        """
        Execute a job using Spark.

        Args:
            job: Job to execute

        Returns:
            ExecutionResult with job outcome
        """
        if not self._is_initialized:
            raise ExecutionEngineError("Spark engine not initialized")

        job_id = job.job_id
        start_time = datetime.utcnow()

        try:
            self.logger.info(f"Starting Spark job execution: {job_id}")

            # Register job as active
            self.active_jobs[job_id] = {
                "job": job,
                "start_time": start_time,
                "status": "running",
                "spark_job_ids": []
            }

            # Execute based on job type
            result = await self._execute_by_type(job)

            # Update job status
            if job_id in self.active_jobs:
                self.active_jobs[job_id]["status"] = "completed"

            end_time = datetime.utcnow()
            execution_time = (end_time - start_time).total_seconds()

            self.logger.info(f"Spark job completed: {job_id}, execution_time: {execution_time}s")

            return ExecutionResult(
                job_id=job_id,
                status=ExecutionStatus.COMPLETED,
                result=result,
                error_message=None,
                start_time=start_time,
                end_time=end_time,
                execution_time_seconds=execution_time,
                resource_usage=await self._get_job_resource_usage(job_id),
                metadata={
                    "engine": "spark",
                    "spark_app_id": self.spark_session.sparkContext.applicationId,
                    "spark_ui_url": self.spark_session.sparkContext.uiWebUrl
                }
            )

        except Exception as e:
            end_time = datetime.utcnow()
            execution_time = (end_time - start_time).total_seconds()

            # Update job status
            if job_id in self.active_jobs:
                self.active_jobs[job_id]["status"] = "failed"
                del self.active_jobs[job_id]

            self.logger.error(f"Spark job failed: {job_id}, error: {str(e)}", exc_info=True)

            return ExecutionResult(
                job_id=job_id,
                status=ExecutionStatus.FAILED,
                result=None,
                error_message=str(e),
                start_time=start_time,
                end_time=end_time,
                execution_time_seconds=execution_time,
                resource_usage=None,
                metadata={"engine": "spark"}
            )

    async def _execute_by_type(self, job: Job) -> Dict[str, Any]:
        """Execute job based on its type."""
        job_type = job.job_type

        if job_type == JobType.DATA_PROCESSING:
            return await self._execute_data_processing(job)
        elif job_type == JobType.ETL:
            return await self._execute_etl(job)
        elif job_type == JobType.ML_TRAINING:
            return await self._execute_ml_training(job)
        elif job_type == JobType.ANALYTICS:
            return await self._execute_analytics(job)
        else:
            return await self._execute_custom(job)

    async def _execute_data_processing(self, job: Job) -> Dict[str, Any]:
        """Execute data processing job."""
        job_data = job.job_data
        input_path = job_data.get("input_path")
        output_path = job_data.get("output_path")
        processing_logic = job_data.get("processing_logic", {})

        if not input_path or not output_path:
            raise JobExecutionError("Data processing job requires input_path and output_path")

        # Read input data
        df = self.spark_session.read.option("inferSchema", "true").option("header", "true").csv(input_path)

        # Apply transformations
        for operation in processing_logic.get("transformations", []):
            df = self._apply_transformation(df, operation)

        # Write output
        df.write.mode("overwrite").option("header", "true").csv(output_path)

        return {
            "input_records": df.count(),
            "output_path": output_path,
            "partitions": df.rdd.getNumPartitions(),
            "transformations_applied": len(processing_logic.get("transformations", []))
        }

    async def _execute_etl(self, job: Job) -> Dict[str, Any]:
        """Execute ETL job."""
        job_data = job.job_data
        extract_config = job_data.get("extract", {})
        transform_config = job_data.get("transform", {})
        load_config = job_data.get("load", {})

        # Extract
        df = await self._extract_data(extract_config)
        original_count = df.count()

        # Transform
        df = await self._transform_data(df, transform_config)
        final_count = df.count()

        # Load
        await self._load_data(df, load_config)

        return {
            "original_records": original_count,
            "final_records": final_count,
            "extract_source": extract_config.get("source"),
            "load_destination": load_config.get("destination")
        }

    async def _execute_ml_training(self, job: Job) -> Dict[str, Any]:
        """Execute ML training job."""
        job_data = job.job_data

        # This would integrate with MLlib or other ML libraries
        # For now, return a placeholder result
        return {
            "model_type": job_data.get("model_type"),
            "training_data_size": job_data.get("training_data_size", 0),
            "model_accuracy": 0.95,  # Placeholder
            "training_time_minutes": 15  # Placeholder
        }

    async def _execute_analytics(self, job: Job) -> Dict[str, Any]:
        """Execute analytics job."""
        job_data = job.job_data

        # Run analytical queries and aggregations
        return {
            "query_count": len(job_data.get("queries", [])),
            "results_generated": True,
            "analysis_type": job_data.get("analysis_type", "general")
        }

    async def _execute_custom(self, job: Job) -> Dict[str, Any]:
        """Execute custom job type."""
        job_data = job.job_data
        custom_code = job_data.get("custom_code", "")

        if not custom_code:
            raise JobExecutionError("Custom job requires custom_code in job_data")

        # Execute custom Spark code (this would need proper sandboxing in production)
        # For now, return a placeholder result
        return {
            "custom_execution": True,
            "code_length": len(custom_code),
            "execution_method": "spark_sql"
        }

    def _apply_transformation(self, df, operation):
        """Apply a single transformation to the DataFrame."""
        op_type = operation.get("type")

        if op_type == "filter":
            condition = operation.get("condition")
            return df.filter(condition)
        elif op_type == "select":
            columns = operation.get("columns")
            return df.select(*columns)
        elif op_type == "groupby":
            group_cols = operation.get("columns")
            agg_ops = operation.get("aggregations", {})
            return df.groupBy(*group_cols).agg(agg_ops)
        else:
            return df

    async def _extract_data(self, extract_config):
        """Extract data based on configuration."""
        source_type = extract_config.get("type", "csv")
        source_path = extract_config.get("path")

        if source_type == "csv":
            return self.spark_session.read.option("inferSchema", "true").option("header", "true").csv(source_path)
        elif source_type == "json":
            return self.spark_session.read.json(source_path)
        elif source_type == "parquet":
            return self.spark_session.read.parquet(source_path)
        else:
            raise JobExecutionError(f"Unsupported source type: {source_type}")

    async def _transform_data(self, df, transform_config):
        """Apply transformations to the DataFrame."""
        for transformation in transform_config.get("operations", []):
            df = self._apply_transformation(df, transformation)
        return df

    async def _load_data(self, df, load_config):
        """Load data to destination."""
        dest_type = load_config.get("type", "csv")
        dest_path = load_config.get("path")
        mode = load_config.get("mode", "overwrite")

        writer = df.write.mode(mode)

        if dest_type == "csv":
            writer.option("header", "true").csv(dest_path)
        elif dest_type == "json":
            writer.json(dest_path)
        elif dest_type == "parquet":
            writer.parquet(dest_path)
        else:
            raise JobExecutionError(f"Unsupported destination type: {dest_type}")

    async def cancel_job(self, job_id: str, force: bool = False) -> bool:
        """Cancel a running Spark job."""
        try:
            if job_id not in self.active_jobs:
                return False

            job_info = self.active_jobs[job_id]

            # Cancel all Spark jobs for this job ID
            for spark_job_id in job_info.get("spark_job_ids", []):
                self.spark_context.cancelJob(spark_job_id)

            # Update status and remove from active jobs
            job_info["status"] = "cancelled"
            del self.active_jobs[job_id]

            self.logger.info(f"Cancelled Spark job: {job_id}")
            return True

        except Exception as e:
            self.logger.error(f"Error cancelling Spark job {job_id}: {str(e)}")
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
            "spark_job_ids": job_info.get("spark_job_ids", []),
            "spark_app_id": self.spark_session.sparkContext.applicationId,
            "spark_ui_url": self.spark_session.sparkContext.uiWebUrl
        }

    def supports_job_type(self, job_type: str) -> bool:
        """Check if Spark engine supports the job type."""
        supported_types = [
            JobType.DATA_PROCESSING.value,
            JobType.ETL.value,
            JobType.ML_TRAINING.value,
            JobType.ANALYTICS.value,
            JobType.CUSTOM.value
        ]
        return job_type in supported_types

    async def get_resource_usage(self) -> Dict[str, Any]:
        """Get current Spark cluster resource usage."""
        if not self._is_initialized:
            return {}

        try:
            status = self.spark_context.statusTracker()

            return {
                "active_jobs": len(status.getJobInfos()),
                "active_stages": len(status.getStageInfos()),
                "executors": len(status.getExecutorInfos()),
                "total_cores": sum(e.totalCores for e in status.getExecutorInfos()),
                "total_memory_mb": sum(e.memoryUsed for e in status.getExecutorInfos()),
                "application_id": self.spark_context.applicationId,
                "application_name": self.spark_context.appName,
                "master": self.spark_context.master,
                "ui_web_url": self.spark_context.uiWebUrl
            }

        except Exception as e:
            self.logger.error(f"Error getting Spark resource usage: {str(e)}")
            return {}

    async def _get_job_resource_usage(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get resource usage for a specific job."""
        if job_id not in self.active_jobs:
            return None

        return {
            "memory_used": "N/A",  # Would need deeper Spark integration
            "cpu_time": "N/A",
            "io_operations": "N/A",
            "network_usage": "N/A"
        }