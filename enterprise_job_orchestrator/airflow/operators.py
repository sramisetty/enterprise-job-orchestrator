"""
Airflow operators for Enterprise Job Orchestrator integration.

Custom operators that enable seamless job execution within Airflow workflows.
"""

import asyncio
from typing import Dict, Any, Optional, Sequence
from airflow import BaseOperator
from airflow.utils.context import Context
from airflow.configuration import conf

from .hooks import EnterpriseJobHook
from ..models.job import Job, JobType, JobPriority
from ..utils.logger import get_logger


class EnterpriseJobOperator(BaseOperator):
    """
    Operator for executing jobs through Enterprise Job Orchestrator.

    This operator submits jobs to the orchestrator and waits for completion,
    integrating distributed job execution with Airflow workflows.
    """

    template_fields: Sequence[str] = (
        'job_name',
        'job_data',
        'job_metadata'
    )

    template_ext: Sequence[str] = ('.json', '.yaml', '.yml')

    def __init__(
        self,
        job_name: str,
        job_type: str,
        job_data: Dict[str, Any],
        execution_engine: str = "spark",
        job_priority: str = "medium",
        job_metadata: Optional[Dict[str, Any]] = None,
        max_retries: int = 3,
        timeout: Optional[int] = None,
        check_interval: int = 30,
        enterprise_job_conn_id: str = "enterprise_job_default",
        **kwargs
    ):
        """
        Initialize the operator.

        Args:
            job_name: Name of the job to execute
            job_type: Type of job (data_processing, etl, ml_training, etc.)
            job_data: Job-specific data and parameters
            execution_engine: Engine to use (spark, local, etc.)
            job_priority: Job priority (low, medium, high, critical)
            job_metadata: Additional metadata for the job
            max_retries: Maximum number of retry attempts
            timeout: Job execution timeout in seconds
            check_interval: Status check interval in seconds
            enterprise_job_conn_id: Airflow connection ID for orchestrator
        """
        super().__init__(**kwargs)
        self.job_name = job_name
        self.job_type = JobType(job_type)
        self.job_data = job_data
        self.execution_engine = execution_engine
        self.job_priority = JobPriority(job_priority)
        self.job_metadata = job_metadata or {}
        self.max_retries = max_retries
        self.timeout = timeout
        self.check_interval = check_interval
        self.enterprise_job_conn_id = enterprise_job_conn_id
        self.logger = get_logger(__name__)

    def execute(self, context: Context) -> Dict[str, Any]:
        """
        Execute the job through Enterprise Job Orchestrator.

        Args:
            context: Airflow task context

        Returns:
            Job execution result
        """
        # Create event loop for async operations
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        try:
            return loop.run_until_complete(self._async_execute(context))
        finally:
            loop.close()

    async def _async_execute(self, context: Context) -> Dict[str, Any]:
        """Async implementation of job execution."""
        hook = EnterpriseJobHook(self.enterprise_job_conn_id)

        try:
            # Create job instance
            job = self._create_job(context)

            self.logger.info(f"Submitting job: {job.job_name}")

            # Submit job to orchestrator
            job_id = await hook.submit_job(job)

            self.logger.info(f"Job submitted with ID: {job_id}")

            # Wait for job completion
            final_status = await hook.wait_for_job_completion(
                job_id=job_id,
                timeout=self.timeout,
                check_interval=self.check_interval
            )

            # Check if job completed successfully
            if final_status.get("status") == "completed":
                self.logger.info(f"Job {job_id} completed successfully")
                return {
                    "job_id": job_id,
                    "status": "completed",
                    "execution_result": final_status
                }
            elif final_status.get("status") == "failed":
                error_msg = final_status.get("error_message", "Job failed without error message")
                self.logger.error(f"Job {job_id} failed: {error_msg}")
                raise Exception(f"Job execution failed: {error_msg}")
            else:
                status = final_status.get("status", "unknown")
                raise Exception(f"Job ended in unexpected status: {status}")

        except Exception as e:
            self.logger.error(f"Error executing job: {str(e)}")
            raise
        finally:
            await hook.close_connection()

    def _create_job(self, context: Context) -> Job:
        """Create Job instance from operator parameters."""
        # Add Airflow context to metadata
        airflow_metadata = {
            "dag_id": context["dag"].dag_id,
            "task_id": context["task"].task_id,
            "run_id": context["run_id"],
            "execution_date": context["execution_date"].isoformat(),
            "try_number": context["task_instance"].try_number,
            "max_tries": context["task_instance"].max_tries
        }

        # Merge with user-provided metadata
        complete_metadata = {**self.job_metadata, **airflow_metadata}

        return Job(
            job_name=self.job_name,
            job_type=self.job_type,
            job_data=self.job_data,
            execution_engine=self.execution_engine,
            priority=self.job_priority,
            max_retries=self.max_retries,
            timeout_seconds=self.timeout,
            job_metadata=complete_metadata
        )


class EnterpriseDataProcessingOperator(EnterpriseJobOperator):
    """
    Specialized operator for data processing jobs.

    Provides convenient interface for common data processing workflows.
    """

    def __init__(
        self,
        input_path: str,
        output_path: str,
        processing_logic: Dict[str, Any],
        input_format: str = "csv",
        output_format: str = "csv",
        **kwargs
    ):
        """
        Initialize data processing operator.

        Args:
            input_path: Path to input data
            output_path: Path to output data
            processing_logic: Data processing transformations
            input_format: Input data format (csv, json, parquet)
            output_format: Output data format (csv, json, parquet)
        """
        job_data = {
            "input_path": input_path,
            "output_path": output_path,
            "processing_logic": processing_logic,
            "input_format": input_format,
            "output_format": output_format
        }

        super().__init__(
            job_type="data_processing",
            job_data=job_data,
            execution_engine="spark",  # Default to Spark for data processing
            **kwargs
        )


class EnterpriseETLOperator(EnterpriseJobOperator):
    """
    Specialized operator for ETL jobs.

    Provides convenient interface for Extract-Transform-Load workflows.
    """

    def __init__(
        self,
        extract_config: Dict[str, Any],
        transform_config: Dict[str, Any],
        load_config: Dict[str, Any],
        **kwargs
    ):
        """
        Initialize ETL operator.

        Args:
            extract_config: Data extraction configuration
            transform_config: Data transformation configuration
            load_config: Data loading configuration
        """
        job_data = {
            "extract": extract_config,
            "transform": transform_config,
            "load": load_config
        }

        super().__init__(
            job_type="etl",
            job_data=job_data,
            execution_engine="spark",  # Default to Spark for ETL
            **kwargs
        )


class EnterpriseMLTrainingOperator(EnterpriseJobOperator):
    """
    Specialized operator for ML training jobs.

    Provides convenient interface for machine learning workflows.
    """

    def __init__(
        self,
        model_type: str,
        training_data_path: str,
        model_output_path: str,
        training_config: Dict[str, Any],
        **kwargs
    ):
        """
        Initialize ML training operator.

        Args:
            model_type: Type of ML model to train
            training_data_path: Path to training data
            model_output_path: Path to save trained model
            training_config: ML training configuration
        """
        job_data = {
            "model_type": model_type,
            "training_data_path": training_data_path,
            "model_output_path": model_output_path,
            "training_config": training_config
        }

        super().__init__(
            job_type="ml_training",
            job_data=job_data,
            execution_engine="spark",  # Default to Spark for ML
            **kwargs
        )


class EnterpriseScriptOperator(EnterpriseJobOperator):
    """
    Specialized operator for script execution jobs.

    Provides convenient interface for running scripts through the orchestrator.
    """

    def __init__(
        self,
        script_path: str,
        script_args: Optional[list] = None,
        interpreter: str = "python",
        working_directory: Optional[str] = None,
        environment_vars: Optional[Dict[str, str]] = None,
        **kwargs
    ):
        """
        Initialize script operator.

        Args:
            script_path: Path to script to execute
            script_args: Arguments to pass to script
            interpreter: Script interpreter (python, bash, etc.)
            working_directory: Working directory for script execution
            environment_vars: Environment variables to set
        """
        job_data = {
            "script_path": script_path,
            "args": script_args or [],
            "interpreter": interpreter,
            "working_dir": working_directory,
            "env_vars": environment_vars or {}
        }

        super().__init__(
            job_type="script",
            job_data=job_data,
            execution_engine="local",  # Default to local for scripts
            **kwargs
        )