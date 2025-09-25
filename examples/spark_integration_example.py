"""
Example demonstrating Spark integration with Enterprise Job Orchestrator.

This example shows how to:
1. Initialize the orchestrator with Spark engine
2. Submit different types of Spark jobs
3. Monitor job execution and resource usage
4. Handle job failures and retries
"""

import asyncio
import json
from datetime import datetime, timedelta

from enterprise_job_orchestrator.core.orchestrator import JobOrchestrator
from enterprise_job_orchestrator.models.job import Job, JobType, JobPriority
from enterprise_job_orchestrator.services.engine_manager import EngineManager
from enterprise_job_orchestrator.utils.database import DatabaseManager
from enterprise_job_orchestrator.utils.logger import get_logger


async def main():
    """Main example function."""
    logger = get_logger(__name__)

    # Database configuration
    db_config = {
        "host": "localhost",
        "port": 5432,
        "database": "enterprise_jobs",
        "user": "orchestrator",
        "password": "orchestrator"
    }

    # Engine configuration
    engine_config = {
        "spark": {
            "enabled": True,
            "app_name": "Enterprise Job Orchestrator Example",
            "master": "spark://localhost:7077",  # Change to your Spark master
            "executor_memory": "2g",
            "executor_cores": "2",
            "max_executors": "4",
            "dynamic_allocation": True
        },
        "local": {
            "enabled": True,
            "max_workers": 4
        }
    }

    # Initialize components
    db_manager = DatabaseManager(db_config)
    orchestrator = JobOrchestrator(db_manager)
    engine_manager = EngineManager(engine_config)

    try:
        # Initialize services
        logger.info("Initializing Enterprise Job Orchestrator...")
        await orchestrator.start()
        await engine_manager.initialize()

        # Example 1: Data Processing Job
        logger.info("=== Example 1: Data Processing Job ===")
        await run_data_processing_example(orchestrator, logger)

        # Example 2: ETL Job
        logger.info("=== Example 2: ETL Job ===")
        await run_etl_example(orchestrator, logger)

        # Example 3: Analytics Job
        logger.info("=== Example 3: Analytics Job ===")
        await run_analytics_example(orchestrator, logger)

        # Example 4: Batch Processing with Multiple Jobs
        logger.info("=== Example 4: Batch Processing ===")
        await run_batch_processing_example(orchestrator, logger)

        # Example 5: Monitor System Health
        logger.info("=== Example 5: System Health Monitoring ===")
        await monitor_system_health(orchestrator, engine_manager, logger)

    except Exception as e:
        logger.error(f"Example execution failed: {str(e)}", exc_info=True)
        raise

    finally:
        # Cleanup
        logger.info("Cleaning up...")
        await engine_manager.shutdown()
        await orchestrator.stop()


async def run_data_processing_example(orchestrator, logger):
    """Example of running a data processing job with Spark."""

    # Create a data processing job
    job = Job(
        job_name="customer_data_processing",
        job_type=JobType.DATA_PROCESSING,
        job_data={
            "input_path": "/data/raw/customer_data.csv",
            "output_path": "/data/processed/customer_summary",
            "processing_logic": {
                "transformations": [
                    {
                        "type": "filter",
                        "condition": "age >= 18 AND status = 'active'"
                    },
                    {
                        "type": "select",
                        "columns": ["customer_id", "name", "age", "total_purchases"]
                    },
                    {
                        "type": "groupby",
                        "columns": ["age_group"],
                        "aggregations": {
                            "avg_purchases": "avg(total_purchases)",
                            "customer_count": "count(*)"
                        }
                    }
                ]
            }
        },
        execution_engine="spark",
        priority=JobPriority.HIGH,
        timeout_seconds=1800  # 30 minutes
    )

    # Submit the job
    job_id = await orchestrator.submit_job(job)
    logger.info(f"Submitted data processing job: {job_id}")

    # Monitor job progress
    await monitor_job_progress(orchestrator, job_id, logger)


async def run_etl_example(orchestrator, logger):
    """Example of running an ETL job."""

    # Create an ETL job
    job = Job(
        job_name="daily_sales_etl",
        job_type=JobType.ETL,
        job_data={
            "extract": {
                "type": "csv",
                "path": "/data/raw/sales/*.csv",
                "schema": {
                    "sale_id": "string",
                    "product_id": "string",
                    "customer_id": "string",
                    "amount": "decimal",
                    "sale_date": "timestamp"
                }
            },
            "transform": {
                "operations": [
                    {
                        "type": "filter",
                        "condition": "amount > 0"
                    },
                    {
                        "type": "add_column",
                        "name": "sale_month",
                        "expression": "month(sale_date)"
                    },
                    {
                        "type": "join",
                        "table": "products",
                        "condition": "product_id = products.id"
                    }
                ]
            },
            "load": {
                "type": "parquet",
                "path": "/data/warehouse/sales_fact",
                "mode": "append",
                "partitions": ["sale_month"]
            }
        },
        execution_engine="spark",
        priority=JobPriority.MEDIUM,
        timeout_seconds=3600  # 60 minutes
    )

    # Submit the job
    job_id = await orchestrator.submit_job(job)
    logger.info(f"Submitted ETL job: {job_id}")

    # Monitor job progress
    await monitor_job_progress(orchestrator, job_id, logger)


async def run_analytics_example(orchestrator, logger):
    """Example of running an analytics job."""

    # Create an analytics job
    job = Job(
        job_name="customer_segmentation_analysis",
        job_type=JobType.ANALYTICS,
        job_data={
            "input_path": "/data/warehouse/customer_fact",
            "output_path": "/data/analytics/customer_segments",
            "analysis_type": "customer_segmentation",
            "queries": [
                """
                SELECT
                    customer_id,
                    total_purchases,
                    avg_order_value,
                    last_purchase_date,
                    CASE
                        WHEN total_purchases > 1000 THEN 'VIP'
                        WHEN total_purchases > 500 THEN 'Premium'
                        WHEN total_purchases > 100 THEN 'Regular'
                        ELSE 'New'
                    END as segment
                FROM customer_summary
                """,
                """
                SELECT
                    segment,
                    COUNT(*) as customer_count,
                    AVG(total_purchases) as avg_purchases,
                    SUM(total_purchases) as total_revenue
                FROM customer_segments
                GROUP BY segment
                ORDER BY total_revenue DESC
                """
            ],
            "ml_features": {
                "features": ["total_purchases", "avg_order_value", "days_since_last_purchase"],
                "algorithm": "kmeans",
                "clusters": 5
            }
        },
        execution_engine="spark",
        priority=JobPriority.MEDIUM,
        timeout_seconds=2400  # 40 minutes
    )

    # Submit the job
    job_id = await orchestrator.submit_job(job)
    logger.info(f"Submitted analytics job: {job_id}")

    # Monitor job progress
    await monitor_job_progress(orchestrator, job_id, logger)


async def run_batch_processing_example(orchestrator, logger):
    """Example of submitting multiple jobs for batch processing."""

    jobs = []
    job_ids = []

    # Create multiple related jobs
    for i in range(3):
        job = Job(
            job_name=f"batch_processing_job_{i+1}",
            job_type=JobType.DATA_PROCESSING,
            job_data={
                "input_path": f"/data/raw/batch_{i+1}/*.csv",
                "output_path": f"/data/processed/batch_{i+1}",
                "processing_logic": {
                    "transformations": [
                        {"type": "filter", "condition": "status = 'valid'"},
                        {"type": "select", "columns": ["id", "value", "timestamp"]}
                    ]
                }
            },
            execution_engine="spark",
            priority=JobPriority.LOW,
            timeout_seconds=1200
        )

        jobs.append(job)

    # Submit all jobs
    for job in jobs:
        job_id = await orchestrator.submit_job(job)
        job_ids.append(job_id)
        logger.info(f"Submitted batch job: {job_id}")

    # Monitor all jobs
    logger.info("Monitoring batch jobs...")

    while True:
        completed_jobs = 0

        for job_id in job_ids:
            status = await orchestrator.get_job_status(job_id)
            if status:
                job_status = status.get("status")
                if job_status in ["completed", "failed", "cancelled"]:
                    completed_jobs += 1

                logger.info(f"Job {job_id}: {job_status}")

        if completed_jobs == len(job_ids):
            logger.info("All batch jobs completed!")
            break

        await asyncio.sleep(10)  # Check every 10 seconds


async def monitor_job_progress(orchestrator, job_id, logger):
    """Monitor a single job's progress until completion."""

    while True:
        status = await orchestrator.get_job_status(job_id)

        if not status:
            logger.warning(f"Could not retrieve status for job {job_id}")
            break

        job_status = status.get("status")
        runtime = status.get("runtime_seconds", 0)

        logger.info(f"Job {job_id} status: {job_status}, runtime: {runtime:.1f}s")

        if job_status in ["completed", "failed", "cancelled"]:
            logger.info(f"Job {job_id} finished with status: {job_status}")

            # Print job results if available
            if job_status == "completed" and "result" in status:
                logger.info(f"Job {job_id} results: {json.dumps(status['result'], indent=2)}")
            elif job_status == "failed" and "error_message" in status:
                logger.error(f"Job {job_id} failed: {status['error_message']}")

            break

        await asyncio.sleep(5)  # Check every 5 seconds


async def monitor_system_health(orchestrator, engine_manager, logger):
    """Monitor system health and resource usage."""

    logger.info("Checking system health...")

    # Get system health from orchestrator
    health = await orchestrator.get_system_health()
    logger.info(f"System Health: {json.dumps(health, indent=2)}")

    # Get engine health from engine manager
    engine_health = await engine_manager.get_engine_health()
    logger.info(f"Engine Health: {json.dumps(engine_health, indent=2)}")

    # Get performance report
    performance = await orchestrator.get_performance_report(hours=1)
    logger.info(f"Performance Report: {json.dumps(performance, indent=2)}")

    # Get queue statistics
    queue_stats = await orchestrator.get_queue_statistics()
    logger.info(f"Queue Statistics: {json.dumps(queue_stats, indent=2)}")


if __name__ == "__main__":
    asyncio.run(main())