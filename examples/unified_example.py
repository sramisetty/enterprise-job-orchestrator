"""
Unified example showing backward compatibility and enhanced features.

This example demonstrates how existing code continues to work while
new features are available when configured.
"""

import asyncio
from enterprise_job_orchestrator.core.orchestrator import JobOrchestrator
from enterprise_job_orchestrator.models.job import Job, JobType, JobPriority
from enterprise_job_orchestrator.utils.database import DatabaseManager
from enterprise_job_orchestrator.utils.logger import get_logger


async def basic_example():
    """Example 1: Basic usage - exactly like v1.0, no changes needed."""
    print("=== Basic Usage (Backward Compatible) ===")

    # Database configuration
    db_config = {
        "host": "localhost",
        "port": 5432,
        "database": "enterprise_jobs",
        "user": "orchestrator",
        "password": "orchestrator"
    }

    # Initialize exactly as before
    db_manager = DatabaseManager(db_config)
    orchestrator = JobOrchestrator(db_manager)  # No extra parameters

    # Start orchestrator
    await orchestrator.start()

    # Create and submit job - exactly the same API
    job = Job(
        job_name="basic_task",
        job_type=JobType.CUSTOM,
        job_data={"message": "Hello, World!"}
    )

    job_id = await orchestrator.submit_job(job)
    print(f"Submitted job: {job_id}")

    # Get status - same API
    status = await orchestrator.get_job_status(job_id)
    print(f"Job status: {status}")

    # List jobs - same API
    jobs = await orchestrator.list_jobs()
    print(f"Total jobs: {len(jobs)}")

    await orchestrator.stop()


async def enhanced_example():
    """Example 2: Enhanced features - automatically enabled when configured."""
    print("\n=== Enhanced Usage (New Features) ===")

    # Database configuration
    db_config = {
        "host": "localhost",
        "port": 5432,
        "database": "enterprise_jobs",
        "user": "orchestrator",
        "password": "orchestrator"
    }

    # Engine configuration - NEW but optional
    engine_config = {
        "spark": {
            "enabled": True,
            "master": "local[*]",
            "executor_memory": "2g",
            "executor_cores": "2"
        },
        "local": {
            "enabled": True,
            "max_workers": 4
        }
    }

    # Initialize with enhanced features
    db_manager = DatabaseManager(db_config)
    orchestrator = JobOrchestrator(
        db_manager,
        engine_config=engine_config,  # NEW - enables Spark/local engines
        enable_fault_tolerance=True,  # NEW - enables retry and fault tolerance
        enable_legacy_workers=True    # Keep traditional workers too
    )

    await orchestrator.start()

    # Same job submission API, but now with automatic engine selection
    spark_job = Job(
        job_name="data_processing_job",
        job_type=JobType.DATA_PROCESSING,  # Automatically uses Spark
        job_data={
            "input_path": "/data/input.csv",
            "output_path": "/data/output.parquet",
            "processing_logic": {
                "transformations": [
                    {"type": "filter", "condition": "amount > 100"},
                    {"type": "groupby", "columns": ["category"]}
                ]
            }
        }
    )

    job_id = await orchestrator.submit_job(spark_job)
    print(f"Submitted Spark job: {job_id}")

    # Enhanced status includes engine information
    status = await orchestrator.get_job_status(job_id)
    if status:
        print(f"Job status: {status.get('status')}")
        if 'engine_status' in status:
            print(f"Engine: {status['engine_status']}")

    # NEW methods available when enhanced features are enabled
    try:
        metrics = await orchestrator.get_job_metrics(hours=1)
        print(f"Job metrics: {metrics}")

        health = await orchestrator.get_system_health()
        print(f"System health: {health.get('overall_status')}")
        if 'engines' in health:
            print(f"Engines available: {list(health['engines'].keys())}")

        # Dead letter queue (fault tolerance feature)
        dlq = await orchestrator.get_dead_letter_queue()
        print(f"Dead letter queue size: {len(dlq)}")

    except Exception as e:
        print(f"Enhanced features not fully available: {e}")

    await orchestrator.stop()


async def migration_example():
    """Example 3: Gradual migration from v1 to enhanced features."""
    print("\n=== Migration Example ===")

    db_config = {
        "host": "localhost",
        "port": 5432,
        "database": "enterprise_jobs",
        "user": "orchestrator",
        "password": "orchestrator"
    }

    # Step 1: Start with basic configuration
    db_manager = DatabaseManager(db_config)
    orchestrator = JobOrchestrator(db_manager)

    await orchestrator.start()

    # All existing code works exactly the same
    job = Job(
        job_name="legacy_job",
        job_type=JobType.SYSTEM,
        job_data={"command": "echo 'Hello from legacy job'"}
    )

    job_id = await orchestrator.submit_job(job)
    print(f"Legacy job submitted: {job_id}")

    # Check what features are available
    engine_status = orchestrator.get_engine_status()
    print(f"Engine status: {engine_status}")

    await orchestrator.stop()

    # Step 2: Enable enhanced features gradually
    print("\nEnabling enhanced features...")

    # Add local engine support
    local_config = {"local": {"enabled": True, "max_workers": 2}}

    orchestrator_enhanced = JobOrchestrator(
        db_manager,
        engine_config=local_config,
        enable_fault_tolerance=False  # Start with basic enhancement
    )

    await orchestrator_enhanced.start()

    # Same job now gets enhanced execution
    enhanced_job = Job(
        job_name="enhanced_system_job",
        job_type=JobType.SYSTEM,
        job_data={"command": "echo 'Hello from enhanced job'"}
    )

    job_id = await orchestrator_enhanced.submit_job(enhanced_job)
    print(f"Enhanced job submitted: {job_id}")

    # New features are now available
    engine_status = orchestrator_enhanced.get_engine_status()
    print(f"Enhanced engine status: {engine_status}")

    await orchestrator_enhanced.stop()


async def compatibility_demo():
    """Demo showing that existing applications continue to work unchanged."""
    print("\n=== Compatibility Demo ===")

    # This represents existing application code that uses the orchestrator
    async def existing_application_code(orchestrator):
        """This is existing application code - no changes needed."""
        # Create job exactly as before
        job = Job(
            job_name="existing_app_job",
            job_type=JobType.CUSTOM,
            job_data={"operation": "process_data", "records": 1000}
        )

        # Submit job - same API
        job_id = await orchestrator.submit_job(job)

        # Monitor job - same API
        status = await orchestrator.get_job_status(job_id)

        # List jobs - same API
        all_jobs = await orchestrator.list_jobs(limit=10)

        return {
            "submitted_job": job_id,
            "job_status": status,
            "total_jobs": len(all_jobs)
        }

    # Test with basic orchestrator (v1 equivalent)
    db_config = {"host": "localhost", "port": 5432, "database": "enterprise_jobs"}
    db_manager = DatabaseManager(db_config)

    basic_orchestrator = JobOrchestrator(db_manager)
    await basic_orchestrator.start()

    result1 = await existing_application_code(basic_orchestrator)
    print(f"Basic orchestrator result: {result1}")
    await basic_orchestrator.stop()

    # Test with enhanced orchestrator - same application code works!
    enhanced_config = {
        "local": {"enabled": True, "max_workers": 2}
    }

    enhanced_orchestrator = JobOrchestrator(
        db_manager,
        engine_config=enhanced_config,
        enable_fault_tolerance=True
    )
    await enhanced_orchestrator.start()

    result2 = await existing_application_code(enhanced_orchestrator)
    print(f"Enhanced orchestrator result: {result2}")

    # But now we also have access to new features
    health = await enhanced_orchestrator.get_system_health()
    print(f"System health available: {health.get('overall_status')}")

    await enhanced_orchestrator.stop()


async def main():
    """Run all examples."""
    print("Enterprise Job Orchestrator - Unified Examples")
    print("=" * 50)

    try:
        await basic_example()
        await enhanced_example()
        await migration_example()
        await compatibility_demo()

        print("\n" + "=" * 50)
        print("✅ All examples completed successfully!")
        print("✅ Existing code works unchanged")
        print("✅ New features available when configured")
        print("✅ Zero breaking changes")

    except Exception as e:
        print(f"Error running examples: {e}")


if __name__ == "__main__":
    asyncio.run(main())