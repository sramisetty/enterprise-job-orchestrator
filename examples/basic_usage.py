"""
Basic usage example for Enterprise Job Orchestrator

This example demonstrates how to use the job orchestrator as a standalone package
in your applications.
"""

import asyncio
import uuid
from datetime import datetime

from enterprise_job_orchestrator import (
    JobOrchestrator,
    Job,
    JobType,
    JobPriority,
    ProcessingStrategy,
    WorkerNode,
    DatabaseManager,
    quick_start
)


async def basic_example():
    """Basic job orchestrator usage example."""
    print("🚀 Starting Enterprise Job Orchestrator Example")

    # Method 1: Quick start for simple use cases
    orchestrator = quick_start("postgresql://localhost/job_orchestrator")
    await orchestrator.start()

    try:
        # Create a simple job
        job = Job(
            job_id=f"example_{uuid.uuid4().hex[:8]}",
            job_name="Example Data Processing Job",
            job_type=JobType.DATA_PROCESSING,
            priority=JobPriority.HIGH,
            processing_strategy=ProcessingStrategy.PARALLEL_CHUNKS,
            config={
                "input_source": "example_data.csv",
                "chunk_size": 10000,
                "output_format": "json"
            }
        )

        # Submit the job
        job_id = await orchestrator.submit_job(job)
        print(f"✅ Job submitted successfully: {job_id}")

        # Check job status
        status = await orchestrator.get_job_status(job_id)
        print(f"📊 Job status: {status['status']}")

        # Get system health
        health = await orchestrator.get_system_health()
        print(f"🏥 System health: {health['overall_status']}")

    finally:
        await orchestrator.stop()
        print("🛑 Orchestrator stopped")


async def advanced_example():
    """Advanced usage with custom configuration."""
    print("\n🔧 Advanced Enterprise Job Orchestrator Example")

    # Method 2: Custom configuration
    db_manager = DatabaseManager(
        "postgresql://user:password@localhost:5432/job_orchestrator",
        pool_size=20,
        max_overflow=30
    )
    await db_manager.initialize()

    orchestrator = JobOrchestrator(db_manager)
    await orchestrator.start()

    try:
        # Register a worker
        worker = WorkerNode(
            worker_id="example_worker_001",
            node_name="Example Processing Node",
            host_name="localhost",
            port=8080,
            capabilities=["data_processing", "machine_learning"],
            max_concurrent_jobs=4,
            worker_metadata={"pool": "production", "region": "us-east-1"}
        )

        success = await orchestrator.register_worker(worker)
        if success:
            print(f"👷 Worker registered: {worker.worker_id}")

        # Create multiple jobs with different priorities
        jobs = []
        for i in range(3):
            job = Job(
                job_id=f"batch_job_{i:03d}",
                job_name=f"Batch Processing Job {i+1}",
                job_type=JobType.BATCH_PROCESSING,
                priority=JobPriority.NORMAL if i < 2 else JobPriority.HIGH,
                processing_strategy=ProcessingStrategy.PARALLEL_CHUNKS,
                config={
                    "batch_id": i,
                    "record_count": 1000000,
                    "chunk_size": 50000
                },
                resource_requirements={
                    "memory_mb": 4096,
                    "cpu_cores": 2
                },
                tags=["batch_processing", f"batch_{i}"]
            )
            jobs.append(job)

        # Submit all jobs
        job_ids = []
        for job in jobs:
            job_id = await orchestrator.submit_job(job)
            job_ids.append(job_id)
            print(f"📝 Submitted job: {job_id} (Priority: {job.priority.value})")

        # Get queue statistics
        queue_stats = await orchestrator.get_queue_statistics()
        print(f"📊 Queue statistics: {queue_stats}")

        # List all jobs
        all_jobs = await orchestrator.list_jobs(limit=10)
        print(f"📋 Total jobs: {len(all_jobs)}")

        # Get workers
        workers = await orchestrator.get_workers()
        print(f"👷 Total workers: {len(workers)}")

        # Performance report
        report = await orchestrator.get_performance_report(hours=1)
        print(f"📈 Performance report generated")

    finally:
        await orchestrator.stop()
        print("🛑 Advanced orchestrator stopped")


async def integration_example():
    """Example of integrating with existing systems."""
    print("\n🔌 Integration Example")

    class MyApplication:
        """Example application class."""

        def __init__(self):
            self.orchestrator = None

        async def start(self):
            """Start the application with job orchestrator."""
            # Initialize orchestrator
            self.orchestrator = quick_start("postgresql://localhost/job_orchestrator")
            await self.orchestrator.start()
            print("🚀 Application started with job orchestrator")

        async def stop(self):
            """Stop the application."""
            if self.orchestrator:
                await self.orchestrator.stop()
                print("🛑 Application stopped")

        async def process_user_data(self, user_id: str, data_file: str):
            """Process user data asynchronously."""
            job = Job(
                job_id=f"user_data_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                job_name=f"Process User Data for {user_id}",
                job_type=JobType.DATA_PROCESSING,
                priority=JobPriority.NORMAL,
                config={
                    "user_id": user_id,
                    "data_file": data_file,
                    "processing_type": "user_analytics"
                }
            )

            job_id = await self.orchestrator.submit_job(job)
            print(f"📊 Processing user data job: {job_id}")
            return job_id

        async def run_ml_training(self, model_name: str, dataset: str):
            """Run machine learning training job."""
            job = Job(
                job_id=f"ml_training_{model_name}_{uuid.uuid4().hex[:8]}",
                job_name=f"Train ML Model: {model_name}",
                job_type=JobType.MACHINE_LEARNING,
                priority=JobPriority.HIGH,
                processing_strategy=ProcessingStrategy.HYBRID,
                config={
                    "model_name": model_name,
                    "dataset": dataset,
                    "training_epochs": 100,
                    "batch_size": 32
                },
                resource_requirements={
                    "memory_mb": 16384,
                    "cpu_cores": 8
                }
            )

            job_id = await self.orchestrator.submit_job(job)
            print(f"🤖 ML training job submitted: {job_id}")
            return job_id

    # Use the application
    app = MyApplication()
    await app.start()

    try:
        # Process some user data
        user_job_id = await app.process_user_data("user_12345", "/data/user_activity.csv")

        # Run ML training
        ml_job_id = await app.run_ml_training("recommendation_model", "/data/training_set.parquet")

        # Check status of jobs
        user_status = await app.orchestrator.get_job_status(user_job_id)
        ml_status = await app.orchestrator.get_job_status(ml_job_id)

        print(f"👤 User job status: {user_status['status'] if user_status else 'Not found'}")
        print(f"🤖 ML job status: {ml_status['status'] if ml_status else 'Not found'}")

    finally:
        await app.stop()


async def main():
    """Run all examples."""
    print("🎯 Enterprise Job Orchestrator - Comprehensive Examples\n")

    try:
        # Run basic example
        await basic_example()

        # Run advanced example
        await advanced_example()

        # Run integration example
        await integration_example()

        print("\n✅ All examples completed successfully!")

    except Exception as e:
        print(f"\n❌ Example failed: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # Run the examples
    asyncio.run(main())