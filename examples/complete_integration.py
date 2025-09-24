"""
Complete integration example for Enterprise Job Orchestrator

This example demonstrates a comprehensive real-world integration scenario
showing how to build a data processing pipeline using the orchestrator.
"""

import asyncio
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any

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


class DataProcessingPipeline:
    """Example data processing pipeline using Enterprise Job Orchestrator"""

    def __init__(self, database_url: str):
        self.database_url = database_url
        self.orchestrator = None
        self.registered_workers = []

    async def initialize(self):
        """Initialize the pipeline with orchestrator and workers"""
        print("🚀 Initializing Data Processing Pipeline")

        # Initialize orchestrator
        self.orchestrator = quick_start(self.database_url)
        await self.orchestrator.start()

        # Register specialized workers
        await self._register_workers()

        print("✅ Pipeline initialized successfully")

    async def _register_workers(self):
        """Register different types of workers for the pipeline"""

        # Data ingestion worker
        ingestion_worker = WorkerNode(
            worker_id="data_ingestion_001",
            node_name="Data Ingestion Worker",
            host_name="localhost",
            port=8081,
            capabilities=["data_ingestion", "file_processing", "validation"],
            max_concurrent_jobs=2,
            worker_metadata={
                "pool": "ingestion",
                "region": "us-east-1",
                "specialization": "file_processing"
            }
        )

        # ETL processing worker
        etl_worker = WorkerNode(
            worker_id="etl_processing_001",
            node_name="ETL Processing Worker",
            host_name="localhost",
            port=8082,
            capabilities=["data_transformation", "etl", "cleansing"],
            max_concurrent_jobs=4,
            worker_metadata={
                "pool": "processing",
                "region": "us-east-1",
                "specialization": "etl"
            }
        )

        # ML training worker
        ml_worker = WorkerNode(
            worker_id="ml_training_001",
            node_name="ML Training Worker",
            host_name="localhost",
            port=8083,
            capabilities=["machine_learning", "training", "inference"],
            max_concurrent_jobs=1,
            worker_metadata={
                "pool": "ml",
                "region": "us-east-1",
                "specialization": "training",
                "gpu_enabled": True
            }
        )

        # Register all workers
        workers = [ingestion_worker, etl_worker, ml_worker]

        for worker in workers:
            success = await self.orchestrator.register_worker(worker)
            if success:
                self.registered_workers.append(worker.worker_id)
                print(f"✅ Registered worker: {worker.worker_id}")
            else:
                print(f"❌ Failed to register worker: {worker.worker_id}")

    async def run_data_pipeline(self, input_files: List[str], pipeline_config: Dict[str, Any]):
        """Run a complete data processing pipeline"""

        print(f"\n🔄 Starting data pipeline with {len(input_files)} files")

        pipeline_id = f"pipeline_{int(datetime.utcnow().timestamp())}"
        job_ids = []

        try:
            # Phase 1: Data Ingestion
            print("📥 Phase 1: Data Ingestion")
            ingestion_jobs = await self._create_ingestion_jobs(input_files, pipeline_id, pipeline_config)
            job_ids.extend(ingestion_jobs)

            # Wait for ingestion to complete
            await self._wait_for_jobs(ingestion_jobs, "Data Ingestion")

            # Phase 2: ETL Processing
            print("🔄 Phase 2: ETL Processing")
            etl_jobs = await self._create_etl_jobs(pipeline_id, pipeline_config)
            job_ids.extend(etl_jobs)

            # Wait for ETL to complete
            await self._wait_for_jobs(etl_jobs, "ETL Processing")

            # Phase 3: ML Training (optional)
            if pipeline_config.get('enable_ml_training', False):
                print("🤖 Phase 3: ML Training")
                ml_jobs = await self._create_ml_jobs(pipeline_id, pipeline_config)
                job_ids.extend(ml_jobs)

                # Wait for ML training to complete
                await self._wait_for_jobs(ml_jobs, "ML Training")

            print(f"✅ Pipeline {pipeline_id} completed successfully!")

            # Generate pipeline report
            await self._generate_pipeline_report(pipeline_id, job_ids)

        except Exception as e:
            print(f"❌ Pipeline {pipeline_id} failed: {str(e)}")
            # Cancel remaining jobs
            await self._cancel_remaining_jobs(job_ids)
            raise

    async def _create_ingestion_jobs(self, input_files: List[str], pipeline_id: str, config: Dict[str, Any]) -> List[str]:
        """Create data ingestion jobs"""
        job_ids = []

        for i, file_path in enumerate(input_files):
            job = Job(
                job_id=f"{pipeline_id}_ingestion_{i:03d}",
                job_name=f"Ingest File {Path(file_path).name}",
                job_type=JobType.DATA_PROCESSING,
                priority=JobPriority.HIGH,
                processing_strategy=ProcessingStrategy.SINGLE_WORKER,
                config={
                    "pipeline_id": pipeline_id,
                    "phase": "ingestion",
                    "input_file": file_path,
                    "validation_rules": config.get("validation_rules", {}),
                    "output_format": config.get("ingestion_output_format", "parquet"),
                    "chunk_size": config.get("ingestion_chunk_size", 10000)
                },
                required_capabilities=["data_ingestion", "file_processing"],
                resource_requirements={
                    "memory_mb": 2048,
                    "cpu_cores": 1
                },
                tags=["pipeline", pipeline_id, "ingestion"]
            )

            job_id = await self.orchestrator.submit_job(job)
            job_ids.append(job_id)
            print(f"  📝 Submitted ingestion job: {job_id}")

        return job_ids

    async def _create_etl_jobs(self, pipeline_id: str, config: Dict[str, Any]) -> List[str]:
        """Create ETL processing jobs"""
        job_ids = []

        # Data transformation job
        transform_job = Job(
            job_id=f"{pipeline_id}_transform",
            job_name="Data Transformation",
            job_type=JobType.BATCH_PROCESSING,
            priority=JobPriority.NORMAL,
            processing_strategy=ProcessingStrategy.PARALLEL_CHUNKS,
            config={
                "pipeline_id": pipeline_id,
                "phase": "transformation",
                "transformations": config.get("transformations", []),
                "output_schema": config.get("output_schema", {}),
                "chunk_size": config.get("etl_chunk_size", 50000)
            },
            required_capabilities=["data_transformation", "etl"],
            resource_requirements={
                "memory_mb": 4096,
                "cpu_cores": 2
            },
            tags=["pipeline", pipeline_id, "etl", "transformation"]
        )

        transform_job_id = await self.orchestrator.submit_job(transform_job)
        job_ids.append(transform_job_id)
        print(f"  🔄 Submitted transformation job: {transform_job_id}")

        # Data quality check job
        quality_job = Job(
            job_id=f"{pipeline_id}_quality_check",
            job_name="Data Quality Check",
            job_type=JobType.DATA_VALIDATION,
            priority=JobPriority.HIGH,
            processing_strategy=ProcessingStrategy.SINGLE_WORKER,
            config={
                "pipeline_id": pipeline_id,
                "phase": "quality_check",
                "quality_rules": config.get("quality_rules", {}),
                "threshold_configs": config.get("quality_thresholds", {}),
                "generate_report": True
            },
            required_capabilities=["data_validation", "quality_check"],
            resource_requirements={
                "memory_mb": 2048,
                "cpu_cores": 1
            },
            tags=["pipeline", pipeline_id, "etl", "quality"]
        )

        quality_job_id = await self.orchestrator.submit_job(quality_job)
        job_ids.append(quality_job_id)
        print(f"  ✅ Submitted quality check job: {quality_job_id}")

        return job_ids

    async def _create_ml_jobs(self, pipeline_id: str, config: Dict[str, Any]) -> List[str]:
        """Create ML training jobs"""
        job_ids = []

        # Feature engineering job
        feature_job = Job(
            job_id=f"{pipeline_id}_feature_engineering",
            job_name="Feature Engineering",
            job_type=JobType.MACHINE_LEARNING,
            priority=JobPriority.NORMAL,
            processing_strategy=ProcessingStrategy.PARALLEL_CHUNKS,
            config={
                "pipeline_id": pipeline_id,
                "phase": "feature_engineering",
                "feature_definitions": config.get("features", {}),
                "feature_store_config": config.get("feature_store", {}),
                "chunk_size": config.get("ml_chunk_size", 100000)
            },
            required_capabilities=["machine_learning", "feature_engineering"],
            resource_requirements={
                "memory_mb": 8192,
                "cpu_cores": 4
            },
            tags=["pipeline", pipeline_id, "ml", "features"]
        )

        feature_job_id = await self.orchestrator.submit_job(feature_job)
        job_ids.append(feature_job_id)
        print(f"  🔧 Submitted feature engineering job: {feature_job_id}")

        # Model training job
        training_job = Job(
            job_id=f"{pipeline_id}_model_training",
            job_name="Model Training",
            job_type=JobType.MACHINE_LEARNING,
            priority=JobPriority.HIGH,
            processing_strategy=ProcessingStrategy.HYBRID,
            config={
                "pipeline_id": pipeline_id,
                "phase": "training",
                "model_type": config.get("model_type", "random_forest"),
                "hyperparameters": config.get("hyperparameters", {}),
                "validation_strategy": config.get("validation_strategy", "cross_validation"),
                "model_registry_config": config.get("model_registry", {})
            },
            required_capabilities=["machine_learning", "training"],
            resource_requirements={
                "memory_mb": 16384,
                "cpu_cores": 8
            },
            tags=["pipeline", pipeline_id, "ml", "training"]
        )

        training_job_id = await self.orchestrator.submit_job(training_job)
        job_ids.append(training_job_id)
        print(f"  🤖 Submitted model training job: {training_job_id}")

        return job_ids

    async def _wait_for_jobs(self, job_ids: List[str], phase_name: str):
        """Wait for a list of jobs to complete"""
        print(f"  ⏳ Waiting for {phase_name} jobs to complete...")

        while True:
            completed = 0
            failed = 0

            for job_id in job_ids:
                status = await self.orchestrator.get_job_status(job_id)
                if status:
                    if status['status'] in ['completed', 'successful']:
                        completed += 1
                    elif status['status'] in ['failed', 'cancelled']:
                        failed += 1
                        print(f"  ❌ Job {job_id} failed: {status.get('error_message', 'Unknown error')}")

            if failed > 0:
                raise Exception(f"{failed} jobs failed in {phase_name}")

            if completed == len(job_ids):
                print(f"  ✅ All {phase_name} jobs completed successfully")
                break

            # Wait before checking again
            await asyncio.sleep(5)

    async def _cancel_remaining_jobs(self, job_ids: List[str]):
        """Cancel any remaining jobs"""
        for job_id in job_ids:
            try:
                await self.orchestrator.cancel_job(job_id)
            except Exception:
                pass  # Ignore cancellation errors

    async def _generate_pipeline_report(self, pipeline_id: str, job_ids: List[str]):
        """Generate a pipeline execution report"""
        print(f"\n📊 Generating pipeline report for {pipeline_id}")

        total_jobs = len(job_ids)
        completed_jobs = 0
        failed_jobs = 0
        total_duration = 0

        for job_id in job_ids:
            status = await self.orchestrator.get_job_status(job_id)
            if status:
                if status['status'] in ['completed', 'successful']:
                    completed_jobs += 1
                elif status['status'] in ['failed', 'cancelled']:
                    failed_jobs += 1

                # Calculate duration if available
                if status.get('started_at') and status.get('completed_at'):
                    start = datetime.fromisoformat(status['started_at'].replace('Z', '+00:00'))
                    end = datetime.fromisoformat(status['completed_at'].replace('Z', '+00:00'))
                    duration = (end - start).total_seconds()
                    total_duration += duration

        print(f"Pipeline Report:")
        print(f"  Pipeline ID: {pipeline_id}")
        print(f"  Total Jobs: {total_jobs}")
        print(f"  Completed: {completed_jobs}")
        print(f"  Failed: {failed_jobs}")
        print(f"  Success Rate: {(completed_jobs / total_jobs * 100):.1f}%")
        print(f"  Total Duration: {total_duration:.1f} seconds")

        # Get system health after pipeline
        health = await self.orchestrator.get_system_health()
        print(f"  System Status: {health['overall_status']}")

    async def shutdown(self):
        """Shutdown the pipeline"""
        if self.orchestrator:
            await self.orchestrator.stop()
            print("🛑 Pipeline shutdown complete")


async def demonstrate_complete_integration():
    """Demonstrate complete integration with various scenarios"""

    print("🎯 Enterprise Job Orchestrator - Complete Integration Example\n")

    # Initialize pipeline
    pipeline = DataProcessingPipeline("postgresql://localhost/job_orchestrator")

    try:
        await pipeline.initialize()

        # Example 1: Small data processing pipeline
        print("\n" + "="*60)
        print("Example 1: Small Data Processing Pipeline")
        print("="*60)

        small_config = {
            "validation_rules": {
                "required_columns": ["id", "timestamp", "value"],
                "data_types": {"id": "int", "value": "float"}
            },
            "transformations": [
                {"type": "clean_nulls", "strategy": "drop"},
                {"type": "normalize", "columns": ["value"]},
                {"type": "add_derived", "column": "processed_at", "value": "current_timestamp"}
            ],
            "quality_rules": {
                "completeness_threshold": 0.95,
                "uniqueness_check": ["id"],
                "range_checks": {"value": {"min": 0, "max": 1000}}
            },
            "ingestion_chunk_size": 5000,
            "etl_chunk_size": 25000
        }

        await pipeline.run_data_pipeline(
            input_files=["data/sample1.csv", "data/sample2.csv"],
            pipeline_config=small_config
        )

        # Example 2: Large data processing with ML
        print("\n" + "="*60)
        print("Example 2: Large Data Processing with ML Training")
        print("="*60)

        large_config = {
            "validation_rules": {
                "required_columns": ["user_id", "event_time", "event_type", "value"],
                "data_types": {"user_id": "int", "value": "float"}
            },
            "transformations": [
                {"type": "clean_nulls", "strategy": "impute"},
                {"type": "feature_scaling", "method": "standard"},
                {"type": "categorical_encoding", "columns": ["event_type"]},
                {"type": "time_features", "column": "event_time"}
            ],
            "quality_rules": {
                "completeness_threshold": 0.98,
                "consistency_checks": ["user_id", "event_time"],
                "outlier_detection": {"value": {"method": "iqr", "threshold": 3}}
            },
            "enable_ml_training": True,
            "features": {
                "numerical": ["value", "hour_of_day", "day_of_week"],
                "categorical": ["event_type"],
                "derived": ["value_rolling_mean", "user_activity_score"]
            },
            "model_type": "gradient_boosting",
            "hyperparameters": {
                "n_estimators": 100,
                "learning_rate": 0.1,
                "max_depth": 6
            },
            "validation_strategy": "time_series_split",
            "ingestion_chunk_size": 50000,
            "etl_chunk_size": 200000,
            "ml_chunk_size": 500000
        }

        await pipeline.run_data_pipeline(
            input_files=["data/large_dataset1.parquet", "data/large_dataset2.parquet", "data/large_dataset3.parquet"],
            pipeline_config=large_config
        )

        # Example 3: Real-time streaming simulation
        print("\n" + "="*60)
        print("Example 3: Real-time Streaming Simulation")
        print("="*60)

        await simulate_streaming_pipeline(pipeline)

        # Generate final system report
        print("\n" + "="*60)
        print("Final System Performance Report")
        print("="*60)

        final_report = await pipeline.orchestrator.get_performance_report(hours=1)

        print("Performance Summary:")
        if 'job_performance' in final_report:
            job_perf = final_report['job_performance']
            print(f"  Total Jobs Processed: {job_perf.get('completed_jobs', 0)}")
            print(f"  Job Success Rate: {job_perf.get('success_rate', 0):.1f}%")
            print(f"  Average Job Duration: {job_perf.get('average_duration', 'N/A')}")

        if 'worker_performance' in final_report:
            worker_perf = final_report['worker_performance']
            print(f"  Average CPU Usage: {worker_perf.get('average_cpu', 0):.1f}%")
            print(f"  Average Memory Usage: {worker_perf.get('average_memory', 0):.1f}%")
            print(f"  Peak Concurrent Workers: {worker_perf.get('peak_workers', 0)}")

        print("\n✅ Complete integration demonstration finished successfully!")

    except Exception as e:
        print(f"\n❌ Integration demonstration failed: {str(e)}")
        import traceback
        traceback.print_exc()

    finally:
        await pipeline.shutdown()


async def simulate_streaming_pipeline(pipeline: DataProcessingPipeline):
    """Simulate a real-time streaming data processing scenario"""

    print("🌊 Simulating real-time streaming data processing...")

    # Submit multiple small jobs to simulate streaming
    streaming_jobs = []

    for i in range(10):
        job = Job(
            job_id=f"stream_batch_{i:03d}",
            job_name=f"Stream Batch {i+1}",
            job_type=JobType.STREAM_PROCESSING,
            priority=JobPriority.HIGH,
            processing_strategy=ProcessingStrategy.SINGLE_WORKER,
            config={
                "batch_id": i,
                "stream_source": "kafka://events-topic",
                "window_size": "5m",
                "processing_delay": "30s",
                "output_sink": "elasticsearch://metrics"
            },
            required_capabilities=["stream_processing", "real_time"],
            resource_requirements={
                "memory_mb": 1024,
                "cpu_cores": 1
            },
            tags=["streaming", "real_time", f"batch_{i}"]
        )

        job_id = await pipeline.orchestrator.submit_job(job)
        streaming_jobs.append(job_id)
        print(f"  📡 Submitted streaming batch: {job_id}")

        # Small delay to simulate streaming
        await asyncio.sleep(2)

    print(f"  ⏳ Processing {len(streaming_jobs)} streaming batches...")

    # Wait for all streaming jobs to complete
    await pipeline._wait_for_jobs(streaming_jobs, "Streaming")

    print("  ✅ Streaming simulation completed")


if __name__ == "__main__":
    # Run the complete integration example
    asyncio.run(demonstrate_complete_integration())