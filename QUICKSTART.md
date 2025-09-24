# Enterprise Job Orchestrator - Quick Start Guide

Get up and running with the Enterprise Job Orchestrator in minutes!

## 🚀 Installation

```bash
# Install from source
git clone <repository-url>
cd enterprise-job-orchestrator
pip install -e .

# Or install from PyPI (when published)
pip install enterprise-job-orchestrator
```

## ⚡ 30-Second Example

```python
import asyncio
from enterprise_job_orchestrator import quick_start, Job, JobType, JobPriority

async def hello_orchestrator():
    # Quick start with default settings
    orchestrator = quick_start("postgresql://localhost/job_orchestrator")
    await orchestrator.start()

    try:
        # Create a simple job
        job = Job(
            job_id="my_first_job",
            job_name="Hello World Job",
            job_type=JobType.DATA_PROCESSING,
            priority=JobPriority.NORMAL,
            config={"message": "Hello, Enterprise Job Orchestrator!"}
        )

        # Submit and track the job
        job_id = await orchestrator.submit_job(job)
        print(f"Job submitted: {job_id}")

        # Check status
        status = await orchestrator.get_job_status(job_id)
        print(f"Job status: {status['status']}")

    finally:
        await orchestrator.stop()

# Run it
asyncio.run(hello_orchestrator())
```

## 🗄️ Database Setup

### PostgreSQL (Recommended)

```bash
# Install PostgreSQL
# Ubuntu/Debian: sudo apt-get install postgresql postgresql-contrib
# macOS: brew install postgresql
# Windows: Download from https://www.postgresql.org/

# Create database
sudo -u postgres createdb job_orchestrator

# Or with custom user
sudo -u postgres psql
CREATE DATABASE job_orchestrator;
CREATE USER job_user WITH PASSWORD 'your_password';
GRANT ALL PRIVILEGES ON DATABASE job_orchestrator TO job_user;
```

### Connection String Examples

```python
# Local PostgreSQL
database_url = "postgresql://localhost/job_orchestrator"

# With credentials
database_url = "postgresql://job_user:password@localhost:5432/job_orchestrator"

# Cloud PostgreSQL (AWS RDS, Google Cloud SQL, etc.)
database_url = "postgresql://user:password@host:5432/database?sslmode=require"
```

## 🔧 Basic Configuration

### Method 1: Quick Start (Easiest)

```python
from enterprise_job_orchestrator import quick_start

# Uses sensible defaults
orchestrator = quick_start("postgresql://localhost/job_orchestrator")
await orchestrator.start()
```

### Method 2: Custom Configuration

```python
from enterprise_job_orchestrator import JobOrchestrator, DatabaseManager

# Custom database settings
db_manager = DatabaseManager(
    "postgresql://localhost/job_orchestrator",
    pool_size=10,
    max_overflow=20
)
await db_manager.initialize()

# Custom orchestrator
orchestrator = JobOrchestrator(db_manager)
await orchestrator.start()
```

## 📝 Creating Your First Job

```python
from enterprise_job_orchestrator import Job, JobType, JobPriority, ProcessingStrategy

job = Job(
    job_id="data_analysis_001",           # Unique identifier
    job_name="Customer Data Analysis",     # Human-readable name
    job_type=JobType.DATA_PROCESSING,     # Type of work
    priority=JobPriority.HIGH,            # Execution priority
    processing_strategy=ProcessingStrategy.PARALLEL_CHUNKS,  # How to process
    config={                              # Job-specific configuration
        "input_file": "/data/customers.csv",
        "output_dir": "/results/",
        "chunk_size": 10000,
        "analysis_type": "behavioral"
    },
    resource_requirements={               # Resource needs
        "memory_mb": 4096,
        "cpu_cores": 2
    },
    tags=["analytics", "customer_data"]   # Searchable tags
)

# Submit the job
job_id = await orchestrator.submit_job(job)
```

## 👷 Adding Workers

### Register a Worker Node

```python
from enterprise_job_orchestrator import WorkerNode

worker = WorkerNode(
    worker_id="worker_001",
    node_name="Data Processing Node 1",
    host_name="worker-server-1",
    port=8080,
    capabilities=["data_processing", "analytics"],
    max_concurrent_jobs=4,
    worker_metadata={
        "pool": "production",
        "region": "us-east-1"
    }
)

success = await orchestrator.register_worker(worker)
if success:
    print("Worker registered successfully!")
```

## 📊 Monitoring Jobs

### Check Individual Job Status

```python
status = await orchestrator.get_job_status("my_job_id")
print(f"Status: {status['status']}")
print(f"Progress: {status.get('progress_percent', 0)}%")
```

### List All Jobs

```python
# Get recent jobs
jobs = await orchestrator.list_jobs(limit=10)

# Filter by status
running_jobs = await orchestrator.list_jobs(status_filter='running')
failed_jobs = await orchestrator.list_jobs(status_filter='failed')
```

### System Health Check

```python
health = await orchestrator.get_system_health()
print(f"System Status: {health['overall_status']}")
print(f"Running Jobs: {health['running_jobs']}")
print(f"Healthy Workers: {health['healthy_workers']}")
```

## 🖥️ Using the CLI

The orchestrator includes a comprehensive command-line interface:

```bash
# Submit a job
enterprise-job-orchestrator job submit "My Job" --job-type data_processing --priority high

# Check job status
enterprise-job-orchestrator job status my_job_id

# List all jobs
enterprise-job-orchestrator job status --all

# Check system health
enterprise-job-orchestrator monitor health

# Register a worker
enterprise-job-orchestrator worker register worker_001 "My Worker" --host localhost --port 8080

# Start HTTP server
enterprise-job-orchestrator server start --port 8000
```

## 🌐 Web Framework Integration

### Flask Example

```python
from flask import Flask, request, jsonify
from enterprise_job_orchestrator import quick_start, Job, JobType

app = Flask(__name__)
orchestrator = None

@app.before_first_request
def setup():
    global orchestrator
    orchestrator = quick_start("postgresql://localhost/job_orchestrator")
    # Note: In production, use proper async setup

@app.route('/jobs', methods=['POST'])
def submit_job():
    data = request.get_json()

    job = Job(
        job_id=f"api_job_{int(time.time())}",
        job_name=data['name'],
        job_type=JobType(data['type'].upper()),
        config=data.get('config', {})
    )

    # In production, use proper async handling
    job_id = asyncio.run(orchestrator.submit_job(job))

    return jsonify({'job_id': job_id, 'status': 'submitted'})
```

### FastAPI Example

```python
from fastapi import FastAPI
from enterprise_job_orchestrator import quick_start, Job, JobType

app = FastAPI()
orchestrator = None

@app.on_event("startup")
async def startup():
    global orchestrator
    orchestrator = quick_start("postgresql://localhost/job_orchestrator")
    await orchestrator.start()

@app.post("/jobs")
async def submit_job(job_data: dict):
    job = Job(
        job_id=f"api_job_{int(time.time())}",
        job_name=job_data['name'],
        job_type=JobType(job_data['type'].upper()),
        config=job_data.get('config', {})
    )

    job_id = await orchestrator.submit_job(job)
    return {"job_id": job_id, "status": "submitted"}
```

## 🔧 Common Patterns

### Batch Processing

```python
# Process large datasets in chunks
job = Job(
    job_id="batch_001",
    job_name="Large Dataset Processing",
    job_type=JobType.BATCH_PROCESSING,
    processing_strategy=ProcessingStrategy.PARALLEL_CHUNKS,
    config={
        "input_file": "large_dataset.csv",
        "chunk_size": 50000,
        "output_format": "parquet"
    }
)
```

### Machine Learning Pipeline

```python
# ML training job
ml_job = Job(
    job_id="ml_training_001",
    job_name="Customer Segmentation Model",
    job_type=JobType.MACHINE_LEARNING,
    priority=JobPriority.HIGH,
    config={
        "algorithm": "kmeans",
        "features": ["age", "income", "purchase_history"],
        "n_clusters": 5,
        "model_output": "/models/customer_segments.pkl"
    },
    resource_requirements={
        "memory_mb": 8192,
        "cpu_cores": 4
    }
)
```

### Real-time Stream Processing

```python
# Stream processing job
stream_job = Job(
    job_id="stream_001",
    job_name="Real-time Event Processing",
    job_type=JobType.STREAM_PROCESSING,
    priority=JobPriority.HIGH,
    processing_strategy=ProcessingStrategy.SINGLE_WORKER,
    config={
        "source": "kafka://events-topic",
        "window_size": "5m",
        "output": "elasticsearch://metrics"
    },
    required_capabilities=["stream_processing"]
)
```

## 🔍 Troubleshooting

### Common Issues

1. **Database Connection Failed**
   ```python
   # Verify database is running and accessible
   # Check connection string format
   # Ensure database exists and user has permissions
   ```

2. **Jobs Stuck in Queue**
   ```python
   # Check if workers are registered and healthy
   workers = await orchestrator.get_workers()
   print(f"Available workers: {len(workers)}")

   # Check job requirements vs worker capabilities
   job_status = await orchestrator.get_job_status("job_id")
   print(f"Required capabilities: {job_status.get('required_capabilities')}")
   ```

3. **High Memory Usage**
   ```python
   # Reduce chunk sizes for large datasets
   # Monitor worker resource usage
   # Scale horizontally with more workers
   ```

### Getting Help

- Check the [examples](./examples/) directory for more use cases
- Review the [API documentation](./docs/) for detailed reference
- Report issues on the project repository

## 🎯 Next Steps

1. **Scale Up**: Add more worker nodes for parallel processing
2. **Monitor**: Set up alerts and dashboards for production monitoring
3. **Optimize**: Tune chunk sizes and resource allocation for your workload
4. **Integrate**: Connect with your existing data pipeline tools
5. **Extend**: Build custom job types for domain-specific processing

Happy orchestrating! 🚀