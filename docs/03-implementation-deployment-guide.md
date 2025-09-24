# Implementation & Deployment Guide

## Overview

This guide provides step-by-step instructions for implementing and deploying the Enterprise Job Orchestrator for identity matching at scale.

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Environment Setup](#environment-setup)
3. [Installation](#installation)
4. [Configuration](#configuration)
5. [Deployment Options](#deployment-options)
6. [Testing](#testing)
7. [Production Checklist](#production-checklist)

---

## Prerequisites

### System Requirements

```yaml
minimum_requirements:
  os: Ubuntu 20.04+ / CentOS 8+ / Windows Server 2019+
  python: 3.8+
  postgresql: 14+
  redis: 6.2+
  docker: 20.10+ (optional)
  kubernetes: 1.21+ (optional)

development_tools:
  - git
  - make
  - gcc
  - python3-dev
  - postgresql-dev
```

### Required Python Packages

```python
# requirements.txt
enterprise-job-orchestrator==1.0.0
identity-matching-engine==1.0.0
asyncpg==0.27.0
redis==4.5.0
pandas==2.0.0
numpy==1.24.0
psutil==5.9.0
click==8.1.0
pydantic==2.0.0
fastapi==0.100.0
uvicorn==0.23.0
```

---

## Environment Setup

### Step 1: Install PostgreSQL

```bash
# Ubuntu/Debian
sudo apt update
sudo apt install postgresql-14 postgresql-contrib-14

# CentOS/RHEL
sudo dnf install postgresql14-server postgresql14-contrib

# macOS
brew install postgresql@14

# Windows (use installer from postgresql.org)
```

### Step 2: Configure PostgreSQL

```bash
# Create database and user
sudo -u postgres psql << EOF
CREATE DATABASE idxr_production;
CREATE USER idxr_user WITH ENCRYPTED PASSWORD 'your_secure_password';
GRANT ALL PRIVILEGES ON DATABASE idxr_production TO idxr_user;

-- Performance tuning
ALTER SYSTEM SET shared_buffers = '16GB';
ALTER SYSTEM SET effective_cache_size = '48GB';
ALTER SYSTEM SET maintenance_work_mem = '2GB';
ALTER SYSTEM SET work_mem = '512MB';
ALTER SYSTEM SET max_connections = 200;
ALTER SYSTEM SET max_parallel_workers = 16;
EOF

# Restart PostgreSQL
sudo systemctl restart postgresql
```

### Step 3: Install Redis

```bash
# Ubuntu/Debian
sudo apt install redis-server

# CentOS/RHEL
sudo dnf install redis

# macOS
brew install redis

# Configure Redis for production
sudo tee /etc/redis/redis.conf << EOF
maxmemory 10gb
maxmemory-policy allkeys-lru
save ""
tcp-keepalive 60
timeout 0
databases 16
EOF

# Start Redis
sudo systemctl enable redis
sudo systemctl start redis
```

### Step 4: Create Directory Structure

```bash
# Create project directories
mkdir -p /opt/idxr/{
  config,
  data/input,
  data/output,
  data/temp,
  logs,
  scripts,
  workers
}

# Set permissions
sudo chown -R $USER:$USER /opt/idxr
chmod -R 755 /opt/idxr
```

---

## Installation

### Option 1: Direct Installation

```bash
# Create virtual environment
python3 -m venv /opt/idxr/venv
source /opt/idxr/venv/bin/activate

# Install packages
pip install --upgrade pip
pip install enterprise-job-orchestrator
pip install identity-matching-engine

# Install IDXR integration
cd /path/to/IDXR
pip install -e .
```

### Option 2: Docker Installation

```dockerfile
# Dockerfile
FROM python:3.10-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy requirements
COPY requirements.txt .

# Install Python packages
RUN pip install --no-cache-dir -r requirements.txt

# Copy application
COPY . .

# Set environment variables
ENV DATABASE_URL="postgresql://idxr_user:password@db:5432/idxr_production"
ENV REDIS_URL="redis://redis:6379"
ENV PYTHONPATH=/app

# Run orchestrator
CMD ["python", "-m", "enterprise_job_orchestrator", "server", "start"]
```

### Option 3: Kubernetes Installation

```yaml
# helm/values.yaml
orchestrator:
  replicas: 3
  image: enterprise-job-orchestrator:latest
  resources:
    requests:
      memory: "8Gi"
      cpu: "2"
    limits:
      memory: "16Gi"
      cpu: "4"

workers:
  replicas: 50
  image: enterprise-job-worker:latest
  resources:
    requests:
      memory: "8Gi"
      cpu: "2"
    limits:
      memory: "16Gi"
      cpu: "4"

postgresql:
  enabled: true
  auth:
    database: idxr_production
    username: idxr_user
  primary:
    resources:
      requests:
        memory: "32Gi"
        cpu: "8"

redis:
  enabled: true
  master:
    resources:
      requests:
        memory: "10Gi"
        cpu: "2"
```

---

## Configuration

### Main Configuration File

```python
# /opt/idxr/config/orchestrator.yaml
orchestrator:
  name: "IDXR Production Orchestrator"
  environment: "production"
  log_level: "INFO"

database:
  url: "postgresql://idxr_user:password@localhost:5432/idxr_production"
  pool_size: 50
  max_overflow: 100
  echo: false

redis:
  url: "redis://localhost:6379/0"
  max_connections: 100
  decode_responses: true

workers:
  count: 50
  max_concurrent_jobs: 2
  heartbeat_interval: 30
  timeout: 300

processing:
  chunk_size: 2500
  max_retries: 3
  retry_delay: 60
  parallel_chunks: 50

monitoring:
  enabled: true
  metrics_port: 9090
  health_check_interval: 60

security:
  api_key_required: true
  ssl_enabled: true
  allowed_ips:
    - "10.0.0.0/8"
    - "172.16.0.0/12"
```

### Identity Matching Configuration

```python
# /opt/idxr/config/matching.yaml
matching:
  algorithms:
    - name: "deterministic"
      enabled: true
      weight: 0.5
      fields:
        - ssn
        - driver_license
        - passport

    - name: "probabilistic"
      enabled: true
      weight: 0.3
      fields:
        - name
        - dob
        - address
        - phone

    - name: "fuzzy"
      enabled: true
      weight: 0.15
      techniques:
        - soundex
        - levenshtein
        - jaro_winkler

    - name: "ai_hybrid"
      enabled: true
      weight: 0.05
      model_path: "/opt/idxr/models/identity_transformer.pt"

  thresholds:
    exact_match: 1.0
    high_confidence: 0.85
    medium_confidence: 0.70
    low_confidence: 0.50

  deduplication:
    enabled: true
    method: "graph_based"
    transitive_closure: true

  household_detection:
    enabled: true
    max_household_size: 10
    rules:
      - same_address
      - shared_phone
      - family_name
```

### Logging Configuration

```python
# /opt/idxr/config/logging.yaml
version: 1
disable_existing_loggers: false

formatters:
  default:
    format: '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
  json:
    class: pythonjsonlogger.jsonlogger.JsonFormatter

handlers:
  console:
    class: logging.StreamHandler
    level: INFO
    formatter: default
    stream: ext://sys.stdout

  file:
    class: logging.handlers.RotatingFileHandler
    level: INFO
    formatter: json
    filename: /opt/idxr/logs/orchestrator.log
    maxBytes: 104857600  # 100MB
    backupCount: 10

  error_file:
    class: logging.handlers.RotatingFileHandler
    level: ERROR
    formatter: json
    filename: /opt/idxr/logs/errors.log
    maxBytes: 104857600
    backupCount: 10

root:
  level: INFO
  handlers: [console, file, error_file]

loggers:
  enterprise_job_orchestrator:
    level: INFO
  identity_matching_engine:
    level: INFO
  asyncpg:
    level: WARNING
  redis:
    level: WARNING
```

---

## Deployment Options

### Option 1: Systemd Service (Linux)

```ini
# /etc/systemd/system/idxr-orchestrator.service
[Unit]
Description=IDXR Job Orchestrator
After=network.target postgresql.service redis.service

[Service]
Type=simple
User=idxr
Group=idxr
WorkingDirectory=/opt/idxr
Environment="PATH=/opt/idxr/venv/bin"
Environment="DATABASE_URL=postgresql://idxr_user:password@localhost/idxr_production"
Environment="REDIS_URL=redis://localhost:6379"
ExecStart=/opt/idxr/venv/bin/python -m enterprise_job_orchestrator server start
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
# Enable and start service
sudo systemctl daemon-reload
sudo systemctl enable idxr-orchestrator
sudo systemctl start idxr-orchestrator
sudo systemctl status idxr-orchestrator
```

### Option 2: Docker Compose

```yaml
# docker-compose.yml
version: '3.8'

services:
  postgres:
    image: postgres:14
    environment:
      POSTGRES_DB: idxr_production
      POSTGRES_USER: idxr_user
      POSTGRES_PASSWORD: secure_password
    volumes:
      - postgres_data:/var/lib/postgresql/data
    ports:
      - "5432:5432"
    networks:
      - idxr_network

  redis:
    image: redis:6.2
    command: redis-server --maxmemory 10gb --maxmemory-policy allkeys-lru
    ports:
      - "6379:6379"
    networks:
      - idxr_network

  orchestrator:
    image: idxr/orchestrator:latest
    depends_on:
      - postgres
      - redis
    environment:
      DATABASE_URL: postgresql://idxr_user:secure_password@postgres/idxr_production
      REDIS_URL: redis://redis:6379
      WORKER_COUNT: 10
    ports:
      - "8000:8000"
      - "9090:9090"
    volumes:
      - ./config:/app/config
      - ./data:/app/data
      - ./logs:/app/logs
    networks:
      - idxr_network
    deploy:
      replicas: 3
      restart_policy:
        condition: on-failure
        delay: 5s

  worker:
    image: idxr/worker:latest
    depends_on:
      - orchestrator
    environment:
      ORCHESTRATOR_URL: http://orchestrator:8000
      DATABASE_URL: postgresql://idxr_user:secure_password@postgres/idxr_production
      REDIS_URL: redis://redis:6379
    volumes:
      - ./data:/app/data
    networks:
      - idxr_network
    deploy:
      replicas: 50
      restart_policy:
        condition: on-failure

networks:
  idxr_network:
    driver: bridge

volumes:
  postgres_data:
```

### Option 3: Kubernetes Deployment

```yaml
# kubernetes/deployment.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: idxr

---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: orchestrator
  namespace: idxr
spec:
  replicas: 3
  selector:
    matchLabels:
      app: orchestrator
  template:
    metadata:
      labels:
        app: orchestrator
    spec:
      containers:
      - name: orchestrator
        image: idxr/orchestrator:latest
        ports:
        - containerPort: 8000
        - containerPort: 9090
        env:
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: idxr-secrets
              key: database-url
        - name: REDIS_URL
          valueFrom:
            secretKeyRef:
              name: idxr-secrets
              key: redis-url
        resources:
          requests:
            memory: "8Gi"
            cpu: "2"
          limits:
            memory: "16Gi"
            cpu: "4"
        livenessProbe:
          httpGet:
            path: /health
            port: 8000
          initialDelaySeconds: 30
          periodSeconds: 10
        readinessProbe:
          httpGet:
            path: /ready
            port: 8000
          initialDelaySeconds: 10
          periodSeconds: 5

---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: workers
  namespace: idxr
spec:
  serviceName: workers
  replicas: 50
  selector:
    matchLabels:
      app: worker
  template:
    metadata:
      labels:
        app: worker
    spec:
      containers:
      - name: worker
        image: idxr/worker:latest
        env:
        - name: ORCHESTRATOR_URL
          value: "http://orchestrator-service:8000"
        - name: DATABASE_URL
          valueFrom:
            secretKeyRef:
              name: idxr-secrets
              key: database-url
        resources:
          requests:
            memory: "8Gi"
            cpu: "2"
          limits:
            memory: "16Gi"
            cpu: "4"
        volumeMounts:
        - name: data
          mountPath: /data
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 100Gi

---
apiVersion: v1
kind: Service
metadata:
  name: orchestrator-service
  namespace: idxr
spec:
  selector:
    app: orchestrator
  ports:
  - name: api
    port: 8000
    targetPort: 8000
  - name: metrics
    port: 9090
    targetPort: 9090
  type: LoadBalancer

---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: worker-hpa
  namespace: idxr
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: StatefulSet
    name: workers
  minReplicas: 20
  maxReplicas: 100
  metrics:
  - type: Resource
    resource:
      name: cpu
      target:
        type: Utilization
        averageUtilization: 70
  - type: Resource
    resource:
      name: memory
      target:
        type: Utilization
        averageUtilization: 80
```

---

## Testing

### Unit Tests

```python
# tests/test_orchestrator.py
import pytest
import asyncio
from enterprise_job_orchestrator import (
    JobOrchestrator,
    Job,
    JobType,
    JobPriority,
    ProcessingStrategy
)

@pytest.mark.asyncio
async def test_job_submission():
    """Test job submission"""
    orchestrator = create_test_orchestrator()
    await orchestrator.start()

    job = Job(
        job_id="test_job",
        job_name="Test Job",
        job_type=JobType.DATA_PROCESSING,
        priority=JobPriority.NORMAL,
        processing_strategy=ProcessingStrategy.SEQUENTIAL,
        config={"test": True}
    )

    job_id = await orchestrator.submit_job(job)
    assert job_id == "test_job"

    status = await orchestrator.get_job_status(job_id)
    assert status is not None
    assert status['status'] in ['queued', 'running']

    await orchestrator.stop()
```

### Integration Tests

```python
# tests/test_integration.py
import pytest
from backend.matching_engine.services.batch_processing_service import (
    BatchProcessingService,
    JobType,
    JobPriority
)

@pytest.mark.asyncio
async def test_batch_processing():
    """Test batch processing with real data"""
    service = BatchProcessingService("postgresql://test/test")
    await service.initialize()

    # Test with sample data
    test_data = [
        {"id": 1, "name": "John Doe", "ssn": "123456789"},
        {"id": 2, "name": "Jane Smith", "ssn": "987654321"}
    ]

    job_id = await service.create_batch_job(
        name="Integration Test",
        job_type=JobType.IDENTITY_MATCHING,
        created_by="test_suite",
        input_data=test_data,
        config={"match_threshold": 0.8},
        priority=JobPriority.HIGH
    )

    # Wait for completion
    status = await wait_for_job(service, job_id, timeout=60)
    assert status['status'] == 'completed'

    await service.stop()
```

### Load Tests

```python
# tests/test_load.py
import asyncio
import time
from concurrent.futures import ThreadPoolExecutor

async def load_test_70m_simulation():
    """Simulate 70M record processing"""

    # Create test data chunks
    total_chunks = 28000  # 70M / 2500
    chunks_per_worker = 560  # 28000 / 50

    start_time = time.time()

    # Simulate parallel processing
    with ThreadPoolExecutor(max_workers=50) as executor:
        futures = []
        for i in range(total_chunks):
            future = executor.submit(process_chunk, i)
            futures.append(future)

        # Wait for completion
        completed = 0
        for future in futures:
            result = future.result()
            completed += 1

            if completed % 1000 == 0:
                elapsed = time.time() - start_time
                rate = completed / elapsed
                eta = (total_chunks - completed) / rate
                print(f"Progress: {completed}/{total_chunks} "
                      f"Rate: {rate:.1f} chunks/sec "
                      f"ETA: {eta:.0f} seconds")

    total_time = time.time() - start_time
    print(f"Load test completed in {total_time:.1f} seconds")
    print(f"Processing rate: {total_chunks/total_time:.1f} chunks/sec")
```

---

## Production Checklist

### Pre-Deployment

- [ ] **Infrastructure**
  - [ ] PostgreSQL 14+ installed and configured
  - [ ] Redis 6+ installed and configured
  - [ ] Sufficient storage provisioned (2TB+)
  - [ ] Network configured (10Gbps+)
  - [ ] Backup system configured
  - [ ] Monitoring tools installed

- [ ] **Security**
  - [ ] SSL/TLS certificates installed
  - [ ] Database encryption enabled
  - [ ] API keys generated
  - [ ] Firewall rules configured
  - [ ] Access controls implemented
  - [ ] Audit logging enabled

- [ ] **Configuration**
  - [ ] Configuration files created
  - [ ] Environment variables set
  - [ ] Connection strings verified
  - [ ] Resource limits configured
  - [ ] Logging configured

- [ ] **Testing**
  - [ ] Unit tests passing
  - [ ] Integration tests passing
  - [ ] Load tests completed
  - [ ] Failover tested
  - [ ] Recovery procedures tested

### Deployment

- [ ] **Initial Deployment**
  - [ ] Database schema created
  - [ ] Initial data loaded
  - [ ] Workers registered
  - [ ] Health checks passing
  - [ ] Monitoring active

- [ ] **Verification**
  - [ ] API endpoints responding
  - [ ] Workers processing jobs
  - [ ] Metrics being collected
  - [ ] Logs being generated
  - [ ] Alerts configured

### Post-Deployment

- [ ] **Monitoring**
  - [ ] Dashboards configured
  - [ ] Alerts tested
  - [ ] Performance baseline established
  - [ ] Resource usage tracked
  - [ ] Error rates monitored

- [ ] **Documentation**
  - [ ] Runbook created
  - [ ] Deployment notes documented
  - [ ] Configuration documented
  - [ ] Contact list updated
  - [ ] Escalation procedures defined

---

## Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| Database connection failed | Check connection string, firewall, credentials |
| Workers not registering | Verify network connectivity, check logs |
| Slow processing | Increase workers, optimize queries, check resources |
| High memory usage | Reduce chunk size, enable streaming |
| Jobs stuck in queue | Check worker health, review error logs |

### Diagnostic Commands

```bash
# Check orchestrator status
enterprise-job-orchestrator monitor health

# View queue statistics
enterprise-job-orchestrator monitor metrics

# List workers
enterprise-job-orchestrator worker list

# View job status
enterprise-job-orchestrator job status <job_id>

# Check logs
tail -f /opt/idxr/logs/orchestrator.log
```

---

## Next Steps

1. Review [Performance Optimization Guide](./04-performance-optimization.md)
2. Check [Monitoring & Operations Guide](./05-monitoring-operations.md)
3. See [Troubleshooting Guide](./06-troubleshooting-guide.md)

---

*Document Version: 1.0*
*Last Updated: 2024*