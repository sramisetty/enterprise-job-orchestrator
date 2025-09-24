# Monitoring & Operations Guide

## Overview

This guide provides comprehensive monitoring and operational procedures for managing the Enterprise Job Orchestrator processing 70 million identity records in production.

---

## Table of Contents

1. [Monitoring Infrastructure](#monitoring-infrastructure)
2. [Key Metrics & KPIs](#key-metrics--kpis)
3. [Alerting & Notifications](#alerting--notifications)
4. [Operational Procedures](#operational-procedures)
5. [Maintenance Tasks](#maintenance-tasks)
6. [Incident Response](#incident-response)
7. [Runbook](#runbook)

---

## Monitoring Infrastructure

### Monitoring Stack

```yaml
# docker-compose-monitoring.yml
version: '3.8'

services:
  prometheus:
    image: prom/prometheus:latest
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml
      - prometheus_data:/prometheus
    ports:
      - "9090:9090"
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.retention.time=30d'

  grafana:
    image: grafana/grafana:latest
    volumes:
      - grafana_data:/var/lib/grafana
      - ./grafana/dashboards:/etc/grafana/provisioning/dashboards
    ports:
      - "3000:3000"
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=admin

  alertmanager:
    image: prom/alertmanager:latest
    volumes:
      - ./alertmanager.yml:/etc/alertmanager/alertmanager.yml
    ports:
      - "9093:9093"

  node_exporter:
    image: prom/node-exporter:latest
    ports:
      - "9100:9100"
    volumes:
      - /proc:/host/proc:ro
      - /sys:/host/sys:ro
      - /:/rootfs:ro

  postgres_exporter:
    image: prometheuscommunity/postgres-exporter:latest
    environment:
      DATA_SOURCE_NAME: "postgresql://user:pass@postgres:5432/idxr?sslmode=disable"
    ports:
      - "9187:9187"

  redis_exporter:
    image: oliver006/redis_exporter:latest
    environment:
      REDIS_ADDR: "redis://redis:6379"
    ports:
      - "9121:9121"

volumes:
  prometheus_data:
  grafana_data:
```

### Prometheus Configuration

```yaml
# prometheus.yml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

alerting:
  alertmanagers:
    - static_configs:
        - targets: ['alertmanager:9093']

rule_files:
  - "alerts.yml"

scrape_configs:
  - job_name: 'orchestrator'
    static_configs:
      - targets: ['orchestrator:9090']
    metrics_path: '/metrics'

  - job_name: 'workers'
    static_configs:
      - targets: ['worker-1:9090', 'worker-2:9090', 'worker-3:9090']

  - job_name: 'node'
    static_configs:
      - targets: ['node_exporter:9100']

  - job_name: 'postgres'
    static_configs:
      - targets: ['postgres_exporter:9187']

  - job_name: 'redis'
    static_configs:
      - targets: ['redis_exporter:9121']
```

### Custom Metrics Exporter

```python
from prometheus_client import Counter, Gauge, Histogram, Summary
from prometheus_client import start_http_server
import time

class OrchestratorMetrics:
    """Custom metrics for job orchestrator"""

    def __init__(self):
        # Job metrics
        self.jobs_submitted = Counter(
            'orchestrator_jobs_submitted_total',
            'Total number of jobs submitted',
            ['job_type', 'priority']
        )
        self.jobs_completed = Counter(
            'orchestrator_jobs_completed_total',
            'Total number of jobs completed',
            ['job_type', 'status']
        )
        self.job_duration = Histogram(
            'orchestrator_job_duration_seconds',
            'Job processing duration',
            ['job_type'],
            buckets=[60, 300, 900, 1800, 3600, 7200]
        )

        # Queue metrics
        self.queue_depth = Gauge(
            'orchestrator_queue_depth',
            'Current queue depth',
            ['priority']
        )
        self.queue_wait_time = Summary(
            'orchestrator_queue_wait_seconds',
            'Time spent waiting in queue'
        )

        # Worker metrics
        self.workers_active = Gauge(
            'orchestrator_workers_active',
            'Number of active workers',
            ['pool']
        )
        self.worker_utilization = Gauge(
            'orchestrator_worker_utilization_percent',
            'Worker CPU utilization',
            ['worker_id']
        )

        # Identity matching metrics
        self.records_processed = Counter(
            'identity_records_processed_total',
            'Total records processed'
        )
        self.matches_found = Counter(
            'identity_matches_found_total',
            'Total matches found',
            ['confidence_level']
        )
        self.processing_rate = Gauge(
            'identity_processing_rate_per_second',
            'Current processing rate'
        )

        # Error metrics
        self.errors_total = Counter(
            'orchestrator_errors_total',
            'Total errors',
            ['error_type', 'component']
        )

    def update_metrics(self, stats):
        """Update metrics from orchestrator stats"""

        # Update job metrics
        self.jobs_submitted.labels(
            job_type='identity_matching',
            priority='high'
        ).inc(stats.get('jobs_submitted', 0))

        # Update queue metrics
        self.queue_depth.labels(priority='high').set(
            stats.get('queue_depth_high', 0)
        )

        # Update worker metrics
        self.workers_active.labels(pool='matching').set(
            stats.get('active_workers', 0)
        )

        # Update processing metrics
        self.processing_rate.set(stats.get('records_per_second', 0))

    def start_metrics_server(self, port=9090):
        """Start Prometheus metrics server"""
        start_http_server(port)
        print(f"Metrics server started on port {port}")
```

---

## Key Metrics & KPIs

### Critical Metrics

| Metric | Target | Alert Threshold | Description |
|--------|--------|-----------------|-------------|
| **Processing Rate** | 3,000-5,000 rec/sec | < 2,000 rec/sec | Records processed per second |
| **Job Success Rate** | > 99% | < 95% | Percentage of successful jobs |
| **Queue Depth** | < 5,000 | > 10,000 | Number of pending jobs |
| **Worker Utilization** | 70-80% | > 90% or < 30% | Average worker CPU usage |
| **Error Rate** | < 0.1% | > 1% | Percentage of failed operations |
| **Response Time** | < 100ms | > 500ms | API response time |
| **Database Latency** | < 10ms | > 50ms | Query execution time |

### Performance KPIs

```python
class PerformanceKPIs:
    """Calculate and track performance KPIs"""

    def __init__(self):
        self.baseline_metrics = {
            'target_processing_time': 6 * 3600,  # 6 hours
            'target_records': 70_000_000,
            'target_accuracy': 0.99,
            'target_availability': 0.999
        }

    def calculate_efficiency(self, actual_time, actual_records):
        """Calculate processing efficiency"""
        expected_rate = self.baseline_metrics['target_records'] / \
                       self.baseline_metrics['target_processing_time']
        actual_rate = actual_records / actual_time

        return {
            'efficiency_percent': (actual_rate / expected_rate) * 100,
            'records_per_second': actual_rate,
            'time_vs_target': actual_time / self.baseline_metrics['target_processing_time']
        }

    def calculate_quality_metrics(self, matches_found, false_positives, false_negatives):
        """Calculate match quality metrics"""
        total_comparisons = matches_found + false_positives + false_negatives

        precision = matches_found / (matches_found + false_positives) if matches_found else 0
        recall = matches_found / (matches_found + false_negatives) if matches_found else 0
        f1_score = 2 * (precision * recall) / (precision + recall) if (precision + recall) else 0

        return {
            'precision': precision,
            'recall': recall,
            'f1_score': f1_score,
            'accuracy': (matches_found / total_comparisons) if total_comparisons else 0
        }

    def calculate_availability(self, uptime_seconds, total_seconds):
        """Calculate system availability"""
        return {
            'availability_percent': (uptime_seconds / total_seconds) * 100,
            'downtime_minutes': (total_seconds - uptime_seconds) / 60,
            'meets_sla': (uptime_seconds / total_seconds) >= self.baseline_metrics['target_availability']
        }
```

### Grafana Dashboards

```json
{
  "dashboard": {
    "title": "Identity Processing - 70M Records",
    "panels": [
      {
        "title": "Processing Progress",
        "type": "graph",
        "targets": [
          {
            "expr": "sum(identity_records_processed_total)",
            "legendFormat": "Records Processed"
          }
        ],
        "gridPos": {"x": 0, "y": 0, "w": 12, "h": 8}
      },
      {
        "title": "Processing Rate",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(identity_records_processed_total[5m])",
            "legendFormat": "Records/sec"
          }
        ],
        "gridPos": {"x": 12, "y": 0, "w": 12, "h": 8}
      },
      {
        "title": "Queue Status",
        "type": "graph",
        "targets": [
          {
            "expr": "orchestrator_queue_depth",
            "legendFormat": "Queue Depth - {{priority}}"
          }
        ],
        "gridPos": {"x": 0, "y": 8, "w": 12, "h": 8}
      },
      {
        "title": "Worker Status",
        "type": "graph",
        "targets": [
          {
            "expr": "orchestrator_workers_active",
            "legendFormat": "Active Workers - {{pool}}"
          }
        ],
        "gridPos": {"x": 12, "y": 8, "w": 12, "h": 8}
      }
    ]
  }
}
```

---

## Alerting & Notifications

### Alert Rules

```yaml
# alerts.yml
groups:
  - name: orchestrator_alerts
    interval: 30s
    rules:
      - alert: HighErrorRate
        expr: rate(orchestrator_errors_total[5m]) > 0.01
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "High error rate detected"
          description: "Error rate is {{ $value }} errors/sec"

      - alert: LowProcessingRate
        expr: identity_processing_rate_per_second < 2000
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "Processing rate below target"
          description: "Current rate: {{ $value }} records/sec"

      - alert: QueueBacklog
        expr: orchestrator_queue_depth > 10000
        for: 15m
        labels:
          severity: warning
        annotations:
          summary: "Queue backlog detected"
          description: "Queue depth: {{ $value }} jobs"

      - alert: WorkerFailure
        expr: orchestrator_workers_active < 20
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Insufficient workers available"
          description: "Only {{ $value }} workers active"

      - alert: DatabaseLatency
        expr: pg_database_query_duration_seconds > 0.05
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "High database latency"
          description: "Query latency: {{ $value }} seconds"

      - alert: MemoryPressure
        expr: node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes < 0.1
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Low memory available"
          description: "Only {{ $value }}% memory available"
```

### Notification Configuration

```python
# notification_manager.py
import smtplib
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

class NotificationManager:
    """Manage alert notifications"""

    def __init__(self, config):
        self.config = config
        self.email_config = config['email']
        self.slack_config = config['slack']
        self.pagerduty_config = config['pagerduty']

    async def send_alert(self, alert):
        """Send alert through configured channels"""

        severity = alert['labels']['severity']

        if severity == 'critical':
            # Send to all channels
            await self.send_email(alert)
            await self.send_slack(alert)
            await self.send_pagerduty(alert)
        elif severity == 'warning':
            # Send to Slack only
            await self.send_slack(alert)
        else:
            # Log only
            self.log_alert(alert)

    async def send_email(self, alert):
        """Send email alert"""
        msg = MIMEMultipart()
        msg['From'] = self.email_config['from']
        msg['To'] = ', '.join(self.email_config['to'])
        msg['Subject'] = f"[{alert['labels']['severity'].upper()}] {alert['annotations']['summary']}"

        body = f"""
        Alert: {alert['annotations']['summary']}
        Description: {alert['annotations']['description']}
        Severity: {alert['labels']['severity']}
        Time: {alert['startsAt']}

        View in Grafana: {self.config['grafana_url']}/dashboard/identity-processing
        """

        msg.attach(MIMEText(body, 'plain'))

        with smtplib.SMTP(self.email_config['smtp_server'], 587) as server:
            server.starttls()
            server.login(self.email_config['username'], self.email_config['password'])
            server.send_message(msg)

    async def send_slack(self, alert):
        """Send Slack notification"""
        color = {
            'critical': 'danger',
            'warning': 'warning',
            'info': 'good'
        }.get(alert['labels']['severity'], 'warning')

        payload = {
            'attachments': [{
                'color': color,
                'title': alert['annotations']['summary'],
                'text': alert['annotations']['description'],
                'fields': [
                    {'title': 'Severity', 'value': alert['labels']['severity'], 'short': True},
                    {'title': 'Component', 'value': alert['labels'].get('component', 'Unknown'), 'short': True}
                ],
                'footer': 'Job Orchestrator',
                'ts': int(time.time())
            }]
        }

        requests.post(self.slack_config['webhook_url'], json=payload)

    async def send_pagerduty(self, alert):
        """Send PagerDuty alert"""
        payload = {
            'routing_key': self.pagerduty_config['integration_key'],
            'event_action': 'trigger',
            'payload': {
                'summary': alert['annotations']['summary'],
                'severity': alert['labels']['severity'],
                'source': 'job-orchestrator',
                'custom_details': alert
            }
        }

        requests.post('https://events.pagerduty.com/v2/enqueue', json=payload)
```

---

## Operational Procedures

### Daily Operations

```python
# daily_operations.py

async def daily_health_check():
    """Perform daily health check"""

    checks = {
        'database_connectivity': False,
        'redis_connectivity': False,
        'worker_availability': False,
        'queue_status': False,
        'disk_space': False,
        'backup_status': False
    }

    # Check database
    try:
        await db_pool.fetchval("SELECT 1")
        checks['database_connectivity'] = True
    except Exception as e:
        print(f"Database check failed: {e}")

    # Check Redis
    try:
        await redis_client.ping()
        checks['redis_connectivity'] = True
    except Exception as e:
        print(f"Redis check failed: {e}")

    # Check workers
    workers = await orchestrator.get_workers()
    checks['worker_availability'] = len(workers) >= 20

    # Check queue
    queue_stats = await orchestrator.get_queue_statistics()
    checks['queue_status'] = queue_stats['queue_depth'] < 10000

    # Check disk space
    disk_usage = psutil.disk_usage('/')
    checks['disk_space'] = disk_usage.percent < 80

    # Generate report
    all_passed = all(checks.values())
    print(f"Daily Health Check: {'PASSED' if all_passed else 'FAILED'}")
    for check, status in checks.items():
        print(f"  {check}: {'✓' if status else '✗'}")

    return all_passed
```

### Job Management Procedures

```bash
#!/bin/bash
# job_management.sh

# Start processing 70M records
start_processing() {
    echo "Starting 70M record processing..."

    # Pre-flight checks
    check_prerequisites

    # Start orchestrator
    enterprise-job-orchestrator server start &

    # Wait for orchestrator to be ready
    wait_for_orchestrator

    # Register workers
    register_workers 50

    # Submit job
    JOB_ID=$(enterprise-job-orchestrator job submit \
        "70M Identity Matching" \
        --job-type identity_matching \
        --priority urgent \
        --input-file /data/identities_70m.csv \
        --config-file /config/matching.yaml)

    echo "Job submitted: $JOB_ID"
    monitor_job $JOB_ID
}

# Monitor job progress
monitor_job() {
    local job_id=$1

    while true; do
        status=$(enterprise-job-orchestrator job status $job_id --format json)
        progress=$(echo $status | jq -r .progress_percent)
        state=$(echo $status | jq -r .status)

        echo "Progress: ${progress}% - Status: $state"

        if [[ "$state" == "completed" ]] || [[ "$state" == "failed" ]]; then
            break
        fi

        sleep 30
    done
}

# Scale workers
scale_workers() {
    local target=$1
    current=$(enterprise-job-orchestrator worker list | wc -l)

    if [[ $current -lt $target ]]; then
        echo "Scaling up from $current to $target workers"
        for i in $(seq $current $target); do
            register_worker "worker_$i"
        done
    else
        echo "Already have $current workers"
    fi
}
```

---

## Maintenance Tasks

### Scheduled Maintenance

```python
# maintenance_scheduler.py
import schedule
import asyncio

class MaintenanceScheduler:
    """Schedule and execute maintenance tasks"""

    def __init__(self, orchestrator):
        self.orchestrator = orchestrator
        self.setup_schedule()

    def setup_schedule(self):
        """Setup maintenance schedule"""

        # Daily tasks
        schedule.every().day.at("02:00").do(self.vacuum_database)
        schedule.every().day.at("03:00").do(self.cleanup_old_jobs)
        schedule.every().day.at("04:00").do(self.backup_database)

        # Weekly tasks
        schedule.every().sunday.at("05:00").do(self.reindex_database)
        schedule.every().sunday.at("06:00").do(self.analyze_statistics)

        # Hourly tasks
        schedule.every().hour.do(self.rotate_logs)
        schedule.every().hour.do(self.clear_cache)

    async def vacuum_database(self):
        """Vacuum and analyze database"""
        print("Starting database vacuum...")

        await db_pool.execute("VACUUM ANALYZE jobs;")
        await db_pool.execute("VACUUM ANALYZE chunks;")
        await db_pool.execute("VACUUM ANALYZE identities;")
        await db_pool.execute("VACUUM ANALYZE matches;")

        print("Database vacuum completed")

    async def cleanup_old_jobs(self):
        """Clean up old completed jobs"""
        print("Cleaning up old jobs...")

        # Delete jobs older than 30 days
        result = await db_pool.execute("""
            DELETE FROM jobs
            WHERE completed_at < NOW() - INTERVAL '30 days'
            AND status IN ('completed', 'failed', 'cancelled')
        """)

        print(f"Deleted {result} old jobs")

    async def backup_database(self):
        """Backup database"""
        print("Starting database backup...")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = f"/backups/idxr_backup_{timestamp}.sql"

        os.system(f"pg_dump {DATABASE_URL} > {backup_file}")
        os.system(f"gzip {backup_file}")

        print(f"Database backed up to {backup_file}.gz")

    async def reindex_database(self):
        """Reindex database tables"""
        print("Reindexing database...")

        await db_pool.execute("REINDEX TABLE CONCURRENTLY jobs;")
        await db_pool.execute("REINDEX TABLE CONCURRENTLY chunks;")
        await db_pool.execute("REINDEX TABLE CONCURRENTLY identities;")

        print("Database reindexing completed")

    async def rotate_logs(self):
        """Rotate application logs"""
        # Handled by logging configuration
        pass

    async def clear_cache(self):
        """Clear expired cache entries"""
        # Clear Redis expired keys
        await redis_client.execute_command('MEMORY PURGE')

    def run_scheduler(self):
        """Run the scheduler"""
        while True:
            schedule.run_pending()
            time.sleep(60)
```

### Database Maintenance

```sql
-- maintenance_queries.sql

-- Update table statistics
ANALYZE jobs;
ANALYZE chunks;
ANALYZE identities;
ANALYZE matches;

-- Find and fix bloated tables
SELECT
    schemaname,
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size,
    n_live_tup,
    n_dead_tup,
    round(n_dead_tup::numeric / NULLIF(n_live_tup + n_dead_tup, 0), 4) AS dead_ratio
FROM pg_stat_user_tables
WHERE n_dead_tup > 1000
ORDER BY dead_ratio DESC;

-- Identify slow queries
SELECT
    query,
    calls,
    mean_exec_time,
    total_exec_time,
    rows
FROM pg_stat_statements
WHERE mean_exec_time > 100
ORDER BY mean_exec_time DESC
LIMIT 20;

-- Find missing indexes
SELECT
    schemaname,
    tablename,
    attname,
    n_distinct,
    correlation
FROM pg_stats
WHERE schemaname = 'public'
    AND n_distinct > 100
    AND correlation < 0.1
ORDER BY n_distinct DESC;

-- Check table sizes
SELECT
    nspname || '.' || relname AS relation,
    pg_size_pretty(pg_total_relation_size(C.oid)) AS total_size
FROM pg_class C
LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
WHERE nspname NOT IN ('pg_catalog', 'information_schema')
    AND C.relkind <> 'i'
    AND nspname !~ '^pg_toast'
ORDER BY pg_total_relation_size(C.oid) DESC
LIMIT 20;
```

---

## Incident Response

### Incident Levels

| Level | Response Time | Examples | Escalation |
|-------|--------------|----------|------------|
| **P1 - Critical** | 15 minutes | System down, data loss | Page on-call immediately |
| **P2 - High** | 1 hour | Performance degradation > 50% | Notify team lead |
| **P3 - Medium** | 4 hours | Single worker failure | Team notification |
| **P4 - Low** | Next business day | Minor issues | Log for review |

### Incident Response Playbook

```python
# incident_response.py

class IncidentResponse:
    """Incident response procedures"""

    async def respond_to_incident(self, incident_type, severity):
        """Main incident response flow"""

        incident_id = self.create_incident(incident_type, severity)

        # Execute response based on type
        if incident_type == 'system_down':
            await self.handle_system_down(incident_id)
        elif incident_type == 'performance_degradation':
            await self.handle_performance_issue(incident_id)
        elif incident_type == 'data_corruption':
            await self.handle_data_corruption(incident_id)
        elif incident_type == 'worker_failure':
            await self.handle_worker_failure(incident_id)

        # Post-incident
        await self.create_incident_report(incident_id)

    async def handle_system_down(self, incident_id):
        """Handle complete system failure"""

        steps = [
            "1. Check orchestrator health",
            "2. Verify database connectivity",
            "3. Check worker status",
            "4. Review error logs",
            "5. Restart failed components",
            "6. Verify job queue integrity",
            "7. Resume processing"
        ]

        for step in steps:
            print(f"Executing: {step}")
            # Execute step
            await self.execute_recovery_step(step)

    async def handle_performance_issue(self, incident_id):
        """Handle performance degradation"""

        # Diagnose issue
        metrics = await self.collect_performance_metrics()

        if metrics['processing_rate'] < 1000:
            # Critical slowdown
            await self.scale_workers(additional=20)
            await self.optimize_queries()
            await self.clear_caches()

        elif metrics['queue_depth'] > 20000:
            # Queue backup
            await self.scale_workers(additional=30)
            await self.increase_chunk_parallelism()

    async def handle_data_corruption(self, incident_id):
        """Handle data corruption incident"""

        # Stop processing immediately
        await self.orchestrator.pause_queue()

        # Identify affected records
        affected = await self.identify_corrupted_data()

        # Restore from backup
        await self.restore_from_backup(affected)

        # Resume processing
        await self.orchestrator.resume_queue()
```

---

## Runbook

### Common Scenarios

#### Scenario 1: Processing Stuck at 50%

**Symptoms:**
- Progress stops at ~35M records
- Queue depth increasing
- Workers showing as busy but no progress

**Resolution:**
```bash
# 1. Check for deadlocks
psql -d idxr_production -c "SELECT * FROM pg_stat_activity WHERE wait_event_type = 'Lock';"

# 2. Kill blocking queries
psql -d idxr_production -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE state = 'idle in transaction' AND age(now(), state_change) > interval '10 minutes';"

# 3. Restart stuck workers
enterprise-job-orchestrator worker restart --pool matching

# 4. Resume processing
enterprise-job-orchestrator queue resume
```

#### Scenario 2: High Error Rate

**Symptoms:**
- Error rate > 1%
- Jobs failing repeatedly
- Workers reporting errors

**Resolution:**
```python
# investigate_errors.py
async def investigate_high_error_rate():
    # Get recent errors
    errors = await db_pool.fetch("""
        SELECT error_type, error_message, COUNT(*) as count
        FROM job_errors
        WHERE created_at > NOW() - INTERVAL '1 hour'
        GROUP BY error_type, error_message
        ORDER BY count DESC
        LIMIT 10
    """)

    for error in errors:
        print(f"Error: {error['error_type']} - Count: {error['count']}")

    # Common fixes
    if "connection timeout" in errors[0]['error_message']:
        # Increase connection pool
        await increase_connection_pool()

    elif "out of memory" in errors[0]['error_message']:
        # Reduce chunk size
        await reduce_chunk_size(new_size=1000)
```

#### Scenario 3: Database Performance Issues

**Symptoms:**
- Query latency > 100ms
- Database CPU > 90%
- Slow job assignment

**Resolution:**
```sql
-- Kill long-running queries
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE state != 'idle'
    AND query_start < NOW() - INTERVAL '5 minutes';

-- Update statistics
ANALYZE jobs;
ANALYZE chunks;

-- Check for missing indexes
EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM jobs
WHERE status = 'queued'
ORDER BY priority DESC, created_at ASC
LIMIT 1;

-- Add missing index if needed
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_jobs_queue_optimization
ON jobs(status, priority DESC, created_at ASC)
WHERE status = 'queued';
```

### Emergency Procedures

#### Complete System Recovery

```bash
#!/bin/bash
# emergency_recovery.sh

echo "Starting emergency recovery..."

# 1. Stop all services
systemctl stop idxr-orchestrator
systemctl stop idxr-workers

# 2. Backup current state
pg_dump idxr_production > /backup/emergency_$(date +%s).sql

# 3. Clear stuck jobs
psql idxr_production << EOF
UPDATE jobs SET status = 'failed'
WHERE status = 'running'
AND started_at < NOW() - INTERVAL '2 hours';
EOF

# 4. Clear cache
redis-cli FLUSHALL

# 5. Restart services
systemctl start postgresql
systemctl start redis
systemctl start idxr-orchestrator
systemctl start idxr-workers

# 6. Verify health
sleep 30
enterprise-job-orchestrator monitor health

echo "Recovery completed"
```

---

## Operational Metrics Dashboard

```python
# dashboard.py
async def display_operational_dashboard():
    """Real-time operational dashboard"""

    while True:
        os.system('clear')
        print("=" * 80)
        print("OPERATIONAL DASHBOARD - 70M Record Processing")
        print("=" * 80)

        # System Status
        health = await orchestrator.get_system_health()
        print(f"\nSystem Status: {health['overall_status'].upper()}")
        print(f"Uptime: {health['uptime_hours']:.1f} hours")

        # Processing Status
        stats = await get_processing_stats()
        print(f"\nProcessing Status:")
        print(f"  Total Records: 70,000,000")
        print(f"  Processed: {stats['processed']:,} ({stats['progress']:.1f}%)")
        print(f"  Remaining: {70_000_000 - stats['processed']:,}")
        print(f"  Rate: {stats['rate']:.0f} rec/sec")
        print(f"  ETA: {stats['eta_hours']:.1f} hours")

        # Resource Usage
        resources = await get_resource_usage()
        print(f"\nResource Usage:")
        print(f"  CPU: {resources['cpu_percent']:.1f}%")
        print(f"  Memory: {resources['memory_gb']:.1f} GB ({resources['memory_percent']:.1f}%)")
        print(f"  Disk I/O: {resources['disk_io_mbps']:.1f} MB/s")
        print(f"  Network: {resources['network_mbps']:.1f} Mbps")

        # Active Incidents
        incidents = await get_active_incidents()
        print(f"\nActive Incidents: {len(incidents)}")
        for incident in incidents[:3]:
            print(f"  - [{incident['severity']}] {incident['description']}")

        await asyncio.sleep(5)
```

---

## Next Steps

1. Review [Troubleshooting Guide](./06-troubleshooting-guide.md)
2. Check [Production Checklist](./03-implementation-deployment-guide.md#production-checklist)

---

*Document Version: 1.0*
*SLA Target: 99.9% availability*
*Support Contact: ops-team@company.com*