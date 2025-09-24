"""
MonitoringService for Enterprise Job Orchestrator

Provides comprehensive monitoring, metrics collection, alerting,
and performance tracking capabilities.
"""

import asyncio
import json
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from decimal import Decimal
from enum import Enum

from ..models.job import Job, JobStatus, JobMetrics, JobAlert
from ..models.execution import JobExecution, ExecutionSummary
from ..models.worker import WorkerNode, WorkerStatus
from ..utils.database import DatabaseManager
from ..utils.logger import get_logger, set_log_context, LoggerContext
from ..core.exceptions import MonitoringError, DatabaseError


class AlertSeverity(Enum):
    """Alert severity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class MetricType(Enum):
    """Types of metrics collected."""
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMER = "timer"


@dataclass
class Alert:
    """Alert data structure."""
    alert_id: str
    alert_type: str
    severity: AlertSeverity
    message: str
    details: Dict[str, Any] = field(default_factory=dict)
    triggered_at: datetime = field(default_factory=datetime.utcnow)
    resolved_at: Optional[datetime] = None
    is_resolved: bool = False

    def resolve(self):
        """Mark alert as resolved."""
        self.is_resolved = True
        self.resolved_at = datetime.utcnow()


@dataclass
class Metric:
    """Metric data structure."""
    name: str
    metric_type: MetricType
    value: float
    tags: Dict[str, str] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.utcnow)


@dataclass
class SystemHealth:
    """Overall system health status."""
    overall_status: str
    total_jobs: int
    running_jobs: int
    failed_jobs: int
    total_workers: int
    healthy_workers: int
    average_cpu_usage: float
    average_memory_usage: float
    queue_size: int
    processing_rate: float
    error_rate: float
    uptime_seconds: float
    timestamp: datetime = field(default_factory=datetime.utcnow)


class MonitoringService:
    """
    Comprehensive monitoring service for the job orchestrator.

    Provides capabilities for:
    - Real-time metrics collection and aggregation
    - Performance monitoring and alerting
    - System health checks and reporting
    - Resource utilization tracking
    - Custom alert rules and notifications
    - Historical data analysis
    """

    def __init__(self,
                 database_manager: DatabaseManager,
                 collection_interval: int = 30,
                 retention_days: int = 30,
                 alert_cooldown: int = 300):
        """
        Initialize MonitoringService.

        Args:
            database_manager: Database connection manager
            collection_interval: Seconds between metric collection
            retention_days: Days to retain historical data
            alert_cooldown: Seconds between repeated alerts
        """
        self.db = database_manager
        self.collection_interval = collection_interval
        self.retention_days = retention_days
        self.alert_cooldown = alert_cooldown

        # Monitoring state
        self.metrics: Dict[str, List[Metric]] = {}
        self.active_alerts: Dict[str, Alert] = {}
        self.alert_rules: List[Callable] = []
        self.alert_handlers: List[Callable] = []

        # Performance tracking
        self.performance_baselines: Dict[str, float] = {
            "max_job_queue_size": 1000,
            "max_worker_cpu_usage": 80.0,
            "max_worker_memory_usage": 85.0,
            "min_processing_rate": 100.0,
            "max_error_rate": 5.0,
            "max_job_duration_hours": 24.0
        }

        # Tasks
        self._collection_task: Optional[asyncio.Task] = None
        self._cleanup_task: Optional[asyncio.Task] = None
        self._shutdown_event = asyncio.Event()

        # System start time
        self.start_time = datetime.utcnow()

        # Logger
        self.logger = get_logger(__name__)
        set_log_context(self.logger, component="monitoring_service")

    async def start(self):
        """Start the monitoring service."""
        self.logger.info("Starting MonitoringService")

        try:
            # Load existing alerts from database
            await self._load_active_alerts()

            # Register default alert rules
            await self._register_default_alert_rules()

            # Start collection and cleanup tasks
            self._collection_task = asyncio.create_task(self._metrics_collection_loop())
            self._cleanup_task = asyncio.create_task(self._cleanup_loop())

            self.logger.info("MonitoringService started successfully")

        except Exception as e:
            self.logger.error("Failed to start MonitoringService", exc_info=True)
            raise MonitoringError(f"Failed to start MonitoringService: {str(e)}")

    async def stop(self):
        """Stop the monitoring service."""
        self.logger.info("Stopping MonitoringService")

        # Signal shutdown
        self._shutdown_event.set()

        # Cancel tasks
        if self._collection_task:
            self._collection_task.cancel()
            try:
                await self._collection_task
            except asyncio.CancelledError:
                pass

        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

        self.logger.info("MonitoringService stopped")

    async def record_metric(self, name: str, value: float, metric_type: MetricType = MetricType.GAUGE, tags: Optional[Dict[str, str]] = None):
        """
        Record a metric value.

        Args:
            name: Metric name
            value: Metric value
            metric_type: Type of metric
            tags: Optional tags for metric
        """
        metric = Metric(
            name=name,
            metric_type=metric_type,
            value=value,
            tags=tags or {}
        )

        if name not in self.metrics:
            self.metrics[name] = []

        self.metrics[name].append(metric)

        # Store in database
        try:
            await self.db.store_metric(metric)
        except Exception as e:
            self.logger.error("Failed to store metric", extra={
                "metric_name": name,
                "error": str(e)
            })

    async def get_system_health(self) -> SystemHealth:
        """
        Get current system health status.

        Returns:
            SystemHealth object with current status
        """
        try:
            # Get job statistics
            job_stats = await self.db.get_job_statistics()
            total_jobs = job_stats.get("total", 0)
            running_jobs = job_stats.get("running", 0)
            failed_jobs = job_stats.get("failed", 0)

            # Get worker statistics
            worker_stats = await self.db.get_worker_statistics()
            total_workers = worker_stats.get("total", 0)
            healthy_workers = worker_stats.get("healthy", 0)

            # Calculate averages
            avg_cpu = await self._get_average_metric("worker_cpu_usage", 300)  # Last 5 minutes
            avg_memory = await self._get_average_metric("worker_memory_usage", 300)

            # Get queue size
            queue_size = await self.db.get_pending_jobs_count()

            # Calculate processing rate (jobs/hour)
            processing_rate = await self._calculate_processing_rate()

            # Calculate error rate
            error_rate = await self._calculate_error_rate()

            # System uptime
            uptime = (datetime.utcnow() - self.start_time).total_seconds()

            # Determine overall status
            overall_status = self._determine_overall_status(
                queue_size, avg_cpu, avg_memory, error_rate, healthy_workers, total_workers
            )

            return SystemHealth(
                overall_status=overall_status,
                total_jobs=total_jobs,
                running_jobs=running_jobs,
                failed_jobs=failed_jobs,
                total_workers=total_workers,
                healthy_workers=healthy_workers,
                average_cpu_usage=avg_cpu,
                average_memory_usage=avg_memory,
                queue_size=queue_size,
                processing_rate=processing_rate,
                error_rate=error_rate,
                uptime_seconds=uptime
            )

        except Exception as e:
            self.logger.error("Failed to get system health", exc_info=True)
            raise MonitoringError(f"Failed to get system health: {str(e)}")

    async def trigger_alert(self, alert_type: str, severity: AlertSeverity, message: str, details: Optional[Dict[str, Any]] = None):
        """
        Trigger a new alert.

        Args:
            alert_type: Type of alert
            severity: Alert severity
            message: Alert message
            details: Additional alert details
        """
        alert_id = f"{alert_type}_{int(datetime.utcnow().timestamp())}"

        # Check for existing alert of same type
        existing_alert = None
        for alert in self.active_alerts.values():
            if alert.alert_type == alert_type and not alert.is_resolved:
                existing_alert = alert
                break

        if existing_alert:
            # Check cooldown period
            time_since_last = (datetime.utcnow() - existing_alert.triggered_at).total_seconds()
            if time_since_last < self.alert_cooldown:
                return  # Skip duplicate alert during cooldown

        # Create new alert
        alert = Alert(
            alert_id=alert_id,
            alert_type=alert_type,
            severity=severity,
            message=message,
            details=details or {}
        )

        self.active_alerts[alert_id] = alert

        # Store in database
        try:
            await self.db.store_alert(alert)
        except Exception as e:
            self.logger.error("Failed to store alert", extra={
                "alert_id": alert_id,
                "error": str(e)
            })

        # Log alert
        self.logger.warning("Alert triggered", extra={
            "alert_id": alert_id,
            "alert_type": alert_type,
            "severity": severity.value,
            "message": message
        })

        # Notify alert handlers
        for handler in self.alert_handlers:
            try:
                await handler(alert)
            except Exception as e:
                self.logger.error("Alert handler failed", extra={
                    "handler": str(handler),
                    "error": str(e)
                })

    async def resolve_alert(self, alert_id: str):
        """
        Resolve an active alert.

        Args:
            alert_id: ID of alert to resolve
        """
        alert = self.active_alerts.get(alert_id)
        if alert and not alert.is_resolved:
            alert.resolve()

            # Update in database
            try:
                await self.db.update_alert(alert)
            except Exception as e:
                self.logger.error("Failed to update resolved alert", extra={
                    "alert_id": alert_id,
                    "error": str(e)
                })

            self.logger.info("Alert resolved", extra={
                "alert_id": alert_id,
                "alert_type": alert.alert_type
            })

    async def get_performance_report(self, hours: int = 24) -> Dict[str, Any]:
        """
        Get performance report for specified time period.

        Args:
            hours: Number of hours to include in report

        Returns:
            Performance report dictionary
        """
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=hours)

            # Get job performance metrics
            job_metrics = await self.db.get_job_metrics_summary(start_time, end_time)

            # Get worker performance metrics
            worker_metrics = await self.db.get_worker_metrics_summary(start_time, end_time)

            # Get system metrics
            system_metrics = await self._get_system_metrics_summary(start_time, end_time)

            # Get alert summary
            alert_summary = await self._get_alert_summary(start_time, end_time)

            return {
                "report_period": {
                    "start_time": start_time.isoformat(),
                    "end_time": end_time.isoformat(),
                    "duration_hours": hours
                },
                "job_performance": job_metrics,
                "worker_performance": worker_metrics,
                "system_metrics": system_metrics,
                "alerts": alert_summary,
                "generated_at": datetime.utcnow().isoformat()
            }

        except Exception as e:
            self.logger.error("Failed to generate performance report", exc_info=True)
            raise MonitoringError(f"Failed to generate performance report: {str(e)}")

    async def add_alert_rule(self, rule_func: Callable):
        """Add a custom alert rule function."""
        self.alert_rules.append(rule_func)

    async def add_alert_handler(self, handler_func: Callable):
        """Add a custom alert handler function."""
        self.alert_handlers.append(handler_func)

    async def _metrics_collection_loop(self):
        """Main metrics collection loop."""
        self.logger.info("Starting metrics collection loop")

        while not self._shutdown_event.is_set():
            try:
                await self._collect_system_metrics()
                await self._check_alert_rules()
                await asyncio.sleep(self.collection_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error("Error in metrics collection loop", exc_info=True)
                await asyncio.sleep(self.collection_interval)

    async def _cleanup_loop(self):
        """Cleanup old metrics and alerts."""
        cleanup_interval = 3600  # Run every hour

        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(cleanup_interval)
                await self._cleanup_old_data()

            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error("Error in cleanup loop", exc_info=True)

    async def _collect_system_metrics(self):
        """Collect system-wide metrics."""
        try:
            # Job queue metrics
            queue_size = await self.db.get_pending_jobs_count()
            await self.record_metric("job_queue_size", queue_size, MetricType.GAUGE)

            # Worker metrics
            workers = await self.db.get_all_workers()
            total_workers = len(workers)
            healthy_workers = sum(1 for w in workers if w.is_healthy())

            await self.record_metric("total_workers", total_workers, MetricType.GAUGE)
            await self.record_metric("healthy_workers", healthy_workers, MetricType.GAUGE)

            # Collect individual worker metrics
            for worker in workers:
                tags = {"worker_id": worker.worker_id}

                if worker.cpu_usage_percent:
                    await self.record_metric("worker_cpu_usage", float(worker.cpu_usage_percent), MetricType.GAUGE, tags)

                if worker.memory_usage_percent:
                    await self.record_metric("worker_memory_usage", float(worker.memory_usage_percent), MetricType.GAUGE, tags)

                await self.record_metric("worker_job_count", worker.current_jobs, MetricType.GAUGE, tags)

            # Processing rate
            processing_rate = await self._calculate_processing_rate()
            await self.record_metric("processing_rate", processing_rate, MetricType.GAUGE)

            # Error rate
            error_rate = await self._calculate_error_rate()
            await self.record_metric("error_rate", error_rate, MetricType.GAUGE)

        except Exception as e:
            self.logger.error("Failed to collect system metrics", exc_info=True)

    async def _check_alert_rules(self):
        """Check all alert rules."""
        for rule in self.alert_rules:
            try:
                await rule()
            except Exception as e:
                self.logger.error("Alert rule check failed", extra={
                    "rule": str(rule),
                    "error": str(e)
                })

    async def _register_default_alert_rules(self):
        """Register default alert rules."""

        async def high_queue_size_rule():
            queue_size = await self.db.get_pending_jobs_count()
            if queue_size > self.performance_baselines["max_job_queue_size"]:
                await self.trigger_alert(
                    "high_queue_size",
                    AlertSeverity.HIGH,
                    f"Job queue size is high: {queue_size}",
                    {"queue_size": queue_size}
                )

        async def high_worker_cpu_rule():
            avg_cpu = await self._get_average_metric("worker_cpu_usage", 300)
            if avg_cpu > self.performance_baselines["max_worker_cpu_usage"]:
                await self.trigger_alert(
                    "high_worker_cpu",
                    AlertSeverity.MEDIUM,
                    f"Average worker CPU usage is high: {avg_cpu:.1f}%",
                    {"cpu_usage": avg_cpu}
                )

        async def high_error_rate_rule():
            error_rate = await self._calculate_error_rate()
            if error_rate > self.performance_baselines["max_error_rate"]:
                await self.trigger_alert(
                    "high_error_rate",
                    AlertSeverity.HIGH,
                    f"Error rate is high: {error_rate:.1f}%",
                    {"error_rate": error_rate}
                )

        self.alert_rules.extend([
            high_queue_size_rule,
            high_worker_cpu_rule,
            high_error_rate_rule
        ])

    async def _get_average_metric(self, metric_name: str, seconds: int) -> float:
        """Get average metric value over specified time period."""
        try:
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(seconds=seconds)

            metrics = await self.db.get_metrics(metric_name, start_time, end_time)
            if metrics:
                return sum(m.value for m in metrics) / len(metrics)
            return 0.0

        except Exception:
            return 0.0

    async def _calculate_processing_rate(self) -> float:
        """Calculate jobs processed per hour."""
        try:
            # Get jobs completed in last hour
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=1)

            completed_jobs = await self.db.get_completed_jobs_count(start_time, end_time)
            return float(completed_jobs)

        except Exception:
            return 0.0

    async def _calculate_error_rate(self) -> float:
        """Calculate error rate percentage."""
        try:
            # Get error rate over last hour
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=1)

            total_jobs = await self.db.get_jobs_count(start_time, end_time)
            failed_jobs = await self.db.get_failed_jobs_count(start_time, end_time)

            if total_jobs > 0:
                return (failed_jobs / total_jobs) * 100
            return 0.0

        except Exception:
            return 0.0

    def _determine_overall_status(self, queue_size: int, cpu_usage: float, memory_usage: float,
                                error_rate: float, healthy_workers: int, total_workers: int) -> str:
        """Determine overall system health status."""

        # Critical conditions
        if healthy_workers == 0 or (total_workers > 0 and healthy_workers / total_workers < 0.5):
            return "critical"

        if error_rate > 10.0:
            return "critical"

        # Degraded conditions
        if queue_size > self.performance_baselines["max_job_queue_size"]:
            return "degraded"

        if cpu_usage > self.performance_baselines["max_worker_cpu_usage"]:
            return "degraded"

        if memory_usage > self.performance_baselines["max_worker_memory_usage"]:
            return "degraded"

        # Warning conditions
        if error_rate > 5.0:
            return "warning"

        if cpu_usage > 70.0 or memory_usage > 75.0:
            return "warning"

        return "healthy"

    async def _load_active_alerts(self):
        """Load active alerts from database."""
        try:
            alerts = await self.db.get_active_alerts()
            for alert in alerts:
                self.active_alerts[alert.alert_id] = alert
        except Exception as e:
            self.logger.error("Failed to load active alerts", exc_info=True)

    async def _get_system_metrics_summary(self, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """Get system metrics summary for time period."""
        # Implementation would query database for system metrics
        return {
            "cpu_usage": {"avg": 0.0, "max": 0.0, "min": 0.0},
            "memory_usage": {"avg": 0.0, "max": 0.0, "min": 0.0},
            "queue_size": {"avg": 0.0, "max": 0.0, "min": 0.0}
        }

    async def _get_alert_summary(self, start_time: datetime, end_time: datetime) -> Dict[str, Any]:
        """Get alert summary for time period."""
        try:
            alerts = await self.db.get_alerts_in_period(start_time, end_time)

            by_severity = {}
            by_type = {}

            for alert in alerts:
                # Count by severity
                severity = alert.severity.value
                by_severity[severity] = by_severity.get(severity, 0) + 1

                # Count by type
                alert_type = alert.alert_type
                by_type[alert_type] = by_type.get(alert_type, 0) + 1

            return {
                "total_alerts": len(alerts),
                "by_severity": by_severity,
                "by_type": by_type
            }

        except Exception:
            return {"total_alerts": 0, "by_severity": {}, "by_type": {}}

    async def _cleanup_old_data(self):
        """Clean up old metrics and resolved alerts."""
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=self.retention_days)

            # Clean up old metrics
            await self.db.cleanup_old_metrics(cutoff_date)

            # Clean up resolved alerts older than retention period
            await self.db.cleanup_old_alerts(cutoff_date)

            self.logger.info("Cleanup completed", extra={
                "cutoff_date": cutoff_date.isoformat()
            })

        except Exception as e:
            self.logger.error("Failed to cleanup old data", exc_info=True)