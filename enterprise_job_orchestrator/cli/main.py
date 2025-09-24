"""
Main CLI entry point for Enterprise Job Orchestrator

Provides command-line interface for job management, worker operations,
and system monitoring.
"""

import asyncio
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Dict, Any

import click
import asyncpg

from ..core.orchestrator import JobOrchestrator
from ..models.job import Job, JobType, JobPriority, ProcessingStrategy
from ..models.worker import WorkerNode
from ..utils.database import DatabaseManager
from ..utils.logger import setup_logger


# Global orchestrator instance
orchestrator: Optional[JobOrchestrator] = None


@click.group()
@click.option('--config', '-c', type=click.Path(exists=True), help='Configuration file path')
@click.option('--database-url', '-d', help='Database connection URL')
@click.option('--log-level', '-l', default='INFO', help='Log level')
@click.option('--verbose', '-v', is_flag=True, help='Verbose output')
@click.pass_context
def cli(ctx, config, database_url, log_level, verbose):
    """Enterprise Job Orchestrator CLI"""

    # Ensure context object exists
    ctx.ensure_object(dict)

    # Set up logging
    logger = setup_logger("orchestrator_cli", level=log_level, structured=not verbose)
    ctx.obj['logger'] = logger

    # Store configuration
    ctx.obj['config'] = config
    ctx.obj['database_url'] = database_url or "postgresql://localhost/job_orchestrator"
    ctx.obj['verbose'] = verbose


@cli.group()
@click.pass_context
def job(ctx):
    """Job management commands"""
    pass


@cli.group()
@click.pass_context
def worker(ctx):
    """Worker management commands"""
    pass


@cli.group()
@click.pass_context
def monitor(ctx):
    """Monitoring and status commands"""
    pass


@cli.group()
@click.pass_context
def server(ctx):
    """Server management commands"""
    pass


# Job Commands
@job.command('submit')
@click.argument('job_name')
@click.option('--job-type', type=click.Choice(['data_processing', 'machine_learning', 'batch_processing', 'custom']),
              default='data_processing', help='Type of job')
@click.option('--priority', type=click.Choice(['low', 'normal', 'high', 'urgent']),
              default='normal', help='Job priority')
@click.option('--strategy', type=click.Choice(['single_worker', 'parallel_chunks', 'map_reduce']),
              default='parallel_chunks', help='Processing strategy')
@click.option('--config-file', type=click.Path(exists=True), help='Job configuration file')
@click.option('--config-json', help='Job configuration as JSON string')
@click.option('--input-file', type=click.Path(exists=True), help='Input data file')
@click.option('--output-dir', type=click.Path(), help='Output directory')
@click.option('--chunk-size', type=int, default=10000, help='Chunk size for parallel processing')
@click.option('--max-retries', type=int, default=3, help='Maximum retry attempts')
@click.pass_context
def submit_job(ctx, job_name, job_type, priority, strategy, config_file, config_json,
               input_file, output_dir, chunk_size, max_retries):
    """Submit a new job for processing"""

    async def _submit():
        try:
            # Initialize orchestrator
            await _initialize_orchestrator(ctx)

            # Parse configuration
            config = {}
            if config_file:
                with open(config_file, 'r') as f:
                    config = json.load(f)
            elif config_json:
                config = json.loads(config_json)

            # Add CLI options to config
            if input_file:
                config['input_file'] = str(Path(input_file).absolute())
            if output_dir:
                config['output_directory'] = str(Path(output_dir).absolute())

            config.update({
                'chunk_size': chunk_size,
                'max_retries': max_retries
            })

            # Create job
            job = Job(
                job_id=f"cli_{int(datetime.utcnow().timestamp())}",
                job_name=job_name,
                job_type=JobType(job_type.upper()),
                priority=JobPriority(priority.upper()),
                processing_strategy=ProcessingStrategy(strategy.upper()),
                config=config
            )

            # Submit job
            job_id = await orchestrator.submit_job(job)

            click.echo(f"Job submitted successfully!")
            click.echo(f"Job ID: {job_id}")
            click.echo(f"Job Name: {job_name}")
            click.echo(f"Priority: {priority}")
            click.echo(f"Strategy: {strategy}")

            if ctx.obj['verbose']:
                click.echo(f"Configuration: {json.dumps(config, indent=2)}")

        except Exception as e:
            click.echo(f"Error submitting job: {str(e)}", err=True)
            sys.exit(1)
        finally:
            if orchestrator:
                await orchestrator.stop()

    asyncio.run(_submit())


@job.command('status')
@click.argument('job_id', required=False)
@click.option('--all', is_flag=True, help='Show all jobs')
@click.option('--running', is_flag=True, help='Show only running jobs')
@click.option('--failed', is_flag=True, help='Show only failed jobs')
@click.option('--limit', type=int, default=10, help='Limit number of jobs to show')
@click.pass_context
def job_status(ctx, job_id, all, running, failed, limit):
    """Get job status and details"""

    async def _status():
        try:
            await _initialize_orchestrator(ctx)

            if job_id:
                # Show specific job
                job_info = await orchestrator.get_job_status(job_id)
                if job_info:
                    _display_job_details(job_info, ctx.obj['verbose'])
                else:
                    click.echo(f"Job {job_id} not found", err=True)
                    sys.exit(1)
            else:
                # Show multiple jobs
                status_filter = None
                if running:
                    status_filter = 'running'
                elif failed:
                    status_filter = 'failed'

                jobs = await orchestrator.list_jobs(status_filter, limit)
                _display_jobs_table(jobs, ctx.obj['verbose'])

        except Exception as e:
            click.echo(f"Error getting job status: {str(e)}", err=True)
            sys.exit(1)
        finally:
            if orchestrator:
                await orchestrator.stop()

    asyncio.run(_status())


@job.command('cancel')
@click.argument('job_id')
@click.option('--force', is_flag=True, help='Force cancellation without graceful shutdown')
@click.pass_context
def cancel_job(ctx, job_id, force):
    """Cancel a running job"""

    async def _cancel():
        try:
            await _initialize_orchestrator(ctx)

            success = await orchestrator.cancel_job(job_id, force=force)

            if success:
                click.echo(f"Job {job_id} cancelled successfully")
            else:
                click.echo(f"Failed to cancel job {job_id}", err=True)
                sys.exit(1)

        except Exception as e:
            click.echo(f"Error cancelling job: {str(e)}", err=True)
            sys.exit(1)
        finally:
            if orchestrator:
                await orchestrator.stop()

    asyncio.run(_cancel())


# Worker Commands
@worker.command('list')
@click.option('--status', type=click.Choice(['online', 'offline', 'busy', 'error']), help='Filter by status')
@click.option('--pool', help='Filter by worker pool')
@click.pass_context
def list_workers(ctx, status, pool):
    """List worker nodes"""

    async def _list():
        try:
            await _initialize_orchestrator(ctx)

            workers = await orchestrator.get_workers(status_filter=status, pool_filter=pool)
            _display_workers_table(workers, ctx.obj['verbose'])

        except Exception as e:
            click.echo(f"Error listing workers: {str(e)}", err=True)
            sys.exit(1)
        finally:
            if orchestrator:
                await orchestrator.stop()

    asyncio.run(_list())


@worker.command('register')
@click.argument('worker_id')
@click.argument('node_name')
@click.option('--host', default='localhost', help='Worker host')
@click.option('--port', type=int, default=8080, help='Worker port')
@click.option('--capabilities', multiple=True, help='Worker capabilities')
@click.option('--max-jobs', type=int, default=1, help='Maximum concurrent jobs')
@click.option('--pool', default='default', help='Worker pool name')
@click.pass_context
def register_worker(ctx, worker_id, node_name, host, port, capabilities, max_jobs, pool):
    """Register a new worker node"""

    async def _register():
        try:
            await _initialize_orchestrator(ctx)

            worker = WorkerNode(
                worker_id=worker_id,
                node_name=node_name,
                host_name=host,
                port=port,
                capabilities=list(capabilities),
                max_concurrent_jobs=max_jobs,
                worker_metadata={'pool': pool}
            )

            success = await orchestrator.register_worker(worker)

            if success:
                click.echo(f"Worker {worker_id} registered successfully")
                click.echo(f"Node: {node_name}")
                click.echo(f"Host: {host}:{port}")
                click.echo(f"Pool: {pool}")
                click.echo(f"Capabilities: {list(capabilities)}")
            else:
                click.echo(f"Failed to register worker {worker_id}", err=True)
                sys.exit(1)

        except Exception as e:
            click.echo(f"Error registering worker: {str(e)}", err=True)
            sys.exit(1)
        finally:
            if orchestrator:
                await orchestrator.stop()

    asyncio.run(_register())


# Monitoring Commands
@monitor.command('health')
@click.pass_context
def system_health(ctx):
    """Show system health status"""

    async def _health():
        try:
            await _initialize_orchestrator(ctx)

            health = await orchestrator.get_system_health()
            _display_system_health(health, ctx.obj['verbose'])

        except Exception as e:
            click.echo(f"Error getting system health: {str(e)}", err=True)
            sys.exit(1)
        finally:
            if orchestrator:
                await orchestrator.stop()

    asyncio.run(_health())


@monitor.command('metrics')
@click.option('--hours', type=int, default=1, help='Hours of metrics to show')
@click.option('--export', type=click.Path(), help='Export metrics to file')
@click.pass_context
def show_metrics(ctx, hours, export):
    """Show system metrics"""

    async def _metrics():
        try:
            await _initialize_orchestrator(ctx)

            report = await orchestrator.get_performance_report(hours)

            if export:
                with open(export, 'w') as f:
                    json.dump(report, f, indent=2)
                click.echo(f"Metrics exported to {export}")
            else:
                _display_performance_report(report, ctx.obj['verbose'])

        except Exception as e:
            click.echo(f"Error getting metrics: {str(e)}", err=True)
            sys.exit(1)
        finally:
            if orchestrator:
                await orchestrator.stop()

    asyncio.run(_metrics())


# Server Commands
@server.command('start')
@click.option('--port', type=int, default=8000, help='HTTP server port')
@click.option('--host', default='0.0.0.0', help='HTTP server host')
@click.pass_context
def start_server(ctx, port, host):
    """Start the orchestrator HTTP server"""

    async def _start():
        try:
            await _initialize_orchestrator(ctx)

            click.echo(f"Starting HTTP server on {host}:{port}")

            # Start the HTTP API server
            await orchestrator.start_http_server(host, port)

            click.echo("Server started. Press Ctrl+C to stop.")

            # Keep running until interrupted
            try:
                while True:
                    await asyncio.sleep(1)
            except KeyboardInterrupt:
                click.echo("Shutting down server...")

        except Exception as e:
            click.echo(f"Error starting server: {str(e)}", err=True)
            sys.exit(1)
        finally:
            if orchestrator:
                await orchestrator.stop()

    asyncio.run(_start())


# Helper Functions
async def _initialize_orchestrator(ctx):
    """Initialize the global orchestrator instance"""
    global orchestrator

    if not orchestrator:
        database_url = ctx.obj['database_url']

        # Create database manager
        db_manager = DatabaseManager(database_url)
        await db_manager.initialize()

        # Create orchestrator
        orchestrator = JobOrchestrator(db_manager)
        await orchestrator.start()


def _display_job_details(job_info: Dict[str, Any], verbose: bool):
    """Display detailed job information"""
    click.echo(f"Job ID: {job_info['job_id']}")
    click.echo(f"Name: {job_info['job_name']}")
    click.echo(f"Status: {job_info['status']}")
    click.echo(f"Priority: {job_info['priority']}")
    click.echo(f"Type: {job_info['job_type']}")
    click.echo(f"Strategy: {job_info['processing_strategy']}")
    click.echo(f"Created: {job_info['created_at']}")

    if job_info.get('started_at'):
        click.echo(f"Started: {job_info['started_at']}")

    if job_info.get('completed_at'):
        click.echo(f"Completed: {job_info['completed_at']}")

    if job_info.get('progress_percent'):
        click.echo(f"Progress: {job_info['progress_percent']:.1f}%")

    if verbose and job_info.get('config'):
        click.echo(f"Configuration:")
        click.echo(json.dumps(job_info['config'], indent=2))


def _display_jobs_table(jobs: list, verbose: bool):
    """Display jobs in table format"""
    if not jobs:
        click.echo("No jobs found")
        return

    # Header
    if verbose:
        click.echo(f"{'Job ID':<20} {'Name':<30} {'Status':<12} {'Priority':<8} {'Progress':<10} {'Created':<20}")
        click.echo("-" * 100)
    else:
        click.echo(f"{'Job ID':<20} {'Name':<30} {'Status':<12} {'Progress':<10}")
        click.echo("-" * 72)

    # Rows
    for job in jobs:
        progress = f"{job.get('progress_percent', 0):.1f}%" if job.get('progress_percent') else "N/A"

        if verbose:
            created = job.get('created_at', 'Unknown')[:19] if job.get('created_at') else 'Unknown'
            click.echo(f"{job['job_id']:<20} {job['job_name']:<30} {job['status']:<12} "
                      f"{job['priority']:<8} {progress:<10} {created:<20}")
        else:
            click.echo(f"{job['job_id']:<20} {job['job_name']:<30} {job['status']:<12} {progress:<10}")


def _display_workers_table(workers: list, verbose: bool):
    """Display workers in table format"""
    if not workers:
        click.echo("No workers found")
        return

    # Header
    if verbose:
        click.echo(f"{'Worker ID':<20} {'Node Name':<20} {'Status':<10} {'Jobs':<5} {'CPU%':<6} {'Mem%':<6} {'Last Seen':<20}")
        click.echo("-" * 87)
    else:
        click.echo(f"{'Worker ID':<20} {'Node Name':<20} {'Status':<10} {'Jobs':<5}")
        click.echo("-" * 55)

    # Rows
    for worker in workers:
        if verbose:
            cpu = f"{worker.get('cpu_usage_percent', 0):.1f}" if worker.get('cpu_usage_percent') else "N/A"
            mem = f"{worker.get('memory_usage_percent', 0):.1f}" if worker.get('memory_usage_percent') else "N/A"
            last_seen = worker.get('last_seen_at', 'Unknown')[:19] if worker.get('last_seen_at') else 'Unknown'
            click.echo(f"{worker['worker_id']:<20} {worker['node_name']:<20} {worker['status']:<10} "
                      f"{worker['current_jobs']:<5} {cpu:<6} {mem:<6} {last_seen:<20}")
        else:
            click.echo(f"{worker['worker_id']:<20} {worker['node_name']:<20} "
                      f"{worker['status']:<10} {worker['current_jobs']:<5}")


def _display_system_health(health: Dict[str, Any], verbose: bool):
    """Display system health information"""
    click.echo(f"Overall Status: {health['overall_status'].upper()}")
    click.echo(f"Uptime: {timedelta(seconds=int(health['uptime_seconds']))}")
    click.echo()

    click.echo("Jobs:")
    click.echo(f"  Total: {health['total_jobs']}")
    click.echo(f"  Running: {health['running_jobs']}")
    click.echo(f"  Failed: {health['failed_jobs']}")
    click.echo(f"  Queue Size: {health['queue_size']}")
    click.echo()

    click.echo("Workers:")
    click.echo(f"  Total: {health['total_workers']}")
    click.echo(f"  Healthy: {health['healthy_workers']}")
    click.echo()

    click.echo("Performance:")
    click.echo(f"  Processing Rate: {health['processing_rate']:.1f} jobs/hour")
    click.echo(f"  Error Rate: {health['error_rate']:.1f}%")
    click.echo(f"  Average CPU: {health['average_cpu_usage']:.1f}%")
    click.echo(f"  Average Memory: {health['average_memory_usage']:.1f}%")


def _display_performance_report(report: Dict[str, Any], verbose: bool):
    """Display performance report"""
    period = report['report_period']
    click.echo(f"Performance Report ({period['duration_hours']} hours)")
    click.echo(f"Period: {period['start_time']} to {period['end_time']}")
    click.echo()

    if 'job_performance' in report:
        job_perf = report['job_performance']
        click.echo("Job Performance:")
        click.echo(f"  Completed Jobs: {job_perf.get('completed_jobs', 0)}")
        click.echo(f"  Failed Jobs: {job_perf.get('failed_jobs', 0)}")
        click.echo(f"  Average Duration: {job_perf.get('average_duration', 'N/A')}")
        click.echo()

    if 'worker_performance' in report:
        worker_perf = report['worker_performance']
        click.echo("Worker Performance:")
        click.echo(f"  Average CPU: {worker_perf.get('average_cpu', 0):.1f}%")
        click.echo(f"  Average Memory: {worker_perf.get('average_memory', 0):.1f}%")
        click.echo(f"  Peak Workers: {worker_perf.get('peak_workers', 0)}")
        click.echo()

    if 'alerts' in report and verbose:
        alerts = report['alerts']
        click.echo("Alerts:")
        click.echo(f"  Total: {alerts.get('total_alerts', 0)}")
        for severity, count in alerts.get('by_severity', {}).items():
            click.echo(f"  {severity.title()}: {count}")


def main():
    """Main CLI entry point"""
    cli()


if __name__ == '__main__':
    main()