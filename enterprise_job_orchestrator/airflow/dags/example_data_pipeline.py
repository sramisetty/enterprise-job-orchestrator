"""
Example Airflow DAG for data processing pipeline using Enterprise Job Orchestrator.

This DAG demonstrates how to create complex data workflows using the orchestrator's
Spark and Airflow integration.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

from enterprise_job_orchestrator.airflow.operators import (
    EnterpriseDataProcessingOperator,
    EnterpriseETLOperator,
    EnterpriseJobOperator
)
from enterprise_job_orchestrator.airflow.sensors import (
    EnterpriseSystemHealthSensor,
    EnterpriseResourceSensor
)

# Default arguments for the DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'enterprise_data_pipeline',
    default_args=default_args,
    description='Enterprise data processing pipeline with Spark integration',
    schedule_interval=timedelta(hours=6),  # Run every 6 hours
    catchup=False,
    max_active_runs=1,
    tags=['data-processing', 'spark', 'enterprise']
)

# Start marker
start_task = DummyOperator(
    task_id='start_pipeline',
    dag=dag
)

# Check system health before starting
health_check = EnterpriseSystemHealthSensor(
    task_id='check_system_health',
    min_healthy_workers=2,
    max_error_rate=0.05,
    max_queue_size=100,
    timeout=300,  # 5 minutes
    poke_interval=30,
    dag=dag
)

# Check resource availability
resource_check = EnterpriseResourceSensor(
    task_id='check_resources',
    min_cpu_percent=30.0,
    min_memory_percent=25.0,
    min_available_workers=2,
    timeout=600,  # 10 minutes
    poke_interval=60,
    dag=dag
)

# Data ingestion ETL job
data_ingestion = EnterpriseETLOperator(
    task_id='data_ingestion',
    job_name='daily_data_ingestion',
    extract_config={
        'type': 'csv',
        'path': '/data/raw/daily_transactions/*.csv'
    },
    transform_config={
        'operations': [
            {
                'type': 'filter',
                'condition': 'amount > 0 AND status = "completed"'
            },
            {
                'type': 'select',
                'columns': ['transaction_id', 'customer_id', 'amount', 'timestamp', 'category']
            }
        ]
    },
    load_config={
        'type': 'parquet',
        'path': '/data/processed/daily_transactions/{{ ds }}',
        'mode': 'overwrite'
    },
    execution_engine='spark',
    job_priority='high',
    timeout=1800,  # 30 minutes
    dag=dag
)

# Customer segmentation processing
customer_segmentation = EnterpriseDataProcessingOperator(
    task_id='customer_segmentation',
    job_name='customer_segmentation_analysis',
    input_path='/data/processed/daily_transactions/{{ ds }}',
    output_path='/data/analytics/customer_segments/{{ ds }}',
    processing_logic={
        'transformations': [
            {
                'type': 'groupby',
                'columns': ['customer_id'],
                'aggregations': {
                    'total_amount': 'sum(amount)',
                    'transaction_count': 'count(*)',
                    'avg_amount': 'avg(amount)'
                }
            },
            {
                'type': 'filter',
                'condition': 'total_amount > 100'
            }
        ]
    },
    input_format='parquet',
    output_format='parquet',
    execution_engine='spark',
    job_priority='medium',
    timeout=2700,  # 45 minutes
    dag=dag
)

# Fraud detection analysis
fraud_detection = EnterpriseJobOperator(
    task_id='fraud_detection',
    job_name='fraud_detection_analysis',
    job_type='analytics',
    job_data={
        'input_path': '/data/processed/daily_transactions/{{ ds }}',
        'output_path': '/data/security/fraud_alerts/{{ ds }}',
        'analysis_type': 'fraud_detection',
        'queries': [
            'SELECT * FROM transactions WHERE amount > 10000',
            'SELECT customer_id, COUNT(*) as tx_count FROM transactions GROUP BY customer_id HAVING tx_count > 50'
        ],
        'alert_threshold': 0.95
    },
    execution_engine='spark',
    job_priority='critical',
    timeout=1800,  # 30 minutes
    dag=dag
)

# Generate daily reports
daily_reporting = EnterpriseJobOperator(
    task_id='daily_reporting',
    job_name='daily_transaction_report',
    job_type='analytics',
    job_data={
        'transaction_data_path': '/data/processed/daily_transactions/{{ ds }}',
        'segment_data_path': '/data/analytics/customer_segments/{{ ds }}',
        'fraud_data_path': '/data/security/fraud_alerts/{{ ds }}',
        'output_path': '/data/reports/daily_summary/{{ ds }}',
        'report_format': 'html',
        'include_charts': True
    },
    execution_engine='local',  # Reports can run locally
    job_priority='medium',
    timeout=900,  # 15 minutes
    dag=dag
)

# Data quality validation
data_validation = EnterpriseJobOperator(
    task_id='data_validation',
    job_name='data_quality_validation',
    job_type='data_processing',
    job_data={
        'validation_rules': [
            {'field': 'amount', 'rule': 'not_null'},
            {'field': 'amount', 'rule': 'greater_than', 'value': 0},
            {'field': 'customer_id', 'rule': 'not_null'},
            {'field': 'timestamp', 'rule': 'valid_date'}
        ],
        'input_path': '/data/processed/daily_transactions/{{ ds }}',
        'output_path': '/data/quality/validation_results/{{ ds }}',
        'fail_on_validation_error': True
    },
    execution_engine='spark',
    job_priority='high',
    timeout=1200,  # 20 minutes
    dag=dag
)

# Archive old data
def archive_old_data(**context):
    """Archive data older than 30 days."""
    from datetime import datetime, timedelta
    import os

    archive_date = datetime.now() - timedelta(days=30)
    archive_path = f"/data/archive/{archive_date.strftime('%Y-%m-%d')}"

    # This would contain logic to move old data to archive
    print(f"Archiving data older than {archive_date} to {archive_path}")
    return f"Archived data to {archive_path}"

archive_task = PythonOperator(
    task_id='archive_old_data',
    python_callable=archive_old_data,
    dag=dag
)

# End marker
end_task = DummyOperator(
    task_id='pipeline_complete',
    dag=dag
)

# Define task dependencies
start_task >> [health_check, resource_check]

[health_check, resource_check] >> data_ingestion

data_ingestion >> [customer_segmentation, fraud_detection, data_validation]

[customer_segmentation, fraud_detection] >> daily_reporting

[daily_reporting, data_validation] >> archive_task

archive_task >> end_task