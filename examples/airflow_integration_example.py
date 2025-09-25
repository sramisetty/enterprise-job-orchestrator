"""
Example demonstrating Airflow integration with Enterprise Job Orchestrator.

This example shows how to create Airflow DAGs that use the Enterprise Job Orchestrator
for executing distributed Spark jobs and managing complex workflows.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator

# Import Enterprise Job Orchestrator operators
from enterprise_job_orchestrator.airflow.operators import (
    EnterpriseJobOperator,
    EnterpriseDataProcessingOperator,
    EnterpriseETLOperator,
    EnterpriseMLTrainingOperator
)
from enterprise_job_orchestrator.airflow.sensors import (
    EnterpriseSystemHealthSensor,
    EnterpriseResourceSensor,
    EnterpriseJobSensor
)

# Default arguments for all DAGs
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email': ['admin@company.com']
}

# Example 1: Simple Data Processing Pipeline
simple_pipeline_dag = DAG(
    'enterprise_simple_pipeline',
    default_args=default_args,
    description='Simple data processing pipeline using Enterprise Job Orchestrator',
    schedule_interval=timedelta(hours=6),
    catchup=False,
    tags=['enterprise', 'data-processing', 'simple']
)

# Check system health before starting
health_check = EnterpriseSystemHealthSensor(
    task_id='check_system_health',
    min_healthy_workers=1,
    max_error_rate=0.1,
    timeout=300,
    dag=simple_pipeline_dag
)

# Simple data processing task
simple_processing = EnterpriseDataProcessingOperator(
    task_id='process_customer_data',
    job_name='simple_customer_processing',
    input_path='/data/raw/customers/{{ ds }}.csv',
    output_path='/data/processed/customers/{{ ds }}',
    processing_logic={
        'transformations': [
            {'type': 'filter', 'condition': 'age >= 18'},
            {'type': 'select', 'columns': ['id', 'name', 'email', 'age']},
        ]
    },
    timeout=1800,
    dag=simple_pipeline_dag
)

health_check >> simple_processing

# Example 2: Complex ML Pipeline
ml_pipeline_dag = DAG(
    'enterprise_ml_pipeline',
    default_args=default_args,
    description='Machine learning pipeline with data prep and model training',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['enterprise', 'machine-learning', 'complex']
)

# Resource check for ML workload
ml_resource_check = EnterpriseResourceSensor(
    task_id='check_ml_resources',
    min_cpu_percent=40.0,
    min_memory_percent=50.0,
    min_available_workers=2,
    timeout=600,
    dag=ml_pipeline_dag
)

# Data preparation for ML
ml_data_prep = EnterpriseETLOperator(
    task_id='prepare_ml_data',
    job_name='ml_data_preparation',
    extract_config={
        'type': 'parquet',
        'path': '/data/warehouse/transactions/{{ ds }}'
    },
    transform_config={
        'operations': [
            {'type': 'filter', 'condition': 'amount > 0'},
            {'type': 'add_column', 'name': 'log_amount', 'expression': 'log(amount)'},
            {'type': 'groupby', 'columns': ['customer_id'], 'aggregations': {
                'total_amount': 'sum(amount)',
                'transaction_count': 'count(*)',
                'avg_amount': 'avg(amount)'
            }}
        ]
    },
    load_config={
        'type': 'parquet',
        'path': '/data/ml/features/{{ ds }}',
        'mode': 'overwrite'
    },
    timeout=3600,
    dag=ml_pipeline_dag
)

# Model training
model_training = EnterpriseMLTrainingOperator(
    task_id='train_customer_model',
    job_name='customer_segmentation_training',
    model_type='kmeans',
    training_data_path='/data/ml/features/{{ ds }}',
    model_output_path='/data/ml/models/customer_segmentation/{{ ds }}',
    training_config={
        'features': ['total_amount', 'transaction_count', 'avg_amount'],
        'k': 5,
        'max_iterations': 100,
        'seed': 42
    },
    timeout=7200,  # 2 hours
    dag=ml_pipeline_dag
)

# Model evaluation
model_evaluation = EnterpriseJobOperator(
    task_id='evaluate_model',
    job_name='model_evaluation',
    job_type='analytics',
    job_data={
        'model_path': '/data/ml/models/customer_segmentation/{{ ds }}',
        'test_data_path': '/data/ml/test/{{ ds }}',
        'output_path': '/data/ml/evaluation/{{ ds }}',
        'metrics': ['silhouette_score', 'inertia', 'calinski_harabasz']
    },
    execution_engine='spark',
    timeout=1800,
    dag=ml_pipeline_dag
)

ml_resource_check >> ml_data_prep >> model_training >> model_evaluation

# Example 3: Event-Driven Pipeline with Conditional Logic
event_driven_dag = DAG(
    'enterprise_event_driven_pipeline',
    default_args=default_args,
    description='Event-driven pipeline with conditional processing',
    schedule_interval=None,  # Triggered externally
    catchup=False,
    tags=['enterprise', 'event-driven', 'conditional']
)

def check_data_availability(**context):
    """Check if data is available for processing."""
    import os
    data_path = f"/data/raw/events/{context['ds']}"

    if os.path.exists(data_path) and len(os.listdir(data_path)) > 0:
        return 'process_events'
    else:
        return 'skip_processing'

# Decision point
data_check = BranchPythonOperator(
    task_id='check_data_availability',
    python_callable=check_data_availability,
    dag=event_driven_dag
)

# Process events if data available
process_events = EnterpriseDataProcessingOperator(
    task_id='process_events',
    job_name='event_processing',
    input_path='/data/raw/events/{{ ds }}/*.json',
    output_path='/data/processed/events/{{ ds }}',
    processing_logic={
        'transformations': [
            {'type': 'filter', 'condition': 'event_type IN ("click", "purchase", "view")'},
            {'type': 'add_column', 'name': 'processing_date', 'expression': 'current_date()'},
        ]
    },
    input_format='json',
    output_format='parquet',
    timeout=2400,
    dag=event_driven_dag
)

# Skip processing
skip_processing = DummyOperator(
    task_id='skip_processing',
    dag=event_driven_dag
)

# Generate alerts for processed events
generate_alerts = EnterpriseJobOperator(
    task_id='generate_alerts',
    job_name='event_alerts',
    job_type='analytics',
    job_data={
        'input_path': '/data/processed/events/{{ ds }}',
        'alert_rules': [
            {'condition': 'event_type = "purchase" AND amount > 1000', 'priority': 'high'},
            {'condition': 'event_count > 10000', 'priority': 'medium'}
        ],
        'output_path': '/data/alerts/events/{{ ds }}'
    },
    execution_engine='local',  # Alerts can run locally
    timeout=600,
    trigger_rule='none_failed_or_skipped',  # Run even if processing was skipped
    dag=event_driven_dag
)

data_check >> [process_events, skip_processing]
[process_events, skip_processing] >> generate_alerts

# Example 4: Multi-Stage ETL Pipeline with Dependencies
multi_stage_dag = DAG(
    'enterprise_multi_stage_etl',
    default_args=default_args,
    description='Multi-stage ETL pipeline with complex dependencies',
    schedule_interval=timedelta(hours=12),
    catchup=False,
    tags=['enterprise', 'etl', 'multi-stage']
)

# Stage 1: Raw data ingestion
ingest_orders = EnterpriseETLOperator(
    task_id='ingest_orders',
    job_name='orders_ingestion',
    extract_config={
        'type': 'csv',
        'path': '/data/sources/orders/{{ ds }}/*.csv'
    },
    transform_config={
        'operations': [
            {'type': 'add_column', 'name': 'ingestion_date', 'expression': 'current_timestamp()'},
            {'type': 'filter', 'condition': 'order_status IS NOT NULL'}
        ]
    },
    load_config={
        'type': 'parquet',
        'path': '/data/staging/orders/{{ ds }}',
        'mode': 'overwrite'
    },
    timeout=1800,
    dag=multi_stage_dag
)

ingest_customers = EnterpriseETLOperator(
    task_id='ingest_customers',
    job_name='customers_ingestion',
    extract_config={
        'type': 'json',
        'path': '/data/sources/customers/{{ ds }}/*.json'
    },
    transform_config={
        'operations': [
            {'type': 'add_column', 'name': 'ingestion_date', 'expression': 'current_timestamp()'},
            {'type': 'filter', 'condition': 'customer_id IS NOT NULL'}
        ]
    },
    load_config={
        'type': 'parquet',
        'path': '/data/staging/customers/{{ ds }}',
        'mode': 'overwrite'
    },
    timeout=1800,
    dag=multi_stage_dag
)

# Stage 2: Data enrichment
enrich_orders = EnterpriseJobOperator(
    task_id='enrich_orders',
    job_name='orders_enrichment',
    job_type='etl',
    job_data={
        'extract': {
            'type': 'parquet',
            'path': '/data/staging/orders/{{ ds }}'
        },
        'transform': {
            'operations': [
                {
                    'type': 'join',
                    'right_table': '/data/staging/customers/{{ ds }}',
                    'condition': 'orders.customer_id = customers.customer_id'
                },
                {
                    'type': 'add_column',
                    'name': 'customer_tier',
                    'expression': 'CASE WHEN total_orders > 100 THEN "gold" WHEN total_orders > 50 THEN "silver" ELSE "bronze" END'
                }
            ]
        },
        'load': {
            'type': 'parquet',
            'path': '/data/enriched/orders/{{ ds }}',
            'mode': 'overwrite'
        }
    },
    execution_engine='spark',
    timeout=2400,
    dag=multi_stage_dag
)

# Stage 3: Generate business metrics
generate_metrics = EnterpriseJobOperator(
    task_id='generate_business_metrics',
    job_name='business_metrics',
    job_type='analytics',
    job_data={
        'input_path': '/data/enriched/orders/{{ ds }}',
        'output_path': '/data/metrics/business/{{ ds }}',
        'metrics': [
            'total_revenue',
            'average_order_value',
            'customer_count',
            'orders_per_customer',
            'revenue_by_tier'
        ]
    },
    execution_engine='spark',
    timeout=1800,
    dag=multi_stage_dag
)

# Wait for both ingestion jobs to complete
ingestion_complete = DummyOperator(
    task_id='ingestion_complete',
    dag=multi_stage_dag
)

# Final validation
validate_pipeline = BashOperator(
    task_id='validate_pipeline',
    bash_command="""
    echo "Validating pipeline results..."
    python /scripts/validate_etl_results.py --date {{ ds }}
    """,
    dag=multi_stage_dag
)

# Define dependencies
[ingest_orders, ingest_customers] >> ingestion_complete
ingestion_complete >> enrich_orders >> generate_metrics >> validate_pipeline

# Example 5: Real-time Processing Pipeline
realtime_dag = DAG(
    'enterprise_realtime_processing',
    default_args=default_args,
    description='Real-time data processing pipeline',
    schedule_interval=timedelta(minutes=15),
    catchup=False,
    tags=['enterprise', 'real-time', 'streaming']
)

# Check for new data
check_new_data = EnterpriseJobOperator(
    task_id='check_new_data',
    job_name='data_availability_check',
    job_type='system',
    job_data={
        'command': 'find /data/streaming -name "*.json" -newer /tmp/last_processed -type f | wc -l',
        'timeout': 30
    },
    execution_engine='local',
    timeout=60,
    dag=realtime_dag
)

# Process streaming data
process_stream = EnterpriseDataProcessingOperator(
    task_id='process_streaming_data',
    job_name='stream_processing',
    input_path='/data/streaming/*.json',
    output_path='/data/processed/streaming/{{ ts_nodash }}',
    processing_logic={
        'transformations': [
            {'type': 'filter', 'condition': 'timestamp > "{{ ts }}"'},
            {'type': 'add_column', 'name': 'batch_id', 'expression': '"{{ ts_nodash }}"'},
            {'type': 'groupby', 'columns': ['event_type'], 'aggregations': {
                'event_count': 'count(*)',
                'unique_users': 'count_distinct(user_id)'
            }}
        ]
    },
    input_format='json',
    output_format='parquet',
    timeout=900,
    dag=realtime_dag
)

# Update dashboard metrics
update_dashboard = EnterpriseJobOperator(
    task_id='update_dashboard',
    job_name='dashboard_update',
    job_type='system',
    job_data={
        'script_path': '/scripts/update_realtime_dashboard.py',
        'args': ['--batch-id', '{{ ts_nodash }}'],
        'timeout': 300
    },
    execution_engine='local',
    timeout=600,
    dag=realtime_dag
)

check_new_data >> process_stream >> update_dashboard