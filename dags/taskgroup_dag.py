"""
TaskGroup DAG Example
=====================

This DAG demonstrates how to use TaskGroups to organize related tasks together.
TaskGroups help create cleaner, more maintainable DAGs by grouping related operations.

Features demonstrated:
- TaskGroup creation and nesting
- Task dependencies within and between groups
- Different ways to organize workflow logic
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

def extract_data(source):
    print(f"Extracting data from {source}")
    return f"data_from_{source}"

def transform_data(data_type):
    print(f"Transforming {data_type} data")
    return f"transformed_{data_type}"

def load_data(destination):
    print(f"Loading data to {destination}")
    return f"loaded_to_{destination}"

def validate_data(dataset):
    print(f"Validating {dataset}")
    return f"validated_{dataset}"

def send_notification(message):
    print(f"Notification: {message}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'taskgroup_example_dag',
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["learning", "taskgroup", "intermediate"]
)

# Start task
start_task = BashOperator(
    task_id='start_pipeline',
    bash_command='echo "Starting ETL Pipeline with TaskGroups"',
    dag=dag
)

# Extract TaskGroup
with TaskGroup("extract_group", dag=dag) as extract_group:
    extract_db = PythonOperator(
        task_id='extract_from_database',
        python_callable=extract_data,
        op_kwargs={'source': 'database'},
    )
    
    extract_api = PythonOperator(
        task_id='extract_from_api',
        python_callable=extract_data,
        op_kwargs={'source': 'api'},
    )
    
    extract_files = PythonOperator(
        task_id='extract_from_files',
        python_callable=extract_data,
        op_kwargs={'source': 'files'},
    )

# Transform TaskGroup
with TaskGroup("transform_group", dag=dag) as transform_group:
    transform_customer_data = PythonOperator(
        task_id='transform_customer_data',
        python_callable=transform_data,
        op_kwargs={'data_type': 'customer'},
    )
    
    transform_product_data = PythonOperator(
        task_id='transform_product_data',
        python_callable=transform_data,
        op_kwargs={'data_type': 'product'},
    )
    
    transform_order_data = PythonOperator(
        task_id='transform_order_data',
        python_callable=transform_data,
        op_kwargs={'data_type': 'order'},
    )

# Load TaskGroup with nested validation
with TaskGroup("load_group", dag=dag) as load_group:
    # Nested TaskGroup for data warehouse operations
    with TaskGroup("data_warehouse", dag=dag) as dw_group:
        load_to_staging = PythonOperator(
            task_id='load_to_staging',
            python_callable=load_data,
            op_kwargs={'destination': 'staging'},
        )
        
        validate_staging = PythonOperator(
            task_id='validate_staging',
            python_callable=validate_data,
            op_kwargs={'dataset': 'staging'},
        )
        
        load_to_production = PythonOperator(
            task_id='load_to_production',
            python_callable=load_data,
            op_kwargs={'destination': 'production'},
        )
        
        load_to_staging >> validate_staging >> load_to_production
    
    # Parallel analytics load
    load_to_analytics = PythonOperator(
        task_id='load_to_analytics',
        python_callable=load_data,
        op_kwargs={'destination': 'analytics'},
    )

# Notification TaskGroup
with TaskGroup("notification_group", dag=dag) as notification_group:
    send_success_email = PythonOperator(
        task_id='send_success_email',
        python_callable=send_notification,
        op_kwargs={'message': 'ETL Pipeline completed successfully'},
    )
    
    send_slack_message = PythonOperator(
        task_id='send_slack_message',
        python_callable=send_notification,
        op_kwargs={'message': 'Data pipeline finished - check analytics dashboard'},
    )
    
    update_monitoring = PythonOperator(
        task_id='update_monitoring',
        python_callable=send_notification,
        op_kwargs={'message': 'Pipeline metrics updated'},
    )

# End task
end_task = BashOperator(
    task_id='end_pipeline',
    bash_command='echo "ETL Pipeline completed successfully"',
    dag=dag
)

# Define dependencies between TaskGroups
start_task >> extract_group >> transform_group >> load_group >> notification_group >> end_task