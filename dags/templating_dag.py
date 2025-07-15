"""
Templating DAG Example
======================

This DAG demonstrates Airflow's powerful Jinja templating capabilities:
- Using built-in macros and variables
- Date/time templating
- Custom template fields
- Template rendering in different operators

Templating allows dynamic content based on execution context, making DAGs more flexible and reusable.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from datetime import datetime, timedelta

def process_templated_data(**context):
    """Process data using templated values from context."""
    print("=== Templated Data Processing ===")
    print(f"Execution Date: {context['ds']}")
    print(f"DAG ID: {context['dag'].dag_id}")
    print(f"Task ID: {context['task'].task_id}")
    print(f"Run ID: {context['run_id']}")
    
    # Access templated parameters
    print(f"Custom message: {context['params'].get('message', 'No message')}")
    print(f"Environment: {context['params'].get('environment', 'development')}")

def use_templated_filename(filename, **context):
    """Demonstrate using templated filename."""
    print(f"Processing file: {filename}")
    print(f"File created for date: {context['ds']}")

# Set some Airflow Variables for templating (you can set these in Airflow UI)
# Variable.set("data_source", "production_db")
# Variable.set("batch_size", "1000")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'templating_example_dag',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    params={
        "message": "Hello from templated DAG!",
        "environment": "development",
        "enable_processing": True
    },
    tags=["learning", "templating", "intermediate"]
)

# Task using built-in Airflow macros in BashOperator
bash_templating = BashOperator(
    task_id='bash_templating_example',
    bash_command="""
    echo "Execution date: {{ ds }}"
    echo "Execution date nodash: {{ ds_nodash }}"
    echo "Yesterday: {{ yesterday_ds }}"
    echo "Tomorrow: {{ tomorrow_ds }}"
    echo "DAG: {{ dag.dag_id }}"
    echo "Task: {{ task.task_id }}"
    echo "Run ID: {{ run_id }}"
    echo "Logical date: {{ logical_date }}"
    echo "Data interval start: {{ data_interval_start }}"
    echo "Data interval end: {{ data_interval_end }}"
    """,
    dag=dag
)

# Task using Variables in templates
variable_templating = BashOperator(
    task_id='variable_templating_example',
    bash_command="""
    echo "Data source: {{ var.value.get('data_source', 'default_db') }}"
    echo "Batch size: {{ var.value.get('batch_size', '100') }}"
    echo "Custom message: {{ params.message }}"
    echo "Environment: {{ params.environment }}"
    """,
    dag=dag
)

# PythonOperator with templated parameters
python_templating = PythonOperator(
    task_id='python_templating_example',
    python_callable=process_templated_data,
    dag=dag
)

# Templated filename example
file_processing = PythonOperator(
    task_id='file_processing_templated',
    python_callable=use_templated_filename,
    op_kwargs={
        'filename': '/data/daily_export_{{ ds_nodash }}.csv'
    },
    dag=dag
)

# Advanced templating with SQL-like operations
sql_templating = BashOperator(
    task_id='sql_templating_example',
    bash_command="""
    echo "Processing data for period:"
    echo "Start: {{ data_interval_start.strftime('%Y-%m-%d %H:%M:%S') }}"
    echo "End: {{ data_interval_end.strftime('%Y-%m-%d %H:%M:%S') }}"
    echo "Week day: {{ logical_date.strftime('%A') }}"
    echo "Is weekend: {{ 'Yes' if logical_date.weekday() >= 5 else 'No' }}"
    """,
    dag=dag
)

# Conditional templating
conditional_task = BashOperator(
    task_id='conditional_templating',
    bash_command="""
    {% if params.enable_processing %}
    echo "Processing is enabled"
    echo "Running full pipeline for {{ ds }}"
    {% else %}
    echo "Processing is disabled"
    echo "Skipping pipeline for {{ ds }}"
    {% endif %}
    """,
    dag=dag
)

# Math operations in templates
math_templating = BashOperator(
    task_id='math_templating_example',
    bash_command="""
    echo "Days since epoch: {{ (logical_date - macros.datetime(1970, 1, 1)).days }}"
    echo "Hours until tomorrow: {{ 24 - logical_date.hour }}"
    echo "Week number: {{ logical_date.strftime('%U') }}"
    echo "Quarter: {{ 'Q' + ((logical_date.month - 1) // 3 + 1)|string }}"
    """,
    dag=dag
)

# Loop templating example
loop_templating = BashOperator(
    task_id='loop_templating_example',
    bash_command="""
    echo "Processing multiple items:"
    {% for item in ['database', 'api', 'files'] %}
    echo "Processing {{ item }} for date {{ ds }}"
    {% endfor %}
    
    echo "Processing last 3 days:"
    {% for i in range(3) %}
    echo "Day {{ i }}: {{ (logical_date - macros.timedelta(days=i)).strftime('%Y-%m-%d') }}"
    {% endfor %}
    """,
    dag=dag
)

# End task
end_task = EmptyOperator(
    task_id='end_templating_examples',
    dag=dag
)

# Define dependencies
[bash_templating, variable_templating] >> python_templating
python_templating >> [file_processing, sql_templating]
[file_processing, sql_templating] >> conditional_task
conditional_task >> [math_templating, loop_templating] >> end_task