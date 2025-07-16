"""
Basic Python Operator DAG - Traditional Approach
=================================================

This DAG demonstrates the traditional way of creating Airflow workflows
using PythonOperator. This approach is still widely used and important
to understand for working with existing DAGs.

Key Learning Points:
- Traditional DAG instantiation with DAG class
- PythonOperator for executing Python functions
- Task dependency definition using >> operator
- Explicit task_id specification
- Default arguments configuration
- Manual DAG object passing to operators

Interview Topics:
- PythonOperator vs @task decorator
- Task dependency syntax
- DAG configuration patterns
- Traditional vs TaskFlow API approaches
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    """
    Function to print a hello message
    
    This function demonstrates:
    - Simple print statement execution
    - Function as python_callable parameter
    - No context variables needed
    - Basic task logic implementation
    """
    print("Hello from Airflow!")
    print("This is a traditional PythonOperator task")
    return "Hello task completed successfully"

def print_goodbye():
    """
    Function to print a goodbye message
    
    This function demonstrates:
    - Sequential task execution
    - Function chaining through dependencies
    - Simple return value handling
    - Basic task completion logging
    """
    print("Goodbye from Airflow!")
    print("This completes the basic Python operator example")
    return "Goodbye task completed successfully"

# Default arguments applied to all tasks in the DAG
default_args = {
    'owner': 'airflow',                     # Task owner identifier
    'start_date': datetime(2024, 1, 1),    # When DAG becomes active
    'retries': 1,                          # Number of retries on failure
    'retry_delay': datetime.timedelta(minutes=5),  # Delay between retries
    'depends_on_past': False,              # Task doesn't depend on past runs
}

# Create DAG instance using traditional approach
dag = DAG(
    'basic_python_operator_dag',            # Unique DAG identifier
    default_args=default_args,              # Apply default args to all tasks
    schedule=None,                          # Manual trigger only (no schedule)
    catchup=False,                          # Don't backfill past dates
    tags=["learning", "basic", "traditional"],  # Tags for DAG organization
    description='Basic PythonOperator usage demonstration',  # DAG description
    max_active_runs=1,                      # Only one DAG run at a time
)

# Create first task using PythonOperator
hello_task = PythonOperator(
    task_id='print_hello',                  # Unique task identifier
    python_callable=print_hello,           # Function to execute
    dag=dag                                 # Associate with DAG instance
)

# Create second task using PythonOperator
goodbye_task = PythonOperator(
    task_id='print_goodbye',                # Unique task identifier
    python_callable=print_goodbye,         # Function to execute
    dag=dag                                 # Associate with DAG instance
)

# Define task dependencies using >> operator
# This means: hello_task must complete before goodbye_task runs
hello_task >> goodbye_task

# Alternative dependency syntax (equivalent to above):
# hello_task.set_downstream(goodbye_task)
# goodbye_task.set_upstream(hello_task)
