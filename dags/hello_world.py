"""
Hello World DAG - TaskFlow API Example
=======================================

This DAG demonstrates the modern TaskFlow API approach in Apache Airflow.
It showcases the @task and @dag decorators which provide a cleaner, more
Pythonic way to define workflows.

Key Learning Points:
- @task decorator automatically creates PythonOperator tasks
- @dag decorator creates DAG instances with cleaner syntax
- TaskFlow API handles XCom serialization automatically
- No need for explicit task_id when using decorators
- Automatic dependency inference from function calls
"""

from airflow.decorators import dag, task
from datetime import datetime

@task
def hello_world():
    """
    Simple task function that prints "Hello World"
    
    This function demonstrates:
    - Basic task definition with @task decorator
    - Automatic task_id generation (uses function name)
    - Simple print statement execution
    - No explicit return value needed
    """
    print("Hello World")
    print("This is a simple task using TaskFlow API")
    return "Hello World task completed"

@dag(
    dag_id='hello_world',                    # Unique identifier for the DAG
    start_date=datetime(2023, 1, 1),        # When the DAG should start running
    schedule='@daily',                       # Run once per day at midnight
    catchup=False,                          # Don't run for past dates
    default_args={'owner': 'airflow', 'retries': 1},  # Default task arguments
    description='Simple Hello World DAG using TaskFlow API',  # DAG description
    tags=['learning', 'basic', 'taskflow'],  # Tags for organization
)
def hello_world_dag():
    """
    DAG function that defines the workflow structure
    
    This function demonstrates:
    - Modern DAG definition using @dag decorator
    - Task instantiation and execution
    - Automatic task dependency management
    - Clean, readable DAG structure
    """
    # Create and execute the hello_world task
    # Task dependencies are automatically inferred
    hello_world_task = hello_world()
    
    # Return value is automatically handled by TaskFlow API
    return hello_world_task

# Instantiate the DAG
# This call is required to register the DAG with Airflow
hello_world_dag()

