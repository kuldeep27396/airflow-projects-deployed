"""
Interview Concepts DAG
======================

This DAG covers essential Airflow concepts frequently asked in interviews:
- Trigger rules (all_success, all_failed, one_success, one_failed, all_done)
- Pool usage for resource management
- SLA (Service Level Agreement) monitoring
- Task priority and weight
- Depends_on_past functionality
- Max_active_runs and max_active_tasks
- Connection and Variable usage
- Callback functions
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.models import Variable
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import time
import random

def demonstrate_variables_and_connections(**context):
    """Demonstrate Airflow Variables and Connections usage."""
    print("=== Variables and Connections Demo ===")
    
    # Get variables (with defaults)
    env = Variable.get("environment", default_var="development")
    batch_size = Variable.get("batch_size", default_var="100")
    
    print(f"Environment: {env}")
    print(f"Batch size: {batch_size}")
    
    # Demonstrate connection usage (conceptually)
    print("Connection usage:")
    print("- Database connections: postgres_default, mysql_default")
    print("- HTTP connections: http_default, api_service")
    print("- S3 connections: aws_default")
    
    return {"environment": env, "batch_size": batch_size}

def task_with_sla_demo(**context):
    """Task that demonstrates SLA monitoring."""
    print("=== SLA Demo Task ===")
    
    # Simulate work that might take varying time
    work_duration = random.uniform(1, 3)
    print(f"Simulating work for {work_duration:.1f} seconds")
    time.sleep(work_duration)
    
    print("Task completed within SLA")
    return {"duration": work_duration}

def high_priority_task(**context):
    """High priority task that should run first."""
    print("=== High Priority Task ===")
    print("This task has high priority and should run before others")
    time.sleep(2)
    return {"priority": "high", "execution_order": 1}

def low_priority_task(**context):
    """Low priority task that runs after high priority tasks."""
    print("=== Low Priority Task ===")
    print("This task has low priority and runs after high priority tasks")
    time.sleep(1)
    return {"priority": "low", "execution_order": 2}

def depends_on_past_task(**context):
    """Task that depends on its previous run success."""
    execution_date = context['execution_date']
    print(f"=== Depends on Past Task - {execution_date} ===")
    
    # Simulate occasional failure for demo
    if random.random() < 0.3:
        print("Task failed - next run will not execute due to depends_on_past=True")
        raise AirflowException("Simulated task failure")
    
    print("Task succeeded - future runs can proceed")
    return {"status": "success", "execution_date": str(execution_date)}

def pool_task_1(**context):
    """Task using shared resource pool."""
    print("=== Pool Task 1 ===")
    print("Using shared resource pool 'processing_pool'")
    time.sleep(3)  # Simulate resource-intensive work
    return {"pool": "processing_pool", "task": "task_1"}

def pool_task_2(**context):
    """Another task using the same resource pool."""
    print("=== Pool Task 2 ===")
    print("Using shared resource pool 'processing_pool'")
    time.sleep(2)  # Simulate resource-intensive work
    return {"pool": "processing_pool", "task": "task_2"}

def success_callback_demo(context):
    """Callback function that runs on task success."""
    task_id = context['task_instance'].task_id
    print(f"✅ SUCCESS CALLBACK: Task {task_id} completed successfully")
    
    # In production, this might send notifications, update metrics, etc.
    return {"callback_type": "success", "task_id": task_id}

def failure_callback_demo(context):
    """Callback function that runs on task failure."""
    task_id = context['task_instance'].task_id
    print(f"❌ FAILURE CALLBACK: Task {task_id} failed")
    
    # In production, this might send alerts, create tickets, etc.
    return {"callback_type": "failure", "task_id": task_id}

def sla_miss_callback_demo(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Callback function that runs when SLA is missed."""
    print(f"⏰ SLA MISS CALLBACK: SLA missed for tasks: {[t.task_id for t in task_list]}")
    
    # In production, this might send escalation alerts
    return {"callback_type": "sla_miss", "affected_tasks": len(task_list)}

# Set some variables for demonstration (these would normally be set via UI or CLI)
try:
    Variable.set("environment", "production")
    Variable.set("batch_size", "1000")
except:
    pass  # Variables might already exist or permissions might not allow

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'sla': timedelta(minutes=5),  # 5 minute SLA
    'on_success_callback': success_callback_demo,
    'on_failure_callback': failure_callback_demo,
}

dag = DAG(
    'interview_concepts_dag',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    max_active_runs=1,  # Only one DAG run at a time
    max_active_tasks=3,  # Maximum 3 tasks running simultaneously
    sla_miss_callback=sla_miss_callback_demo,
    tags=["interview", "concepts", "advanced"]
)

# Start task
start = EmptyOperator(
    task_id='start',
    dag=dag
)

# Variables and connections demo
variables_task = PythonOperator(
    task_id='variables_and_connections_demo',
    python_callable=demonstrate_variables_and_connections,
    dag=dag
)

# SLA demonstration
sla_task = PythonOperator(
    task_id='sla_demo_task',
    python_callable=task_with_sla_demo,
    sla=timedelta(seconds=30),  # Short SLA for demonstration
    dag=dag
)

# Priority demonstration
high_priority = PythonOperator(
    task_id='high_priority_task',
    python_callable=high_priority_task,
    priority_weight=10,  # Higher priority
    dag=dag
)

low_priority = PythonOperator(
    task_id='low_priority_task',
    python_callable=low_priority_task,
    priority_weight=1,  # Lower priority
    dag=dag
)

# Depends on past demonstration
depends_past = PythonOperator(
    task_id='depends_on_past_task',
    python_callable=depends_on_past_task,
    depends_on_past=True,
    dag=dag
)

# Pool demonstration (these would compete for the same pool)
pool_task1 = PythonOperator(
    task_id='pool_task_1',
    python_callable=pool_task_1,
    pool='processing_pool',  # This pool needs to be created in Airflow UI
    dag=dag
)

pool_task2 = PythonOperator(
    task_id='pool_task_2',
    python_callable=pool_task_2,
    pool='processing_pool',  # This pool needs to be created in Airflow UI
    dag=dag
)

# Trigger rule demonstrations
all_success_task = EmptyOperator(
    task_id='all_success_trigger',
    trigger_rule='all_success',  # Default - all upstream tasks must succeed
    dag=dag
)

all_failed_task = EmptyOperator(
    task_id='all_failed_trigger',
    trigger_rule='all_failed',  # All upstream tasks must fail
    dag=dag
)

one_success_task = EmptyOperator(
    task_id='one_success_trigger',
    trigger_rule='one_success',  # At least one upstream task must succeed
    dag=dag
)

one_failed_task = EmptyOperator(
    task_id='one_failed_trigger',
    trigger_rule='one_failed',  # At least one upstream task must fail
    dag=dag
)

all_done_task = EmptyOperator(
    task_id='all_done_trigger',
    trigger_rule='all_done',  # All upstream tasks must be done (success or failed)
    dag=dag
)

# Task that sometimes fails for trigger rule demonstration
sometimes_fails = BashOperator(
    task_id='sometimes_fails',
    bash_command='if [ $((RANDOM % 2)) -eq 0 ]; then echo "Success"; else echo "Failure" && exit 1; fi',
    retries=0,
    dag=dag
)

# End task
end = EmptyOperator(
    task_id='end',
    dag=dag
)

# Define complex dependencies to demonstrate trigger rules
start >> variables_task >> sla_task

# Priority tasks (these will be scheduled based on priority when resources are limited)
start >> [high_priority, low_priority] >> depends_past

# Pool tasks (these will compete for the same resource pool)
start >> [pool_task1, pool_task2]

# Trigger rule demonstrations
[variables_task, sla_task, sometimes_fails] >> all_success_task
[variables_task, sla_task, sometimes_fails] >> all_failed_task
[variables_task, sla_task, sometimes_fails] >> one_success_task
[variables_task, sla_task, sometimes_fails] >> one_failed_task
[variables_task, sla_task, sometimes_fails] >> all_done_task

# All trigger rule tasks lead to end
[all_success_task, all_failed_task, one_success_task, one_failed_task, all_done_task] >> end
[depends_past, pool_task1, pool_task2] >> end