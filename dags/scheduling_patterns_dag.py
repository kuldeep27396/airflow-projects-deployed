"""
Scheduling Patterns DAG Example
================================

This DAG demonstrates various scheduling patterns in Airflow:
- Different schedule intervals (cron, preset, timedelta)
- Catchup behavior
- Data interval concepts
- Schedule dependencies

Note: This file contains multiple DAG definitions to show different scheduling patterns.
Each DAG demonstrates a specific scheduling concept.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

def log_schedule_info(**context):
    """Log information about the current schedule and execution."""
    print("=== Schedule Information ===")
    print(f"Logical Date: {context['logical_date']}")
    print(f"Data Interval Start: {context['data_interval_start']}")
    print(f"Data Interval End: {context['data_interval_end']}")
    print(f"Execution Date: {context['ds']}")
    print(f"Next Execution: {context['next_ds']}")
    print(f"Previous Execution: {context['prev_ds']}")

# Common default args
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# ============================================================================
# DAG 1: Daily Schedule with Catchup
# ============================================================================
daily_dag = DAG(
    'daily_schedule_with_catchup',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule='@daily',  # Run every day at midnight
    catchup=True,  # Will backfill from start_date to current
    max_active_runs=3,
    tags=["learning", "scheduling", "daily"]
)

daily_task = PythonOperator(
    task_id='daily_processing',
    python_callable=log_schedule_info,
    dag=daily_dag
)

# ============================================================================
# DAG 2: Hourly Schedule without Catchup
# ============================================================================
hourly_dag = DAG(
    'hourly_schedule_no_catchup',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule='@hourly',  # Run every hour
    catchup=False,  # No backfilling
    max_active_runs=1,
    tags=["learning", "scheduling", "hourly"]
)

hourly_task = PythonOperator(
    task_id='hourly_processing',
    python_callable=log_schedule_info,
    dag=hourly_dag
)

# ============================================================================
# DAG 3: Custom Cron Schedule
# ============================================================================
cron_dag = DAG(
    'custom_cron_schedule',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule='30 2 * * 1-5',  # 2:30 AM, Monday to Friday
    catchup=False,
    tags=["learning", "scheduling", "cron"]
)

cron_task = BashOperator(
    task_id='weekday_processing',
    bash_command="""
    echo "Running weekday batch job at 2:30 AM"
    echo "Current time: $(date)"
    echo "This runs Monday through Friday only"
    """,
    dag=cron_dag
)

# ============================================================================
# DAG 4: Timedelta Schedule
# ============================================================================
timedelta_dag = DAG(
    'timedelta_schedule',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=timedelta(hours=6),  # Run every 6 hours
    catchup=False,
    tags=["learning", "scheduling", "timedelta"]
)

timedelta_task = PythonOperator(
    task_id='every_six_hours',
    python_callable=log_schedule_info,
    dag=timedelta_dag
)

# ============================================================================
# DAG 5: Manual Only (No Schedule)
# ============================================================================
manual_dag = DAG(
    'manual_trigger_only',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=["learning", "scheduling", "manual"]
)

manual_task = BashOperator(
    task_id='manual_processing',
    bash_command='echo "This DAG only runs when manually triggered"',
    dag=manual_dag
)

# ============================================================================
# DAG 6: Weekly Schedule with Complex Logic
# ============================================================================
def weekly_processing(**context):
    """Weekly processing with different logic based on week number."""
    logical_date = context['logical_date']
    week_number = int(logical_date.strftime('%U'))
    
    print(f"=== Weekly Processing - Week {week_number} ===")
    print(f"Date: {logical_date.strftime('%Y-%m-%d')}")
    
    if week_number % 2 == 0:
        print("Even week: Running full data refresh")
    else:
        print("Odd week: Running incremental update")
    
    if logical_date.day <= 7:  # First week of month
        print("First week of month: Running monthly reports")

weekly_dag = DAG(
    'weekly_schedule_complex',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule='0 3 * * 0',  # 3 AM every Sunday
    catchup=False,
    tags=["learning", "scheduling", "weekly"]
)

weekly_task = PythonOperator(
    task_id='weekly_processing',
    python_callable=weekly_processing,
    dag=weekly_dag
)

# ============================================================================
# DAG 7: Monthly Schedule (First Day of Month)
# ============================================================================
monthly_dag = DAG(
    'monthly_schedule_first_day',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule='0 4 1 * *',  # 4 AM on the 1st of every month
    catchup=False,
    tags=["learning", "scheduling", "monthly"]
)

monthly_task = BashOperator(
    task_id='monthly_processing',
    bash_command="""
    echo "Monthly processing on first day of month"
    echo "Current date: $(date)"
    echo "Processing previous month's data"
    """,
    dag=monthly_dag
)

# ============================================================================
# DAG 8: Multiple Schedule Intervals Demo
# ============================================================================
def demonstrate_intervals(**context):
    """Demonstrate different schedule interval formats."""
    print("=== Schedule Interval Examples ===")
    print("@once - Run once when DAG is deployed")
    print("@hourly - 0 * * * * (every hour)")
    print("@daily - 0 0 * * * (daily at midnight)")
    print("@weekly - 0 0 * * 0 (weekly on Sunday)")
    print("@monthly - 0 0 1 * * (monthly on 1st)")
    print("@yearly - 0 0 1 1 * (yearly on Jan 1st)")
    print("")
    print("Custom examples:")
    print("'*/15 * * * *' - Every 15 minutes")
    print("'0 9,17 * * 1-5' - 9 AM and 5 PM on weekdays")
    print("'0 0 */2 * *' - Every 2 days")
    print("'0 0 * * 1' - Every Monday")

demo_dag = DAG(
    'schedule_intervals_demo',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Manual only for demo
    catchup=False,
    tags=["learning", "scheduling", "demo"]
)

demo_task = PythonOperator(
    task_id='demonstrate_intervals',
    python_callable=demonstrate_intervals,
    dag=demo_dag
)