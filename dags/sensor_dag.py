"""
Sensor DAG Example
==================

This DAG demonstrates different types of sensors in Airflow:
- FileSensor: Waits for a file to appear in filesystem
- HttpSensor: Waits for HTTP endpoint to return success
- DateTimeSensor: Waits until a specific datetime
- TimeDeltaSensor: Waits for a specific time duration

Sensors are useful for waiting for external conditions before proceeding with workflow execution.
"""
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
# from airflow.providers.http.sensors.http import HttpSensor  # Requires HTTP provider
from airflow.sensors.date_time import DateTimeSensor
from airflow.sensors.time_delta import TimeDeltaSensor
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

def create_test_file():
    """Create a test file that the FileSensor will detect."""
    file_path = "/tmp/test_sensor_file.txt"
    with open(file_path, "w") as f:
        f.write("This file was created by Airflow!")
    print(f"Created test file: {file_path}")

def process_after_sensors():
    """Function that runs after all sensors complete."""
    print("All sensors completed successfully!")
    print("Processing can now begin...")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'sensor_examples_dag',
    default_args=default_args,
    schedule=None,  # Manual trigger for testing
    catchup=False,
    tags=["learning", "sensors", "intermediate"]
)

# Create test file task
create_file_task = PythonOperator(
    task_id='create_test_file',
    python_callable=create_test_file,
    dag=dag
)

# FileSensor - waits for file to exist
file_sensor = FileSensor(
    task_id='wait_for_file',
    filepath='/tmp/test_sensor_file.txt',
    poke_interval=10,  # Check every 10 seconds
    timeout=300,  # Timeout after 5 minutes
    dag=dag
)

# HttpSensor - waits for HTTP endpoint (using a public API)
# http_sensor = HttpSensor(
#     task_id='wait_for_api',
#     http_conn_id='http_default',  # Uses default HTTP connection
#     endpoint='https://httpbin.org/status/200',
#     poke_interval=30,
#     timeout=300,
#     dag=dag
# )

# Mock HTTP sensor with Python task
def mock_http_sensor():
    """Mock HTTP sensor functionality."""
    try:
        response = requests.get('https://httpbin.org/status/200', timeout=10)
        if response.status_code == 200:
            print("✅ HTTP endpoint is available")
        else:
            print(f"❌ HTTP endpoint returned status: {response.status_code}")
    except Exception as e:
        print(f"❌ HTTP request failed: {e}")

http_sensor = PythonOperator(
    task_id='wait_for_api',
    python_callable=mock_http_sensor,
    dag=dag
)

# DateTimeSensor - waits until specific datetime (1 minute from start)
datetime_sensor = DateTimeSensor(
    task_id='wait_for_datetime',
    target_time="{{ (execution_date + macros.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S') }}",
    poke_interval=10,
    dag=dag
)

# TimeDeltaSensor - waits for specific duration (30 seconds)
timedelta_sensor = TimeDeltaSensor(
    task_id='wait_for_timedelta',
    delta=timedelta(seconds=30),
    dag=dag
)

# Cleanup task
cleanup_task = BashOperator(
    task_id='cleanup',
    bash_command='rm -f /tmp/test_sensor_file.txt',
    dag=dag
)

# Final processing task
process_task = PythonOperator(
    task_id='process_after_sensors',
    python_callable=process_after_sensors,
    dag=dag
)

# Define dependencies
create_file_task >> file_sensor
[file_sensor, http_sensor, datetime_sensor, timedelta_sensor] >> process_task >> cleanup_task