"""
Error Handling and Retry DAG Example
=====================================

This DAG demonstrates comprehensive error handling and retry strategies in Airflow:
- Different types of failures and recovery mechanisms
- Retry policies and exponential backoff
- Task failure callbacks and notifications
- Circuit breaker patterns
- Graceful degradation strategies

These patterns are essential for building resilient production workflows.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.exceptions import AirflowException, AirflowSkipException
from datetime import datetime, timedelta
import random
import time
import json
import os
from typing import Dict, Any

def simulate_transient_failure(**context):
    """Simulate a transient failure that might succeed on retry."""
    task_instance = context['task_instance']
    try_number = task_instance.try_number
    
    print(f"=== Transient Failure Simulation - Attempt {try_number} ===")
    
    # Simulate transient failure - 70% chance of failure on first try, 30% on second
    failure_probability = 0.7 if try_number == 1 else 0.3 if try_number == 2 else 0.1
    
    if random.random() < failure_probability:
        print(f"💥 Simulating transient failure (attempt {try_number})")
        print("This might be a temporary network issue, database timeout, or resource contention")
        raise AirflowException(f"Transient failure occurred on attempt {try_number}")
    
    print(f"✅ Success on attempt {try_number}")
    return {
        'status': 'success',
        'attempts': try_number,
        'message': f'Task succeeded after {try_number} attempts'
    }

def simulate_data_processing_with_errors(**context):
    """Simulate data processing that might fail with different error types."""
    task_instance = context['task_instance']
    try_number = task_instance.try_number
    
    print(f"=== Data Processing - Attempt {try_number} ===")
    
    # Different error scenarios based on attempt number
    if try_number == 1:
        # Simulate missing input file
        print("❌ Input file not found - this is a common data pipeline error")
        raise FileNotFoundError("Input data file /tmp/missing_file.csv not found")
    
    elif try_number == 2:
        # Simulate data validation error
        print("❌ Data validation failed - corrupted or invalid data")
        raise ValueError("Data validation failed: Invalid data format detected")
    
    elif try_number == 3:
        # Simulate resource exhaustion
        print("❌ Resource exhaustion - memory or disk space issues")
        raise MemoryError("Insufficient memory to process dataset")
    
    else:
        # Finally succeed
        print("✅ Data processing completed successfully")
        return {
            'status': 'success',
            'records_processed': 1000,
            'processing_time': '45 seconds'
        }

def simulate_external_api_failure(**context):
    """Simulate external API calls that might fail."""
    task_instance = context['task_instance']
    try_number = task_instance.try_number
    
    print(f"=== External API Call - Attempt {try_number} ===")
    
    # Simulate different API failure scenarios
    failure_scenarios = [
        "Connection timeout",
        "HTTP 500 Internal Server Error",
        "Rate limit exceeded",
        "Authentication failure",
        "Service unavailable"
    ]
    
    if try_number <= len(failure_scenarios):
        error_type = failure_scenarios[try_number - 1]
        print(f"🌐 API call failed: {error_type}")
        raise AirflowException(f"External API error: {error_type}")
    
    print("✅ API call successful")
    return {
        'status': 'success',
        'api_response': {'data': 'Successfully retrieved from external API'},
        'response_time': '2.3 seconds'
    }

def conditional_failure_task(**context):
    """Task that fails conditionally based on business logic."""
    execution_date = context['execution_date']
    
    print(f"=== Conditional Failure Check - {execution_date} ===")
    
    # Example: Fail if execution is on weekends (for demo purposes)
    if execution_date.weekday() >= 5:  # Saturday = 5, Sunday = 6
        print("⚠️  Weekend execution detected - skipping task")
        raise AirflowSkipException("Task skipped: Weekend execution not allowed")
    
    # Example: Fail if hour is odd (for demo purposes)
    if execution_date.hour % 2 == 1:
        print("❌ Odd hour execution - simulating business rule failure")
        raise AirflowException("Business rule violation: Odd hour execution not permitted")
    
    print("✅ All conditions met - task proceeding")
    return {'status': 'success', 'conditions_checked': ['weekday', 'hour']}

def circuit_breaker_task(**context):
    """Implement circuit breaker pattern for external service calls."""
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    
    print("=== Circuit Breaker Pattern ===")
    
    # Check circuit breaker state (in production, this would use a shared state store)
    circuit_breaker_file = f'/tmp/circuit_breaker_{dag_id}.json'
    circuit_state = {'state': 'closed', 'failure_count': 0, 'last_failure': None}
    
    if os.path.exists(circuit_breaker_file):
        with open(circuit_breaker_file, 'r') as f:
            circuit_state = json.load(f)
    
    print(f"Circuit breaker state: {circuit_state['state']}")
    print(f"Failure count: {circuit_state['failure_count']}")
    
    # Check if circuit is open
    if circuit_state['state'] == 'open':
        last_failure = datetime.fromisoformat(circuit_state['last_failure'])
        cooldown_period = timedelta(minutes=5)  # 5 minute cooldown
        
        if datetime.now() - last_failure < cooldown_period:
            print("🔴 Circuit breaker is OPEN - failing fast")
            raise AirflowException("Circuit breaker is open - service unavailable")
        else:
            print("🟡 Circuit breaker moving to HALF-OPEN state")
            circuit_state['state'] = 'half_open'
    
    # Simulate service call
    success = random.random() > 0.4  # 60% success rate
    
    if success:
        print("✅ Service call successful")
        # Reset circuit breaker
        circuit_state = {'state': 'closed', 'failure_count': 0, 'last_failure': None}
        result = {'status': 'success', 'circuit_state': 'closed'}
    else:
        print("❌ Service call failed")
        circuit_state['failure_count'] += 1
        circuit_state['last_failure'] = datetime.now().isoformat()
        
        # Open circuit if failure threshold reached
        if circuit_state['failure_count'] >= 3:
            circuit_state['state'] = 'open'
            print("🔴 Circuit breaker opened due to repeated failures")
        
        result = {'status': 'failed', 'circuit_state': circuit_state['state']}
    
    # Save circuit breaker state
    with open(circuit_breaker_file, 'w') as f:
        json.dump(circuit_state, f)
    
    if not success:
        raise AirflowException("Service call failed")
    
    return result

def graceful_degradation_task(**context):
    """Demonstrate graceful degradation when primary service fails."""
    print("=== Graceful Degradation Example ===")
    
    # Try primary service
    try:
        print("🎯 Attempting primary service...")
        # Simulate primary service failure
        if random.random() < 0.7:  # 70% failure rate
            raise AirflowException("Primary service unavailable")
        
        print("✅ Primary service successful")
        return {'status': 'success', 'service_used': 'primary', 'quality': 'high'}
    
    except AirflowException:
        print("⚠️  Primary service failed, trying secondary service...")
        
        # Try secondary service
        try:
            time.sleep(1)  # Simulate processing time
            if random.random() < 0.3:  # 30% failure rate
                raise AirflowException("Secondary service also unavailable")
            
            print("✅ Secondary service successful")
            return {'status': 'success', 'service_used': 'secondary', 'quality': 'medium'}
        
        except AirflowException:
            print("⚠️  Secondary service failed, using cached data...")
            
            # Use cached/fallback data
            return {'status': 'success', 'service_used': 'cache', 'quality': 'low'}

def task_failure_callback(context):
    """Callback function that runs when a task fails."""
    task_instance = context['task_instance']
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    execution_date = context['execution_date']
    
    print(f"🚨 TASK FAILURE CALLBACK")
    print(f"   DAG: {dag_id}")
    print(f"   Task: {task_id}")
    print(f"   Execution Date: {execution_date}")
    print(f"   Try Number: {task_instance.try_number}")
    
    # In production, this would send notifications, create tickets, etc.
    failure_report = {
        'dag_id': dag_id,
        'task_id': task_id,
        'execution_date': execution_date.isoformat(),
        'try_number': task_instance.try_number,
        'failure_time': datetime.now().isoformat()
    }
    
    # Save failure report
    with open(f'/tmp/failure_report_{dag_id}_{task_id}.json', 'w') as f:
        json.dump(failure_report, f, indent=2)
    
    print("   Failure report saved and notifications sent")

def task_success_callback(context):
    """Callback function that runs when a task succeeds after failures."""
    task_instance = context['task_instance']
    
    if task_instance.try_number > 1:
        print(f"🎉 TASK RECOVERY SUCCESS")
        print(f"   Task succeeded after {task_instance.try_number} attempts")
        
        # Clean up circuit breaker state on success
        dag_id = context['dag'].dag_id
        circuit_breaker_file = f'/tmp/circuit_breaker_{dag_id}.json'
        if os.path.exists(circuit_breaker_file):
            os.remove(circuit_breaker_file)
            print("   Circuit breaker state reset")

def retry_delay_function(context):
    """Custom retry delay function with exponential backoff."""
    try_number = context['task_instance'].try_number
    
    # Exponential backoff: 30s, 60s, 120s, 240s, 300s (max 5 minutes)
    delay_seconds = min(30 * (2 ** (try_number - 1)), 300)
    
    print(f"⏱️  Retry delay for attempt {try_number}: {delay_seconds} seconds")
    return timedelta(seconds=delay_seconds)

def cleanup_error_handling_files():
    """Clean up files created during error handling demonstration."""
    cleanup_patterns = [
        '/tmp/circuit_breaker_*.json',
        '/tmp/failure_report_*.json'
    ]
    
    import glob
    cleaned_files = []
    
    for pattern in cleanup_patterns:
        for file_path in glob.glob(pattern):
            if os.path.exists(file_path):
                os.remove(file_path)
                cleaned_files.append(file_path)
    
    print(f"Cleaned up {len(cleaned_files)} error handling files")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 3,  # Allow up to 3 retries
    'retry_delay': timedelta(minutes=1),  # Default retry delay
    'on_failure_callback': task_failure_callback,
    'on_success_callback': task_success_callback,
}

dag = DAG(
    'error_handling_retry_dag',
    default_args=default_args,
    schedule=None,  # Manual trigger for testing
    catchup=False,
    tags=["learning", "error-handling", "retry", "resilience"]
)

# Start task
start_task = EmptyOperator(
    task_id='start_error_handling_demo',
    dag=dag
)

# Transient failure with standard retry
transient_failure_task = PythonOperator(
    task_id='transient_failure_example',
    python_callable=simulate_transient_failure,
    retries=3,
    retry_delay=timedelta(seconds=30),
    dag=dag
)

# Data processing with multiple error types
data_processing_task = PythonOperator(
    task_id='data_processing_with_errors',
    python_callable=simulate_data_processing_with_errors,
    retries=4,  # More retries for data processing
    retry_delay=timedelta(seconds=45),
    dag=dag
)

# External API with exponential backoff
api_failure_task = PythonOperator(
    task_id='external_api_failure',
    python_callable=simulate_external_api_failure,
    retries=5,
    retry_delay=retry_delay_function,  # Custom retry delay function
    dag=dag
)

# Conditional failure/skip
conditional_task = PythonOperator(
    task_id='conditional_failure',
    python_callable=conditional_failure_task,
    retries=1,  # Don't retry business logic failures
    dag=dag
)

# Circuit breaker pattern
circuit_breaker_task = PythonOperator(
    task_id='circuit_breaker_pattern',
    python_callable=circuit_breaker_task,
    retries=2,
    retry_delay=timedelta(seconds=60),
    dag=dag
)

# Graceful degradation
degradation_task = PythonOperator(
    task_id='graceful_degradation',
    python_callable=graceful_degradation_task,
    retries=0,  # Handle failures within the task
    dag=dag
)

# Task that always fails (for callback demonstration)
always_fail_task = BashOperator(
    task_id='always_fail_task',
    bash_command='echo "This task will always fail" && exit 1',
    retries=2,
    retry_delay=timedelta(seconds=15),
    dag=dag
)

# Recovery task that runs after failures
recovery_task = PythonOperator(
    task_id='recovery_and_notification',
    python_callable=lambda: print("🔄 Recovery procedures initiated"),
    trigger_rule='one_failed',  # Run if any upstream task fails
    dag=dag
)

# Cleanup task
cleanup_task = PythonOperator(
    task_id='cleanup_error_handling_files',
    python_callable=cleanup_error_handling_files,
    trigger_rule='all_done',  # Run regardless of upstream task states
    dag=dag
)

# Define dependencies
start_task >> [transient_failure_task, data_processing_task, api_failure_task]
[transient_failure_task, data_processing_task, api_failure_task] >> conditional_task
conditional_task >> [circuit_breaker_task, degradation_task, always_fail_task]
[circuit_breaker_task, degradation_task, always_fail_task] >> recovery_task
recovery_task >> cleanup_task