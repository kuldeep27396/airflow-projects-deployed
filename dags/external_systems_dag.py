"""
External Systems DAG Example
=============================

This DAG demonstrates integration with external systems:
- BashOperator for shell commands and scripts
- HTTP requests to external APIs
- File system operations
- Email notifications (mock)
- SSH operations (simulated)

These patterns are common in production workflows for system integration.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import requests
import json
import os

def check_system_health():
    """Check external system health via API calls."""
    systems = [
        {'name': 'JSONPlaceholder API', 'url': 'https://jsonplaceholder.typicode.com/posts/1'},
        {'name': 'HTTPBin Service', 'url': 'https://httpbin.org/status/200'},
        {'name': 'GitHub API', 'url': 'https://api.github.com'}
    ]
    
    results = {}
    
    for system in systems:
        try:
            response = requests.get(system['url'], timeout=10)
            if response.status_code == 200:
                results[system['name']] = 'healthy'
                print(f"✓ {system['name']}: Healthy (Status: {response.status_code})")
            else:
                results[system['name']] = f'unhealthy (Status: {response.status_code})'
                print(f"✗ {system['name']}: Unhealthy (Status: {response.status_code})")
        except Exception as e:
            results[system['name']] = f'error: {str(e)}'
            print(f"✗ {system['name']}: Error - {str(e)}")
    
    return results

def fetch_external_data():
    """Fetch data from external API and save to file."""
    try:
        # Fetch sample data from JSONPlaceholder
        response = requests.get('https://jsonplaceholder.typicode.com/users')
        response.raise_for_status()
        
        users_data = response.json()
        
        # Save to temporary file
        output_file = '/tmp/external_users_data.json'
        with open(output_file, 'w') as f:
            json.dump(users_data, f, indent=2)
        
        print(f"Successfully fetched {len(users_data)} users")
        print(f"Data saved to: {output_file}")
        
        return output_file
    
    except Exception as e:
        print(f"Error fetching external data: {str(e)}")
        raise

def process_external_data():
    """Process the fetched external data."""
    input_file = '/tmp/external_users_data.json'
    
    if not os.path.exists(input_file):
        raise FileNotFoundError(f"Input file not found: {input_file}")
    
    with open(input_file, 'r') as f:
        users_data = json.load(f)
    
    # Process data - extract email domains
    email_domains = {}
    for user in users_data:
        email = user.get('email', '')
        if '@' in email:
            domain = email.split('@')[1]
            email_domains[domain] = email_domains.get(domain, 0) + 1
    
    # Save processed data
    output_file = '/tmp/processed_email_domains.json'
    with open(output_file, 'w') as f:
        json.dump(email_domains, f, indent=2)
    
    print("=== Email Domain Analysis ===")
    for domain, count in email_domains.items():
        print(f"{domain}: {count} users")
    
    print(f"Processed data saved to: {output_file}")

def simulate_ssh_operation():
    """Simulate SSH operation to remote server."""
    print("=== Simulating SSH Operation ===")
    print("Connecting to remote server...")
    print("Executing remote commands:")
    print("  $ whoami")
    print("  airflow_user")
    print("  $ df -h")
    print("  Filesystem      Size  Used Avail Use% Mounted on")
    print("  /dev/sda1        20G   15G  4.2G  78% /")
    print("  $ uptime")
    print("  12:34:56 up 42 days,  3:21,  1 user,  load average: 0.15, 0.20, 0.18")
    print("SSH operation completed successfully")

def send_notification(message_type, **context):
    """Simulate sending notifications to external systems."""
    execution_date = context['ds']
    dag_id = context['dag'].dag_id
    
    if message_type == 'start':
        message = f"DAG {dag_id} started processing for {execution_date}"
    elif message_type == 'success':
        message = f"DAG {dag_id} completed successfully for {execution_date}"
    else:
        message = f"DAG {dag_id} notification for {execution_date}"
    
    print("=== Notification Simulation ===")
    print(f"📧 Email: {message}")
    print(f"💬 Slack: {message}")
    print(f"📱 SMS: {message}")
    print("Notifications sent successfully")

def cleanup_temp_files():
    """Clean up temporary files created during processing."""
    temp_files = [
        '/tmp/external_users_data.json',
        '/tmp/processed_email_domains.json',
        '/tmp/system_report.txt'
    ]
    
    cleaned_files = []
    for file_path in temp_files:
        if os.path.exists(file_path):
            os.remove(file_path)
            cleaned_files.append(file_path)
    
    if cleaned_files:
        print(f"Cleaned up {len(cleaned_files)} temporary files:")
        for file_path in cleaned_files:
            print(f"  - {file_path}")
    else:
        print("No temporary files to clean up")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': False,  # Would be True in production
    'email_on_retry': False,
}

dag = DAG(
    'external_systems_integration_dag',
    default_args=default_args,
    schedule=None,  # Manual trigger for testing
    catchup=False,
    tags=["learning", "external", "integration"]
)

# Start notification
start_notification = PythonOperator(
    task_id='send_start_notification',
    python_callable=send_notification,
    op_kwargs={'message_type': 'start'},
    dag=dag
)

# System health checks
health_check_task = PythonOperator(
    task_id='check_system_health',
    python_callable=check_system_health,
    dag=dag
)

# File system operations
create_directories = BashOperator(
    task_id='create_work_directories',
    bash_command="""
    echo "Creating work directories..."
    mkdir -p /tmp/airflow_work/input
    mkdir -p /tmp/airflow_work/output
    mkdir -p /tmp/airflow_work/logs
    echo "Work directories created successfully"
    ls -la /tmp/airflow_work/
    """,
    dag=dag
)

# External data operations
fetch_data_task = PythonOperator(
    task_id='fetch_external_data',
    python_callable=fetch_external_data,
    dag=dag
)

process_data_task = PythonOperator(
    task_id='process_external_data',
    python_callable=process_external_data,
    dag=dag
)

# System operations
ssh_operation = PythonOperator(
    task_id='simulate_ssh_operation',
    python_callable=simulate_ssh_operation,
    dag=dag
)

# Generate system report
generate_report = BashOperator(
    task_id='generate_system_report',
    bash_command="""
    echo "=== System Report ===" > /tmp/system_report.txt
    echo "Generated on: $(date)" >> /tmp/system_report.txt
    echo "" >> /tmp/system_report.txt
    echo "System Information:" >> /tmp/system_report.txt
    echo "OS: $(uname -s)" >> /tmp/system_report.txt
    echo "Kernel: $(uname -r)" >> /tmp/system_report.txt
    echo "Architecture: $(uname -m)" >> /tmp/system_report.txt
    echo "" >> /tmp/system_report.txt
    echo "Disk Usage:" >> /tmp/system_report.txt
    df -h >> /tmp/system_report.txt
    echo "" >> /tmp/system_report.txt
    echo "Memory Usage:" >> /tmp/system_report.txt
    if command -v free >/dev/null 2>&1; then
        free -h >> /tmp/system_report.txt
    else
        echo "Memory info not available on this system" >> /tmp/system_report.txt
    fi
    echo ""
    echo "System report generated:"
    cat /tmp/system_report.txt
    """,
    dag=dag
)

# Archive and compress data
archive_data = BashOperator(
    task_id='archive_processed_data',
    bash_command="""
    echo "Creating archive of processed data..."
    cd /tmp
    tar -czf airflow_external_data_{{ ds_nodash }}.tar.gz *.json system_report.txt 2>/dev/null || echo "Some files may not exist"
    echo "Archive created: airflow_external_data_{{ ds_nodash }}.tar.gz"
    ls -lh airflow_external_data_{{ ds_nodash }}.tar.gz
    """,
    dag=dag
)

# Success notification
success_notification = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_notification,
    op_kwargs={'message_type': 'success'},
    dag=dag
)

# Cleanup
cleanup_task = PythonOperator(
    task_id='cleanup_temp_files',
    python_callable=cleanup_temp_files,
    dag=dag
)

# Define dependencies
start_notification >> [health_check_task, create_directories]
health_check_task >> fetch_data_task >> process_data_task
create_directories >> ssh_operation >> generate_report
[process_data_task, generate_report] >> archive_data >> success_notification >> cleanup_task