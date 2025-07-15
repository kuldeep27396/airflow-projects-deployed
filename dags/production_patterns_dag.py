"""
Production Patterns DAG
========================

This DAG demonstrates production-ready patterns commonly discussed in interviews:
- Data quality validation
- Monitoring and alerting
- Resource optimization
- Performance patterns
- Operational patterns
- Testing strategies
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
# from airflow.utils.dates import days_ago  # Deprecated in newer versions
from airflow.models import Variable
from datetime import datetime, timedelta
import json
import time
import random
import os

def data_quality_check(**context):
    """Comprehensive data quality validation."""
    print("=== Data Quality Check ===")
    
    # Simulate data quality metrics
    quality_metrics = {
        'completeness': random.uniform(0.95, 1.0),
        'accuracy': random.uniform(0.90, 0.98),
        'consistency': random.uniform(0.92, 0.99),
        'timeliness': random.uniform(0.88, 0.95),
        'validity': random.uniform(0.93, 0.99)
    }
    
    # Quality thresholds
    thresholds = {
        'completeness': 0.95,
        'accuracy': 0.90,
        'consistency': 0.90,
        'timeliness': 0.85,
        'validity': 0.90
    }
    
    # Check quality metrics
    failed_checks = []
    for metric, value in quality_metrics.items():
        threshold = thresholds[metric]
        status = "PASS" if value >= threshold else "FAIL"
        print(f"{metric.upper()}: {value:.3f} (threshold: {threshold:.3f}) - {status}")
        
        if value < threshold:
            failed_checks.append(metric)
    
    # Save quality report
    quality_report = {
        'timestamp': datetime.now().isoformat(),
        'metrics': quality_metrics,
        'thresholds': thresholds,
        'failed_checks': failed_checks,
        'overall_status': 'PASS' if not failed_checks else 'FAIL'
    }
    
    with open('/tmp/quality_report.json', 'w') as f:
        json.dump(quality_report, f, indent=2)
    
    if failed_checks:
        print(f"❌ Quality check failed for: {failed_checks}")
        # In production, this might trigger alerts but continue processing
        context['ti'].xcom_push(key='quality_status', value='FAIL')
    else:
        print("✅ All quality checks passed")
        context['ti'].xcom_push(key='quality_status', value='PASS')
    
    return quality_report

def performance_monitoring(**context):
    """Monitor task and system performance."""
    print("=== Performance Monitoring ===")
    
    # Simulate performance metrics
    start_time = time.time()
    
    # Simulate work
    work_duration = random.uniform(1, 3)
    time.sleep(work_duration)
    
    end_time = time.time()
    
    # Calculate performance metrics
    performance_metrics = {
        'execution_time': end_time - start_time,
        'memory_usage': random.uniform(50, 200),  # MB
        'cpu_usage': random.uniform(10, 80),      # %
        'disk_io': random.uniform(5, 50),         # MB/s
        'network_io': random.uniform(1, 10)       # MB/s
    }
    
    # Performance thresholds
    thresholds = {
        'execution_time': 5.0,    # seconds
        'memory_usage': 500,      # MB
        'cpu_usage': 90,          # %
        'disk_io': 100,           # MB/s
        'network_io': 50          # MB/s
    }
    
    # Check performance thresholds
    warnings = []
    for metric, value in performance_metrics.items():
        threshold = thresholds[metric]
        if value > threshold:
            warnings.append(f"{metric}: {value:.2f} > {threshold}")
    
    if warnings:
        print("⚠️  Performance warnings:")
        for warning in warnings:
            print(f"  - {warning}")
    else:
        print("✅ All performance metrics within thresholds")
    
    # Save performance data
    performance_data = {
        'timestamp': datetime.now().isoformat(),
        'metrics': performance_metrics,
        'thresholds': thresholds,
        'warnings': warnings
    }
    
    with open('/tmp/performance_data.json', 'w') as f:
        json.dump(performance_data, f, indent=2)
    
    return performance_data

def resource_optimization(**context):
    """Demonstrate resource optimization techniques."""
    print("=== Resource Optimization ===")
    
    # Simulate resource usage optimization
    optimization_techniques = {
        'memory_pooling': 'Reusing memory allocations',
        'connection_pooling': 'Reusing database connections',
        'parallel_processing': 'Using multiple CPU cores',
        'lazy_loading': 'Loading data on demand',
        'caching': 'Caching frequently accessed data'
    }
    
    for technique, description in optimization_techniques.items():
        print(f"✅ {technique}: {description}")
    
    # Simulate optimized processing
    batch_size = 1000
    total_records = 10000
    batches = total_records // batch_size
    
    print(f"Processing {total_records} records in {batches} batches of {batch_size}")
    
    for batch_num in range(1, batches + 1):
        # Simulate batch processing
        time.sleep(0.1)  # Reduced time due to optimization
        if batch_num % 3 == 0:
            print(f"  Processed batch {batch_num}/{batches}")
    
    optimization_report = {
        'total_records': total_records,
        'batch_size': batch_size,
        'batches_processed': batches,
        'techniques_used': list(optimization_techniques.keys()),
        'processing_time': batches * 0.1
    }
    
    return optimization_report

def operational_monitoring(**context):
    """Monitor operational aspects of the pipeline."""
    print("=== Operational Monitoring ===")
    
    # Check system health
    system_health = {
        'database_connection': random.choice(['healthy', 'degraded', 'unhealthy']),
        'external_api': random.choice(['available', 'slow', 'unavailable']),
        'file_system': random.choice(['normal', 'high_usage', 'full']),
        'network': random.choice(['stable', 'intermittent', 'down']),
        'dependencies': random.choice(['all_up', 'some_down', 'critical_down'])
    }
    
    # Evaluate overall system health
    critical_issues = []
    warnings = []
    
    for component, status in system_health.items():
        if status in ['unhealthy', 'unavailable', 'full', 'down', 'critical_down']:
            critical_issues.append(f"{component}: {status}")
        elif status in ['degraded', 'slow', 'high_usage', 'intermittent', 'some_down']:
            warnings.append(f"{component}: {status}")
    
    # Determine overall health
    if critical_issues:
        overall_health = 'CRITICAL'
        print("🚨 CRITICAL: System health issues detected")
        for issue in critical_issues:
            print(f"  - {issue}")
    elif warnings:
        overall_health = 'WARNING'
        print("⚠️  WARNING: System performance degraded")
        for warning in warnings:
            print(f"  - {warning}")
    else:
        overall_health = 'HEALTHY'
        print("✅ System health: All components operating normally")
    
    # Operational metrics
    operational_metrics = {
        'timestamp': datetime.now().isoformat(),
        'system_health': system_health,
        'overall_health': overall_health,
        'critical_issues': critical_issues,
        'warnings': warnings,
        'uptime': random.uniform(95, 99.9),  # %
        'error_rate': random.uniform(0, 5),   # %
        'throughput': random.uniform(800, 1200)  # records/min
    }
    
    return operational_metrics

def testing_validation(**context):
    """Demonstrate testing and validation strategies."""
    print("=== Testing and Validation ===")
    
    # Schema validation
    print("1. Schema Validation:")
    expected_schema = {
        'user_id': 'integer',
        'email': 'string',
        'created_at': 'datetime',
        'status': 'string'
    }
    
    actual_schema = {
        'user_id': 'integer',
        'email': 'string',
        'created_at': 'datetime',
        'status': 'string'
    }
    
    schema_match = expected_schema == actual_schema
    print(f"   Schema validation: {'PASS' if schema_match else 'FAIL'}")
    
    # Data type validation
    print("2. Data Type Validation:")
    data_types_valid = True
    print(f"   Data types valid: {'PASS' if data_types_valid else 'FAIL'}")
    
    # Business rule validation
    print("3. Business Rule Validation:")
    business_rules = {
        'email_format': random.choice([True, False]),
        'date_range': True,
        'required_fields': True,
        'referential_integrity': random.choice([True, False])
    }
    
    for rule, valid in business_rules.items():
        print(f"   {rule}: {'PASS' if valid else 'FAIL'}")
    
    # Unit test simulation
    print("4. Unit Test Results:")
    unit_tests = {
        'test_data_extraction': True,
        'test_data_transformation': True,
        'test_data_loading': random.choice([True, False]),
        'test_error_handling': True
    }
    
    for test, passed in unit_tests.items():
        print(f"   {test}: {'PASS' if passed else 'FAIL'}")
    
    # Integration test simulation
    print("5. Integration Test Results:")
    integration_tests = {
        'database_connectivity': True,
        'api_integration': random.choice([True, False]),
        'end_to_end_flow': True
    }
    
    for test, passed in integration_tests.items():
        print(f"   {test}: {'PASS' if passed else 'FAIL'}")
    
    # Overall test results
    all_tests = {**business_rules, **unit_tests, **integration_tests}
    failed_tests = [test for test, passed in all_tests.items() if not passed]
    
    if failed_tests:
        print(f"❌ Tests failed: {failed_tests}")
        test_status = 'FAIL'
    else:
        print("✅ All tests passed")
        test_status = 'PASS'
    
    return {
        'test_status': test_status,
        'failed_tests': failed_tests,
        'total_tests': len(all_tests),
        'passed_tests': len(all_tests) - len(failed_tests)
    }

def alerting_notification(**context):
    """Demonstrate alerting and notification patterns."""
    print("=== Alerting and Notification ===")
    
    # Get previous task results
    quality_status = context['ti'].xcom_pull(key='quality_status', task_ids='data_quality_check')
    
    # Determine alert level
    if quality_status == 'FAIL':
        alert_level = 'HIGH'
        alert_message = "Data quality checks failed - immediate attention required"
    else:
        alert_level = 'INFO'
        alert_message = "Pipeline completed successfully"
    
    # Alert channels
    alert_channels = {
        'email': ['data-team@company.com', 'ops-team@company.com'],
        'slack': ['#data-alerts', '#ops-alerts'],
        'pagerduty': 'data-team-service' if alert_level == 'HIGH' else None,
        'dashboard': 'update_metrics',
        'metrics': 'push_to_monitoring'
    }
    
    # Send alerts
    for channel, config in alert_channels.items():
        if config:
            print(f"📢 {channel.upper()}: {alert_message}")
            if isinstance(config, list):
                print(f"   Recipients: {', '.join(config)}")
            else:
                print(f"   Target: {config}")
    
    # Create incident if critical
    if alert_level == 'HIGH':
        incident_id = f"INC-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        print(f"🚨 Critical incident created: {incident_id}")
        
        incident_details = {
            'id': incident_id,
            'severity': 'high',
            'title': 'Data Quality Failure',
            'description': alert_message,
            'created_at': datetime.now().isoformat(),
            'assigned_to': 'data-team',
            'status': 'open'
        }
        
        with open(f'/tmp/incident_{incident_id}.json', 'w') as f:
            json.dump(incident_details, f, indent=2)
    
    return {
        'alert_level': alert_level,
        'message': alert_message,
        'channels_notified': list(alert_channels.keys())
    }

def cleanup_production_files():
    """Clean up files created during production patterns demo."""
    files_to_remove = [
        '/tmp/quality_report.json',
        '/tmp/performance_data.json'
    ]
    
    import glob
    # Also remove any incident files
    incident_files = glob.glob('/tmp/incident_*.json')
    files_to_remove.extend(incident_files)
    
    removed_files = []
    for file_path in files_to_remove:
        if os.path.exists(file_path):
            os.remove(file_path)
            removed_files.append(file_path)
    
    print(f"Cleaned up {len(removed_files)} production files")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'production_patterns_dag',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=["interview", "production", "patterns", "monitoring"]
) as dag:

    # Start
    start = EmptyOperator(task_id='start')

    # Data Quality Group
    with TaskGroup("data_quality_group") as quality_group:
        quality_check = PythonOperator(
            task_id='data_quality_check',
            python_callable=data_quality_check,
        )
        
        testing_task = PythonOperator(
            task_id='testing_validation',
            python_callable=testing_validation,
        )
        
        quality_check >> testing_task

    # Performance Monitoring Group
    with TaskGroup("performance_group") as performance_group:
        performance_task = PythonOperator(
            task_id='performance_monitoring',
            python_callable=performance_monitoring,
        )
        
        optimization_task = PythonOperator(
            task_id='resource_optimization',
            python_callable=resource_optimization,
        )
        
        performance_task >> optimization_task

    # Operational Monitoring
    operational_task = PythonOperator(
        task_id='operational_monitoring',
        python_callable=operational_monitoring,
    )

    # Alerting and Notifications
    alerting_task = PythonOperator(
        task_id='alerting_notification',
        python_callable=alerting_notification,
    )

    # Cleanup
    cleanup_task = PythonOperator(
        task_id='cleanup_production_files',
        python_callable=cleanup_production_files,
    )

    # Dependencies
    start >> [quality_group, performance_group, operational_task]
    [quality_group, performance_group, operational_task] >> alerting_task >> cleanup_task