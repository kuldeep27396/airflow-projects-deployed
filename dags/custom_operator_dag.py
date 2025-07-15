"""
Custom Operator DAG Example
============================

This DAG demonstrates how to create and use custom operators in Airflow:
- Custom Python operators with specific business logic
- Custom sensor operators
- Custom hook integration
- Operator inheritance and reusability

Custom operators are useful for encapsulating complex business logic
and making it reusable across multiple DAGs.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import BaseOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context
# apply_defaults is deprecated in newer Airflow versions
from datetime import datetime, timedelta
import time
import random
import json
import os
from typing import Any, Dict, Optional


class DataValidationOperator(BaseOperator):
    """
    Custom operator for data validation with configurable rules.
    
    This operator validates data files against specified rules and
    can fail the task if validation criteria are not met.
    """
    
    template_fields = ['file_path', 'validation_rules']
    
    def __init__(
        self,
        file_path: str,
        validation_rules: Dict[str, Any],
        fail_on_error: bool = True,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.file_path = file_path
        self.validation_rules = validation_rules
        self.fail_on_error = fail_on_error
    
    def execute(self, context: Context):
        self.log.info(f"Starting data validation for: {self.file_path}")
        
        # Check if file exists
        if not os.path.exists(self.file_path):
            error_msg = f"File not found: {self.file_path}"
            if self.fail_on_error:
                raise FileNotFoundError(error_msg)
            else:
                self.log.warning(error_msg)
                return {'status': 'warning', 'message': error_msg}
        
        # Read file content
        try:
            with open(self.file_path, 'r') as f:
                if self.file_path.endswith('.json'):
                    data = json.load(f)
                else:
                    data = f.read()
        except Exception as e:
            error_msg = f"Error reading file: {str(e)}"
            if self.fail_on_error:
                raise Exception(error_msg)
            else:
                self.log.warning(error_msg)
                return {'status': 'warning', 'message': error_msg}
        
        # Validate data
        validation_results = []
        
        for rule_name, rule_config in self.validation_rules.items():
            self.log.info(f"Applying validation rule: {rule_name}")
            
            if rule_name == 'min_size':
                if len(str(data)) < rule_config:
                    validation_results.append(f"FAIL: Data size {len(str(data))} < {rule_config}")
                else:
                    validation_results.append(f"PASS: Data size validation")
            
            elif rule_name == 'required_keys' and isinstance(data, (dict, list)):
                if isinstance(data, dict):
                    missing_keys = set(rule_config) - set(data.keys())
                    if missing_keys:
                        validation_results.append(f"FAIL: Missing keys: {missing_keys}")
                    else:
                        validation_results.append(f"PASS: Required keys validation")
                elif isinstance(data, list) and data:
                    # Check first item if it's a list of dicts
                    if isinstance(data[0], dict):
                        missing_keys = set(rule_config) - set(data[0].keys())
                        if missing_keys:
                            validation_results.append(f"FAIL: Missing keys in list items: {missing_keys}")
                        else:
                            validation_results.append(f"PASS: Required keys validation")
            
            elif rule_name == 'not_empty':
                if not data or (isinstance(data, (list, dict)) and len(data) == 0):
                    validation_results.append(f"FAIL: Data is empty")
                else:
                    validation_results.append(f"PASS: Not empty validation")
        
        # Log results
        self.log.info("Validation Results:")
        for result in validation_results:
            self.log.info(f"  {result}")
        
        # Check if any validation failed
        failed_validations = [r for r in validation_results if r.startswith('FAIL')]
        
        if failed_validations and self.fail_on_error:
            raise ValueError(f"Data validation failed: {failed_validations}")
        
        return {
            'status': 'success' if not failed_validations else 'warning',
            'results': validation_results,
            'file_path': self.file_path
        }


class FileProcessingOperator(BaseOperator):
    """
    Custom operator for file processing with configurable actions.
    
    This operator can perform various file operations like copying,
    moving, transforming, or archiving files.
    """
    
    template_fields = ['source_path', 'destination_path', 'processing_config']
    
    def __init__(
        self,
        source_path: str,
        destination_path: str,
        processing_config: Dict[str, Any],
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.source_path = source_path
        self.destination_path = destination_path
        self.processing_config = processing_config
    
    def execute(self, context: Context):
        self.log.info(f"Processing file: {self.source_path} -> {self.destination_path}")
        
        # Create sample data if source doesn't exist
        if not os.path.exists(self.source_path):
            self.log.info(f"Creating sample data at: {self.source_path}")
            sample_data = {
                'timestamp': datetime.now().isoformat(),
                'data': [{'id': i, 'value': random.randint(1, 100)} for i in range(10)]
            }
            with open(self.source_path, 'w') as f:
                json.dump(sample_data, f, indent=2)
        
        # Read source file
        with open(self.source_path, 'r') as f:
            if self.source_path.endswith('.json'):
                data = json.load(f)
            else:
                data = f.read()
        
        # Apply processing actions
        processed_data = data
        
        for action, config in self.processing_config.items():
            self.log.info(f"Applying processing action: {action}")
            
            if action == 'filter' and isinstance(processed_data, dict) and 'data' in processed_data:
                # Filter data based on criteria
                if 'min_value' in config:
                    processed_data['data'] = [
                        item for item in processed_data['data']
                        if item.get('value', 0) >= config['min_value']
                    ]
                    self.log.info(f"Filtered data: {len(processed_data['data'])} items remaining")
            
            elif action == 'transform' and isinstance(processed_data, dict):
                # Add transformation metadata
                processed_data['processing_info'] = {
                    'processed_at': datetime.now().isoformat(),
                    'dag_id': context['dag'].dag_id,
                    'task_id': context['task'].task_id
                }
                self.log.info("Added transformation metadata")
            
            elif action == 'aggregate' and isinstance(processed_data, dict) and 'data' in processed_data:
                # Add aggregation statistics
                values = [item.get('value', 0) for item in processed_data['data']]
                processed_data['statistics'] = {
                    'count': len(values),
                    'sum': sum(values),
                    'average': sum(values) / len(values) if values else 0,
                    'min': min(values) if values else 0,
                    'max': max(values) if values else 0
                }
                self.log.info("Added aggregation statistics")
        
        # Write processed data
        with open(self.destination_path, 'w') as f:
            if self.destination_path.endswith('.json'):
                json.dump(processed_data, f, indent=2)
            else:
                f.write(str(processed_data))
        
        self.log.info(f"File processing completed: {self.destination_path}")
        
        return {
            'source_path': self.source_path,
            'destination_path': self.destination_path,
            'processing_actions': list(self.processing_config.keys()),
            'status': 'completed'
        }


class CustomThresholdSensor(BaseSensorOperator):
    """
    Custom sensor that monitors a metric and triggers when threshold is met.
    
    This sensor can monitor various metrics like file size, data count,
    or custom business metrics.
    """
    
    template_fields = ['metric_source', 'threshold_config']
    
    def __init__(
        self,
        metric_source: str,
        threshold_config: Dict[str, Any],
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.metric_source = metric_source
        self.threshold_config = threshold_config
    
    def poke(self, context: Context) -> bool:
        self.log.info(f"Checking threshold for metric source: {self.metric_source}")
        
        # Get current metric value
        current_value = self._get_metric_value()
        
        # Check threshold
        threshold_type = self.threshold_config.get('type', 'greater_than')
        threshold_value = self.threshold_config.get('value', 0)
        
        if threshold_type == 'greater_than':
            condition_met = current_value > threshold_value
        elif threshold_type == 'less_than':
            condition_met = current_value < threshold_value
        elif threshold_type == 'equals':
            condition_met = current_value == threshold_value
        else:
            condition_met = False
        
        self.log.info(f"Current value: {current_value}, Threshold: {threshold_value} ({threshold_type})")
        self.log.info(f"Condition met: {condition_met}")
        
        return condition_met
    
    def _get_metric_value(self) -> float:
        """Get the current metric value from the source."""
        if self.metric_source == 'file_size':
            # Check file size
            file_path = '/tmp/sample_data.json'
            if os.path.exists(file_path):
                return os.path.getsize(file_path)
            return 0
        
        elif self.metric_source == 'random_metric':
            # Generate random value for demonstration
            return random.randint(1, 100)
        
        elif self.metric_source == 'data_count':
            # Check data count in JSON file
            file_path = '/tmp/processed_data.json'
            if os.path.exists(file_path):
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                    if isinstance(data, dict) and 'data' in data:
                        return len(data['data'])
                    elif isinstance(data, list):
                        return len(data)
                except:
                    return 0
            return 0
        
        return 0


class NotificationOperator(BaseOperator):
    """
    Custom operator for sending notifications with different channels.
    
    This operator can send notifications via email, Slack, webhooks,
    or other communication channels.
    """
    
    template_fields = ['message', 'notification_config']
    
    def __init__(
        self,
        message: str,
        notification_config: Dict[str, Any],
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.message = message
        self.notification_config = notification_config
    
    def execute(self, context: Context):
        self.log.info("Sending notifications...")
        
        # Get context information
        dag_id = context['dag'].dag_id
        task_id = context['task'].task_id
        execution_date = context['ds']
        
        # Format message with context
        formatted_message = self.message.format(
            dag_id=dag_id,
            task_id=task_id,
            execution_date=execution_date
        )
        
        notification_results = []
        
        # Send notifications to configured channels
        for channel, config in self.notification_config.items():
            self.log.info(f"Sending notification to {channel}")
            
            if channel == 'email':
                # Simulate email notification
                self.log.info(f"📧 Email sent to: {config.get('recipients', ['default@example.com'])}")
                self.log.info(f"   Subject: {config.get('subject', 'Airflow Notification')}")
                self.log.info(f"   Message: {formatted_message}")
                notification_results.append({'channel': 'email', 'status': 'sent'})
            
            elif channel == 'slack':
                # Simulate Slack notification
                self.log.info(f"💬 Slack message sent to: {config.get('channel', '#general')}")
                self.log.info(f"   Message: {formatted_message}")
                notification_results.append({'channel': 'slack', 'status': 'sent'})
            
            elif channel == 'webhook':
                # Simulate webhook notification
                self.log.info(f"🔗 Webhook sent to: {config.get('url', 'https://example.com/webhook')}")
                self.log.info(f"   Payload: {{'message': '{formatted_message}'}}")
                notification_results.append({'channel': 'webhook', 'status': 'sent'})
            
            elif channel == 'log':
                # Log notification
                self.log.info(f"📝 Log notification: {formatted_message}")
                notification_results.append({'channel': 'log', 'status': 'logged'})
        
        self.log.info("All notifications sent successfully")
        
        return {
            'message': formatted_message,
            'channels': list(self.notification_config.keys()),
            'results': notification_results,
            'status': 'completed'
        }


def create_sample_data_for_validation():
    """Create sample data files for custom operator testing."""
    # Create valid JSON file
    valid_data = {
        'timestamp': datetime.now().isoformat(),
        'data': [{'id': i, 'name': f'Item {i}', 'value': i * 10} for i in range(1, 6)],
        'metadata': {
            'source': 'sample_generator',
            'version': '1.0'
        }
    }
    
    with open('/tmp/valid_data.json', 'w') as f:
        json.dump(valid_data, f, indent=2)
    
    # Create invalid JSON file (missing required keys)
    invalid_data = {
        'timestamp': datetime.now().isoformat(),
        'items': [{'id': i} for i in range(1, 3)]  # Missing 'data' key
    }
    
    with open('/tmp/invalid_data.json', 'w') as f:
        json.dump(invalid_data, f, indent=2)
    
    print("Sample data files created for validation testing")


def cleanup_custom_operator_files():
    """Clean up files created by custom operators."""
    files_to_remove = [
        '/tmp/valid_data.json',
        '/tmp/invalid_data.json',
        '/tmp/sample_data.json',
        '/tmp/processed_data.json'
    ]
    
    removed_files = []
    for file_path in files_to_remove:
        if os.path.exists(file_path):
            os.remove(file_path)
            removed_files.append(file_path)
    
    print(f"Cleaned up {len(removed_files)} files from custom operator testing")


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'custom_operator_examples_dag',
    default_args=default_args,
    schedule=None,  # Manual trigger for testing
    catchup=False,
    tags=["learning", "custom-operators", "advanced"]
)

# Setup task
setup_task = PythonOperator(
    task_id='create_sample_data',
    python_callable=create_sample_data_for_validation,
    dag=dag
)

# Custom data validation operator
validate_good_data = DataValidationOperator(
    task_id='validate_good_data',
    file_path='/tmp/valid_data.json',
    validation_rules={
        'min_size': 50,
        'required_keys': ['timestamp', 'data', 'metadata'],
        'not_empty': True
    },
    fail_on_error=True,
    dag=dag
)

validate_bad_data = DataValidationOperator(
    task_id='validate_bad_data',
    file_path='/tmp/invalid_data.json',
    validation_rules={
        'min_size': 50,
        'required_keys': ['timestamp', 'data', 'metadata'],
        'not_empty': True
    },
    fail_on_error=False,  # Don't fail the task, just warn
    dag=dag
)

# Custom file processing operator
process_file = FileProcessingOperator(
    task_id='process_sample_file',
    source_path='/tmp/sample_data.json',
    destination_path='/tmp/processed_data.json',
    processing_config={
        'filter': {'min_value': 50},
        'transform': {},
        'aggregate': {}
    },
    dag=dag
)

# Custom threshold sensor
threshold_sensor = CustomThresholdSensor(
    task_id='wait_for_data_threshold',
    metric_source='data_count',
    threshold_config={
        'type': 'greater_than',
        'value': 0
    },
    poke_interval=10,
    timeout=120,
    dag=dag
)

# Custom notification operator
send_notification = NotificationOperator(
    task_id='send_completion_notification',
    message='Custom operator pipeline completed successfully for DAG {dag_id} on {execution_date}',
    notification_config={
        'email': {
            'recipients': ['admin@example.com'],
            'subject': 'Airflow Custom Operator Pipeline Complete'
        },
        'slack': {
            'channel': '#data-engineering'
        },
        'log': {}
    },
    dag=dag
)

# Regular cleanup task
cleanup_task = PythonOperator(
    task_id='cleanup_custom_operator_files',
    python_callable=cleanup_custom_operator_files,
    dag=dag
)

# Define dependencies
setup_task >> [validate_good_data, validate_bad_data] >> process_file
process_file >> threshold_sensor >> send_notification >> cleanup_task