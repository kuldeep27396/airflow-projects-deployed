"""
AWS EMR (Elastic MapReduce) DAG Example
========================================

This DAG demonstrates how to use Apache Airflow with AWS EMR for running
Apache Spark and Hadoop jobs. EMR is AWS's managed big data platform.

Key Concepts:
- EmrCreateJobFlowOperator: Create EMR clusters
- EmrAddStepsOperator: Submit Spark/Hadoop jobs
- EmrTerminateJobFlowOperator: Terminate clusters
- EmrJobFlowSensor: Monitor cluster state
- S3 integration for data and logs
- Cost optimization with spot instances

Prerequisites:
- AWS CLI configured
- IAM roles for EMR service
- S3 buckets for data and logs
- airflow-providers-amazon package installed
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import json
import boto3

# Note: In production, install airflow-providers-amazon and use actual operators:
# from airflow.providers.amazon.aws.operators.emr import (
#     EmrCreateJobFlowOperator,
#     EmrAddStepsOperator,
#     EmrTerminateJobFlowOperator,
# )
# from airflow.providers.amazon.aws.sensors.emr import EmrJobFlowSensor

def create_emr_cluster():
    """
    Create an AWS EMR cluster for big data processing.
    
    This demonstrates EMR cluster configuration:
    - Instance types and counts
    - Spark and Hadoop applications
    - Bootstrap actions
    - Spot instances for cost savings
    - Security groups and VPC settings
    """
    print("=== Creating AWS EMR Cluster ===")
    
    # Mock EMR cluster configuration
    cluster_config = {
        "Name": "airflow-emr-cluster",
        "ReleaseLabel": "emr-6.4.0",  # Latest EMR version
        "Applications": [
            {"Name": "Spark"},
            {"Name": "Hadoop"},
            {"Name": "Livy"},
            {"Name": "JupyterHub"}
        ],
        "Instances": {
            "InstanceGroups": [
                {
                    "Name": "Master",
                    "Market": "ON_DEMAND",
                    "InstanceRole": "MASTER",
                    "InstanceType": "m5.xlarge",
                    "InstanceCount": 1,
                },
                {
                    "Name": "Core",
                    "Market": "SPOT",  # Use spot instances for cost savings
                    "InstanceRole": "CORE",
                    "InstanceType": "m5.large",
                    "InstanceCount": 2,
                    "BidPrice": "0.10",  # Spot price
                },
                {
                    "Name": "Task",
                    "Market": "SPOT",
                    "InstanceRole": "TASK",
                    "InstanceType": "m5.large",
                    "InstanceCount": 2,
                    "BidPrice": "0.10",
                }
            ],
            "Ec2KeyName": "your-key-pair",
            "KeepJobFlowAliveWhenNoSteps": True,
            "TerminationProtected": False,
            "Ec2SubnetId": "subnet-12345678",
        },
        "BootstrapActions": [
            {
                "Name": "Install Python packages",
                "ScriptBootstrapAction": {
                    "Path": "s3://your-bucket/bootstrap/install-python-packages.sh",
                    "Args": ["pandas", "numpy", "scikit-learn"]
                }
            }
        ],
        "Configurations": [
            {
                "Classification": "spark-defaults",
                "Properties": {
                    "spark.sql.adaptive.enabled": "true",
                    "spark.sql.adaptive.coalescePartitions.enabled": "true",
                    "spark.executor.memory": "4g",
                    "spark.executor.cores": "2",
                    "spark.dynamicAllocation.enabled": "true"
                }
            }
        ],
        "LogUri": "s3://your-bucket/emr-logs/",
        "ServiceRole": "EMR_DefaultRole",
        "JobFlowRole": "EMR_EC2_DefaultRole",
        "VisibleToAllUsers": True,
        "Tags": [
            {"Key": "Environment", "Value": "Production"},
            {"Key": "Project", "Value": "DataProcessing"},
            {"Key": "Owner", "Value": "DataEngineering"}
        ]
    }
    
    print(f"EMR Cluster Configuration: {json.dumps(cluster_config, indent=2)}")
    print("✓ EMR cluster creation initiated")
    print("✓ Cluster will be ready in 5-10 minutes")
    
    # Mock cluster ID
    cluster_id = "j-1234567890123"
    print(f"✓ Cluster ID: {cluster_id}")
    
    return {"cluster_id": cluster_id, "config": cluster_config}

def submit_spark_step():
    """
    Submit a Spark job as an EMR step.
    
    This demonstrates Spark job submission:
    - PySpark application submission
    - Command line arguments
    - Spark configuration
    - JAR dependencies
    """
    print("=== Submitting Spark Step to EMR ===")
    
    # Mock Spark step configuration
    spark_step = {
        "Name": "Data Processing with Spark",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                "--master", "yarn",
                "--conf", "spark.executor.memory=4g",
                "--conf", "spark.executor.cores=2",
                "--conf", "spark.sql.adaptive.enabled=true",
                "--jars", "s3://your-bucket/jars/spark-bigquery-latest.jar",
                "--py-files", "s3://your-bucket/scripts/utils.py",
                "s3://your-bucket/spark-jobs/data-processing.py",
                "--input-path", "s3://your-bucket/input-data/",
                "--output-path", "s3://your-bucket/output-data/",
                "--date", "{{ ds }}",
                "--partition-by", "event_date"
            ]
        }
    }
    
    print(f"Spark Step Configuration: {json.dumps(spark_step, indent=2)}")
    print("✓ Spark step submitted successfully")
    
    # Mock step ID
    step_id = "s-1234567890123"
    print(f"✓ Step ID: {step_id}")
    
    return {"step_id": step_id, "step_config": spark_step}

def submit_hadoop_step():
    """
    Submit a Hadoop MapReduce job as an EMR step.
    
    This demonstrates Hadoop job submission:
    - MapReduce application
    - Custom JAR execution
    - HDFS operations
    - Job configuration
    """
    print("=== Submitting Hadoop Step to EMR ===")
    
    # Mock Hadoop step configuration
    hadoop_step = {
        "Name": "Word Count with Hadoop",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "s3://your-bucket/hadoop-jobs/wordcount.jar",
            "MainClass": "com.example.WordCount",
            "Args": [
                "s3://your-bucket/input-text/",
                "s3://your-bucket/output-wordcount/"
            ],
            "Properties": {
                "mapreduce.job.maps": "4",
                "mapreduce.job.reduces": "2",
                "mapreduce.map.memory.mb": "2048",
                "mapreduce.reduce.memory.mb": "4096"
            }
        }
    }
    
    print(f"Hadoop Step Configuration: {json.dumps(hadoop_step, indent=2)}")
    print("✓ Hadoop step submitted successfully")
    
    # Mock step ID
    step_id = "s-2234567890123"
    print(f"✓ Step ID: {step_id}")
    
    return {"step_id": step_id, "step_config": hadoop_step}

def monitor_cluster_status():
    """
    Monitor EMR cluster and step execution status.
    
    This demonstrates cluster monitoring:
    - Cluster state checking
    - Step status monitoring
    - Error handling
    - Log retrieval
    """
    print("=== Monitoring EMR Cluster Status ===")
    
    # Mock cluster status
    cluster_status = {
        "cluster_id": "j-1234567890123",
        "state": "RUNNING",
        "state_change_reason": "User request",
        "timeline": {
            "creation_date_time": datetime.now().isoformat(),
            "ready_date_time": (datetime.now() + timedelta(minutes=10)).isoformat(),
        },
        "master_public_dns": "ec2-12-34-56-78.compute-1.amazonaws.com",
        "log_uri": "s3://your-bucket/emr-logs/j-1234567890123/",
        "steps": [
            {
                "step_id": "s-1234567890123",
                "name": "Data Processing with Spark",
                "state": "COMPLETED",
                "timeline": {
                    "start_date_time": datetime.now().isoformat(),
                    "end_date_time": (datetime.now() + timedelta(minutes=15)).isoformat(),
                }
            },
            {
                "step_id": "s-2234567890123",
                "name": "Word Count with Hadoop",
                "state": "COMPLETED",
                "timeline": {
                    "start_date_time": datetime.now().isoformat(),
                    "end_date_time": (datetime.now() + timedelta(minutes=8)).isoformat(),
                }
            }
        ]
    }
    
    print(f"Cluster Status: {json.dumps(cluster_status, indent=2)}")
    print("✓ All steps completed successfully")
    print("✓ Cluster is healthy and ready")
    
    return cluster_status

def validate_s3_output():
    """
    Validate output data stored in S3.
    
    This demonstrates S3 data validation:
    - File existence checks
    - Data size validation
    - Schema verification
    - Data quality checks
    """
    print("=== Validating S3 Output Data ===")
    
    # Mock S3 validation
    s3_validation = {
        "spark_output": {
            "path": "s3://your-bucket/output-data/",
            "file_count": 24,
            "total_size_gb": 5.2,
            "partition_count": 30,
            "row_count": 2500000,
            "schema_valid": True
        },
        "hadoop_output": {
            "path": "s3://your-bucket/output-wordcount/",
            "file_count": 2,
            "total_size_mb": 150,
            "word_count": 50000,
            "schema_valid": True
        },
        "validation_timestamp": datetime.now().isoformat(),
        "data_quality_score": 0.97
    }
    
    print(f"S3 Validation Results: {json.dumps(s3_validation, indent=2)}")
    print("✓ Output data validation completed")
    print("✓ All files present and valid")
    
    return s3_validation

def cleanup_emr_cluster():
    """
    Terminate EMR cluster to save costs.
    
    This demonstrates cluster lifecycle management:
    - Graceful cluster termination
    - Data backup to S3
    - Cost optimization
    - Resource cleanup
    """
    print("=== Terminating EMR Cluster ===")
    
    # Mock termination process
    termination_info = {
        "cluster_id": "j-1234567890123",
        "termination_reason": "User request",
        "termination_time": datetime.now().isoformat(),
        "final_state": "TERMINATED",
        "total_runtime": "45 minutes",
        "estimated_cost": "$12.50",
        "data_backup": {
            "logs": "s3://your-bucket/emr-logs/j-1234567890123/",
            "output": "s3://your-bucket/output-data/",
            "status": "COMPLETED"
        }
    }
    
    print(f"Termination Info: {json.dumps(termination_info, indent=2)}")
    print("✓ EMR cluster terminated successfully")
    print("✓ All data backed up to S3")
    print("✓ Cost optimization achieved")
    
    return termination_info

def send_completion_notification():
    """
    Send notification about job completion.
    
    This demonstrates notification patterns:
    - Email notifications
    - Slack integration
    - SNS notifications
    - Dashboard updates
    """
    print("=== Sending Completion Notification ===")
    
    # Mock notification
    notification_info = {
        "notification_type": "EMR_JOB_COMPLETED",
        "recipients": ["data-team@company.com"],
        "message": "EMR data processing job completed successfully",
        "details": {
            "cluster_id": "j-1234567890123",
            "runtime": "45 minutes",
            "cost": "$12.50",
            "output_location": "s3://your-bucket/output-data/",
            "data_quality": "97%"
        },
        "timestamp": datetime.now().isoformat()
    }
    
    print(f"Notification Info: {json.dumps(notification_info, indent=2)}")
    print("✓ Notifications sent successfully")
    
    return notification_info

# DAG Configuration
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'aws_emr_dag',
    default_args=default_args,
    description='AWS EMR integration example for Spark and Hadoop jobs',
    schedule='@daily',  # Daily data processing
    catchup=False,
    tags=['aws', 'emr', 'spark', 'hadoop', 'big-data', 's3'],
    max_active_runs=1,  # Prevent concurrent cluster creation
)

# Define tasks with comprehensive comments

# Task 1: Start the EMR pipeline
start_task = EmptyOperator(
    task_id='start_emr_pipeline',
    dag=dag
)

# Task 2: Create EMR cluster with optimized configuration
create_cluster_task = PythonOperator(
    task_id='create_emr_cluster',
    python_callable=create_emr_cluster,
    dag=dag
)

# Task 3: Submit Spark job for data processing
submit_spark_task = PythonOperator(
    task_id='submit_spark_step',
    python_callable=submit_spark_step,
    dag=dag
)

# Task 4: Submit Hadoop job for additional processing
submit_hadoop_task = PythonOperator(
    task_id='submit_hadoop_step',
    python_callable=submit_hadoop_step,
    dag=dag
)

# Task 5: Monitor cluster and step execution
monitor_cluster_task = PythonOperator(
    task_id='monitor_cluster_status',
    python_callable=monitor_cluster_status,
    dag=dag
)

# Task 6: Validate output data in S3
validate_output_task = PythonOperator(
    task_id='validate_s3_output',
    python_callable=validate_s3_output,
    dag=dag
)

# Task 7: Terminate cluster to save costs
terminate_cluster_task = PythonOperator(
    task_id='cleanup_emr_cluster',
    python_callable=cleanup_emr_cluster,
    dag=dag
)

# Task 8: Send completion notification
notification_task = PythonOperator(
    task_id='send_completion_notification',
    python_callable=send_completion_notification,
    dag=dag
)

# Task 9: End the pipeline
end_task = EmptyOperator(
    task_id='end_emr_pipeline',
    dag=dag
)

# Define task dependencies
# Sequential execution for proper resource management
start_task >> create_cluster_task >> [submit_spark_task, submit_hadoop_task]
[submit_spark_task, submit_hadoop_task] >> monitor_cluster_task
monitor_cluster_task >> validate_output_task >> terminate_cluster_task
terminate_cluster_task >> notification_task >> end_task

# EMR Best Practices Demonstrated:
# 1. Use spot instances for cost savings
# 2. Proper cluster sizing and configuration
# 3. Bootstrap actions for custom setup
# 4. Comprehensive monitoring and logging
# 5. Automatic cluster termination
# 6. S3 integration for data and logs
# 7. Error handling and retry logic
# 8. Cost tracking and optimization