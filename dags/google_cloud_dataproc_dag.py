"""
Google Cloud Dataproc DAG Example
==================================

This DAG demonstrates how to use Apache Airflow with Google Cloud Dataproc 
for running Apache Spark jobs. Dataproc is Google's managed Spark and Hadoop service.

Key Concepts:
- DataprocClusterCreateOperator: Create ephemeral clusters
- DataprocSubmitJobOperator: Submit Spark/Hadoop jobs
- DataprocClusterDeleteOperator: Clean up clusters
- Cluster lifecycle management
- Cost optimization with ephemeral clusters

Prerequisites:
- Google Cloud SDK configured
- Service account with Dataproc permissions
- airflow-providers-google package installed
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import json

# Note: In production, install airflow-providers-google and use actual operators:
# from airflow.providers.google.cloud.operators.dataproc import (
#     DataprocCreateClusterOperator,
#     DataprocSubmitJobOperator,
#     DataprocDeleteClusterOperator,
# )

def create_dataproc_cluster():
    """
    Create a Google Cloud Dataproc cluster for Spark job execution.
    
    In production, this would use DataprocCreateClusterOperator with:
    - Cluster configuration (machine types, disk sizes, etc.)
    - Initialization scripts
    - Autoscaling settings
    - Preemptible instances for cost savings
    """
    print("=== Creating Google Cloud Dataproc Cluster ===")
    
    # Mock cluster configuration
    cluster_config = {
        "project_id": "your-gcp-project-id",
        "cluster_name": "airflow-dataproc-cluster",
        "region": "us-central1",
        "zone": "us-central1-a",
        "num_masters": 1,
        "num_workers": 2,
        "master_machine_type": "n1-standard-2",
        "worker_machine_type": "n1-standard-2",
        "master_disk_size": "50GB",
        "worker_disk_size": "100GB",
        "initialization_actions": [
            "gs://your-bucket/scripts/install-dependencies.sh"
        ],
        "properties": {
            "spark:spark.sql.adaptive.enabled": "true",
            "spark:spark.sql.adaptive.coalescePartitions.enabled": "true"
        }
    }
    
    print(f"Cluster Configuration: {json.dumps(cluster_config, indent=2)}")
    print("✓ Dataproc cluster creation initiated")
    print("✓ Cluster will be ready in 2-3 minutes")
    
    return cluster_config

def submit_spark_job():
    """
    Submit a Spark job to the Dataproc cluster.
    
    This demonstrates common Spark job patterns:
    - Data processing from Cloud Storage
    - Writing results back to Cloud Storage
    - Using Spark SQL for transformations
    - Monitoring job execution
    """
    print("=== Submitting Spark Job to Dataproc ===")
    
    # Mock Spark job configuration
    spark_job_config = {
        "reference": {"project_id": "your-gcp-project-id"},
        "placement": {"cluster_name": "airflow-dataproc-cluster"},
        "pyspark_job": {
            "main_python_file_uri": "gs://your-bucket/spark-jobs/data-processing.py",
            "python_file_uris": [
                "gs://your-bucket/spark-jobs/utils.py",
                "gs://your-bucket/spark-jobs/transformations.py"
            ],
            "args": [
                "--input-path", "gs://your-bucket/input-data/",
                "--output-path", "gs://your-bucket/output-data/",
                "--date", "{{ ds }}",
                "--partition-column", "event_date"
            ],
            "jar_file_uris": [
                "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"
            ],
            "properties": {
                "spark.executor.memory": "4g",
                "spark.executor.cores": "2",
                "spark.sql.adaptive.enabled": "true",
                "spark.sql.adaptive.coalescePartitions.enabled": "true"
            }
        }
    }
    
    print(f"Spark Job Configuration: {json.dumps(spark_job_config, indent=2)}")
    print("✓ Spark job submitted successfully")
    print("✓ Job execution will take 10-15 minutes")
    
    # Simulate job monitoring
    job_status = {
        "job_id": "spark-job-12345",
        "state": "RUNNING",
        "start_time": datetime.now().isoformat(),
        "driver_output_resource_uri": "gs://your-bucket/logs/spark-job-12345/"
    }
    
    print(f"Job Status: {json.dumps(job_status, indent=2)}")
    
    return job_status

def submit_hadoop_job():
    """
    Submit a Hadoop MapReduce job to the Dataproc cluster.
    
    This demonstrates Hadoop job execution patterns:
    - MapReduce job submission
    - HDFS operations
    - Job monitoring and logging
    """
    print("=== Submitting Hadoop Job to Dataproc ===")
    
    # Mock Hadoop job configuration
    hadoop_job_config = {
        "reference": {"project_id": "your-gcp-project-id"},
        "placement": {"cluster_name": "airflow-dataproc-cluster"},
        "hadoop_job": {
            "main_class": "com.example.WordCount",
            "main_jar_file_uri": "gs://your-bucket/hadoop-jobs/wordcount.jar",
            "args": [
                "gs://your-bucket/input-text/",
                "gs://your-bucket/output-wordcount/"
            ],
            "properties": {
                "mapreduce.job.maps": "4",
                "mapreduce.job.reduces": "2",
                "mapreduce.map.memory.mb": "2048",
                "mapreduce.reduce.memory.mb": "4096"
            }
        }
    }
    
    print(f"Hadoop Job Configuration: {json.dumps(hadoop_job_config, indent=2)}")
    print("✓ Hadoop MapReduce job submitted successfully")
    
    return hadoop_job_config

def monitor_job_execution():
    """
    Monitor Dataproc job execution and handle failures.
    
    This demonstrates job monitoring patterns:
    - Job status checking
    - Log retrieval
    - Error handling
    - Retry logic
    """
    print("=== Monitoring Dataproc Job Execution ===")
    
    # Mock job monitoring
    job_metrics = {
        "job_id": "spark-job-12345",
        "state": "DONE",
        "final_status": "SUCCEEDED",
        "execution_time": "14 minutes",
        "driver_output_uri": "gs://your-bucket/logs/spark-job-12345/",
        "yarn_applications": [
            {
                "name": "Data Processing Job",
                "state": "FINISHED",
                "final_status": "SUCCEEDED",
                "progress": 100,
                "tracking_url": "http://cluster-m:8088/proxy/application_12345/"
            }
        ]
    }
    
    print(f"Job Metrics: {json.dumps(job_metrics, indent=2)}")
    print("✓ Job execution completed successfully")
    print("✓ Output data available in Cloud Storage")
    
    return job_metrics

def cleanup_dataproc_cluster():
    """
    Clean up the Dataproc cluster to save costs.
    
    This demonstrates cluster lifecycle management:
    - Graceful cluster shutdown
    - Data persistence to Cloud Storage
    - Cost optimization
    - Resource cleanup
    """
    print("=== Cleaning up Dataproc Cluster ===")
    
    # Mock cleanup operations
    cleanup_summary = {
        "cluster_name": "airflow-dataproc-cluster",
        "deletion_time": datetime.now().isoformat(),
        "final_status": "DELETED",
        "cost_savings": "Cluster deleted after 1 hour runtime",
        "data_persistence": "All output data saved to Cloud Storage"
    }
    
    print(f"Cleanup Summary: {json.dumps(cleanup_summary, indent=2)}")
    print("✓ Dataproc cluster deleted successfully")
    print("✓ All resources cleaned up")
    
    return cleanup_summary

def validate_output_data():
    """
    Validate the output data from Dataproc jobs.
    
    This demonstrates data validation patterns:
    - Output file verification
    - Data quality checks
    - Schema validation
    - Row count verification
    """
    print("=== Validating Output Data ===")
    
    # Mock data validation
    validation_results = {
        "output_path": "gs://your-bucket/output-data/",
        "file_count": 12,
        "total_size": "2.4 GB",
        "row_count": 1500000,
        "schema_validation": "PASSED",
        "data_quality_score": 0.98,
        "validation_timestamp": datetime.now().isoformat()
    }
    
    print(f"Validation Results: {json.dumps(validation_results, indent=2)}")
    print("✓ Output data validation completed")
    print("✓ Data quality meets requirements")
    
    return validation_results

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
    'google_cloud_dataproc_dag',
    default_args=default_args,
    description='Google Cloud Dataproc integration example',
    schedule='@daily',  # Run daily data processing
    catchup=False,
    tags=['gcp', 'dataproc', 'spark', 'hadoop', 'big-data'],
    max_active_runs=1,  # Prevent concurrent cluster creation
)

# Define tasks with comprehensive comments

# Task 1: Start the pipeline
start_task = EmptyOperator(
    task_id='start_dataproc_pipeline',
    dag=dag
)

# Task 2: Create Dataproc cluster
create_cluster_task = PythonOperator(
    task_id='create_dataproc_cluster',
    python_callable=create_dataproc_cluster,
    dag=dag
)

# Task 3: Submit Spark job for data processing
submit_spark_task = PythonOperator(
    task_id='submit_spark_job',
    python_callable=submit_spark_job,
    dag=dag
)

# Task 4: Submit Hadoop job for additional processing
submit_hadoop_task = PythonOperator(
    task_id='submit_hadoop_job',
    python_callable=submit_hadoop_job,
    dag=dag
)

# Task 5: Monitor job execution
monitor_jobs_task = PythonOperator(
    task_id='monitor_job_execution',
    python_callable=monitor_job_execution,
    dag=dag
)

# Task 6: Validate output data
validate_data_task = PythonOperator(
    task_id='validate_output_data',
    python_callable=validate_output_data,
    dag=dag
)

# Task 7: Clean up cluster to save costs
cleanup_task = PythonOperator(
    task_id='cleanup_dataproc_cluster',
    python_callable=cleanup_dataproc_cluster,
    dag=dag
)

# Task 8: Send completion notification
notification_task = BashOperator(
    task_id='send_completion_notification',
    bash_command='''
    echo "Dataproc pipeline completed successfully"
    echo "Processed data available at: gs://your-bucket/output-data/"
    echo "Job logs available at: gs://your-bucket/logs/"
    echo "Cluster deleted for cost optimization"
    ''',
    dag=dag
)

# Task 9: End the pipeline
end_task = EmptyOperator(
    task_id='end_dataproc_pipeline',
    dag=dag
)

# Define task dependencies
# Sequential execution for resource management
start_task >> create_cluster_task >> [submit_spark_task, submit_hadoop_task]
[submit_spark_task, submit_hadoop_task] >> monitor_jobs_task
monitor_jobs_task >> validate_data_task >> cleanup_task
cleanup_task >> notification_task >> end_task

# In production, you would also add:
# - Error handling tasks
# - Cluster monitoring tasks
# - Cost tracking tasks
# - Data lineage tracking
# - Integration with data catalog