"""
Google BigQuery DAG Example
============================

This DAG demonstrates how to use Apache Airflow with Google BigQuery
for data warehousing and analytics operations.

Key Concepts:
- BigQueryCreateDatasetOperator: Create datasets
- BigQueryInsertJobOperator: Run queries and jobs
- BigQueryCheckOperator: Data quality checks
- BigQueryToCloudStorageOperator: Export data
- BigQueryHook: Custom operations
- Data pipeline orchestration

Prerequisites:
- Google Cloud SDK configured
- Service account with BigQuery permissions
- airflow-providers-google package installed
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import json

# Note: In production, install airflow-providers-google and use actual operators:
# from airflow.providers.google.cloud.operators.bigquery import (
#     BigQueryCreateDatasetOperator,
#     BigQueryInsertJobOperator,
#     BigQueryCheckOperator,
#     BigQueryToCloudStorageOperator,
# )
# from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

def create_bigquery_dataset():
    """
    Create a BigQuery dataset for data warehousing.
    
    This demonstrates dataset creation with:
    - Dataset configuration
    - Access controls
    - Data location settings
    - Expiration policies
    """
    print("=== Creating BigQuery Dataset ===")
    
    # Mock dataset configuration
    dataset_config = {
        "dataset_id": "analytics_warehouse",
        "project_id": "your-gcp-project-id",
        "location": "US",
        "description": "Analytics data warehouse for reporting",
        "default_table_expiration_ms": 30 * 24 * 60 * 60 * 1000,  # 30 days
        "labels": {
            "environment": "production",
            "team": "data-engineering",
            "purpose": "analytics"
        },
        "access": [
            {
                "role": "READER",
                "userByEmail": "analytics-team@company.com"
            },
            {
                "role": "WRITER",
                "userByEmail": "data-engineering@company.com"
            }
        ]
    }
    
    print(f"Dataset Configuration: {json.dumps(dataset_config, indent=2)}")
    print("✓ BigQuery dataset created successfully")
    print("✓ Access controls configured")
    
    return dataset_config

def load_data_to_bigquery():
    """
    Load data from Cloud Storage to BigQuery.
    
    This demonstrates data loading patterns:
    - CSV to BigQuery loading
    - Schema auto-detection
    - Data validation
    - Partitioning and clustering
    """
    print("=== Loading Data to BigQuery ===")
    
    # Mock data loading job
    load_job_config = {
        "job_id": "load_customer_data_{{ ds_nodash }}",
        "configuration": {
            "load": {
                "source_uris": [
                    "gs://your-bucket/customer-data/{{ ds }}/*.csv"
                ],
                "destination_table": {
                    "project_id": "your-gcp-project-id",
                    "dataset_id": "analytics_warehouse",
                    "table_id": "customers"
                },
                "schema": {
                    "fields": [
                        {"name": "customer_id", "type": "INTEGER", "mode": "REQUIRED"},
                        {"name": "name", "type": "STRING", "mode": "REQUIRED"},
                        {"name": "email", "type": "STRING", "mode": "NULLABLE"},
                        {"name": "signup_date", "type": "DATE", "mode": "REQUIRED"},
                        {"name": "country", "type": "STRING", "mode": "NULLABLE"},
                        {"name": "total_spend", "type": "FLOAT", "mode": "NULLABLE"}
                    ]
                },
                "write_disposition": "WRITE_TRUNCATE",
                "source_format": "CSV",
                "skip_leading_rows": 1,
                "autodetect": False,
                "time_partitioning": {
                    "type": "DAY",
                    "field": "signup_date"
                },
                "clustering": {
                    "fields": ["country", "signup_date"]
                }
            }
        }
    }
    
    print(f"Load Job Configuration: {json.dumps(load_job_config, indent=2)}")
    print("✓ Data loaded to BigQuery successfully")
    print("✓ Table partitioned and clustered for performance")
    
    return load_job_config

def run_data_transformation():
    """
    Run SQL transformations in BigQuery.
    
    This demonstrates data transformation patterns:
    - Complex SQL queries
    - Window functions
    - Aggregations
    - Data enrichment
    """
    print("=== Running Data Transformation ===")
    
    # Mock transformation SQL
    transformation_sql = """
    CREATE OR REPLACE TABLE `your-gcp-project-id.analytics_warehouse.customer_analytics` AS
    WITH customer_metrics AS (
        SELECT 
            customer_id,
            name,
            email,
            country,
            signup_date,
            total_spend,
            -- Calculate customer lifetime value
            total_spend / DATE_DIFF(CURRENT_DATE(), signup_date, DAY) as daily_value,
            -- Customer segmentation
            CASE 
                WHEN total_spend > 1000 THEN 'High Value'
                WHEN total_spend > 500 THEN 'Medium Value'
                ELSE 'Low Value'
            END as customer_segment,
            -- Recency analysis
            DATE_DIFF(CURRENT_DATE(), signup_date, DAY) as days_since_signup,
            -- Country-based statistics
            AVG(total_spend) OVER (PARTITION BY country) as avg_country_spend,
            RANK() OVER (PARTITION BY country ORDER BY total_spend DESC) as country_rank
        FROM `your-gcp-project-id.analytics_warehouse.customers`
        WHERE signup_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
    ),
    enriched_data AS (
        SELECT 
            *,
            -- Additional enrichment
            CASE 
                WHEN daily_value > avg_country_spend / 365 THEN 'Above Average'
                ELSE 'Below Average'
            END as performance_vs_country,
            -- Cohort analysis
            FORMAT_DATE('%Y-%m', signup_date) as signup_cohort
        FROM customer_metrics
    )
    SELECT * FROM enriched_data
    ORDER BY total_spend DESC;
    """
    
    transformation_job = {
        "job_id": "transform_customer_data_{{ ds_nodash }}",
        "configuration": {
            "query": {
                "query": transformation_sql,
                "use_legacy_sql": False,
                "priority": "INTERACTIVE",
                "labels": {
                    "env": "production",
                    "team": "data-engineering"
                }
            }
        }
    }
    
    print("Transformation SQL:")
    print(transformation_sql)
    print(f"✓ Transformation job configuration: {json.dumps(transformation_job, indent=2)}")
    print("✓ Data transformation completed successfully")
    
    return transformation_job

def run_data_quality_checks():
    """
    Run data quality checks on BigQuery tables.
    
    This demonstrates data quality patterns:
    - Row count validation
    - Null value checks
    - Data range validation
    - Schema validation
    """
    print("=== Running Data Quality Checks ===")
    
    # Mock data quality checks
    quality_checks = [
        {
            "check_name": "row_count_validation",
            "sql": """
                SELECT COUNT(*) as row_count 
                FROM `your-gcp-project-id.analytics_warehouse.customer_analytics`
            """,
            "expected_min": 1000,
            "expected_max": 100000
        },
        {
            "check_name": "null_customer_id_check",
            "sql": """
                SELECT COUNT(*) as null_count 
                FROM `your-gcp-project-id.analytics_warehouse.customer_analytics`
                WHERE customer_id IS NULL
            """,
            "expected_result": 0
        },
        {
            "check_name": "spend_range_validation",
            "sql": """
                SELECT 
                    MIN(total_spend) as min_spend,
                    MAX(total_spend) as max_spend,
                    AVG(total_spend) as avg_spend
                FROM `your-gcp-project-id.analytics_warehouse.customer_analytics`
            """,
            "validation_rules": {
                "min_spend": ">= 0",
                "max_spend": "<= 10000",
                "avg_spend": "BETWEEN 100 AND 1000"
            }
        },
        {
            "check_name": "segment_distribution",
            "sql": """
                SELECT 
                    customer_segment,
                    COUNT(*) as count,
                    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage
                FROM `your-gcp-project-id.analytics_warehouse.customer_analytics`
                GROUP BY customer_segment
            """,
            "expected_segments": ["High Value", "Medium Value", "Low Value"]
        }
    ]
    
    # Mock quality check results
    quality_results = {
        "timestamp": datetime.now().isoformat(),
        "overall_status": "PASSED",
        "checks": [
            {
                "check_name": "row_count_validation",
                "status": "PASSED",
                "actual_value": 15000,
                "expected_range": "1000-100000"
            },
            {
                "check_name": "null_customer_id_check",
                "status": "PASSED",
                "actual_value": 0,
                "expected_value": 0
            },
            {
                "check_name": "spend_range_validation",
                "status": "PASSED",
                "results": {
                    "min_spend": 0,
                    "max_spend": 5000,
                    "avg_spend": 450
                }
            },
            {
                "check_name": "segment_distribution",
                "status": "PASSED",
                "distribution": {
                    "High Value": {"count": 1500, "percentage": 10},
                    "Medium Value": {"count": 4500, "percentage": 30},
                    "Low Value": {"count": 9000, "percentage": 60}
                }
            }
        ]
    }
    
    print(f"Quality Checks: {json.dumps(quality_checks, indent=2)}")
    print(f"Quality Results: {json.dumps(quality_results, indent=2)}")
    print("✓ All data quality checks passed")
    
    return quality_results

def export_to_cloud_storage():
    """
    Export BigQuery data to Cloud Storage.
    
    This demonstrates data export patterns:
    - Query results export
    - Different file formats
    - Partitioned exports
    - Compression options
    """
    print("=== Exporting Data to Cloud Storage ===")
    
    # Mock export job configuration
    export_job = {
        "job_id": "export_customer_analytics_{{ ds_nodash }}",
        "configuration": {
            "extract": {
                "source_table": {
                    "project_id": "your-gcp-project-id",
                    "dataset_id": "analytics_warehouse",
                    "table_id": "customer_analytics"
                },
                "destination_uris": [
                    "gs://your-bucket/exports/customer_analytics/{{ ds }}/*.csv"
                ],
                "destination_format": "CSV",
                "compression": "GZIP",
                "field_delimiter": ",",
                "print_header": True
            }
        }
    }
    
    # Mock export results
    export_results = {
        "job_id": "export_customer_analytics_20240115",
        "status": "COMPLETED",
        "exported_files": [
            "gs://your-bucket/exports/customer_analytics/2024-01-15/000000000000.csv.gz",
            "gs://your-bucket/exports/customer_analytics/2024-01-15/000000000001.csv.gz"
        ],
        "total_bytes": 52428800,  # 50MB
        "total_rows": 15000,
        "compression_ratio": 0.75
    }
    
    print(f"Export Job Configuration: {json.dumps(export_job, indent=2)}")
    print(f"Export Results: {json.dumps(export_results, indent=2)}")
    print("✓ Data exported to Cloud Storage successfully")
    
    return export_results

def create_materialized_view():
    """
    Create a materialized view for performance optimization.
    
    This demonstrates materialized view patterns:
    - Automated view refresh
    - Query optimization
    - Cost reduction
    - Performance improvement
    """
    print("=== Creating Materialized View ===")
    
    # Mock materialized view SQL
    materialized_view_sql = """
    CREATE MATERIALIZED VIEW `your-gcp-project-id.analytics_warehouse.customer_summary`
    PARTITION BY DATE(signup_date)
    CLUSTER BY country, customer_segment
    AS
    SELECT 
        country,
        customer_segment,
        DATE(signup_date) as signup_date,
        COUNT(*) as customer_count,
        SUM(total_spend) as total_revenue,
        AVG(total_spend) as avg_spend,
        MIN(total_spend) as min_spend,
        MAX(total_spend) as max_spend,
        COUNT(DISTINCT email) as unique_emails
    FROM `your-gcp-project-id.analytics_warehouse.customer_analytics`
    GROUP BY country, customer_segment, DATE(signup_date)
    """
    
    materialized_view_config = {
        "view_name": "customer_summary",
        "refresh_interval": "1 HOUR",
        "enable_refresh": True,
        "sql": materialized_view_sql,
        "partitioning": {
            "field": "signup_date",
            "type": "DAY"
        },
        "clustering": ["country", "customer_segment"]
    }
    
    print("Materialized View SQL:")
    print(materialized_view_sql)
    print(f"✓ Materialized view configuration: {json.dumps(materialized_view_config, indent=2)}")
    print("✓ Materialized view created successfully")
    
    return materialized_view_config

def monitor_bigquery_costs():
    """
    Monitor BigQuery costs and usage.
    
    This demonstrates cost monitoring patterns:
    - Query cost analysis
    - Slot usage monitoring
    - Storage cost tracking
    - Optimization recommendations
    """
    print("=== Monitoring BigQuery Costs ===")
    
    # Mock cost monitoring
    cost_analysis = {
        "date": "{{ ds }}",
        "project_id": "your-gcp-project-id",
        "query_costs": {
            "total_bytes_processed": 5368709120,  # 5GB
            "total_queries": 25,
            "avg_bytes_per_query": 214748364,    # ~200MB
            "estimated_cost": 0.025  # $0.025
        },
        "storage_costs": {
            "active_storage_gb": 1024,
            "long_term_storage_gb": 5120,
            "estimated_monthly_cost": 23.55
        },
        "optimization_recommendations": [
            "Consider partitioning large tables",
            "Use clustering for frequently filtered columns",
            "Implement column pruning in queries",
            "Use approximate aggregation functions where possible"
        ],
        "cost_alerts": {
            "daily_threshold": 10.0,
            "current_usage": 0.025,
            "alert_status": "GREEN"
        }
    }
    
    print(f"Cost Analysis: {json.dumps(cost_analysis, indent=2)}")
    print("✓ Cost monitoring completed")
    print("✓ Usage within acceptable limits")
    
    return cost_analysis

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
    'google_bigquery_dag',
    default_args=default_args,
    description='Google BigQuery data warehouse operations',
    schedule='@daily',  # Daily analytics processing
    catchup=False,
    tags=['gcp', 'bigquery', 'data-warehouse', 'analytics', 'sql'],
    max_active_runs=1,
)

# Define tasks with comprehensive comments

# Task 1: Start the BigQuery pipeline
start_task = EmptyOperator(
    task_id='start_bigquery_pipeline',
    dag=dag
)

# Task 2: Create dataset if it doesn't exist
create_dataset_task = PythonOperator(
    task_id='create_bigquery_dataset',
    python_callable=create_bigquery_dataset,
    dag=dag
)

# Task 3: Load raw data from Cloud Storage
load_data_task = PythonOperator(
    task_id='load_data_to_bigquery',
    python_callable=load_data_to_bigquery,
    dag=dag
)

# Task 4: Transform data with SQL
transform_data_task = PythonOperator(
    task_id='run_data_transformation',
    python_callable=run_data_transformation,
    dag=dag
)

# Task 5: Run data quality checks
quality_checks_task = PythonOperator(
    task_id='run_data_quality_checks',
    python_callable=run_data_quality_checks,
    dag=dag
)

# Task 6: Create materialized view for performance
create_view_task = PythonOperator(
    task_id='create_materialized_view',
    python_callable=create_materialized_view,
    dag=dag
)

# Task 7: Export processed data
export_data_task = PythonOperator(
    task_id='export_to_cloud_storage',
    python_callable=export_to_cloud_storage,
    dag=dag
)

# Task 8: Monitor costs and usage
monitor_costs_task = PythonOperator(
    task_id='monitor_bigquery_costs',
    python_callable=monitor_bigquery_costs,
    dag=dag
)

# Task 9: Send completion notification
notification_task = BashOperator(
    task_id='send_completion_notification',
    bash_command='''
    echo "BigQuery analytics pipeline completed successfully"
    echo "Data transformed and loaded into analytics warehouse"
    echo "Quality checks: PASSED"
    echo "Cost monitoring: WITHIN LIMITS"
    echo "Materialized views: REFRESHED"
    ''',
    dag=dag
)

# Task 10: End the pipeline
end_task = EmptyOperator(
    task_id='end_bigquery_pipeline',
    dag=dag
)

# Define task dependencies
# Sequential execution for data consistency
start_task >> create_dataset_task >> load_data_task >> transform_data_task
transform_data_task >> quality_checks_task >> create_view_task
create_view_task >> [export_data_task, monitor_costs_task]
[export_data_task, monitor_costs_task] >> notification_task >> end_task

# BigQuery Best Practices Demonstrated:
# 1. Proper dataset and table organization
# 2. Partitioning and clustering for performance
# 3. Data quality validation
# 4. Cost monitoring and optimization
# 5. Materialized views for query performance
# 6. Proper error handling and retries
# 7. Data export capabilities
# 8. SQL best practices for analytics