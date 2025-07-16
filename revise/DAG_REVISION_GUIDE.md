# Apache Airflow DAG Revision Guide

## Overview

This comprehensive guide provides detailed explanations of all 18 DAGs in the project, covering operators, functions, patterns, and concepts essential for Apache Airflow mastery and interview preparation.

## Table of Contents

1. [Basic DAGs](#basic-dags)
2. [Intermediate DAGs](#intermediate-dags)
3. [Advanced DAGs](#advanced-dags)
4. [Production DAGs](#production-dags)
5. [All Operators Summary](#all-operators-summary)

---

## Basic DAGs

### 1. Hello World DAG
**File:** [hello_world.py](https://github.com/kuldeep735/astro-project-deployed/blob/main/dags/hello_world.py)

**Purpose:** Demonstrates the modern TaskFlow API approach

**Key Concepts:**
- **@dag decorator**: Modern way to create DAGs
- **@task decorator**: Define tasks as Python functions
- **TaskFlow API**: Automatic XCom handling and cleaner syntax

**Code Analysis:**
```python
@task
def hello_world():
    print("Hello World")

@dag(
    dag_id='hello_world',
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    default_args={'owner': 'airflow', 'retries': 1},
)
def hello_world_dag():
    hello_world_task = hello_world()
```

**Operators Used:**
- `@task` (TaskFlow API)

**Interview Points:**
- TaskFlow API vs traditional approach
- Automatic XCom serialization
- Cleaner, more Pythonic syntax

---

### 2. Basic Python Operator DAG
**File:** [basic_python_operator_dag.py](https://github.com/kuldeep735/astro-project-deployed/blob/main/dags/basic_python_operator_dag.py)

**Purpose:** Shows traditional PythonOperator usage

**Key Concepts:**
- **PythonOperator**: Execute Python functions
- **Task Dependencies**: Using `>>` operator
- **Default Args**: Common task configurations

**Code Analysis:**
```python
def print_hello():
    print("Hello from Airflow!")

hello_task = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag
)

hello_task >> goodbye_task
```

**Operators Used:**
- `PythonOperator`

**Interview Points:**
- Traditional vs TaskFlow API
- Task dependency syntax
- DAG instantiation patterns

---

### 3. XCom Example DAG
**File:** [xcom_example_dag.py](https://github.com/kuldeep735/astro-project-deployed/blob/main/dags/xcom_example_dag.py)

**Purpose:** Demonstrates inter-task communication using XComs

**Key Concepts:**
- **XCom Push/Pull**: Share data between tasks
- **Task Instance (ti)**: Access task metadata
- **Key-Value Storage**: Named data sharing

**Code Analysis:**
```python
def push_xcom(ti=None):
    ti.xcom_push(key='sample_key', value='Airflow XComs are cool!')

def pull_xcom(ti=None):
    value = ti.xcom_pull(key='sample_key', task_ids='push_task')
    print(f"Pulled XCom value: {value}")
```

**Operators Used:**
- `PythonOperator`

**Interview Points:**
- XCom size limitations
- Data serialization
- Task dependency requirements for XCom

---

## Intermediate DAGs

### 4. Branching DAG
**File:** [branching_dag.py](https://github.com/kuldeep735/astro-project-deployed/blob/main/dags/branching_dag.py)

**Purpose:** Shows conditional workflow execution

**Key Concepts:**
- **BranchPythonOperator**: Conditional task execution
- **Task Skipping**: Skip branches based on logic
- **Conditional Logic**: Dynamic workflow paths

**Code Analysis:**
```python
def choose_branch():
    return 'branch_a' if random.choice([True, False]) else 'branch_b'

branch = BranchPythonOperator(
    task_id='branching',
    python_callable=choose_branch,
    dag=dag
)

start >> branch >> [branch_a, branch_b] >> end
```

**Operators Used:**
- `BranchPythonOperator`
- `PythonOperator`
- `EmptyOperator`

**Interview Points:**
- Conditional workflows
- Task skipping behavior
- Branch convergence patterns

---

### 5. Sensor Examples DAG
**File:** [sensor_dag.py](https://github.com/kuldeep735/astro-project-deployed/blob/main/dags/sensor_dag.py)

**Purpose:** Demonstrates various sensor types for waiting on external conditions

**Key Concepts:**
- **FileSensor**: Wait for file system events
- **DateTimeSensor**: Wait for specific datetime
- **TimeDeltaSensor**: Wait for time duration
- **Poke Interval**: Sensor check frequency
- **Timeout**: Maximum wait time

**Code Analysis:**
```python
file_sensor = FileSensor(
    task_id='wait_for_file',
    filepath='/tmp/test_sensor_file.txt',
    poke_interval=10,  # Check every 10 seconds
    timeout=300,  # Timeout after 5 minutes
    dag=dag
)

datetime_sensor = DateTimeSensor(
    task_id='wait_for_datetime',
    target_time="{{ (execution_date + macros.timedelta(minutes=1)).strftime('%Y-%m-%d %H:%M:%S') }}",
    poke_interval=10,
    dag=dag
)
```

**Operators Used:**
- `FileSensor`
- `DateTimeSensor`
- `TimeDeltaSensor`
- `PythonOperator` (Mock HTTP sensor)
- `BashOperator`

**Interview Points:**
- Sensor vs Operator differences
- Poke vs Reschedule mode
- Timeout handling strategies

---

### 6. TaskGroup Example DAG
**File:** [taskgroup_dag.py](https://github.com/kuldeep735/astro-project-deployed/blob/main/dags/taskgroup_dag.py)

**Purpose:** Shows task organization using TaskGroups

**Key Concepts:**
- **TaskGroup**: Logical task grouping
- **Nested TaskGroups**: Hierarchical organization
- **Group Dependencies**: Inter-group relationships
- **ETL Pattern**: Extract, Transform, Load workflow

**Code Analysis:**
```python
with TaskGroup("extract_group") as extract_group:
    extract_db = PythonOperator(
        task_id='extract_from_database',
        python_callable=extract_data,
        op_kwargs={'source': 'database'},
    )

# Nested TaskGroup
with TaskGroup("load_group") as load_group:
    with TaskGroup("data_warehouse") as dw_group:
        load_to_staging = PythonOperator(...)
        validate_staging = PythonOperator(...)
        load_to_staging >> validate_staging
```

**Operators Used:**
- `PythonOperator`
- `BashOperator`
- `TaskGroup`

**Interview Points:**
- TaskGroup vs SubDAG
- Visual organization benefits
- Task dependency patterns within groups

---

### 7. Templating Example DAG
**File:** [templating_dag.py](https://github.com/kuldeep735/astro-project-deployed/blob/main/dags/templating_dag.py)

**Purpose:** Demonstrates Jinja templating capabilities

**Key Concepts:**
- **Jinja Templates**: Dynamic content generation
- **Built-in Macros**: `{{ ds }}`, `{{ logical_date }}`
- **Variables**: `{{ var.value.get('key') }}`
- **Conditional Templating**: `{% if %}` statements
- **Loop Templating**: `{% for %}` loops

**Code Analysis:**
```python
bash_templating = BashOperator(
    task_id='bash_templating_example',
    bash_command="""
    echo "Execution date: {{ ds }}"
    echo "DAG: {{ dag.dag_id }}"
    echo "Task: {{ task.task_id }}"
    echo "Logical date: {{ logical_date }}"
    """,
    dag=dag
)

conditional_task = BashOperator(
    bash_command="""
    {% if params.enable_processing %}
    echo "Processing enabled for {{ ds }}"
    {% else %}
    echo "Processing disabled for {{ ds }}"
    {% endif %}
    """,
    dag=dag
)
```

**Operators Used:**
- `BashOperator`
- `PythonOperator`
- `EmptyOperator`

**Interview Points:**
- Template fields and rendering
- Macro availability and usage
- Dynamic parameter handling

---

## Advanced DAGs

### 8. Scheduling Patterns DAG
**File:** [scheduling_patterns_dag.py](https://github.com/kuldeep735/astro-project-deployed/blob/main/dags/scheduling_patterns_dag.py)

**Purpose:** Shows various scheduling strategies

**Key Concepts:**
- **Cron Expressions**: `'30 2 * * 1-5'` (2:30 AM weekdays)
- **Preset Schedules**: `@daily`, `@hourly`, `@weekly`
- **Timedelta Scheduling**: `timedelta(hours=6)`
- **Catchup Behavior**: Historical data processing
- **Manual Triggers**: `schedule=None`

**Code Analysis:**
```python
# Daily with catchup
daily_dag = DAG(
    'daily_schedule_with_catchup',
    schedule='@daily',
    catchup=True,  # Backfill from start_date
    max_active_runs=3,
)

# Custom cron
cron_dag = DAG(
    'custom_cron_schedule',
    schedule='30 2 * * 1-5',  # 2:30 AM weekdays
    catchup=False,
)

# Timedelta
timedelta_dag = DAG(
    'timedelta_schedule',
    schedule=timedelta(hours=6),
    catchup=False,
)
```

**Operators Used:**
- `PythonOperator`
- `BashOperator`

**Interview Points:**
- Cron expression syntax
- Catchup vs no-catchup implications
- Schedule interval vs logical date

---

### 9. Error Handling and Retry DAG
**File:** [error_handling_retry_dag.py](https://github.com/kuldeep735/astro-project-deployed/blob/main/dags/error_handling_retry_dag.py)

**Purpose:** Comprehensive error handling and resilience patterns

**Key Concepts:**
- **Retry Policies**: Exponential backoff
- **Circuit Breaker**: Prevent cascading failures
- **Graceful Degradation**: Fallback strategies
- **Failure Callbacks**: Custom error handling
- **Task Recovery**: Success after failures

**Code Analysis:**
```python
def simulate_transient_failure(**context):
    task_instance = context['task_instance']
    try_number = task_instance.try_number
    
    failure_probability = 0.7 if try_number == 1 else 0.3
    
    if random.random() < failure_probability:
        raise AirflowException(f"Transient failure on attempt {try_number}")
    
    return {'status': 'success', 'attempts': try_number}

def task_failure_callback(context):
    task_instance = context['task_instance']
    print(f"🚨 TASK FAILURE: {context['task'].task_id}")
    # Send notifications, create tickets, etc.

transient_failure_task = PythonOperator(
    task_id='transient_failure_example',
    python_callable=simulate_transient_failure,
    retries=3,
    retry_delay=timedelta(seconds=30),
    on_failure_callback=task_failure_callback,
    dag=dag
)
```

**Operators Used:**
- `PythonOperator`
- `BashOperator`
- `EmptyOperator`

**Interview Points:**
- Retry strategies and exponential backoff
- Circuit breaker pattern implementation
- Failure callback mechanisms
- Task recovery scenarios

---

### 10. Data Pipeline ETL DAG
**File:** [data_pipeline_etl_dag.py](https://github.com/kuldeep735/astro-project-deployed/blob/main/dags/data_pipeline_etl_dag.py)

**Purpose:** Comprehensive ETL pipeline example

**Key Concepts:**
- **Extract Phase**: Multiple data sources (CSV, JSON, API)
- **Transform Phase**: Data cleaning and validation
- **Load Phase**: Target system integration
- **Data Quality**: Monitoring and validation
- **Pipeline Orchestration**: TaskGroups for organization

**Code Analysis:**
```python
def extract_csv_data():
    print("=== Extracting CSV Data ===")
    df = pd.read_csv('/tmp/customers.csv')
    extracted_path = '/tmp/extracted_customers.csv'
    df.to_csv(extracted_path, index=False)
    return extracted_path

def transform_data():
    print("=== Transforming Data ===")
    # Data cleaning, validation, aggregation
    customers_df = pd.read_csv('/tmp/extracted_customers.csv')
    # Apply transformations
    return transformed_data

with TaskGroup("extract_phase") as extract_phase:
    extract_csv = PythonOperator(
        task_id='extract_csv_data',
        python_callable=extract_csv_data,
    )
```

**Operators Used:**
- `PythonOperator`
- `BashOperator`
- `TaskGroup`

**Interview Points:**
- ETL vs ELT patterns
- Data pipeline architecture
- Error handling in data workflows
- Data quality monitoring

---

### 11. Production Patterns DAG
**File:** [production_patterns_dag.py](https://github.com/kuldeep735/astro-project-deployed/blob/main/dags/production_patterns_dag.py)

**Purpose:** Production-ready patterns and monitoring

**Key Concepts:**
- **Data Quality Validation**: Automated quality checks
- **Performance Monitoring**: Resource usage tracking
- **Alerting Integration**: Notification systems
- **Operational Metrics**: System health monitoring
- **Testing Strategies**: Automated testing

**Code Analysis:**
```python
def data_quality_check(**context):
    print("=== Data Quality Check ===")
    
    quality_metrics = {
        'completeness': random.uniform(0.95, 1.0),
        'accuracy': random.uniform(0.90, 0.98),
        'consistency': random.uniform(0.92, 0.99),
    }
    
    thresholds = {
        'completeness': 0.95,
        'accuracy': 0.90,
        'consistency': 0.90,
    }
    
    failed_checks = []
    for metric, value in quality_metrics.items():
        if value < thresholds[metric]:
            failed_checks.append(metric)
    
    if failed_checks:
        context['ti'].xcom_push(key='quality_status', value='FAIL')
    else:
        context['ti'].xcom_push(key='quality_status', value='PASS')
```

**Operators Used:**
- `PythonOperator`
- `BashOperator`
- `EmptyOperator`
- `TaskGroup`

**Interview Points:**
- Production monitoring strategies
- Data quality frameworks
- Alerting and notification patterns
- Performance optimization

---

### 12. Custom Operator DAG
**File:** [custom_operator_dag.py](https://github.com/kuldeep735/astro-project-deployed/blob/main/dags/custom_operator_dag.py)

**Purpose:** Creating reusable custom operators

**Key Concepts:**
- **BaseOperator Inheritance**: Custom operator creation
- **Template Fields**: Jinja template support
- **Custom Sensors**: BaseSensorOperator extension
- **Reusable Components**: Encapsulated business logic
- **Operator Parameters**: Configurable behavior

**Code Analysis:**
```python
class DataValidationOperator(BaseOperator):
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
        # Custom validation logic
        self.log.info(f"Validating: {self.file_path}")
        # Implementation...

# Usage
validate_data = DataValidationOperator(
    task_id='validate_customer_data',
    file_path='/tmp/customers.json',
    validation_rules={
        'min_size': 100,
        'required_keys': ['customer_id', 'name', 'email']
    },
    dag=dag
)
```

**Operators Used:**
- Custom `DataValidationOperator`
- Custom `FileProcessingSensor`
- Custom `ApiResponseOperator`

**Interview Points:**
- Custom operator design patterns
- Template field implementation
- Operator inheritance strategies
- Reusability and maintainability

---

## Cloud Services DAGs

### 13. Google Cloud Dataproc DAG
**File:** [google_cloud_dataproc_dag.py](https://github.com/kuldeep735/astro-project-deployed/blob/main/dags/google_cloud_dataproc_dag.py)

**Purpose:** Demonstrates Apache Airflow integration with Google Cloud Dataproc for running Spark and Hadoop jobs

**Key Concepts:**
- **Dataproc Cluster Management**: Create and destroy ephemeral clusters
- **Spark Job Submission**: Submit PySpark jobs to Dataproc
- **Hadoop Job Execution**: Run MapReduce jobs
- **Cost Optimization**: Ephemeral clusters and spot instances
- **Cloud Storage Integration**: Data input/output from GCS

**Code Analysis:**
```python
def create_dataproc_cluster():
    cluster_config = {
        "project_id": "your-gcp-project-id",
        "cluster_name": "airflow-dataproc-cluster",
        "num_masters": 1,
        "num_workers": 2,
        "initialization_actions": ["gs://bucket/install-deps.sh"],
        "properties": {
            "spark:spark.sql.adaptive.enabled": "true"
        }
    }
    return cluster_config

def submit_spark_job():
    spark_job_config = {
        "pyspark_job": {
            "main_python_file_uri": "gs://bucket/spark-jobs/process.py",
            "args": ["--input-path", "gs://bucket/input/"],
            "properties": {
                "spark.executor.memory": "4g",
                "spark.executor.cores": "2"
            }
        }
    }
    return spark_job_config
```

**Operators Used:**
- `PythonOperator` (mock implementation)
- `BashOperator`
- `EmptyOperator`

**Production Operators:**
- `DataprocCreateClusterOperator`
- `DataprocSubmitJobOperator`
- `DataprocDeleteClusterOperator`

**Interview Points:**
- Big data processing with managed services
- Spark job optimization and configuration
- Cost management with ephemeral clusters
- Data pipeline orchestration in cloud environments

---

### 14. AWS EMR DAG
**File:** [aws_emr_dag.py](https://github.com/kuldeep735/astro-project-deployed/blob/main/dags/aws_emr_dag.py)

**Purpose:** Demonstrates Apache Airflow integration with AWS EMR for big data processing

**Key Concepts:**
- **EMR Cluster Lifecycle**: Create, configure, and terminate clusters
- **Spot Instances**: Cost optimization with EC2 spot instances
- **Bootstrap Actions**: Custom cluster initialization
- **S3 Integration**: Data storage and retrieval
- **Multi-step Processing**: Spark and Hadoop job coordination

**Code Analysis:**
```python
def create_emr_cluster():
    cluster_config = {
        "Name": "airflow-emr-cluster",
        "ReleaseLabel": "emr-6.4.0",
        "Applications": [{"Name": "Spark"}, {"Name": "Hadoop"}],
        "Instances": {
            "InstanceGroups": [
                {
                    "Name": "Core",
                    "Market": "SPOT",
                    "InstanceType": "m5.large",
                    "InstanceCount": 2,
                    "BidPrice": "0.10"
                }
            ]
        },
        "LogUri": "s3://your-bucket/emr-logs/"
    }
    return cluster_config

def submit_spark_step():
    spark_step = {
        "Name": "Data Processing with Spark",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "spark-submit",
                "--deploy-mode", "cluster",
                "s3://bucket/spark-jobs/process.py"
            ]
        }
    }
    return spark_step
```

**Operators Used:**
- `PythonOperator` (mock implementation)
- `BashOperator`
- `EmptyOperator`

**Production Operators:**
- `EmrCreateJobFlowOperator`
- `EmrAddStepsOperator`
- `EmrTerminateJobFlowOperator`
- `EmrJobFlowSensor`

**Interview Points:**
- AWS EMR vs Google Dataproc comparison
- Spot instance cost optimization
- Multi-step job coordination
- S3 data lake integration

---

### 15. Google BigQuery DAG
**File:** [google_bigquery_dag.py](https://github.com/kuldeep735/astro-project-deployed/blob/main/dags/google_bigquery_dag.py)

**Purpose:** Demonstrates Apache Airflow integration with Google BigQuery for data warehousing

**Key Concepts:**
- **Dataset Management**: Create and configure BigQuery datasets
- **Data Loading**: Load data from Cloud Storage to BigQuery
- **SQL Transformations**: Complex analytical queries
- **Data Quality**: Validation and monitoring
- **Materialized Views**: Performance optimization

**Code Analysis:**
```python
def run_data_transformation():
    transformation_sql = """
    CREATE OR REPLACE TABLE `project.dataset.customer_analytics` AS
    WITH customer_metrics AS (
        SELECT 
            customer_id,
            total_spend,
            CASE 
                WHEN total_spend > 1000 THEN 'High Value'
                WHEN total_spend > 500 THEN 'Medium Value'
                ELSE 'Low Value'
            END as customer_segment,
            AVG(total_spend) OVER (PARTITION BY country) as avg_country_spend
        FROM `project.dataset.customers`
    )
    SELECT * FROM customer_metrics
    """
    return transformation_sql

def run_data_quality_checks():
    quality_checks = [
        {
            "check_name": "row_count_validation",
            "sql": "SELECT COUNT(*) FROM table",
            "expected_min": 1000
        },
        {
            "check_name": "null_validation",
            "sql": "SELECT COUNT(*) FROM table WHERE id IS NULL",
            "expected_result": 0
        }
    ]
    return quality_checks
```

**Operators Used:**
- `PythonOperator` (mock implementation)
- `BashOperator`
- `EmptyOperator`

**Production Operators:**
- `BigQueryCreateDatasetOperator`
- `BigQueryInsertJobOperator`
- `BigQueryCheckOperator`
- `BigQueryToCloudStorageOperator`

**Interview Points:**
- Data warehouse architecture patterns
- SQL optimization for analytics
- Cost monitoring and optimization
- Data quality validation strategies

---

## Production DAGs

### 16. Kubernetes Docker DAG
**File:** [kubernetes_docker_dag.py](https://github.com/kuldeep735/astro-project-deployed/blob/main/dags/kubernetes_docker_dag.py)

**Purpose:** Container orchestration concepts

**Key Concepts:**
- **Container Patterns**: Docker integration
- **Kubernetes Concepts**: Pod, Service, Deployment
- **Resource Management**: CPU, memory limits
- **Scaling Strategies**: Horizontal and vertical
- **Container Lifecycle**: Build, deploy, monitor

**Interview Points:**
- Container orchestration in data pipelines
- Kubernetes resource management
- Scaling strategies for data workloads
- Container security considerations

---

### 14. ML Pipeline DAG
**File:** [ml_pipeline_dag.py](https://github.com/kuldeep735/astro-project-deployed/blob/main/dags/ml_pipeline_dag.py)

**Purpose:** Machine learning workflow orchestration

**Key Concepts:**
- **Model Training**: Automated ML workflows
- **Feature Engineering**: Data preprocessing
- **Model Evaluation**: Performance metrics
- **Model Deployment**: Production deployment
- **MLOps Patterns**: ML operations integration

**Interview Points:**
- ML pipeline architecture
- Feature store integration
- Model versioning and deployment
- ML monitoring and retraining

---

### 15. Database Operations DAG
**File:** [database_operations_dag.py](https://github.com/kuldeep735/astro-project-deployed/blob/main/dags/database_operations_dag.py)

**Purpose:** Database integration patterns

**Key Concepts:**
- **SQL Operations**: Database queries
- **Connection Management**: Database connections
- **Transaction Handling**: ACID properties
- **Data Migration**: Schema changes
- **Performance Optimization**: Query optimization

**Interview Points:**
- Database connection patterns
- Transaction management
- SQL operation best practices
- Database performance considerations

---

### 16. External Systems Integration DAG
**File:** [external_systems_dag.py](https://github.com/kuldeep735/astro-project-deployed/blob/main/dags/external_systems_dag.py)

**Purpose:** External system integration patterns

**Key Concepts:**
- **API Integration**: RESTful service calls
- **File System Operations**: File processing
- **Message Queues**: Asynchronous communication
- **Third-party Services**: External service integration
- **Error Handling**: Robust error management

**Interview Points:**
- API integration best practices
- Asynchronous processing patterns
- Error handling strategies
- Rate limiting and throttling

---

### 17. Interview Concepts DAG
**File:** [interview_concepts_dag.py](https://github.com/kuldeep735/astro-project-deployed/blob/main/dags/interview_concepts_dag.py)

**Purpose:** Common interview topics and patterns

**Key Concepts:**
- **Core Concepts**: DAG, Task, Operator, Sensor
- **Advanced Patterns**: Dynamic DAGs, custom operators
- **Production Considerations**: Monitoring, alerting, scaling
- **Best Practices**: Code organization, testing, deployment

**Interview Points:**
- Airflow architecture understanding
- Common interview questions
- Production deployment strategies
- Troubleshooting scenarios

---

### 18. Example DAG
**File:** [exampledag.py](https://github.com/kuldeep735/astro-project-deployed/blob/main/dags/exampledag.py)

**Purpose:** Basic example from Astronomer template

**Key Concepts:**
- **Astronomer Template**: Basic DAG structure
- **Simple Workflow**: Basic task execution
- **Getting Started**: Initial learning example

---

## All Operators Summary

| Operator | Purpose | Key Features | Use Cases | Interview Importance |
|----------|---------|--------------|-----------|---------------------|
| **PythonOperator** | Execute Python functions | - Direct function calls<br>- Parameter passing<br>- XCom integration | - Data processing<br>- Custom logic<br>- API calls | ⭐⭐⭐⭐⭐ |
| **BashOperator** | Execute bash commands | - Shell command execution<br>- Environment variables<br>- Return code handling | - File operations<br>- System commands<br>- Scripts | ⭐⭐⭐⭐ |
| **EmptyOperator** | Placeholder/dummy tasks | - No operation<br>- Workflow structure<br>- Dependencies | - DAG organization<br>- Placeholders<br>- Testing | ⭐⭐⭐ |
| **BranchPythonOperator** | Conditional execution | - Dynamic branching<br>- Skip logic<br>- Conditional workflows | - Decision points<br>- Conditional processing<br>- Dynamic paths | ⭐⭐⭐⭐ |
| **TaskGroup** | Task organization | - Logical grouping<br>- Visual organization<br>- Nested groups | - ETL phases<br>- Module organization<br>- Complex workflows | ⭐⭐⭐⭐ |
| **FileSensor** | File system monitoring | - File existence check<br>- Poke interval<br>- Timeout handling | - File-based triggers<br>- Data availability<br>- External dependencies | ⭐⭐⭐⭐ |
| **DateTimeSensor** | Time-based waiting | - Specific datetime<br>- Template support<br>- Timezone handling | - Scheduled delays<br>- Time-based triggers<br>- Coordination | ⭐⭐⭐ |
| **TimeDeltaSensor** | Duration-based waiting | - Relative time waiting<br>- Flexible duration<br>- Simple delays | - Processing delays<br>- Rate limiting<br>- Coordination | ⭐⭐⭐ |
| **@task (TaskFlow)** | Modern Python tasks | - Automatic XCom<br>- Type hints<br>- Cleaner syntax | - Modern workflows<br>- Data pipelines<br>- Pythonic approach | ⭐⭐⭐⭐⭐ |
| **@dag (TaskFlow)** | Modern DAG creation | - Decorator syntax<br>- Automatic instantiation<br>- Cleaner code | - Modern DAG patterns<br>- Simplified syntax<br>- Best practices | ⭐⭐⭐⭐⭐ |
| **Custom Operators** | Reusable components | - BaseOperator inheritance<br>- Template fields<br>- Business logic | - Domain-specific logic<br>- Reusability<br>- Maintainability | ⭐⭐⭐⭐⭐ |
| **Custom Sensors** | Custom monitoring | - BaseSensorOperator<br>- Poke logic<br>- External systems | - External dependencies<br>- Custom conditions<br>- Integration | ⭐⭐⭐⭐ |
| **DataprocCreateClusterOperator** | GCP Dataproc cluster creation | - Ephemeral clusters<br>- Spot instances<br>- Auto-scaling | - Big data processing<br>- Spark jobs<br>- Cost optimization | ⭐⭐⭐⭐⭐ |
| **DataprocSubmitJobOperator** | Submit Spark/Hadoop jobs | - PySpark jobs<br>- JAR dependencies<br>- Job monitoring | - Data processing<br>- ETL pipelines<br>- Analytics | ⭐⭐⭐⭐⭐ |
| **EmrCreateJobFlowOperator** | AWS EMR cluster creation | - Spot instances<br>- Bootstrap actions<br>- Multi-step jobs | - Big data processing<br>- Hadoop/Spark<br>- Cost optimization | ⭐⭐⭐⭐⭐ |
| **EmrAddStepsOperator** | Submit EMR steps | - Step coordination<br>- JAR execution<br>- S3 integration | - Multi-step processing<br>- Data pipelines<br>- Job orchestration | ⭐⭐⭐⭐⭐ |
| **BigQueryCreateDatasetOperator** | Create BigQuery datasets | - Dataset configuration<br>- Access controls<br>- Location settings | - Data warehouse setup<br>- Analytics preparation<br>- Data organization | ⭐⭐⭐⭐ |
| **BigQueryInsertJobOperator** | Run BigQuery queries | - SQL execution<br>- Job monitoring<br>- Result handling | - Data transformation<br>- Analytics queries<br>- ETL processing | ⭐⭐⭐⭐⭐ |
| **BigQueryCheckOperator** | Data quality validation | - SQL-based checks<br>- Threshold validation<br>- Quality metrics | - Data validation<br>- Quality assurance<br>- Monitoring | ⭐⭐⭐⭐ |

## Key Functions and Patterns

### XCom Functions
```python
# Push data
ti.xcom_push(key='data_key', value=data)

# Pull data
data = ti.xcom_pull(key='data_key', task_ids='source_task')

# Automatic XCom with TaskFlow
@task
def process_data():
    return {"processed": True}  # Automatically stored in XCom

@task
def use_data(data):  # Automatically pulled from XCom
    print(data["processed"])
```

### Templating Functions
```python
# Built-in macros
{{ ds }}                    # Execution date (YYYY-MM-DD)
{{ ts }}                    # Execution timestamp
{{ logical_date }}          # Logical date object
{{ data_interval_start }}   # Data interval start
{{ data_interval_end }}     # Data interval end
{{ dag.dag_id }}           # DAG ID
{{ task.task_id }}         # Task ID
{{ run_id }}               # Run ID

# Variables
{{ var.value.get('key') }}  # Airflow Variable
{{ params.parameter }}      # DAG parameters

# Conditional templating
{% if condition %}
    # Conditional content
{% endif %}

# Loop templating
{% for item in items %}
    # Loop content
{% endfor %}
```

### Error Handling Functions
```python
# Retry configuration
def retry_delay_function(context):
    try_number = context['task_instance'].try_number
    return timedelta(seconds=30 * (2 ** (try_number - 1)))

# Failure callback
def task_failure_callback(context):
    # Send alerts, create tickets, log failures
    pass

# Success callback
def task_success_callback(context):
    # Log recovery, send notifications
    pass
```

### Sensor Functions
```python
# Custom poke function
def poke_function(context):
    # Check external condition
    return condition_met  # True/False

# Sensor with timeout
sensor = FileSensor(
    task_id='wait_for_file',
    filepath='/path/to/file',
    poke_interval=60,    # Check every minute
    timeout=3600,        # Timeout after 1 hour
    mode='poke',         # vs 'reschedule'
    dag=dag
)
```

## Interview Preparation Summary

### Must-Know Concepts
1. **DAG Structure**: Dependencies, scheduling, parameters
2. **Task Types**: Operators, sensors, task groups
3. **Data Flow**: XComs, templating, parameters
4. **Scheduling**: Cron expressions, catchup, intervals
5. **Error Handling**: Retries, callbacks, circuit breakers
6. **Production Patterns**: Monitoring, alerting, scaling
7. **Modern Patterns**: TaskFlow API, custom operators

### Common Interview Questions
1. "How do you handle task failures?"
2. "Explain XCom usage and limitations"
3. "What's the difference between schedule_interval and catchup?"
4. "How do you implement conditional workflows?"
5. "Describe sensor vs operator differences"
6. "How do you monitor production DAGs?"
7. "What are TaskGroups and when to use them?"
8. "How do you integrate Airflow with cloud services?"
9. "Explain the difference between AWS EMR and Google Dataproc"
10. "How do you optimize costs in cloud-based data pipelines?"
11. "What are the best practices for BigQuery integration?"
12. "How do you handle ephemeral cluster management?"
13. "Explain data quality validation strategies"
14. "How do you implement data lineage tracking?"

### Best Practices Demonstrated
- Use TaskFlow API for modern workflows
- Implement proper error handling and retries
- Use sensors for external dependencies
- Organize tasks with TaskGroups
- Apply templating for dynamic content
- Monitor data quality and performance
- Create reusable custom operators

This guide provides comprehensive coverage of all DAG patterns, operators, and concepts essential for Airflow mastery and interview success.