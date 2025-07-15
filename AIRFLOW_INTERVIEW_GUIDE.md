# Airflow Learning DAGs - Complete Interview Guide

This repository contains a comprehensive collection of Apache Airflow DAGs designed for learning and interview preparation. Each DAG demonstrates specific concepts, patterns, and best practices commonly asked about in data engineering interviews.

## 📚 Learning Path

### **Basic Level (Start Here)**
1. **hello_world.py** - TaskFlow API Fundamentals
2. **basic_python_operator_dag.py** - Traditional Operators
3. **xcom_example_dag.py** - Data Passing
4. **branching_dag.py** - Control Flow

### **Intermediate Level**
5. **sensor_dag.py** - Sensor Operations
6. **taskgroup_dag.py** - Task Organization
7. **templating_dag.py** - Jinja Templating
8. **scheduling_patterns_dag.py** - Scheduling Strategies

### **Advanced Level**
9. **database_operations_dag.py** - Database Integration
10. **external_systems_dag.py** - External Integration
11. **ml_pipeline_dag.py** - Machine Learning Pipeline
12. **data_pipeline_etl_dag.py** - Complete ETL Pipeline

### **Expert Level**
13. **custom_operator_dag.py** - Custom Operators
14. **error_handling_retry_dag.py** - Error Handling
15. **interview_concepts_dag.py** - Interview Concepts
16. **production_patterns_dag.py** - Production Patterns
17. **kubernetes_docker_dag.py** - Container Orchestration
18. **exampledag.py** - Production Example

## 🎯 Interview Topics Covered

### **Core Airflow Concepts**
- **DAG (Directed Acyclic Graph)**: Workflow definition and structure
- **Tasks**: Unit of work in Airflow
- **Operators**: Define what gets executed
- **Hooks**: Interface to external systems
- **Sensors**: Wait for conditions to be met
- **XComs**: Cross-communication between tasks
- **Context**: Task execution environment

### **Operators Deep Dive**

#### **Python Operators**
- **PythonOperator**: Execute Python functions
- **BranchPythonOperator**: Conditional workflow branching
- **@task decorator**: TaskFlow API for cleaner code
- **PythonVirtualenvOperator**: Isolated Python environments

#### **Bash Operators**
- **BashOperator**: Execute shell commands
- **Environment variables**: Pass data to shell scripts
- **Template fields**: Dynamic command generation

#### **Sensor Operators**
- **FileSensor**: Wait for file existence
- **HttpSensor**: Monitor HTTP endpoints
- **DateTimeSensor**: Wait for specific time
- **TimeDeltaSensor**: Wait for time duration
- **Custom sensors**: Build your own waiting logic

#### **Database Operators**
- **SQLOperator**: Execute SQL queries
- **PostgresOperator**: PostgreSQL specific operations
- **MySQLOperator**: MySQL specific operations
- **Connection management**: Database connection pooling

#### **Container Operators**
- **DockerOperator**: Run tasks in Docker containers
- **KubernetesPodOperator**: Kubernetes pod execution
- **Resource management**: CPU/memory limits
- **Volume mounts**: Data persistence

#### **Transfer Operators**
- **S3ToRedshiftOperator**: Data transfer between systems
- **MsSqlToHiveOperator**: Cross-platform data movement
- **Generic transfer patterns**: ETL operations

### **Advanced Concepts**

#### **TaskGroups vs SubDAGs**
- **TaskGroups**: Modern task organization (Airflow 2.0+)
- **SubDAGs**: Legacy task grouping (deprecated)
- **Parallelism**: Task group execution patterns
- **Dependencies**: Inter-group relationships

#### **Scheduling and Triggers**
- **Schedule intervals**: Cron expressions, presets, timedeltas
- **Catchup**: Historical data processing
- **Backfill**: Reprocessing historical runs
- **Trigger rules**: all_success, all_failed, one_success, one_failed, all_done
- **Manual triggers**: Ad-hoc execution

#### **Error Handling and Retries**
- **Retry mechanisms**: Exponential backoff, max retries
- **Failure callbacks**: Custom error handling
- **SLA monitoring**: Service level agreements
- **Circuit breaker**: Prevent cascading failures
- **Graceful degradation**: Fallback strategies

#### **Data Quality and Monitoring**
- **Data validation**: Schema, type, business rule checks
- **Quality metrics**: Completeness, accuracy, consistency
- **Monitoring**: Performance, resource usage, health
- **Alerting**: Multi-channel notifications
- **Incident management**: Automated response

#### **Resource Management**
- **Pools**: Resource allocation and limiting
- **Priority weights**: Task scheduling priority
- **Concurrency**: max_active_runs, max_active_tasks
- **Resource optimization**: Memory, CPU, I/O

#### **Security and Access Control**
- **Connections**: Encrypted credential storage
- **Variables**: Configuration management
- **Secrets backend**: External secret management
- **RBAC**: Role-based access control
- **Audit logging**: Security monitoring

#### **Performance Optimization**
- **Parallelism**: Task and DAG level concurrency
- **Dynamic task mapping**: Runtime task generation
- **Resource pooling**: Connection and memory management
- **Caching strategies**: Data and computation caching
- **Batch processing**: Efficient data handling

### **Production Patterns**

#### **Deployment Strategies**
- **CI/CD integration**: Automated testing and deployment
- **Environment management**: Dev, staging, production
- **DAG versioning**: Code version control
- **Blue-green deployment**: Zero-downtime updates

#### **Monitoring and Observability**
- **Metrics collection**: Custom and system metrics
- **Log aggregation**: Centralized logging
- **Distributed tracing**: End-to-end visibility
- **Health checks**: System status monitoring

#### **Data Pipeline Architecture**
- **ETL vs ELT**: Extract-Transform-Load patterns
- **Data lineage**: Tracking data flow
- **Schema evolution**: Handling data changes
- **Data versioning**: Dataset version control

#### **Scalability and High Availability**
- **Executor types**: Sequential, Local, Celery, Kubernetes
- **Database scaling**: PostgreSQL optimization
- **Load balancing**: Web server distribution
- **Disaster recovery**: Backup and restore strategies

## 🛠 Technical Implementation Details

### **DAG Configuration**
```python
# Modern DAG definition (Airflow 2.0+)
@dag(
    dag_id='example_dag',
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    default_args={'retries': 2}
)
def example_dag():
    # TaskFlow API tasks
    @task
    def extract_data():
        return data
    
    @task
    def transform_data(data):
        return processed_data
    
    # Define dependencies
    transform_data(extract_data())
```

### **Task Dependencies**
```python
# Sequential dependencies
task_a >> task_b >> task_c

# Parallel dependencies
task_a >> [task_b, task_c] >> task_d

# Mixed dependencies
[task_a, task_b] >> task_c >> [task_d, task_e]

# Conditional dependencies
task_a >> branch_task >> [task_b, task_c]
```

### **XCom Usage**
```python
# Push data to XCom
def push_data(**context):
    context['ti'].xcom_push(key='result', value=data)

# Pull data from XCom
def pull_data(**context):
    data = context['ti'].xcom_pull(key='result', task_ids='upstream_task')
```

### **Error Handling**
```python
# Custom retry logic
def retry_on_failure(context):
    if context['task_instance'].try_number <= 3:
        return True  # Retry
    return False  # Don't retry

# Task with error handling
task = PythonOperator(
    task_id='error_prone_task',
    python_callable=risky_function,
    retries=3,
    retry_delay=timedelta(minutes=5),
    on_failure_callback=failure_callback
)
```

## 📊 Common Interview Questions & Answers

### **Q: What is the difference between Airflow 1.x and 2.x?**
**A:** Key differences include:
- **TaskFlow API**: Simplified task definition with decorators
- **Scheduler improvements**: Better performance and reliability
- **UI enhancements**: Modern React-based interface
- **Task groups**: Replace SubDAGs for better organization
- **Stable API**: REST API for external integrations
- **Security**: Enhanced RBAC and security features

### **Q: How do you handle task failures in production?**
**A:** Multiple strategies:
- **Retry mechanisms**: Exponential backoff, maximum retries
- **Failure callbacks**: Custom error handling and notifications
- **SLA monitoring**: Service level agreement tracking
- **Circuit breaker**: Prevent cascading failures
- **Graceful degradation**: Fallback to alternative data sources
- **Alerting**: Multi-channel notifications (email, Slack, PagerDuty)

### **Q: How do you optimize Airflow performance?**
**A:** Performance optimization techniques:
- **Parallelism**: Increase concurrent task execution
- **Executor choice**: Kubernetes/Celery for distributed processing
- **Resource pooling**: Manage database connections and memory
- **Task design**: Avoid long-running tasks, use appropriate operators
- **Database optimization**: PostgreSQL tuning, connection pooling
- **Monitoring**: Track performance metrics and bottlenecks

### **Q: How do you handle data quality in Airflow?**
**A:** Data quality strategies:
- **Validation tasks**: Schema, type, and business rule checks
- **Quality metrics**: Completeness, accuracy, consistency monitoring
- **Branching logic**: Route data based on quality results
- **Alerting**: Notify on quality failures
- **Quarantine**: Isolate bad data for investigation
- **Lineage tracking**: Understand data flow and dependencies

### **Q: How do you manage secrets and connections?**
**A:** Security best practices:
- **Connections**: Encrypted storage in Airflow database
- **Variables**: Configuration management with encryption
- **Secrets backend**: External systems (AWS Secrets Manager, HashiCorp Vault)
- **Environment variables**: Runtime configuration
- **RBAC**: Role-based access control
- **Audit logging**: Track access and modifications

### **Q: How do you test Airflow DAGs?**
**A:** Testing strategies:
- **Unit tests**: Test individual task logic
- **Integration tests**: Test DAG execution flow
- **Data quality tests**: Validate data transformations
- **Performance tests**: Load and stress testing
- **Smoke tests**: Basic functionality verification
- **CI/CD integration**: Automated testing pipeline

## 🚀 Getting Started

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd astro-project
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Start Airflow**
   ```bash
   astro dev start
   ```

4. **Access the UI**
   Open http://localhost:8080 in your browser

5. **Run the DAGs**
   - Start with basic examples
   - Progress through intermediate concepts
   - Explore advanced patterns
   - Study production implementations

## 📖 Study Guide

### **Week 1: Fundamentals**
- Basic DAG structure and syntax
- Core operators (Python, Bash, Empty)
- Task dependencies and XComs
- Scheduling basics

### **Week 2: Intermediate Concepts**
- Sensors and external system integration
- TaskGroups and workflow organization
- Templating and dynamic content
- Error handling and retries

### **Week 3: Advanced Patterns**
- Custom operators and hooks
- Database operations and ETL
- Machine learning pipelines
- Container orchestration

### **Week 4: Production Ready**
- Monitoring and alerting
- Performance optimization
- Security and access control
- Deployment strategies

## 🎓 Certification Preparation

This collection covers topics for:
- **Apache Airflow Certification**
- **Data Engineering Interviews**
- **Cloud Platform Certifications** (AWS, GCP, Azure)
- **Production System Design**

## 🔗 Additional Resources

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Astronomer Learn](https://www.astronomer.io/learn/)
- [Airflow Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [DAG Writing Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#writing-a-dag)

## 📝 Notes

- All DAGs are designed to be self-contained and educational
- Examples include comprehensive documentation and comments
- Production patterns demonstrate real-world scenarios
- Error handling examples show resilient design patterns
- Performance examples highlight optimization techniques

---

**Happy Learning!** 🎉

Use this repository as your comprehensive guide to mastering Apache Airflow for interviews and production environments.