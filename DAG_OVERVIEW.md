# DAG Overview - Quick Reference

## 📋 Complete DAG List

| DAG Name | Level | Key Concepts | Operators Used | Interview Topics |
|----------|-------|-------------|----------------|------------------|
| **hello_world.py** | Basic | TaskFlow API, @task decorator | @task, @dag | Modern Airflow syntax |
| **basic_python_operator_dag.py** | Basic | PythonOperator, task dependencies | PythonOperator | Traditional workflow |
| **xcom_example_dag.py** | Basic | XCom, data passing | PythonOperator | Inter-task communication |
| **branching_dag.py** | Basic | Conditional logic, branching | BranchPythonOperator, EmptyOperator | Control flow |
| **sensor_dag.py** | Intermediate | Sensors, waiting logic | FileSensor, HttpSensor, DateTimeSensor | External dependencies |
| **taskgroup_dag.py** | Intermediate | Task organization | TaskGroup, PythonOperator | Workflow organization |
| **templating_dag.py** | Intermediate | Jinja templating, macros | BashOperator, templating | Dynamic content |
| **scheduling_patterns_dag.py** | Intermediate | Scheduling, cron | Multiple DAGs, scheduling | Time-based execution |
| **database_operations_dag.py** | Advanced | Database ops, SQL | PythonOperator, SQLite | Data persistence |
| **external_systems_dag.py** | Advanced | API integration, HTTP | BashOperator, requests | System integration |
| **ml_pipeline_dag.py** | Advanced | ML workflow, scikit-learn | PythonOperator, ML libraries | Data science pipelines |
| **data_pipeline_etl_dag.py** | Advanced | ETL, data transformation | TaskGroup, pandas | Data engineering |
| **custom_operator_dag.py** | Expert | Custom operators, extensibility | Custom classes | Advanced patterns |
| **error_handling_retry_dag.py** | Expert | Error handling, retries | Callbacks, retry logic | Resilience patterns |
| **interview_concepts_dag.py** | Expert | Interview topics | Trigger rules, pools, SLA | Core concepts |
| **production_patterns_dag.py** | Expert | Production readiness | Monitoring, alerting | Operations |
| **kubernetes_docker_dag.py** | Expert | Containerization | Container concepts | Modern deployment |
| **exampledag.py** | Expert | Production example | TaskFlow API, dynamic mapping | Real-world example |

## 🎯 Operator Categories

### **Core Operators**
- **PythonOperator**: Execute Python functions
- **BashOperator**: Execute shell commands
- **EmptyOperator**: Placeholder/grouping tasks
- **BranchPythonOperator**: Conditional workflow branching

### **Sensor Operators**
- **FileSensor**: Wait for file existence
- **HttpSensor**: Monitor HTTP endpoints
- **DateTimeSensor**: Wait for specific datetime
- **TimeDeltaSensor**: Wait for time duration
- **CustomThresholdSensor**: Custom waiting logic

### **Database Operators**
- **SQLOperator**: Execute SQL queries
- **PostgresOperator**: PostgreSQL operations
- **MySQLOperator**: MySQL operations
- **Custom database operators**: Specialized DB operations

### **Container Operators**
- **DockerOperator**: Docker container execution
- **KubernetesPodOperator**: Kubernetes pod execution
- **Container patterns**: Resource management, volumes

### **Transfer Operators**
- **S3ToRedshiftOperator**: Cloud data transfer
- **MsSqlToHiveOperator**: Cross-platform transfer
- **Generic transfer patterns**: ETL operations

### **Custom Operators**
- **DataValidationOperator**: Data quality checks
- **FileProcessingOperator**: File transformations
- **NotificationOperator**: Multi-channel alerts
- **Business logic operators**: Domain-specific operations

## 🔧 Technical Patterns

### **Task Dependencies**
```python
# Sequential
task_a >> task_b >> task_c

# Parallel
task_a >> [task_b, task_c] >> task_d

# Mixed
[task_a, task_b] >> task_c >> [task_d, task_e]

# Conditional
task_a >> branch_task >> [task_b, task_c]
```

### **XCom Patterns**
```python
# Push data
context['ti'].xcom_push(key='result', value=data)

# Pull data
data = context['ti'].xcom_pull(key='result', task_ids='upstream_task')

# TaskFlow API (automatic XCom)
@task
def process_data(data):
    return processed_data
```

### **Error Handling**
```python
# Retry configuration
retries=3,
retry_delay=timedelta(minutes=5),
on_failure_callback=failure_callback

# Custom retry logic
def custom_retry_delay(context):
    return timedelta(seconds=30 * (2 ** context['task_instance'].try_number))
```

### **Trigger Rules**
- **all_success**: All upstream tasks succeed (default)
- **all_failed**: All upstream tasks fail
- **one_success**: At least one upstream task succeeds
- **one_failed**: At least one upstream task fails
- **all_done**: All upstream tasks complete (success or failure)

## 📊 Interview Question Mapping

### **Basic Questions**
| Question | DAG Reference | Key Concepts |
|----------|---------------|-------------|
| "What is a DAG?" | hello_world.py | DAG structure, tasks, dependencies |
| "How do tasks communicate?" | xcom_example_dag.py | XCom, data passing |
| "How do you handle conditional logic?" | branching_dag.py | BranchPythonOperator |
| "What are operators?" | basic_python_operator_dag.py | PythonOperator, BashOperator |

### **Intermediate Questions**
| Question | DAG Reference | Key Concepts |
|----------|---------------|-------------|
| "How do you wait for external events?" | sensor_dag.py | Sensors, polling |
| "How do you organize complex workflows?" | taskgroup_dag.py | TaskGroups, organization |
| "How do you use dynamic content?" | templating_dag.py | Jinja templates, macros |
| "How do you schedule DAGs?" | scheduling_patterns_dag.py | Cron, schedules |

### **Advanced Questions**
| Question | DAG Reference | Key Concepts |
|----------|---------------|-------------|
| "How do you handle database operations?" | database_operations_dag.py | SQL, connections |
| "How do you integrate with external systems?" | external_systems_dag.py | APIs, HTTP |
| "How do you build ML pipelines?" | ml_pipeline_dag.py | ML workflow, data science |
| "How do you design ETL pipelines?" | data_pipeline_etl_dag.py | ETL, data transformation |

### **Expert Questions**
| Question | DAG Reference | Key Concepts |
|----------|---------------|-------------|
| "How do you create custom operators?" | custom_operator_dag.py | Custom classes, extensibility |
| "How do you handle failures?" | error_handling_retry_dag.py | Retries, callbacks |
| "What are production considerations?" | production_patterns_dag.py | Monitoring, alerting |
| "How do you use containers?" | kubernetes_docker_dag.py | Docker, Kubernetes |

## 🎓 Study Progression

### **Week 1: Fundamentals**
1. **hello_world.py** - Understand DAG basics
2. **basic_python_operator_dag.py** - Learn operators
3. **xcom_example_dag.py** - Data passing
4. **branching_dag.py** - Control flow

### **Week 2: Intermediate**
1. **sensor_dag.py** - External dependencies
2. **taskgroup_dag.py** - Organization
3. **templating_dag.py** - Dynamic content
4. **scheduling_patterns_dag.py** - Timing

### **Week 3: Advanced**
1. **database_operations_dag.py** - Data persistence
2. **external_systems_dag.py** - Integration
3. **ml_pipeline_dag.py** - ML workflows
4. **data_pipeline_etl_dag.py** - ETL patterns

### **Week 4: Expert**
1. **custom_operator_dag.py** - Custom development
2. **error_handling_retry_dag.py** - Resilience
3. **interview_concepts_dag.py** - Core concepts
4. **production_patterns_dag.py** - Operations

## 🚀 Quick Commands

### **Start Airflow**
```bash
astro dev start
```

### **Access UI**
```
http://localhost:8080
```

### **Test DAG**
```bash
astro dev run dags test <dag_id> <execution_date>
```

### **List DAGs**
```bash
astro dev run dags list
```

### **Trigger DAG**
```bash
astro dev run dags trigger <dag_id>
```

## 📝 Key Takeaways

1. **Start Simple**: Begin with basic DAGs and progress gradually
2. **Practice Regularly**: Run DAGs and observe behavior
3. **Understand Concepts**: Don't just memorize, understand the why
4. **Focus on Patterns**: Learn common patterns and anti-patterns
5. **Production Ready**: Always think about production considerations
6. **Stay Updated**: Airflow evolves rapidly, keep learning

## 🔍 Common Pitfalls

1. **Don't use top-level code**: Avoid expensive operations in DAG definition
2. **Don't use dynamic DAG generation**: Avoid complex dynamic DAGs
3. **Don't ignore dependencies**: Always define clear task dependencies
4. **Don't skip error handling**: Always plan for failures
5. **Don't ignore monitoring**: Production requires observability
6. **Don't use SubDAGs**: Use TaskGroups instead (Airflow 2.0+)

---

**Use this overview as your quick reference guide during interviews and daily work!**