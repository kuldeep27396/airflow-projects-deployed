"""
A DAG demonstrating XComs for passing data between tasks.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def push_xcom(ti=None):
    ti.xcom_push(key='sample_key', value='Airflow XComs are cool!')

def pull_xcom(ti=None):
    value = ti.xcom_pull(key='sample_key', task_ids='push_task')
    print(f"Pulled XCom value: {value}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'xcom_example_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["learning", "xcom"]
)

push_task = PythonOperator(
    task_id='push_task',
    python_callable=push_xcom,
    provide_context=True,
    dag=dag
)
pull_task = PythonOperator(
    task_id='pull_task',
    python_callable=pull_xcom,
    provide_context=True,
    dag=dag
)

push_task >> pull_task
