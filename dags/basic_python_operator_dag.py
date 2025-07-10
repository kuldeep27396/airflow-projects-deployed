"""
A simple DAG using PythonOperator to print messages.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    print("Hello from Airflow!")

def print_goodbye():
    print("Goodbye from Airflow!")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'basic_python_operator_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["learning", "basic"]
)

hello_task = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello,
    dag=dag
)

goodbye_task = PythonOperator(
    task_id='print_goodbye',
    python_callable=print_goodbye,
    dag=dag
)

hello_task >> goodbye_task
