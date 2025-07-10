"""
Branching DAG Example
=====================

This DAG demonstrates how to use the BranchPythonOperator in Airflow to control the flow of execution based on logic.

How it works:
-------------
1. The DAG starts with an EmptyOperator (`start`).
2. The `branch` task uses BranchPythonOperator to randomly choose between two branches: `branch_a` or `branch_b`.
3. Only the chosen branch will run; the other will be skipped.
4. Both branches converge at the `end` task (EmptyOperator).

This pattern is useful for conditional workflows, such as running different tasks based on data or external conditions.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import random

def choose_branch():
    return 'branch_a' if random.choice([True, False]) else 'branch_b'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'branching_dag',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["learning", "branching"]
)

start = EmptyOperator(task_id='start', dag=dag)
branch = BranchPythonOperator(
    task_id='branching',
    python_callable=choose_branch,
    dag=dag
)
branch_a = PythonOperator(
    task_id='branch_a',
    python_callable=lambda: print("Branch A chosen"),
    dag=dag
)
branch_b = PythonOperator(
    task_id='branch_b',
    python_callable=lambda: print("Branch B chosen"),
    dag=dag
)
end = EmptyOperator(task_id='end', dag=dag)

start >> branch >> [branch_a, branch_b] >> end
