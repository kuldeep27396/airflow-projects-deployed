#!/usr/bin/env python3
"""
Test script to verify the Airflow environment is working properly.
"""

def test_imports():
    """Test that all required modules can be imported."""
    print("Testing imports...")
    
    try:
        import airflow
        print(f"✅ Airflow version: {airflow.__version__}")
    except ImportError as e:
        print(f"❌ Airflow import failed: {e}")
    
    try:
        import pandas as pd
        print(f"✅ Pandas version: {pd.__version__}")
    except ImportError as e:
        print(f"❌ Pandas import failed: {e}")
    
    try:
        import numpy as np
        print(f"✅ NumPy version: {np.__version__}")
    except ImportError as e:
        print(f"❌ NumPy import failed: {e}")
    
    try:
        import requests
        print(f"✅ Requests version: {requests.__version__}")
    except ImportError as e:
        print(f"❌ Requests import failed: {e}")
    
    try:
        import sklearn
        print(f"✅ Scikit-learn version: {sklearn.__version__}")
    except ImportError as e:
        print(f"⚠️  Scikit-learn not available: {e}")
    
    try:
        from airflow.operators.python import PythonOperator
        from airflow.operators.bash import BashOperator
        from airflow.operators.empty import EmptyOperator
        print("✅ Core operators imported successfully")
    except ImportError as e:
        print(f"❌ Core operators import failed: {e}")
    
    try:
        from airflow.sensors.filesystem import FileSensor
        from airflow.sensors.date_time import DateTimeSensor
        print("✅ Sensors imported successfully")
    except ImportError as e:
        print(f"❌ Sensors import failed: {e}")
    
    try:
        from airflow.utils.task_group import TaskGroup
        print("✅ TaskGroup imported successfully")
    except ImportError as e:
        print(f"❌ TaskGroup import failed: {e}")

def test_dag_compilation():
    """Test that basic DAG can be compiled."""
    print("\nTesting DAG compilation...")
    
    try:
        from airflow import DAG
        from airflow.operators.python import PythonOperator
        from datetime import datetime
        
        def test_task():
            return "Hello World"
        
        dag = DAG(
            'test_dag',
            start_date=datetime(2024, 1, 1),
            schedule=None,
            catchup=False
        )
        
        task = PythonOperator(
            task_id='test_task',
            python_callable=test_task,
            dag=dag
        )
        
        print("✅ Basic DAG compilation successful")
        
    except Exception as e:
        print(f"❌ DAG compilation failed: {e}")

if __name__ == "__main__":
    test_imports()
    test_dag_compilation()
    print("\n🎉 Environment test completed!")