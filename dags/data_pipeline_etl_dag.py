"""
Data Pipeline ETL DAG Example
==============================

This DAG demonstrates a comprehensive ETL (Extract, Transform, Load) pipeline:
- Extract data from multiple sources (CSV, JSON, API)
- Transform data with cleaning, validation, and aggregation
- Load data into target systems
- Data quality monitoring and lineage tracking

This represents a typical production data pipeline workflow.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import pandas as pd
import json
import csv
import requests
import os
from typing import Dict, List

def create_sample_data_sources():
    """Create sample data sources for ETL demonstration."""
    print("=== Creating Sample Data Sources ===")
    
    # Create CSV data source - Customer data
    customers_data = [
        ['customer_id', 'name', 'email', 'signup_date', 'country'],
        [1, 'John Doe', 'john@example.com', '2024-01-15', 'USA'],
        [2, 'Jane Smith', 'jane@example.com', '2024-01-16', 'Canada'],
        [3, 'Bob Johnson', 'bob@example.com', '2024-01-17', 'UK'],
        [4, 'Alice Wilson', 'alice@example.com', '2024-01-18', 'Australia'],
        [5, 'Charlie Brown', 'charlie@example.com', '2024-01-19', 'Germany']
    ]
    
    customers_csv = '/tmp/customers.csv'
    with open(customers_csv, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(customers_data)
    
    # Create JSON data source - Product data
    products_data = [
        {'product_id': 1, 'name': 'Laptop', 'category': 'Electronics', 'price': 999.99, 'stock': 50},
        {'product_id': 2, 'name': 'Mouse', 'category': 'Electronics', 'price': 29.99, 'stock': 200},
        {'product_id': 3, 'name': 'Keyboard', 'category': 'Electronics', 'price': 79.99, 'stock': 150},
        {'product_id': 4, 'name': 'Monitor', 'category': 'Electronics', 'price': 299.99, 'stock': 75},
        {'product_id': 5, 'name': 'Webcam', 'category': 'Electronics', 'price': 89.99, 'stock': 100}
    ]
    
    products_json = '/tmp/products.json'
    with open(products_json, 'w') as f:
        json.dump(products_data, f, indent=2)
    
    # Create transactions data (simulated from API)
    transactions_data = [
        {'transaction_id': 1, 'customer_id': 1, 'product_id': 1, 'quantity': 1, 'transaction_date': '2024-01-20'},
        {'transaction_id': 2, 'customer_id': 2, 'product_id': 2, 'quantity': 2, 'transaction_date': '2024-01-21'},
        {'transaction_id': 3, 'customer_id': 3, 'product_id': 3, 'quantity': 1, 'transaction_date': '2024-01-22'},
        {'transaction_id': 4, 'customer_id': 1, 'product_id': 4, 'quantity': 1, 'transaction_date': '2024-01-23'},
        {'transaction_id': 5, 'customer_id': 4, 'product_id': 5, 'quantity': 3, 'transaction_date': '2024-01-24'},
        {'transaction_id': 6, 'customer_id': 2, 'product_id': 1, 'quantity': 1, 'transaction_date': '2024-01-25'}
    ]
    
    transactions_json = '/tmp/transactions.json'
    with open(transactions_json, 'w') as f:
        json.dump(transactions_data, f, indent=2)
    
    print(f"✓ Created customers CSV: {customers_csv}")
    print(f"✓ Created products JSON: {products_json}")
    print(f"✓ Created transactions JSON: {transactions_json}")

def extract_csv_data():
    """Extract data from CSV source."""
    print("=== Extracting CSV Data ===")
    
    customers_csv = '/tmp/customers.csv'
    df = pd.read_csv(customers_csv)
    
    print(f"Extracted {len(df)} customer records")
    print(f"Columns: {list(df.columns)}")
    
    # Save extracted data
    extracted_path = '/tmp/extracted_customers.csv'
    df.to_csv(extracted_path, index=False)
    
    return extracted_path

def extract_json_data():
    """Extract data from JSON sources."""
    print("=== Extracting JSON Data ===")
    
    # Extract products
    with open('/tmp/products.json', 'r') as f:
        products_data = json.load(f)
    
    products_df = pd.DataFrame(products_data)
    products_path = '/tmp/extracted_products.csv'
    products_df.to_csv(products_path, index=False)
    
    # Extract transactions
    with open('/tmp/transactions.json', 'r') as f:
        transactions_data = json.load(f)
    
    transactions_df = pd.DataFrame(transactions_data)
    transactions_path = '/tmp/extracted_transactions.csv'
    transactions_df.to_csv(transactions_path, index=False)
    
    print(f"Extracted {len(products_df)} product records")
    print(f"Extracted {len(transactions_df)} transaction records")
    
    return products_path, transactions_path

def extract_api_data():
    """Simulate extracting data from external API."""
    print("=== Extracting API Data ===")
    
    # Simulate API call for exchange rates
    try:
        # Use a real API for demo (JSONPlaceholder)
        response = requests.get('https://jsonplaceholder.typicode.com/posts', timeout=10)
        response.raise_for_status()
        
        api_data = response.json()[:5]  # Take first 5 records
        
        # Transform to relevant format
        enrichment_data = [
            {'country': 'USA', 'currency': 'USD', 'exchange_rate': 1.0},
            {'country': 'Canada', 'currency': 'CAD', 'exchange_rate': 1.35},
            {'country': 'UK', 'currency': 'GBP', 'exchange_rate': 0.82},
            {'country': 'Australia', 'currency': 'AUD', 'exchange_rate': 1.52},
            {'country': 'Germany', 'currency': 'EUR', 'exchange_rate': 0.92}
        ]
        
        enrichment_df = pd.DataFrame(enrichment_data)
        enrichment_path = '/tmp/extracted_enrichment.csv'
        enrichment_df.to_csv(enrichment_path, index=False)
        
        print(f"Extracted {len(enrichment_df)} enrichment records")
        print("✓ API extraction successful")
        
    except Exception as e:
        print(f"API extraction failed: {e}")
        print("Using fallback data...")
        
        # Fallback data
        fallback_data = [
            {'country': 'USA', 'currency': 'USD', 'exchange_rate': 1.0},
            {'country': 'Canada', 'currency': 'CAD', 'exchange_rate': 1.35},
            {'country': 'UK', 'currency': 'GBP', 'exchange_rate': 0.82}
        ]
        
        enrichment_df = pd.DataFrame(fallback_data)
        enrichment_path = '/tmp/extracted_enrichment.csv'
        enrichment_df.to_csv(enrichment_path, index=False)

def validate_extracted_data():
    """Validate extracted data quality."""
    print("=== Data Validation ===")
    
    validation_results = {}
    
    # Validate customers data
    customers_df = pd.read_csv('/tmp/extracted_customers.csv')
    validation_results['customers'] = {
        'row_count': len(customers_df),
        'null_values': customers_df.isnull().sum().sum(),
        'duplicate_emails': customers_df['email'].duplicated().sum(),
        'invalid_emails': (~customers_df['email'].str.contains('@')).sum()
    }
    
    # Validate products data
    products_df = pd.read_csv('/tmp/extracted_products.csv')
    validation_results['products'] = {
        'row_count': len(products_df),
        'null_values': products_df.isnull().sum().sum(),
        'negative_prices': (products_df['price'] < 0).sum(),
        'zero_stock': (products_df['stock'] == 0).sum()
    }
    
    # Validate transactions data
    transactions_df = pd.read_csv('/tmp/extracted_transactions.csv')
    validation_results['transactions'] = {
        'row_count': len(transactions_df),
        'null_values': transactions_df.isnull().sum().sum(),
        'invalid_quantities': (transactions_df['quantity'] <= 0).sum(),
        'future_dates': (pd.to_datetime(transactions_df['transaction_date']) > pd.Timestamp.now()).sum()
    }
    
    print("Validation Results:")
    for table, results in validation_results.items():
        print(f"\n{table.upper()}:")
        for metric, value in results.items():
            status = "✓" if value == 0 else "⚠️"
            print(f"  {status} {metric}: {value}")
    
    # Save validation results
    validation_path = '/tmp/validation_results.json'
    with open(validation_path, 'w') as f:
        json.dump(validation_results, f, indent=2)
    
    print(f"\nValidation results saved to: {validation_path}")

def clean_and_standardize_data():
    """Clean and standardize extracted data."""
    print("=== Data Cleaning and Standardization ===")
    
    # Clean customers data
    customers_df = pd.read_csv('/tmp/extracted_customers.csv')
    customers_df['email'] = customers_df['email'].str.lower().str.strip()
    customers_df['name'] = customers_df['name'].str.title()
    customers_df['signup_date'] = pd.to_datetime(customers_df['signup_date'])
    
    # Clean products data
    products_df = pd.read_csv('/tmp/extracted_products.csv')
    products_df['name'] = products_df['name'].str.strip()
    products_df['category'] = products_df['category'].str.upper()
    products_df['price'] = products_df['price'].round(2)
    
    # Clean transactions data
    transactions_df = pd.read_csv('/tmp/extracted_transactions.csv')
    transactions_df['transaction_date'] = pd.to_datetime(transactions_df['transaction_date'])
    
    # Save cleaned data
    customers_df.to_csv('/tmp/cleaned_customers.csv', index=False)
    products_df.to_csv('/tmp/cleaned_products.csv', index=False)
    transactions_df.to_csv('/tmp/cleaned_transactions.csv', index=False)
    
    print("✓ Data cleaning completed")
    print(f"  - Customers: {len(customers_df)} records")
    print(f"  - Products: {len(products_df)} records")
    print(f"  - Transactions: {len(transactions_df)} records")

def transform_and_enrich_data():
    """Transform and enrich data with business logic."""
    print("=== Data Transformation and Enrichment ===")
    
    # Load cleaned data
    customers_df = pd.read_csv('/tmp/cleaned_customers.csv')
    products_df = pd.read_csv('/tmp/cleaned_products.csv')
    transactions_df = pd.read_csv('/tmp/cleaned_transactions.csv')
    enrichment_df = pd.read_csv('/tmp/extracted_enrichment.csv')
    
    # Enrich customers with currency info
    customers_enriched = customers_df.merge(enrichment_df, on='country', how='left')
    
    # Create comprehensive transaction fact table
    transactions_enriched = transactions_df.merge(customers_enriched, on='customer_id', how='left')
    transactions_enriched = transactions_enriched.merge(products_df, on='product_id', how='left')
    
    # Calculate derived fields
    transactions_enriched['total_amount'] = transactions_enriched['quantity'] * transactions_enriched['price']
    transactions_enriched['total_amount_local'] = transactions_enriched['total_amount'] * transactions_enriched['exchange_rate']
    
    # Create aggregated metrics
    customer_metrics = transactions_enriched.groupby('customer_id').agg({
        'total_amount': ['sum', 'count', 'mean'],
        'transaction_date': ['min', 'max']
    }).round(2)
    
    customer_metrics.columns = ['total_spent', 'transaction_count', 'avg_transaction', 'first_purchase', 'last_purchase']
    customer_metrics = customer_metrics.reset_index()
    
    # Product performance metrics
    product_metrics = transactions_enriched.groupby('product_id').agg({
        'quantity': 'sum',
        'total_amount': 'sum',
        'customer_id': 'nunique'
    }).round(2)
    
    product_metrics.columns = ['units_sold', 'revenue', 'unique_customers']
    product_metrics = product_metrics.reset_index()
    
    # Save transformed data
    transactions_enriched.to_csv('/tmp/transformed_transactions.csv', index=False)
    customer_metrics.to_csv('/tmp/transformed_customer_metrics.csv', index=False)
    product_metrics.to_csv('/tmp/transformed_product_metrics.csv', index=False)
    
    print("✓ Data transformation completed")
    print(f"  - Enriched transactions: {len(transactions_enriched)} records")
    print(f"  - Customer metrics: {len(customer_metrics)} records")
    print(f"  - Product metrics: {len(product_metrics)} records")

def create_data_marts():
    """Create data marts for different business use cases."""
    print("=== Creating Data Marts ===")
    
    # Load transformed data
    transactions_df = pd.read_csv('/tmp/transformed_transactions.csv')
    customer_metrics_df = pd.read_csv('/tmp/transformed_customer_metrics.csv')
    product_metrics_df = pd.read_csv('/tmp/transformed_product_metrics.csv')
    
    # Sales Analytics Mart
    sales_mart = transactions_df.groupby(['country', 'category']).agg({
        'total_amount': 'sum',
        'quantity': 'sum',
        'customer_id': 'nunique'
    }).reset_index()
    sales_mart.columns = ['country', 'category', 'revenue', 'units_sold', 'customers']
    
    # Customer Analytics Mart
    customers_df = pd.read_csv('/tmp/cleaned_customers.csv')
    customer_mart = customers_df.merge(customer_metrics_df, on='customer_id', how='left')
    customer_mart['customer_segment'] = pd.cut(
        customer_mart['total_spent'].fillna(0),
        bins=[0, 100, 500, 1000, float('inf')],
        labels=['Low', 'Medium', 'High', 'Premium']
    )
    
    # Product Analytics Mart
    products_df = pd.read_csv('/tmp/cleaned_products.csv')
    product_mart = products_df.merge(product_metrics_df, on='product_id', how='left')
    product_mart['performance_score'] = (
        product_mart['revenue'].fillna(0) * 0.5 +
        product_mart['units_sold'].fillna(0) * 0.3 +
        product_mart['unique_customers'].fillna(0) * 0.2
    )
    
    # Save data marts
    sales_mart.to_csv('/tmp/sales_mart.csv', index=False)
    customer_mart.to_csv('/tmp/customer_mart.csv', index=False)
    product_mart.to_csv('/tmp/product_mart.csv', index=False)
    
    print("✓ Data marts created")
    print(f"  - Sales mart: {len(sales_mart)} records")
    print(f"  - Customer mart: {len(customer_mart)} records")
    print(f"  - Product mart: {len(product_mart)} records")

def generate_data_quality_report():
    """Generate comprehensive data quality report."""
    print("=== Generating Data Quality Report ===")
    
    report = {
        'pipeline_run_date': datetime.now().isoformat(),
        'source_systems': ['CSV', 'JSON', 'API'],
        'data_quality_metrics': {}
    }
    
    # Check each data mart
    marts = ['sales_mart', 'customer_mart', 'product_mart']
    
    for mart in marts:
        df = pd.read_csv(f'/tmp/{mart}.csv')
        
        report['data_quality_metrics'][mart] = {
            'row_count': len(df),
            'column_count': len(df.columns),
            'null_percentage': (df.isnull().sum().sum() / (len(df) * len(df.columns))) * 100,
            'duplicate_rows': df.duplicated().sum(),
            'data_types': df.dtypes.astype(str).to_dict()
        }
    
    # Overall pipeline metrics
    report['pipeline_metrics'] = {
        'total_customers_processed': len(pd.read_csv('/tmp/cleaned_customers.csv')),
        'total_products_processed': len(pd.read_csv('/tmp/cleaned_products.csv')),
        'total_transactions_processed': len(pd.read_csv('/tmp/cleaned_transactions.csv')),
        'data_marts_created': len(marts)
    }
    
    # Save report
    report_path = '/tmp/data_quality_report.json'
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)
    
    print("✓ Data quality report generated")
    print(f"Report saved to: {report_path}")
    
    # Print summary
    print("\n=== Pipeline Summary ===")
    for metric, value in report['pipeline_metrics'].items():
        print(f"  {metric}: {value}")

def simulate_data_loading():
    """Simulate loading data to target systems."""
    print("=== Simulating Data Loading ===")
    
    loading_targets = [
        {'system': 'Data Warehouse', 'tables': ['dim_customers', 'dim_products', 'fact_transactions']},
        {'system': 'Analytics Database', 'tables': ['sales_mart', 'customer_mart', 'product_mart']},
        {'system': 'Operational Database', 'tables': ['customer_metrics', 'product_metrics']}
    ]
    
    for target in loading_targets:
        print(f"\n📊 Loading to {target['system']}:")
        for table in target['tables']:
            print(f"  ✓ {table} - loaded successfully")
    
    # Create loading manifest
    loading_manifest = {
        'loading_date': datetime.now().isoformat(),
        'targets': loading_targets,
        'status': 'completed'
    }
    
    manifest_path = '/tmp/loading_manifest.json'
    with open(manifest_path, 'w') as f:
        json.dump(loading_manifest, f, indent=2)
    
    print(f"\nLoading manifest saved to: {manifest_path}")

def cleanup_etl_files():
    """Clean up ETL temporary files."""
    etl_files = [
        '/tmp/customers.csv', '/tmp/products.json', '/tmp/transactions.json',
        '/tmp/extracted_customers.csv', '/tmp/extracted_products.csv', '/tmp/extracted_transactions.csv',
        '/tmp/extracted_enrichment.csv', '/tmp/cleaned_customers.csv', '/tmp/cleaned_products.csv',
        '/tmp/cleaned_transactions.csv', '/tmp/transformed_transactions.csv',
        '/tmp/transformed_customer_metrics.csv', '/tmp/transformed_product_metrics.csv',
        '/tmp/sales_mart.csv', '/tmp/customer_mart.csv', '/tmp/product_mart.csv',
        '/tmp/validation_results.json', '/tmp/data_quality_report.json', '/tmp/loading_manifest.json'
    ]
    
    cleaned_files = []
    for file_path in etl_files:
        if os.path.exists(file_path):
            os.remove(file_path)
            cleaned_files.append(file_path)
    
    print(f"Cleaned up {len(cleaned_files)} ETL files")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'data_pipeline_etl_dag',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=["learning", "etl", "data-pipeline", "advanced"]
)

# Setup task
setup_task = PythonOperator(
    task_id='create_sample_data_sources',
    python_callable=create_sample_data_sources,
    dag=dag
)

# Extract TaskGroup
with TaskGroup("extract_data", dag=dag) as extract_group:
    extract_csv_task = PythonOperator(
        task_id='extract_csv_data',
        python_callable=extract_csv_data,
    )
    
    extract_json_task = PythonOperator(
        task_id='extract_json_data',
        python_callable=extract_json_data,
    )
    
    extract_api_task = PythonOperator(
        task_id='extract_api_data',
        python_callable=extract_api_data,
    )

# Validation task
validate_task = PythonOperator(
    task_id='validate_extracted_data',
    python_callable=validate_extracted_data,
    dag=dag
)

# Transform TaskGroup
with TaskGroup("transform_data", dag=dag) as transform_group:
    clean_task = PythonOperator(
        task_id='clean_and_standardize_data',
        python_callable=clean_and_standardize_data,
    )
    
    transform_task = PythonOperator(
        task_id='transform_and_enrich_data',
        python_callable=transform_and_enrich_data,
    )
    
    clean_task >> transform_task

# Data marts creation
create_marts_task = PythonOperator(
    task_id='create_data_marts',
    python_callable=create_data_marts,
    dag=dag
)

# Quality and loading tasks
quality_report_task = PythonOperator(
    task_id='generate_data_quality_report',
    python_callable=generate_data_quality_report,
    dag=dag
)

load_task = PythonOperator(
    task_id='simulate_data_loading',
    python_callable=simulate_data_loading,
    dag=dag
)

# Cleanup task
cleanup_task = PythonOperator(
    task_id='cleanup_etl_files',
    python_callable=cleanup_etl_files,
    dag=dag
)

# Define dependencies
setup_task >> extract_group >> validate_task >> transform_group
transform_group >> create_marts_task >> quality_report_task >> load_task >> cleanup_task