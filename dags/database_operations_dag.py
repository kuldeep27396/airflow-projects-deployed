"""
Database Operations DAG Example
================================

This DAG demonstrates database operations in Airflow:
- SQLiteOperator for simple database operations
- Python operators for database connectivity
- Data quality checks
- Database table creation and management

Note: This example uses SQLite for simplicity and portability.
In production, you would use PostgresOperator, MySqlOperator, etc.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import sqlite3
import pandas as pd
import os

def create_database():
    """Create SQLite database and sample tables."""
    db_path = '/tmp/airflow_example.db'
    
    # Remove existing database
    if os.path.exists(db_path):
        os.remove(db_path)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create customers table
    cursor.execute("""
        CREATE TABLE customers (
            customer_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT UNIQUE,
            created_date DATE,
            status TEXT DEFAULT 'active'
        )
    """)
    
    # Create orders table
    cursor.execute("""
        CREATE TABLE orders (
            order_id INTEGER PRIMARY KEY,
            customer_id INTEGER,
            product_name TEXT,
            amount DECIMAL(10,2),
            order_date DATE,
            FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
        )
    """)
    
    # Create analytics table
    cursor.execute("""
        CREATE TABLE daily_stats (
            date DATE PRIMARY KEY,
            total_orders INTEGER,
            total_revenue DECIMAL(10,2),
            new_customers INTEGER,
            avg_order_value DECIMAL(10,2)
        )
    """)
    
    conn.commit()
    conn.close()
    print(f"Database created at: {db_path}")

def insert_sample_data():
    """Insert sample data into the database."""
    db_path = '/tmp/airflow_example.db'
    conn = sqlite3.connect(db_path)
    
    # Sample customers
    customers_data = [
        (1, 'John Doe', 'john@example.com', '2024-01-01', 'active'),
        (2, 'Jane Smith', 'jane@example.com', '2024-01-02', 'active'),
        (3, 'Bob Johnson', 'bob@example.com', '2024-01-03', 'inactive'),
        (4, 'Alice Wilson', 'alice@example.com', '2024-01-04', 'active'),
        (5, 'Charlie Brown', 'charlie@example.com', '2024-01-05', 'active')
    ]
    
    conn.executemany("""
        INSERT INTO customers (customer_id, name, email, created_date, status)
        VALUES (?, ?, ?, ?, ?)
    """, customers_data)
    
    # Sample orders
    orders_data = [
        (1, 1, 'Laptop', 999.99, '2024-01-10'),
        (2, 1, 'Mouse', 29.99, '2024-01-10'),
        (3, 2, 'Keyboard', 79.99, '2024-01-11'),
        (4, 3, 'Monitor', 299.99, '2024-01-12'),
        (5, 4, 'Webcam', 89.99, '2024-01-13'),
        (6, 2, 'Headphones', 149.99, '2024-01-14'),
        (7, 5, 'Tablet', 399.99, '2024-01-15')
    ]
    
    conn.executemany("""
        INSERT INTO orders (order_id, customer_id, product_name, amount, order_date)
        VALUES (?, ?, ?, ?, ?)
    """, orders_data)
    
    conn.commit()
    conn.close()
    print("Sample data inserted successfully")

def run_data_quality_checks():
    """Run data quality checks on the database."""
    db_path = '/tmp/airflow_example.db'
    conn = sqlite3.connect(db_path)
    
    print("=== Data Quality Checks ===")
    
    # Check for duplicate emails
    cursor = conn.cursor()
    cursor.execute("SELECT email, COUNT(*) FROM customers GROUP BY email HAVING COUNT(*) > 1")
    duplicates = cursor.fetchall()
    if duplicates:
        print(f"WARNING: Found duplicate emails: {duplicates}")
    else:
        print("✓ No duplicate emails found")
    
    # Check for orders without customers
    cursor.execute("""
        SELECT COUNT(*) FROM orders o 
        LEFT JOIN customers c ON o.customer_id = c.customer_id 
        WHERE c.customer_id IS NULL
    """)
    orphaned_orders = cursor.fetchone()[0]
    if orphaned_orders > 0:
        print(f"WARNING: Found {orphaned_orders} orders without customers")
    else:
        print("✓ All orders have valid customers")
    
    # Check for negative amounts
    cursor.execute("SELECT COUNT(*) FROM orders WHERE amount < 0")
    negative_amounts = cursor.fetchone()[0]
    if negative_amounts > 0:
        print(f"WARNING: Found {negative_amounts} orders with negative amounts")
    else:
        print("✓ No negative order amounts found")
    
    # Summary statistics
    cursor.execute("SELECT COUNT(*) FROM customers")
    customer_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM orders")
    order_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT AVG(amount) FROM orders")
    avg_order_value = cursor.fetchone()[0]
    
    print(f"\n=== Summary Statistics ===")
    print(f"Total customers: {customer_count}")
    print(f"Total orders: {order_count}")
    print(f"Average order value: ${avg_order_value:.2f}")
    
    conn.close()

def generate_daily_analytics(**context):
    """Generate daily analytics and insert into analytics table."""
    db_path = '/tmp/airflow_example.db'
    conn = sqlite3.connect(db_path)
    
    # Get execution date
    execution_date = context['ds']
    
    # Calculate daily stats
    query = """
        SELECT 
            COUNT(*) as total_orders,
            SUM(amount) as total_revenue,
            AVG(amount) as avg_order_value
        FROM orders 
        WHERE order_date = ?
    """
    
    cursor = conn.cursor()
    cursor.execute(query, (execution_date,))
    result = cursor.fetchone()
    
    total_orders = result[0] if result[0] else 0
    total_revenue = result[1] if result[1] else 0.0
    avg_order_value = result[2] if result[2] else 0.0
    
    # Count new customers for the day
    cursor.execute("SELECT COUNT(*) FROM customers WHERE created_date = ?", (execution_date,))
    new_customers = cursor.fetchone()[0]
    
    # Insert into analytics table
    cursor.execute("""
        INSERT OR REPLACE INTO daily_stats 
        (date, total_orders, total_revenue, new_customers, avg_order_value)
        VALUES (?, ?, ?, ?, ?)
    """, (execution_date, total_orders, total_revenue, new_customers, avg_order_value))
    
    conn.commit()
    conn.close()
    
    print(f"=== Daily Analytics for {execution_date} ===")
    print(f"Total orders: {total_orders}")
    print(f"Total revenue: ${total_revenue:.2f}")
    print(f"New customers: {new_customers}")
    print(f"Average order value: ${avg_order_value:.2f}")

def export_analytics_report():
    """Export analytics report to CSV."""
    db_path = '/tmp/airflow_example.db'
    conn = sqlite3.connect(db_path)
    
    # Read analytics data
    df = pd.read_sql_query("SELECT * FROM daily_stats ORDER BY date", conn)
    
    # Export to CSV
    csv_path = '/tmp/daily_analytics_report.csv'
    df.to_csv(csv_path, index=False)
    
    print(f"Analytics report exported to: {csv_path}")
    print(f"Report contains {len(df)} days of data")
    
    conn.close()

def cleanup_database():
    """Clean up temporary database files."""
    files_to_remove = ['/tmp/airflow_example.db', '/tmp/daily_analytics_report.csv']
    
    for file_path in files_to_remove:
        if os.path.exists(file_path):
            os.remove(file_path)
            print(f"Removed: {file_path}")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'database_operations_dag',
    default_args=default_args,
    schedule=None,  # Manual trigger for testing
    catchup=False,
    tags=["learning", "database", "analytics"]
)

# Database setup tasks
setup_start = EmptyOperator(
    task_id='setup_start',
    dag=dag
)

create_db_task = PythonOperator(
    task_id='create_database',
    python_callable=create_database,
    dag=dag
)

insert_data_task = PythonOperator(
    task_id='insert_sample_data',
    python_callable=insert_sample_data,
    dag=dag
)

# Data quality tasks
quality_check_task = PythonOperator(
    task_id='run_data_quality_checks',
    python_callable=run_data_quality_checks,
    dag=dag
)

# Analytics tasks
generate_analytics_task = PythonOperator(
    task_id='generate_daily_analytics',
    python_callable=generate_daily_analytics,
    dag=dag
)

export_report_task = PythonOperator(
    task_id='export_analytics_report',
    python_callable=export_analytics_report,
    dag=dag
)

# Show database contents
show_contents_task = BashOperator(
    task_id='show_database_contents',
    bash_command="""
    echo "=== Database Contents ==="
    echo "Customers table:"
    sqlite3 /tmp/airflow_example.db "SELECT * FROM customers;"
    echo ""
    echo "Orders table:"
    sqlite3 /tmp/airflow_example.db "SELECT * FROM orders;"
    echo ""
    echo "Analytics table:"
    sqlite3 /tmp/airflow_example.db "SELECT * FROM daily_stats;"
    """,
    dag=dag
)

# Cleanup task
cleanup_task = PythonOperator(
    task_id='cleanup_database',
    python_callable=cleanup_database,
    dag=dag
)

# Define dependencies
setup_start >> create_db_task >> insert_data_task >> quality_check_task
quality_check_task >> generate_analytics_task >> export_report_task
export_report_task >> show_contents_task >> cleanup_task