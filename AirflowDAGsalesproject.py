# superstore_sales_dag.py

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from airflow_scripts.data_transform import transform_data  # Import your function

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 7, 9),
    'retries': 1,
}

dag = DAG(
    'superstore_sales_transformation',
    default_args=default_args,
    description='A simple DAG to transform superstore sales data',
    schedule_interval='@daily',
)

def run_transformation(filepath):
    return transform_data(filepath)  # Call your function

transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=run_transformation,
    op_args=['/path/to/your/superstoresales.csv'],  # Replace with your CSV file path
    dag=dag,
)

transform_task
