from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from salesproject import extract_data_to_csv, transform_data, push_to_azure_blob_storage

default_args = {
    "owner": 'shawon simon',
    "start_date": datetime(2023, 1, 1),
    'retries': 1,
}

dag = DAG(
    'superstore_sales_ETL',
    default_args=default_args,
    description='A DAG to transform superstore sales data',
    schedule_interval=None,
)

extract_data = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data_to_csv,
    provide_context=True,
    dag=dag,
)

transform_data = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_data = PythonOperator(
    task_id='push_data_to_blob',
    python_callable=push_to_azure_blob_storage,
    provide_context=True,
    dag=dag,
)

extract_data >> transform_data >> load_data