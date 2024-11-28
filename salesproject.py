import os
import json
import pandas as pd
from sqlalchemy import create_engine
from airflow.models import Variable
from azure.storage.blob import BlobServiceClient
from datetime import datetime
from airflow.exceptions import AirflowException

salesdata_file_path='/home/shawon/airflow/files/superstoresales.csv'
cleaned_salesdata_filepath='/home/shawon/airflow/files/transformed_data.csv'
dim_product_filepath='/home/shawon/airflow/files/DimProduct.csv'
dim_customer_filepath='/home/shawon/airflow/files/DimCustomer.csv'
dim_date_filepath='/home/shawon/airflow/files/DimDate.csv'
dim_shipmode_filepath='/home/shawon/airflow/files/DimShipMode.csv'
dim_city_filepath='/home/shawon/airflow/files/DimCity.csv'
fact_table_filepath='/home/shawon/airflow/files/FactTable.csv'

def get_db_connection_from_airflow():
    """
    Retrieve database connection details from Airflow variables and return a SQLAlchemy engine.
    """

    # Retrieve database credentials from Airflow variables
    dbname = Variable.get("PG_DBNAME")
    user = Variable.get("PG_USER")
    password = Variable.get("PG_PASSWORD")
    host = Variable.get("PG_HOST")
    port = Variable.get("PG_PORT")

    # Construct the database connection URL for PostgreSQL
    connection_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

    # Create a SQLAlchemy engine
    engine = create_engine(connection_url)

    return engine

def extract_data_to_csv(**kwargs):
    """
    Connects to PostgreSQL using SQLAlchemy, extracts data from the tables, and saves them as a CSV files.
    """
    # Define table mappings with their corresponding file paths
    table_mappings = {
        'superstoresales': salesdata_file_path,
        'product_dimension': dim_product_filepath,
        'customer_dimension': dim_customer_filepath,
        'date_dimension': dim_date_filepath,
        'shipmode_dimension': dim_shipmode_filepath,
        'city_dimension': dim_city_filepath,
        'fact_sales': fact_table_filepath
    }

    extraction_status = {}
    connection = None

    try:
        # Get SQLAlchemy engine
        engine = get_db_connection_from_airflow()

        # Establish connection to the database
        connection = engine.connect()
        print("Database connection established successfully.")

        # Extract data from each table
        for table_name, file_path in table_mappings.items():
            try:
                # Execute query and save to CSV
                query = f"SELECT * FROM superstoreschema.{table_name}"
                df = pd.read_sql(query, engine)
                df.to_csv(file_path, index=False)
                print(f"Successfully extracted data from {table_name} to {file_path}")
                extraction_status[table_name] = "Success"

            except Exception as table_error:
                error_message = f"Error extracting from {table_name}: {str(table_error)}"
                print(error_message)
                extraction_status[table_name] = error_message

    except Exception as e:
        error_message = f"Database connection error: {str(e)}"
        print(error_message)
        extraction_status['connection_error'] = error_message

    finally:
        # Ensure the connection is closed
        if 'connection' in locals() and connection:
            connection.close()
            print("Database connection closed.")

def transform_data(**kwargs):
    data = pd.read_csv(salesdata_file_path)

    # Change column names
    column_mappings = {
        'Row ID': 'RowID',
        'Order ID': 'OrderID',
        'Order Date': 'OrderDate',
        'Ship Date': 'ShipDate',
        'Ship Mode': 'ShipMode',
        'Customer ID': 'CustomerID',
        'Customer Name': 'CustomerName',
        'Postal Code': 'PostalCode',
        'Product ID': 'ProductID',
        'Product Name': 'ProductName'
    }
    data.rename(columns=column_mappings, inplace=True)

    data.to_csv(cleaned_salesdata_filepath, index=False)
    print(f"Data successfully saved to {cleaned_salesdata_filepath}")

def push_to_azure_blob_storage(**kwargs):
    csv_directory_path='/home/shawon/airflow/files'
    try:
        # Azure Blob Storage connection settings
        connect_str = Variable.get("connect_str")
        container_name = "superstore"

        # Initialize BlobServiceClient
        blob_service_client = BlobServiceClient.from_connection_string(connect_str)

        # List all CSV files in the specified directory
        csv_files = [os.path.join(csv_directory_path, f) for f in os.listdir(csv_directory_path) if f.endswith(".csv")]

        # Iterate through the list of CSV files
        for file_path in csv_files:
            blob_name = file_path.split("/")[-1]  # Extract the file name from the path

            # Get blob client
            blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)

            with open(file_path, "rb") as data:
                    blob_client.upload_blob(data)

            print(f"File uploaded to {blob_name} in container {container_name}.")
    except Exception as e:
        print(f"Error uploading file to Azure Blob Storage: {str(e)}")