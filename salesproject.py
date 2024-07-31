import pandas as pd
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os

def transform_data(filepath):
    # Load data from CSV file
    data = pd.read_csv(filepath)
    
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
    
    # Convert date columns to datetime format
    data['OrderDate'] = pd.to_datetime(data['OrderDate'], format='%d/%m/%Y')
    data['ShipDate'] = pd.to_datetime(data['ShipDate'], format='%d/%m/%Y')
    
    # Remove duplicates
    data.drop_duplicates(inplace=True)
    
    # Update OrderDate where ShipDate is later than OrderDate
    data.loc[data['ShipDate'] > data['OrderDate'], 'OrderDate'] = data['ShipDate']
    
    # Filter and print rows where ShipDate is greater than OrderDate
    filtered_data = data[data['ShipDate'] > data['OrderDate']]
    print(filtered_data.loc[:, ['OrderDate', 'ShipDate']])
    
    return data

# Filepath to the CSV file
filepath = r"C:\Users\simon\Downloads\superstoresales.csv"

# Transform the sales data
transformed_data = transform_data(filepath)

def convert_to_csv(data, output_filepath):
    data.to_csv(output_filepath, index=False)
    print(f"Data successfully saved to {output_filepath}")

convert_to_csv(transformed_data, '/path/to/save/transformed_data.csv')

#First, ensure you have the library installed:
#Run in terminal pip install azure-storage-blob


def push_to_azure_blob_storage(connection_string, container_name, file_path):
    try:
        # Create a BlobServiceClient using the connection string
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        # Get a client to interact with a specific container
        container_client = blob_service_client.get_container_client(container_name)

        # Get the file name from the file path
        file_name = os.path.basename(file_path)

        # Create a blob client using the container client and file name
        blob_client = container_client.get_blob_client(file_name)

        # Upload the file to Azure Blob Storage
        with open(file_path, "rb") as data:
            blob_client.upload_blob(data)

        print(f"File '{file_name}' uploaded to Azure Blob Storage successfully.")

    except Exception as e:
        print(f"Error uploading file to Azure Blob Storage: {str(e)}")

# Example usage:
# Replace with your actual Azure Blob Storage connection string and container name
# push_to_azure_blob_storage('<your_connection_string>', '<your_container_name>', '/path/to/save/transformed_data.csv')
