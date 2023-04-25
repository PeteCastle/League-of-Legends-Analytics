# Loading parquet in Databricks has been a high blood for me as Parquet can't "convert" data types from int to double (srsly), I'll convert them to csv instead.

import pandas as pd
import pyarrow.parquet as pq
import pyarrow.fs as pafs
from azure.storage.blob import BlobServiceClient
import json

import os

credentials = json.load(open('credentials/credentials.json', 'r'))


# Set up Azure Blob Storage connection
connection_string = credentials["AZURE_BLOB_STORAGE_CREDENTIALS"]
container_name = credentials["AZURE_BLOB_CONTAINER"]
project_name = credentials["AZURE_BLOB_PROJECT_NAME"]
directory_name = "resources"
blob_service_client = BlobServiceClient.from_connection_string(connection_string)
container_client = blob_service_client.get_container_client(container_name)


# Get a list of all Parquet files in the directory
blob_list = container_client.list_blobs(name_starts_with=directory_name)
parquet_files = [blob.name for blob in blob_list if blob.name.endswith('.parquet')]

# Iterate through each Parquet file and convert to CSV
for file in parquet_files:
    print(f"Converting {file} to CSV...")
    # Read the Parquet file into a Pandas DataFrame
    url = f"https://{project_name}.blob.core.windows.net/{container_name}/{file}"
    print(url)
    df = pd.read_parquet(url)
    

    # Write the DataFrame to a CSV file in the same directory
    csv_file = file[:-8] + '.csv'  # Change file extension from .parquet to .csv

    # output_dir = os.path.join("file", os.path.splitext(os.path.basename(file))[0])
    # os.makedirs(output_dir, exist_ok=True)

    data = df.to_csv( index=False)

    # Upload the CSV file to the Azure Blob Storage directory
    print(f"Uploading {csv_file} to Azure Blob Storage...")
    # with open(csv_file, "rb") as data:
    container_client.upload_blob(name=csv_file, data=data)

    # Delete the original Parquet file
    print(f"Deleting {file} from Azure Blob Storage...")
    container_client.delete_blob(file)
    # os.remove(file)
