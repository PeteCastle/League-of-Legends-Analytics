import pandas as pd
from prefect import flow,task
import json
import os
from pathlib import Path
from prefect_azure import AzureBlobStorageCredentials
from prefect_azure.blob_storage import blob_storage_upload

@task(name = "cleanDataFrame", log_prints=True)
def cleanDataframe(df: pd.DataFrame, table_name: str ) -> pd.DataFrame:
    print("Cleaning dataframe on table: " + table_name)
    dataframe_config = json.load(open('configs/dataframe_config.json'))[table_name]
    df.drop(dataframe_config["temporary_columns"],axis = 1, inplace=True)
    df.drop(dataframe_config["redundant_columns"],axis = 1, inplace = True)
    # df = 

    # # df = parseDataFrameDtypes(df, table_name, dataframe_config) Removed
    return df.reindex(columns=dataframe_config["ordered_columns"])

def parseDataFrameDtypes(df: pd.DataFrame, table_name, dataframe_config) -> pd.DataFrame:
    for dtypes in dataframe_config["column_dtypes"].keys():
        for columns in dataframe_config["column_dtypes"][dtypes]:
            print("Converting column: " + columns + " to type: " + dtypes)
            df.astype({columns : dtypes})

        # print("Converting columns: " + str(dataframe_config["column_dtypes"][dtypes]) + " to type: " + dtypes)
        # df = df.astype({column : dtypes for column in dataframe_config["column_dtypes"][dtypes]})

# @flow(name = "saveOrUploadDataFrame", log_prints=True)
def save_or_upload_dataframe(df: pd.DataFrame , folder_path: Path, file_name : str, save_local : bool = False,  save_cloud : bool = True) -> None:
    full_path = "resources\\" + str(folder_path) + "\\" + file_name
    print(df.columns)
    if save_local:
        Path(full_path).mkdir(parents=True, exist_ok=True)
        df.to_parquet(full_path, index=False)
    if save_cloud:
        storage_credentials = json.load(open("./credentials/credentials.json"))["AZURE_BLOB_STORAGE_CREDENTIALS"]
        
        blob_storage  = AzureBlobStorageCredentials(connection_string=storage_credentials)
        # df.to_parquet("temp.parquet", index=False)
        # with open("temp.parquet", "rb") as f:
        #     data = f.read()
        blob_storage_upload(data= df.to_csv( index = False) ,container="zoomcampcontainerproject",blob=full_path, blob_storage_credentials=blob_storage,  overwrite=True)
        # os.remove("temp.parquet")
        print(f"Saved file {full_path} to Azure storage account")
        
        

