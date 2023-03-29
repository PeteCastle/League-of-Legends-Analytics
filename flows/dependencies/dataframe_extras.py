import pandas as pd
from prefect import flow,task
import json
import os
from pathlib import Path

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

@task(name = "saveOrUploadDataFrame", log_prints=True)
def save_or_upload_dataframe(df: pd.DataFrame , folder_path: Path, file_name : str, save_local : bool = True,  save_cloud : bool = True) -> None:
    
    print(df.columns)
    if save_local:
        folder_path.mkdir(parents=True, exist_ok=True)
        df.to_parquet(str(folder_path.absolute()) + "\\" + file_name, index=False)
    if save_cloud:
        print("Saving dataframe to cloud")
        print("Not yet implemented")
        