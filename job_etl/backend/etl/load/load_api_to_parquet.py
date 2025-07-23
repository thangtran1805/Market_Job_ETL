import os
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime

def get_latest_file_from_directory(directory,extension):
    """
    Get the list of latest files in the directory with the specific extension
    :param: directory: A directory to search for files.
    :param: extension: File's extension.
    :return: A List of the latest files or None if no files were found.
    """
    files = [os.path.join(directory,f) for f in os.listdir(directory) if f.endswith(extension)]
    if not files:
        return None
    latest_file = max(files,key=os.path.getmtime)
    return latest_file

def load_json_from_file(filepath):
    # Load JSON data from a file
    with open(filepath,'r',encoding='utf-8') as f:
        data = json.load(f)
    return data

def save_json_to_parquet(data,output_filepath):
    # Convert JSON data to a pyarrow table
    table = pa.Table.from_pandas(pd.DataFrame(data))

    # Save the pyarrow table as a Parquet fil
    pq.write_table(table,output_filepath)

def load_db_to_dl(input_directory,output_directory):
    extension = '.json'

    # Get the latest JSON file in a directory
    latest_file = get_latest_file_from_directory(input_directory,extension)

    if latest_file:
        # Read the JSON data
        data = load_json_from_file(latest_file)
        print(f'Reading file: {latest_file}')

        # Set the Parquet file name corresponding to the JSON file name
        # Path to the output Parquet file
        filename = os.path.basename(latest_file).replace('.json','.parquet')
        output_filepath = os.path.join(output_directory,filename)

        # Save the JSON data as a Parquet file
        save_json_to_parquet(data,output_filepath)
        print(f'Convert {latest_file} to Parquet successfully!')
    else:
        print('No JSON files were found in directory')

def load_api_to_parquet():
    # Convert jobs JSON files to parquet
    # Path to the directory containing the JSON files.
    input_directory = r'/home/thangtranquoc/job_market_etl/job_etl/backend/data/raw/raw_jobs'
    # Path to the directory to save the parquet files.
    output_directory = r'/home/thangtranquoc/job_market_etl/job_etl/backend/data/processed/load_api_jobs_to_dl'
    # Run
    load_db_to_dl(input_directory,output_directory)

    # Convert job_detail JSON files to parquet
    # Path to the directory containing the JSON files
    input_directory = r'/home/thangtranquoc/job_market_etl/job_etl/backend/data/raw/raw_jobs_detail'
    # Path to the directory to save the parquet files.
    output_directory = r'/home/thangtranquoc/job_market_etl/job_etl/backend/data/processed/load_api_job_detail_to_dl'
    # Run
    load_db_to_dl(input_directory,output_directory)

    # Convert job_salary JSON files to parquet
    # Path to the directory containing the JSON files.
    input_directory = r'/home/thangtranquoc/job_market_etl/job_etl/backend/data/raw/raw_jobs_salary'
    output_directory = r'/home/thangtranquoc/job_market_etl/job_etl/backend/data/processed/load_api_job_salary_to_dl'
    # Run
    load_db_to_dl(input_directory,output_directory)

load_api_to_parquet()