import requests as r
import keyring
import pandas as pd
import os
import datetime

from google.cloud import bigquery, storage
from concurrent.futures import ThreadPoolExecutor
from threading import Lock
from src.utils.custom_logger import setup_logger


def read_columns_to_dict_bq(input_logger,table, columns):
    """
    Reads specified columns from a BigQuery table and returns the data as a list of dictionaries.

    Params:
        input_logger (logging.Logger): Logger instance for logging information.
        table (str): Full name of the BigQuery table in the format `project.dataset.table`.
        columns (list): List of column names to retrieve.

    Returns:
    list: A list of dictionaries where each dictionary represents a row from the table.

    """
    input_logger.info("----------- Reading Big Query Table Columns to Dict ---------")
    # Construct a BigQuery client object
    client = bigquery.Client()

    # Validate input
    if not columns:
        raise ValueError("At least one column must be specified.")

    # Dynamically format the column names
    column_string = ", ".join(columns)  # Join column names with commas

    # Add backticks around the table name to handle special characters
    table_with_backticks = f"`{table}`"

    # Construct the query
    query = f"""
        SELECT 
            {column_string}
        FROM 
            {table_with_backticks}
    """

    # Execute the query
    query_job = client.query(query)  # Make an API request

    # Collect and return the results
    results = query_job.result()
    return [dict(row) for row in results]  # Convert rows to dictionaries


def places_api_call(full_address, input_logger):
    """
    Makes an API call to the Google Places API to retrieve place information for a given address.

    Params:
        input_logger (logging.Logger): Logger instance for logging information.
        full_address (str): The full address to query in the Places API.

    Returns:
        dict: The API response containing place information, or an empty "places" list in case of failure.

    """
    # input_logger.info("----------- Calling Place API (New) ---------")
    url = "https://places.googleapis.com/v1/places:searchText"

    API_KEY = keyring.get_password("uk_gam_gp", "PLACES_API") #USE ENVIRONMENT VARIABLE

    myobj = {
        "textQuery": full_address
    }

    headers = {
        "Content-Type": "application/json",
        "X-Goog-FieldMask": "places.formattedAddress,places.businessStatus,places.id",
    }

    response = r.post(f"{url}?key={API_KEY}", headers=headers, json=myobj)

    # Ensure the response status is OK and return the JSON content
    if response.status_code == 200:
        input_logger.info(f"STATUS: {response.status_code}")
        return response.json()  # Convert the response to a Python dictionary
    else:
        input_logger.error(f"STATUS{response.status_code}")
        return {"places": []}  # Return an empty list in case of error or no data
    

def add_places_to_dict_parallel(input_logger, rows, key_column, value_column, max_workers=3):
    """
    Retrieves Google Places API information for a list of rows in parallel using threading.

    Params:
        input_logger (logging.Logger): Logger instance for logging information.
        rows (list): A list of dictionaries where each dictionary contains address and premises ID information.
        key_column (str): The key in the dictionaries representing the full address.
        value_column (str): The key in the dictionaries representing the premises ID.
        max_workers (int): Number of worker threads to use for parallel processing. Default is 3.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the enriched data with Google Places API results.
    """
    input_logger.info(f"----------- Parallelism process returning Place API info for {key_column}s ---------")

    if not rows:
        input_logger.error("Input 'rows' is empty. No processing will be done.")
        return pd.DataFrame()
    
    result = []
    lock = Lock()

    def process_row(row):
        address = row[key_column]
        premisesid = row[value_column]
        try:
            places_data = places_api_call(address, input_logger)
            places = places_data.get("places", [])

            if places:
                with lock:
                    place_info = places[0]
                    result.append({
                        "full_address": address,
                        "premisesid": premisesid,
                        "g_place_id": place_info.get("id", "Missing"),
                        "g_formatted_address_1": place_info.get("formattedAddress", "Missing"),
                        "g_business_status_1": place_info.get("businessStatus", "Missing"),
                        "api_call_date": pd.to_datetime(datetime.datetime.now())
                    })
                if len(result) % 100 == 0:
                    input_logger.info(f"Processed {len(result)} addresses so far.")
            else:
                input_logger.warning(f"No place info for {address}")
        except Exception as e:
            if "429" in str(e):
                input_logger.error(f"Rate limit hit for {address}. Consider increasing delay or reducing workers.")
            else:
                input_logger.error(f"Error processing {address}: {e}")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        executor.map(process_row, rows)
        if not result:
            input_logger.error(f"Result list is empty - no places_info")

    if result:
        df = pd.DataFrame(result)
        input_logger.info(f"DataFrame created with {len(df)} rows.")
    else:
        input_logger.error("Result list is empty. No data added to DataFrame.")
        df = pd.DataFrame()
    input_logger.info("Parallel processing of API calls completed.")


            


def upload_df_to_gcs(input_logger,bucket_name,directory,df,df_name,extension):
    """
    Uploads a pandas DataFrame as a CSV file to a specified Google Cloud Storage bucket.

    Params:
        input_logger (logging.Logger): Logger instance for logging information.
        bucket_name (str): The name of the GCS bucket to upload the file to.
        directory (str): The directory path in the GCS bucket to store the file.
        df (pd.DataFrame): The DataFrame to upload.
        df_name (str): The name of the file to be saved (without extension).
        extension (str): The file extension (e.g., ".csv").

    Returns:
        None: The function does not return a value but logs the upload status.
    """
    input_logger.info(f"----------- Writing file to BUCKETNAME_REMOVED / {directory} / {df_name}{extension} ---------")
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    gcs_path = os.path.join(directory,f"{df_name}{extension}").replace("\\", "/")

    # Save the responses dict to a JSON file in-memory
    if df is not None:
        try:
            # Create a blob in GCS and upload the JSON data
            blob = bucket.blob(gcs_path)
            csv_data = df.to_csv(index=False) 
            blob.upload_from_string(csv_data, content_type="text/csv")
            print("File has been uploaded to GCS")
        except Exception as e:
            print(f"File was not saved to Google Cloud Storage: {e}")
    else:
        print(f"{df_name} dataFrame is None")
    return None 


def main():
    logger = setup_logger(log_file_name="data_ingestion.log")
    columns = ['full_address','premisesid'] 

    table = "gambling-premises-data.silver_ew.dim_premises"

    address_premid = read_columns_to_dict_bq(logger,table,columns)

    df = add_places_to_dict_parallel(logger,address_premid,columns[0],columns[1])
    
    print("Input bucketname: ")
    bucket_name = input() #USE ENVIRONMENT VARIABLE
    dir = "RAW"
    df_name = "places_api_results"
    extension = ".csv"

    upload_df_to_gcs(logger,bucket_name,dir,df,df_name,extension)


if __name__ == "__main__":
        main()