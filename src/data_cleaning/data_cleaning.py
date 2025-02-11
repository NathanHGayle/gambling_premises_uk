import pandas as pd
from google.cloud import storage
from src.utils.gc_logger import setup_cloud_logger
import os
import re
import json

# Import data from GCS

def read_gcs_file(input_logger,bucket_name, blob_name):
    """Write and read a blob from GCS using file-like IO"""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    input_logger.info(f"----------- Reading {blob_name} from GCS ---------")
    with blob.open("r" ) as f:  #encoding="utf-8" add if needed here
        if ".csv" in blob_name:
            try:
                df = pd.read_csv(f,encoding="utf-8")
                input_logger.info("Successfully read CSV file to pd.DataFrame")
                return df
            except Exception as e:
                 input_logger.error(f"Unsuccessful CSV read to pd.DataFrame. {e}")
        
        elif ".xlsx" in blob_name:
            try:
                df = pd.read_excel(f, engine="openpyxl")
                input_logger.info("Successfully read XLSX file to pd.DataFrame")
                return df
            except Exception as e:
                 input_logger.error(f"Unsuccessful XLSX read to pd.DataFrame. {e}")
    
        elif ".json" in blob_name:
            try:
                json_data = json.load(f)
                input_logger.info("Successfully read JSON file to memory")
                return json_data
            except Exception as e:
                input_logger.error(f"Unsuccessful JSON read {e}")
        else:
            input_logger.debug(f"The blob_name must be a '.csv', an '.xlsx' or a '.json' file")
            return None


# Template Cleaning Modules

def standardize_name(name):
    """
    Standardizes a single string based on BigQuery lexical structure and syntax:
    - Converts to lowercase
    - Replaces spaces with underscores
    - Removes invalid characters
    - Trims length to 300 characters
    - Handles banned prefixes
    - Adds 'column_' prefix for empty or digit-starting names
    
    Parameters:
        name (str): The name to standardize.
    
    Returns:
        str: The standardized name.
    """
    # Define banned prefixes
    banned_prefixes = ["_TABLE_", "_FILE_", "_PARTITION", "_ROW_TIMESTAMP", "__ROOT__", "_COLIDENTIFIER"]
    banned_prefixes = [prefix.lower() for prefix in banned_prefixes]
    
    # Ensure name is a string and convert to lowercase
    name = str(name).strip().lower()

    # Replace spaces with underscores and keep valid characters only
    name = re.sub(r"[^a-z0-9_]", "", name.replace(" ", "_"))

    # Enforce max length of 300 characters
    name = name[:300]

    # Handle banned prefixes
    for prefix in banned_prefixes:
        if name.startswith(prefix):
            name = name.replace(prefix, "default", 1)

    # Prepend "column" if the name starts with a digit or is empty
    if not name or name[0].isdigit():
        name = f"column_{name}"
    
    return name


def header_standardization(input_logger, df, df_name):
    """
    Standardizes column names based on BigQuery lexical structure and syntax.
    
    Parameters:
        df (DataFrame): The DataFrame to standardize.
        df_name (str): The name of the DataFrame (for logging purposes).
    
    Returns:
        DataFrame: The DataFrame with standardized column names.
    """
    input_logger.info("----------- Standardizing {} headers ---------".format(df_name))
    column_names = df.columns

    # Initialize storage for validated column names and duplicates
    validated = []
    seen = set()

    for name in column_names:
        # Use the helper function to standardize the column name
        standardized_name = standardize_name(name)

        # Resolve duplicates by appending a counter
        original_name = standardized_name
        counter = 1
        while standardized_name in seen:
            standardized_name = f"{original_name}_{counter}"
            counter += 1

        # Add the standardized name to the result and mark it as seen
        validated.append(standardized_name)
        seen.add(standardized_name)

    df.columns = validated
    input_logger.info(f"Successfully standardized {df_name} headers")
    return df


def deduplicate(input_logger,df,df_name, metadata):
    """
    Handles deduplication for the entire dataset based on metadata.
    Checks for duplicates before attempting to drop any.
    
    Args:
        df (pd.DataFrame): Input DataFrame.
        metadata (dict): Metadata defining deduplication rules.
    
    Returns:
        pd.DataFrame: DataFrame after applying deduplication rules.
    """
    input_logger.info(f"----------- Checking for deduplication {df_name} ---------") 
    deduplication_required = metadata.get("deduplication_rules", {}).get("deduplicate", False)
    if deduplication_required:
        # Check if there are any duplicate rows in the DataFrame
        duplicate_count = df.duplicated().sum()
        
        if duplicate_count > 0:
            # If duplicates are found, drop them
            df = df.drop_duplicates(keep="first")
            input_logger.info(f"Dropped {duplicate_count} duplicate rows.")
    else:
        input_logger.debug(f"No duplicates in {df_name} to drop.")
    
    return df


def clean_and_impute_data(input_logger,df, df_name, metadata):
    """
    Cleans and imputes data based on provided metadata.

    Args:
        df (pd.DataFrame): The DataFrame to process.
        metadata (dict): Metadata for cleaning, specifying column types and imputation strategies.
    
    Returns:
        pd.DataFrame: The cleaned and imputed DataFrame.
    """
    input_logger.info(f"----------- Cleaning and imputing {df_name} ---------")
    for column, rules in metadata.items():
        column = standardize_name(column)       
        if column not in df.columns:
            input_logger.warning(f"Column '{column}' not found in DataFrame. Skipping...")
            continue      
        col_type = rules.get("type")
        impute_method = rules.get("method")
        reference_column = rules.get("reference_column")
 
        # Handle data type conversions based on rules
        if col_type == "categorical":
            if df[column].dtype != "object":  # Only convert if not already a string
                df[column] = df[column].astype(str)
                input_logger.info(f"{column} converted to string")
        elif col_type == "numeric":
            if df[column].dtype != "float64":  # Only convert if not already numeric
                df[column] = pd.to_numeric(df[column], errors="coerce")
                input_logger.info(f"{column} converted to numeric")
        elif col_type == "datetime":
            if df[column].dtype != "datetime64[ns]":  # Check if already datetime
                df[column] = pd.to_datetime(df[column], format="%Y-%m-%d %H:%M:%S", errors="coerce")
                input_logger.info(f"{column} converted to datetime")
        elif col_type == "date":
            if not pd.api.types.is_datetime64_any_dtype(df[column]):  # Check for datetime type first
                df[column] = pd.to_datetime(df[column], format="%Y-%m-%d", errors="coerce").dt.date
                input_logger.info(f"{column} converted to date")

        # Impute missing values based on specified method
        if impute_method == "NMAR":
            df.fillna({column:"Missing"}, inplace=True)
            input_logger.info(f"Filled nulls in {column} with 'Missing'")
        elif impute_method == "MAR":
            if reference_column:
                # Use the reference column to fill missing values
                df[column] = df.groupby(reference_column)[column].transform(
                    lambda x: x.ffill().bfill() if x.isnull().any() else x
                )
                input_logger.info(f"Filled nulls in {column} using forward/backward fill based on {reference_column}")
            else:
                input_logger.info(f"Skipping imputation for {column} due to missing reference_column")
        else:
            input_logger.info(f"No imputation strategy specified for {column}")
    
    return df


def clean_strings(input_logger,df, df_name, metadata):
    """
    Cleans string columns in a DataFrame by applying deduplication rules and logging changes.
    Generates a cleaning audit DataFrame with details of changes.

    Args:
        input_logger (logging.Logger): Logger for logging info and debug messages.
        df (pd.DataFrame): The input DataFrame to clean.
        df_name (str): Name of the DataFrame for logging purposes.
        metadata (dict): Dictionary with deduplication rules per column.

    Returns:
        pd.DataFrame: Cleaned DataFrame.
        pd.DataFrame: Simple cleaning audit DataFrame.
    """
    input_logger.info(f"----------- Replacing values based on {df_name}_metadata.json ---------")
    audit_data = []

    for column, replacements in (metadata or {}).items():
        column = standardize_name(column)
        if column not in df.columns:
            input_logger.warning(f"Column '{column}' not found in DataFrame. Skipping...")
            continue

        df[column] = df[column].astype(str).str.strip().str.lower()

        for old_value, new_value in replacements.items():
            old_value = old_value.strip().lower()
            new_value = new_value.strip().lower()
            occurrences = (df[column] == old_value).sum()
            status = "Not Replaced" if occurrences == 0 else "Replaced"

            audit_data.append({
                "table": df_name, 
                "column": column,
                "action": "string_replacement",
                "original_value":old_value,
                "new_value": new_value,
                "occurrences": occurrences,
                "status": status
            })

            if occurrences > 0:
                df[column] = df[column].replace({old_value: new_value}, regex=True)

    temp_audit_df = pd.DataFrame(audit_data)
    input_logger.info("Completed all replacements.")

    return df, temp_audit_df


def pipeline(input_logger,name,extension, bucketname):
    input_logger.info(f"----------- Running pipeline to clean {name} ---------")
    
    # Read metadata from GCS
    meta_directory = "META"
    meta_tag = "_metadata.json"
    metadata = read_gcs_file(input_logger, bucketname,os.path.join(meta_directory, f"{name}{meta_tag}").replace("\\", "/"))
   
    if not isinstance(metadata, dict):
        input_logger.error("Metadata is not a valid dictionary")
        return None
    
    # Metadata labels
    deduplication = metadata.get("deduplication_rules", {})
    imputation = metadata.get("imputation_rules", {})
    replacement = metadata.get("replacement_rules", {})

    # Ensure necessary keys exist in metadata
    required_keys = ["deduplication_rules", "imputation_rules", "replacement_rules"]
    for key in required_keys:
        if key not in metadata:
            input_logger.warning(f"Metadata missing key: {key}")
            metadata[key] = {}

    try: 
        # Read data from GCS
        df = read_gcs_file(input_logger, bucketname,name + extension) 

        if df is None:
            input_logger.error(f"{name}{extension} is None from GCS.")
            return None     
        # Standardize headers
        df = header_standardization(input_logger,df,name)
        # Deduplicate
        if deduplication:   
            df = deduplicate(input_logger, df,name,deduplication)
        # Impute missing values
        if imputation:
            df = clean_and_impute_data(input_logger,df,name,imputation)
        # Clean Column Strings
        audit_df = None 
        if replacement:
            df, audit_df = clean_strings(input_logger, df, name, replacement)

        return df, audit_df if audit_df is not None else pd.DataFrame()
    
    except Exception as e:
        input_logger.error(f"Unsuccessful cleaning opperations for {name}: {e}")
        return None


def upload_df_to_gcs(input_logger, bucket_name,directory,df,df_name,extension):
    """
    Uploads a pandas DataFrame to Google Cloud Storage as a CSV file.

    Args:
        df (pd.DataFrame): DataFrame to upload.
        bucket_name (str): GCS bucket name.
        file_path (str): Path in the bucket where the file will be stored.

    Returns:
        bool: True if upload is successful, False otherwise.
    """
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
            input_logger.info("File has been uploaded to GCS")
        except Exception as e:
            input_logger.debug(f"File was not saved to Google Cloud Storage: {e}")
    else:
        input_logger.info(f"{df_name} dataFrame is None")
    return None 


def main():
    # Setup
    logger = setup_cloud_logger(log_file_name="data_cleaning.log")   
    print("Input bucketname:")
    bucketname = input()

    # Initialize an empty dataframe to hold all the audits
    all_audits_df = pd.DataFrame()

    # List of files
    files = [
        {"name": "premises-licence-register", "extension": ".csv"},
        {"name": "Data constituencies_deprivation-dashboard", "extension": ".csv"},
        {"name": "NS-SEC_2021", "extension": ".csv"},
        {"name": "postcodes", "extension": ".csv"}
    ]

    # Loop through files to clean and accumulate audit data
    for file in files:
        # Clean datasets
        clean_df, audit_df = pipeline(logger, file["name"], file["extension"], bucketname)

        # Append the current audit_df to the all_audits_df
        all_audits_df = pd.concat([all_audits_df, audit_df], ignore_index=True)

        # Write the clean_df to GCS
        gsc_dir = "CLEAN"
        upload_df_to_gcs(logger, bucketname, gsc_dir, clean_df, file["name"], file["extension"])

    # Upload the combined audit dataframe
    gsc_dir_audit = "AUDITS"
    upload_df_to_gcs(logger, bucketname, gsc_dir_audit, all_audits_df, "combined_audit", ".csv")
 
 
if __name__ == "__main__":
        main()
