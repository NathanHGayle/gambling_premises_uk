# REDUNDANT 

import json
import undetected_chromedriver as uc
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import NoSuchElementException
from src.utils.custom_logger import setup_logger
from google.cloud import storage
from google.cloud.storage import Blob


# Add Google Cloud Storage read dataframe function here to read the Postcodes dataset.

def read_gcs_file(input_logger,bucket_name, blob_name):
    """Write and read a blob from GCS using file-like IO"""
    # The ID of your GCS bucket
    # bucket_name = "your-bucket-name"

    # The ID of your new GCS object
    # blob_name = "storage-object-name"

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    # Mode can be specified as wb/rb for bytes mode.
    # See: https://docs.python.org/3/library/io.html
    with blob.open("r" ,encoding="utf-8") as f:
        try:
            df = pd.read_csv(f)
            input_logger.info('Successfully converted file to pd.DataFrame')
            # Test
            # df.to_csv('test_this_dataset.csv')
            return df
        except Exception as e:
            input_logger.info(f'Unsuccessful conversion to pd.DataFrame. {e}')
            return None


def webscrape_local_council(logger,list_of_postcodes):
    """
    Performs a webscrape operation that extracts a dictionary of 'postcodes':'local authority' from .gov.uk/find-local-council
    Params:
    list_of_postcodes (list): A list of strings of postcodes to input into the website
    
    """
    if list_of_postcodes == None:
        logger.info('No postcodes in list')
        
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

    responses = {}

    for x, y in enumerate(list_of_postcodes):
        driver.get('https://www.gov.uk/find-local-council')
        post_code_entry = driver.find_element(By.NAME, 'postcode')
        post_code_entry.send_keys(y)
        find_button = driver.find_element(By.CSS_SELECTOR,'#local-locator-form > button')
        find_button.click()
        try:
            two_local_authorities = driver.find_element(By.CSS_SELECTOR,'#content > div > div.govuk-grid-column-two-thirds.article-container > h2')
            if two_local_authorities.text == 'Services in your area are provided by two local authorities':
                first_local_authority = driver.find_element(By.CSS_SELECTOR,'#content > div > div.govuk-grid-column-two-thirds.article-container > div > div.county-result.group.govuk-\!-margin-bottom-8 > h3')
                second_local_authority = driver.find_element(By.CSS_SELECTOR,'#content > div > div.govuk-grid-column-two-thirds.article-container > div > div.district-result.group > h3')
                responses[y] = f'{first_local_authority.text} / {second_local_authority.text}'
            else:
                try:
                    local_council_answer = driver.find_element(By.CSS_SELECTOR,'#content > div > div.govuk-grid-column-two-thirds.article-container > div > div > p:nth-child(1) > strong')
                    responses[y] = f'{local_council_answer.text}'
                except NoSuchElementException:
                    responses[y] = 'No Local Council data Available'
        except NoSuchElementException:
            try:
                local_council_answer = driver.find_element(By.CSS_SELECTOR,'#content > div > div.govuk-grid-column-two-thirds.article-container > div > div > p:nth-child(1) > strong')
                responses[y]  = f'{local_council_answer.text}'
            except NoSuchElementException:
                responses[y] = 'No Local Council data available'

    # Save to a JSON file
    logger.info('Backup: Local save JSON as: "missing_local_authorities.json"')
    with open('missing_local_authorities.json', 'w') as file:
        json.dump(responses, file, indent=4)

    return responses


def filtered_list(df,mask,column):
    df = df.copy()
    # Filter and select the 'Postcode' column
    filtered_postcodes = df.loc[mask,column]
    # Convert to a list
    return filtered_postcodes.unique().tolist()


def upload_to_gcs(input_logger, bucket_name,json_file, gcs_path):
    """
    Uploads the json file directly to Google Cloud Storage using blob.
    Params:

    """
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Save the responses dict to a JSON file in-memory
    try:
        json_data = json.dumps(json_file, indent=4)
        input_logger.info('File has been saved in memory')
        # Create a blob in GCS and upload the JSON data
        blob = bucket.blob(gcs_path)
        blob.upload_from_string(json_data, content_type='application/json')
        input_logger.info('File has been uploaded to GCS')
    except Exception as e:
        input_logger.info(f'File was not saved to Google Cloud Storage: {e}')
    return None 


def main():
    logger = setup_logger(log_file_name="data_ingestion.log")
    
    file_name = 'postcodes.csv'
    print('Input bucketname:')
    bucketname = input()
    
    # Read file from GCS
    logger.info("----------- Read file from Google Cloud Storage from BUCKETNAME / RAW / Postcode.csv ---------")
    df = read_gcs_file(logger, bucketname,file_name)

    # Extract list of postcodes
    logger.info("----------- Extract Postcodes into list ---------")

    mask = (df["Constituency"].notna()) & (df["Postcode"].notna())

    postcodes_list = filtered_list(df,mask,"Postcode")

    # Webscrape Local Council data based on feature
    logger.info("----------- Launch webscraper to extract Local Coucil names from 'https://www.gov.uk/find-local-council' ---------")
    
    local_council_json = webscrape_local_council(logger, list_of_postcodes= postcodes_list)
    
    # # Alternative - loading data from local storage
    # test_json = 'C:\\Users\\natha\PycharmProjects\\gambling_premises_uk\\notebooks\\missing_local_authorities.json'
    # with open(test_json, 'r') as file:
    #     local_council_json = json.load(file)

    logger.info("----------- Writing file to BUCKETNAME / RAW / Postcode_Local Council.csv ---------")

    upload_to_gcs(logger,bucket_name= bucketname,
                  json_file=local_council_json,
                  gcs_path="RAW/local_authorities.json") 



if __name__ == "__main__":
        main()