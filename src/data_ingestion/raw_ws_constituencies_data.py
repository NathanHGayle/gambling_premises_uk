import logging
import pandas as pd
import numpy as np
import re
import undetected_chromedriver as uc
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import NoSuchElementException
from src.utils.utils import find_from_config


def webscrape_constituencies(df_col, url=find_from_config('webscrape', 'ukp_url')):
    responses = []
    postcode_inputs = []
    values = df_col.values
    driver = uc.Chrome()  # service=Service(ChromeDriverManager().install()))
    constituency_responses = []
    postcode_input = []
    for x, y in enumerate(values):
        input_val = values[x]
        find_constituency_url = url
        driver.get(find_constituency_url)
        search_id = find_from_config('webscrape', 'ukp_search_id')
        click_button = find_from_config('webscrape', 'ukp_click_button')
        pull_name = find_from_config('webscrape', 'ukp_grab')
        try:
            post_code_entry = driver.find_element(By.ID, search_id)
            logging.info('By.css_selector, Search_selector worked')
        except NoSuchElementException:
            logging.info("Element not found")
        else:
            post_code_entry.send_keys(input_val)
            try:
                find_button = driver.find_element(By.CSS_SELECTOR, click_button)
                logging.info('by.css_selector,click search button worked')
            except NoSuchElementException:
                logging.info("Element not found")
            else:
                find_button.click()
                try:
                    found_constituency = driver.find_element(By.CSS_SELECTOR, pull_name)
                    logging.info('by.css_selector and pull name worked')
                except NoSuchElementException:
                    not_found_constituency = f'{input_val}not found'
                    constituency_responses.append(not_found_constituency)
                    driver.delete_all_cookies()
                else:
                    constituency_responses.append(f'{found_constituency.text},|,{input_val}')
                    driver.delete_all_cookies()
    print('con_rep:', constituency_responses)
    scrape_results = constituency_responses
    split_data = [entry.split(',|,') for entry in scrape_results]
    df_scrape = pd.DataFrame(split_data, columns=['con_rep', 'postcode_inputs_webscrape'])
    df_to_csv(df_scrape, find_from_config('output_datasets', 'check_webscrape'))
    return df_scrape
