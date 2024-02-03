# Connecting to Git premises-licence-register.csv dataset

import pandas as pd
from selenium.common.exceptions import NoSuchElementException

from project_functions import df_to_csv
from project_functions import find_from_config
from project_functions import left_join
from project_functions import list_to_df
from project_functions import null_filter
from project_functions import url_to_dataframe
from project_functions import not_null_filter


# functions

def nulls_matrix(df,col):
    unpivot = df.melt(id_vars = col, var_name = 'column_name')
    distinct = unpivot.groupby('column_name').count()
    return pd.DataFrame(df.isna().sum(),columns = {'total_nulls':'0'}).rename_axis('column_name').reset_index()
def fill_na(df,col,fill):
    return df[col].fillna(fill)

def webscrape_local_council(df,col):
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    local_council_responses = []
    for x, y in enumerate(df.values):
        input_value = df[col].values[x]
        driver.get(find_from_config('webscrape', 'local_council_url'))
        post_code_entry = driver.find_element(By.NAME,find_from_config('webscrape','local_council_postcode_search'))
        post_code_entry.send_keys(input_value)
        find_button = driver.find_element(By.CSS_SELECTOR, find_from_config('webscrape','local_council_button_selector'))
        find_button.click()
        try:
            two_local_authorities = driver.find_element(By.CSS_SELECTOR,find_from_config('webscrape','local_council_two_options'))
            if two_local_authorities.text == 'Services in your area are provided by two local authorities':
                first_local_authority = driver.find_element(By.CSS_SELECTOR, find_from_config('webscrape','local_council_first_option'))
                second_local_authority = driver.find_element(By.CSS_SELECTOR, find_from_config('webscrape','local_council_second_option'))
                local_council_responses.append(first_local_authority.text + '/' + second_local_authority.text)
            else:
                try:
                    local_council_answer = driver.find_element(By.CSS_SELECTOR,find_from_config('webscrape','local_council_grab'))
                    local_council_responses.append(local_council_answer.text)
                except NoSuchElementException:
                    local_council_responses.append('No Local Council Data Available')
        except NoSuchElementException:
            try:
                local_council_answer = driver.find_element(By.CSS_SELECTOR,find_from_config('webscrape','local_council_grab'))
                local_council_responses.append(local_council_answer.text)
            except NoSuchElementException:
                local_council_responses.append('No Local Council Data Available')
    results = list_to_df(local_council_responses, ['Local Authority Filled'])
    df_to_csv(results,find_from_config('output_datasets','local_authority_filled'))
    return results

def nan_dataframe(df):
    return pd.DataFrame(df.isna().sum(), columns={'total_nulls': '0'}).rename_axis('column_name').reset_index()

def drop_cols(df,columns_list):
    return df.drop(columns_list, axis= 1)

def main():
    df = url_to_dataframe(find_from_config('path','original_file'))
    nulls_df = nulls_matrix(df,'Account Number')
    # replacing NaNs in 'Premises Activity' with Other category
    df['Premises Activity'] = fill_na(df,'Premises Activity','Other')
    # Replacing NaNs in 'Local Authority' using GOV.UK's Find You Local Council tool
    no_local_authority = null_filter(df,'Local Authority') # formerly LA_Empty
    filled_local_authority = webscrape_local_council(no_local_authority,'Postcode')
    #filled_local_authority = url_to_dataframe(find_from_config('output_datasets','local_authority_filled'))
    filled_local_authority = filled_local_authority.reset_index()
    no_local_authority = no_local_authority.reset_index()
    print(no_local_authority.columns)
    print(filled_local_authority.columns)

    no_local_authority = no_local_authority[['index', 'Account Number']]

    combined = left_join(no_local_authority,
                         filled_local_authority,
                         'index',
                         'index'
                         )
    print(combined.columns)

    df = df.reset_index()
    df = left_join(df,
                          combined,
                         'index',
                         'index'
                         )
    df['Local Authority'] = df['Local Authority'].fillna(df['Local Authority Filled'])

    # Final Checks
    total_nans = pd.DataFrame(df.isna().sum(),columns = {'total_nulls':'0'}).rename_axis('column_name').reset_index()
    print(total_nans)
    # Replacing the single NaN in the Postcode field with B74 2XH post a google search...')
    df['Postcode'] = df['Postcode'].fillna('B74 2XH')
    total_nans = nan_dataframe(df)
    coloumns_to_drop = ['Local Authority Filled','Delete_Me','Postcode_copy','Extra']
    df['Postcode_copy'] = df['Postcode']
    df[['Postcode District','Delete_Me','Extra']] = df['Postcode_copy'].str.split(' ',expand=True)
    df = drop_cols(df,coloumns_to_drop)
    df_to_csv(df,find_from_config('output_datasets','nan_replacement'))
    print('Null handling complete!')

if __name__ == "__main__":
    main()
else:
    pass




