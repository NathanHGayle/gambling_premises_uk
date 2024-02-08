import logging
import pandas as pd
import numpy as np
import re
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
import undetected_chromedriver as uc
from project_functions import find_from_config
from project_functions import url_to_dataframe
from project_functions import left_join
from project_functions import df_to_csv


# Functions
def extract_postcodes(extract_from):
    uk_pattern = re.compile(r'\b[A-Z]{1,2}\d{1,2}[A-Z]?(\s*\d[A-Z0-9]{0,2})?\b')
    postcodes = [uk_pattern.search(address).group() if uk_pattern.search(address) else None for address in extract_from]
    return postcodes


def split_by_delimiter(df_col, by, stop=0):
    return df_col.str.split(by).str[stop]


def distinct_values(df_col):
    list_unique_values_ = df_col.unique()
    return len(list_unique_values_)


def calculate_z_score(x, mean, stdv):
    if pd.notna(x) and x != 0:
        return (x - mean) / stdv
    else:
        return np.nan


def apply_z_score(df_col, mean, stdv):
    return df_col.apply(lambda x: calculate_z_score(x, mean, stdv))


def pivot_df(df, col_val, idx, cols_label, agg='sum', fill=0):
    return pd.pivot_table(
        df,
        values=col_val,
        index=idx,
        columns=cols_label,
        aggfunc=agg,
        fill_value=fill
    ).reset_index()


def sum_cols(total_name, df, cols):
    try:
        df[total_name] = df[cols].sum(axis=1)
    except KeyError as e:
        logging.info(f"One or more oclumns don't exist: {e}")


def list_to_df(your_list, your_columns):
    return pd.DataFrame(your_list, index=range(len(your_list)), columns=your_columns)


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


def df_col_mean(df_col):
    return df_col.replace(0, np.nan).dropna().mean()


def df_col_std(df_col):
    return df_col.replace(0, np.nan).dropna().std()


def null_filter(df, col):
    return df[df[col].isnull()]


def not_null_filter(df, col):
    return df[df[col].notnull()]


def unique_vals(df, col):
    n = len(pd.unique(df[col]))
    return "No.of.unique values :", n


def main():
    # check the below files from yaml config.
    check_file = find_from_config('output_datasets', 'google_check_file')
    # read in data
    prem = url_to_dataframe(find_from_config('path', 'post_google_api'))
    # create variable based on address field from Google's api
    addresses = prem['formatted_address_google_api_'].values
    # extract the postcodes from this address
    postcodes = extract_postcodes(addresses)
    # create new columns for simple original postcode vs google api postcode comparison
    prem['postcode_single'] = split_by_delimiter(prem['Postcode'], '/', 0)
    prem['postcode_district_single'] = split_by_delimiter(prem['Postcode_District'], '/', 0)
    prem['extracted_postcode'] = postcodes
    # Compare districts
    prem['extracted_district'] = split_by_delimiter(prem['extracted_postcode'], ' ', 0)
    # Replace blanks with original
    prem['google_api_correction'] = prem['extracted_postcode'] == prem['postcode_single']
    prem['google_district_check'] = prem['extracted_district'] == prem['postcode_district_single']
    prem['original_address'] = prem['Full_Address']  # for eyes delete after eyeballing
    df_to_csv(prem, check_file)
    # filter to missing google postcodes
    missing_postcodes = null_filter(prem, 'extracted_postcode')
    # postcodes data
    postcode_dtypes = {
        'Postcode': 'str',
        'Constituency': 'str',
        'In Use?': 'str',
        'Population': 'Int64',
        'Latitude': 'float',
        'Longitude': 'float',
        'Households': 'Int64'}
    postcode_data = url_to_dataframe(find_from_config('input_datasets', 'postcodes'),
                                     list(postcode_dtypes.keys()),
                                     postcode_dtypes,
                                     False,
                                     '_postcode_data')
    # national social-economic-class data
    nsec_data = url_to_dataframe(
        find_from_config('input_datasets', 'social_eco_class'),
        suffix='_nsec_data')
    # English indices of deprivation data
    indices_deprivation = url_to_dataframe(
        find_from_config('input_datasets', 'indices_deprivation'),
        suffix='_id_2019_data'
    )

    # most recent premises dataset
    logging.info('distinct premises:', distinct_values(prem['Flatten_ID']))

    # Joining datasets
    add_constituency = left_join(prem,
                                 postcode_data,
                                 'Postcode',
                                 'Postcode_postcode_data')
    # find distinct set of constituencies
    distinct_constituencies = postcode_data.groupby('Constituency_postcode_data').size() \
        .reset_index(name='count_occurances_postcode_data')
    # join these to the prem dataset on constituency to find those don't exist
    join_prem = left_join(distinct_constituencies,
                          add_constituency,
                          'Constituency_postcode_data',
                          'Constituency_postcode_data')

    no_match_constituency = null_filter(join_prem, 'Postcode')

    add_constituency = pd.concat([add_constituency, no_match_constituency])

    # fill missing constituencies
    not_null_postcode = not_null_filter(add_constituency, 'postcode_single')
    is_null_constituency = null_filter(not_null_postcode, 'Constituency_postcode_data')
    logging.info('missing constituencies count:', is_null_constituency.value_counts())
    df_to_csv(is_null_constituency, find_from_config('output_datasets', 'null_constituency'))
    found_constituencies = webscrape_constituencies(is_null_constituency['postcode_single'])
    # manual amendment to document check missing Constituencies, search online if not found replace postcodes if null
    # re-import
    found_constituencies = url_to_dataframe(find_from_config('path', 'manually_checked'))
    # drop duplicates
    # found_constituencies.drop_duplicates(subset=['postcode_inputs_webscrape'],inplace=True)
    add_constituency = left_join(add_constituency,
                                 found_constituencies,
                                 'postcode_single',
                                 'postcode_inputs_webscrape')

    add_constituency['Constituency_postcode_data'] = add_constituency['Constituency_postcode_data'] \
        .fillna(add_constituency['fill_constituency_webscrape'])

    # "ConstituencyPopulation_nsec_data": "sum" FIX THIS -- maybe not worth the funciton for all this trouble
    pop_by_con = add_constituency.groupby('Constituency_postcode_data').agg(
        Population_by_Constituency_postcode_data=('Population_postcode_data', 'sum'), ).reset_index()

    add_constituency_with_population = left_join(add_constituency,
                                                 pop_by_con,
                                                 'Constituency_postcode_data',
                                                 # changed to match above -- swap if wrong way
                                                 'Constituency_postcode_data')
    # Create metric variables for scaling population
    population_mean = df_col_mean(add_constituency['Population_postcode_data'])
    population_stdv = df_col_std(add_constituency['Population_postcode_data'])
    add_constituency_with_population['scaled_population_by_constituency_postcode_data'] = \
        apply_z_score(add_constituency_with_population['Population_by_Constituency_postcode_data']
                      , population_mean, population_stdv)
    # Total Premises by Constituency
    total_prem_by_con = add_constituency_with_population.groupby('Constituency_postcode_data').agg(
        Total_Premises_by_Constituency=('Flatten_ID', 'count'),
        Total_Operational_Premises_by_Constituency=(
            'business_status_google_api_',
            lambda x: (x == 'OPERATIONAL').sum())).reset_index()

    # total_prem features join to data
    add_constituency_two = left_join(add_constituency_with_population,
                                     total_prem_by_con,
                                     'Constituency_postcode_data',
                                     'Constituency_postcode_data')
    # rename national social economic class fields for better interpretation
    nsec_data_cols_rename = {'Con_pc_nsec_data': 'ConstituencyPopulation_nsec_data',
                             'RN_pc_nsec_data': 'Region Nation_nsec_data',
                             'Nat_pc_nsec_data': 'England Wales_nsec_data'}
    nsec_data.rename(columns=nsec_data_cols_rename, inplace=True)
    # Group these metrics by Region and Constituency
    print(nsec_data.columns)
    grouped_fields = ['RegNationID_nsec_data', 'RegNationName_nsec_data', 'ConstituencyName_nsec_data',
                      'groups_nsec_data']

    grouped_nsec = nsec_data.groupby(grouped_fields).agg({"ConstituencyPopulation_nsec_data": "sum"}).reset_index()

    groups = ['RegNationID_nsec_data', 'RegNationName_nsec_data', 'ConstituencyName_nsec_data']

    # Pivot to have numerical fields against groupings
    pivoted_nsec = pivot_df(grouped_nsec,
                            'ConstituencyPopulation_nsec_data',
                            groups,
                            'groups_nsec_data')

    numeric_cols = ['Managerial, administrative and professional occupations',
                    'Intermediate occupations',
                    'Routine and manual occupations',
                    'Full-time students',
                    'Never worked / long-term unemployed']
    # Total for row level check - should = 100%
    sum_cols('Total Population By Constituency_nsec_calculated', pivoted_nsec, numeric_cols)
    # Select columns we want to bring in
    constituency_metrics = pivoted_nsec[['ConstituencyName_nsec_data',
                                         'Managerial, administrative and professional occupations',
                                         'Intermediate occupations',
                                         'Routine and manual occupations',
                                         'Full-time students',
                                         'Never worked / long-term unemployed',
                                         'Total Population By Constituency_nsec_calculated']]
    add_constituency_metrics = left_join(add_constituency_two,
                                         constituency_metrics,
                                         'Constituency_postcode_data',
                                         'ConstituencyName_nsec_data')
    # Select columns from indices_deprivation
    id_2019_selected_cols = indices_deprivation[['ConstituencyName_id_2019_data',
                                                 'DateOfThisUpdate_id_2019_data',
                                                 'DateOfDataset_id_2019_data',
                                                 'IMD rank 2019_id_2019_data',
                                                 'IMD rank 2015_id_2019_data',
                                                 'Change in rank since 2015_id_2019_data',
                                                 'Number of LSOAs in most deprived decile_id_2019_data',
                                                 'Share of LSOAs in most deprived decile_id_2019_data',
                                                 'Income_id_2019_data',
                                                 'Employment_id_2019_data',
                                                 'Education, skills and training_id_2019_data',
                                                 'Health deprivation and disability_id_2019_data',
                                                 'Crime_id_2019_data',
                                                 'Barriers to housing and services_id_2019_data',
                                                 'Living environment_id_2019_data',
                                                 'IDACI_id_2019_data',
                                                 'IDAOPI_id_2019_data']]
    add_dep_metrics = left_join(add_constituency_metrics,
                                id_2019_selected_cols,
                                'ConstituencyName_nsec_data',
                                'ConstituencyName_id_2019_data')

    df_to_csv(add_dep_metrics, find_from_config('output_datasets', 'final_dataset'))

    print(unique_vals(add_dep_metrics, 'ConstituencyName_nsec_data'))


if __name__ == "__main__":
    main()
else:
    pass
