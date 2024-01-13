import pandas as pd
import numpy as np
import re

prem = pd.read_csv('C:\\Users\\natha\\OneDrive\\Desktop\\Kaggle\\operation_vs_permclose_premises-licence-register.csv')

addresses = prem['formatted_address_google_api_'].values

postcode_pattern = re.compile(r'\b[A-Z]{1,2}\d{1,2}[A-Z]?\s*\d[A-Z]{2}\b')
postcode_pattern_flex = re.compile(r'\b[A-Z]{1,2}\d{1,2}[A-Z]?(\s*\d[A-Z]{2})?\b')
postcode_pattern_flex_2 = re.compile(r'\b[A-Z]{1,2}\d{1,2}[A-Z]?(\s*\d[A-Z0-9]{0,2})?\b')

#use the last one - it only leaves 29 that - when you fiter place id to exclude NOT FOUND - don't have any postcodes!
#551 postcodes NOT FOUND by google. Find them with via full address at royal mail test second_edit manually 1st

postcodes = []
for address in addresses:
    match = postcode_pattern_flex_2.search(address)
    if match:
        postcodes.append(match.group())
    else:
        postcodes.append(None)  # If no match, you can append None or any other value as needed

# Create a new column in the DataFrame

prem['Postcode_single'] = prem['Postcode'].str.split('/').str[0]

prem['extracted_postcode'] = postcodes

prem['google_api_incorrect'] = prem['extracted_postcode'] == prem['Postcode_single']

prem['original_address'] = prem['Full_Address']

prem.to_csv('C:\\Users\\natha\\OneDrive\\Desktop\\Kaggle\\second_edit_operation_vs_permclose_premises-licence-register.csv')


# FIlter to where extracted postcode is null. Then run a webscrape through all those addresses here:
# https://www.royalmail.com/find-a-postcode
# from selenium.webdriver.common.keys import Keys
# WebElement.sendKeys(Keys.RETURN);
# If nothing happens declare not opperation, if it's found make it operational'

breakpoint()


# Specify data types for specific columns
postcode_dtypes = {
    'Postcode': 'str',
    'Constituency': 'str',
    'In Use?': 'str',
    'Population': 'Int64',
    'Latitude': 'float',
    'Longitude': 'float',
    'Households': 'Int64',
    'Rural/urban': 'str'
}

postcodes = pd.read_csv('C:\\Users\\natha\\OneDrive\\Desktop\\Kaggle\\source data\\postcodes\\postcodes.csv',
                        usecols=['Postcode','Constituency','In Use?','Population','Latitude','Longitude','Households',
                                 'Rural/urban'],dtype=postcode_dtypes,low_memory=False)\
                        .add_suffix('_postcode_data')

nsec = pd.read_csv('C:\\Users\\natha\\PycharmProjects\\gambling_premises_in_the_uk\\Preparation & Cleaning Redundancies'
                   '\\Key Stage Downloads - CSVs\\source_data\\NS-SEC_2021.csv').add_suffix('_nsec_data')

indices_deprivation = pd.read_csv('C:\\Users\\natha\\OneDrive\\Desktop\\Kaggle\\source data\\'
                                  'Data constituencies_deprivation-dashboard.csv').add_suffix('_id_2019_data')

# Distinct IDs
list_distinct_ids_ = prem['Flatten_ID'].unique()
distinct_ids = len(list_distinct_ids_)
print(distinct_ids)

#Split the postcodes by delemetre -- consider creating a new column

prem['Postcode'] = prem['Postcode'].str.split('/').str[0]

# left join the dataset with constituency onto the premises dataset on Postcodes

add_constituency = prem.merge(postcodes,
                              how='left',
                              left_on='Postcode',
                              right_on='Postcode_postcode_data')

## Indentify missing constituencies

null_constituency = add_constituency[add_constituency['Constituency_postcode_data'].isnull()]

# quick checker for avoiding this step everytime
print('Do you want to authorize the webscrape job, y/n? (approximate run time: x minutes...')
ans = input()
constituency_responses = []
postcode_input = []

if ans == 'y':
    print("Let's grab those constituency names..")

    #import library items
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.common.exceptions import NoSuchElementException
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

    #create lists for conditional expressino

    # run for loop through null values to grab the postcodes

    for x, y in enumerate(null_constituency.values):
        input = null_constituency['Postcode'].values[x]
        find_constituency_url = 'https://www.theyworkforyou.com/'
        driver.get(find_constituency_url)
        better_search_id = 'postcode'
        better_click_button = 'body > div.hero > div > div.hero__mp-search > div > div > form > div.medium-3.columns > input'
        pull_name = 'body > div.person-header > div > div > div.person-header__about > p > span.person-header__about__position__constituency'

        try:
            post_code_entry = driver.find_element(By.ID,better_search_id)
        except NoSuchElementException:
            print("Element not found")
        else:
            post_code_entry.send_keys(input)
            try:
                find_button = driver.find_element(By.CSS_SELECTOR,better_click_button)
            except NoSuchElementException:
                print('Element not found')
            else:
                find_button.click()
                try:
                    found_constituency = driver.find_element(By.CSS_SELECTOR,pull_name)
                except NoSuchElementException:
                    not_found_constituency = np.nan
                    constituency_responses.append(not_found_constituency)
                    driver.delete_all_cookies()
                else:
                    constituency_responses.append(found_constituency.text)
                    postcode_input.append(input)
                    driver.delete_all_cookies()

    print(constituency_responses)
    print(postcode_input)
elif ans == 'n':
    print('We will grab the list of constituencies and postcodes previously identified from files instead')

# manually amend these here:

con = [nan, nan, nan, 'Oxford East', 'Morley and Outwood', 'Walsall North', nan, nan, nan, nan, 'Louth and Horncastle', 'Suffolk Coastal', nan, nan, nan, 'Louth and Horncastle', nan, 'North Devon', 'North Tyneside', 'Southampton, Itchen']
pos = ['oX4 4XP', 'LS27  9EB', 'Wv13 2AA', 'LN12 1AD.', 'IP11 2QN.', 'PE24 5TU.', 'Ex34 7BA', 'NE12 7 AX', 'SO14  2DF']

# combine and add back into the df as a condition

#combine = zip(postcode_input,constituency_responses)


print('stop script here')

breakpoint()










# Rolling up Population by Constituency - not all postcodes have population data and the comparison is at constituency

pop_by_con = add_constituency.groupby('Constituency_postcode_data')\
    .agg(
        Population_by_Constituency_postcode_data=('Population_postcode_data', 'sum'),
    )\
    .reset_index()


# Left Join this rolled-up metric to the original dataset

add_constituency_with_population = add_constituency.merge(pop_by_con,
                       how='left',
                       left_on='Constituency_postcode_data',
                       right_on='Constituency_postcode_data')

# population_mean = add_constituency['Population_postcode_data'].replace(0, np.nan).dropna().mean()
# population_stdv = add_constituency['Population_postcode_data'].replace(0, np.nan).dropna().std()

# Create variables that extract the mean and standard deviation from this Population metric excluding NaNs and Zeros
population_mean = add_constituency_with_population['Population_by_Constituency_postcode_data'].replace(0, np.nan).dropna().mean()
population_stdv = add_constituency_with_population['Population_by_Constituency_postcode_data'].replace(0, np.nan).dropna().std()

# create a function to handle this within an apply statement
def calculate_z_score(x, mean, stdv):
    if pd.notna(x) and x != 0:
        return (x - mean) / stdv
    else:
        return np.nan

# create a scaled version of the population metric to handle variance better, just in case, using the function
add_constituency_with_population['scaled_population_by_constituency_postcode_data'] =\
    add_constituency_with_population['Population_by_Constituency_postcode_data']\
    .apply(lambda x: calculate_z_score(x, population_mean, population_stdv))


# add_constituency['scaled_population_postcode_data'] = add_constituency['Population_postcode_data']\
#     .apply(lambda x: calculate_z_score(x, population_mean, population_stdv))


# Total Premises by Constituency'
total_prem_by_con = add_constituency_with_population.groupby('Constituency_postcode_data')\
    .agg(
        Total_Premises_by_Constituency=('Flatten_ID', 'count'),
        Total_Operational_Premises_by_Constituency=('business_status_google_api_', lambda x: (x == 'OPERATIONAL').sum())
    )\
    .reset_index()


# Left Join total prem values onto dataset

add_constituency_two = add_constituency_with_population.merge(total_prem_by_con,
                       how='left',
                       left_on='Constituency_postcode_data',
                       right_on='Constituency_postcode_data')


# rename national social economic class fields for better interpretation

nsec['ConstituencyPopulation_nsec_data'] = nsec['Con_pc_nsec_data']
nsec['Region Nation_nsec_data'] = nsec['RN_pc_nsec_data']
nsec['England Wales_nsec_data'] = nsec['Nat_pc_nsec_data']

# Group these metrics by Region and Constituency

grouped_nsec = nsec.groupby(['RegNationID_nsec_data','RegNationName_nsec_data','ConstituencyName_nsec_data','groups_nsec_data'])\
    .agg({"ConstituencyPopulation_nsec_data": "sum"})\
    .reset_index()

# Pivot the groups and sum the Constituency Population

pivoted_nsec = pd.pivot_table(
    grouped_nsec,
    values='ConstituencyPopulation_nsec_data',
    index=['RegNationID_nsec_data', 'RegNationName_nsec_data', 'ConstituencyName_nsec_data'],
    columns='groups_nsec_data',
    aggfunc='sum',
    fill_value=0  # Replace NaN with 0 if there are missing values
).reset_index()

numeric_columns = pivoted_nsec[['Managerial, administrative and professional occupations',
                       'Intermediate occupations',
                        'Routine and manual occupations',
                        'Full-time students',
                        'Never worked / long-term unemployed']]

# Create a total field to check these = 100%

pivoted_nsec['Total Population By Constituency_nsec_calculated'] = pivoted_nsec[
                                                       'Managerial, administrative and professional occupations'] \
                                                   + pivoted_nsec['Intermediate occupations'] \
                                                   + pivoted_nsec['Routine and manual occupations']\
                                                   + pivoted_nsec['Full-time students'] \
                                                   + pivoted_nsec['Never worked / long-term unemployed']

constituency_metrics = pivoted_nsec[['ConstituencyName_nsec_data',
                                     'Managerial, administrative and professional occupations',
                                     'Intermediate occupations',
                                     'Routine and manual occupations',
                                     'Full-time students',
                                     'Never worked / long-term unemployed',
                                     'Total Population By Constituency_nsec_calculated']]

# Left join these nationa social economic class metrics to the original dataset via the constituency label

add_constituency_metrics = add_constituency_two.merge(constituency_metrics,
                                                  how='left',
                                                  left_on='Constituency_postcode_data',
                                                  right_on='ConstituencyName_nsec_data')



ID_2019_selected_cols = indices_deprivation[['ConstituencyName_id_2019_data',
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

# left join the indices deprivation fields to this on constituency name

add_dep_metrics = add_constituency_metrics.merge(ID_2019_selected_cols,
                                                 how='left',
                                                 left_on='ConstituencyName_nsec_data',
                                                 right_on='ConstituencyName_id_2019_data')


# save a copy to local drive as csv file

#add_dep_metrics.to_csv('C:\\Users\\natha\\PycharmProjects\\gambling_premises_in_the_uk\\Preparation '
#                      '& Cleaning Redundancies\\Key Stage Downloads - CSVs\\The_UK_gambling_premises_social_class_and_'
#                       'deprivation_dataset.csv')



# replace whre constituency is not matched:

no_constituency = add_dep_metrics[add_dep_metrics['Constituency_postcode_data'].isnull()]

print('Do you want to authorize the webscrape job, y/n? (approximate run time: x minutes...')
ans = input()

def list_to_df(your_list, your_columns):
    return pd.DataFrame(your_list, index=range(len(your_list)), columns=your_columns)

if ans == 'y':
    print('Running webscraping operation...')
    from selenium import webdriver
    from time import sleep
    from selenium.webdriver.common.by import By
    from selenium.webdriver.chrome.service import Service
    from webdriver_manager.chrome import ChromeDriverManager
    from selenium.common.exceptions import NoSuchElementException
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    constituency_responses = []
    postcode_input = []
    for x, y in enumerate(no_constituency.values):
        input = no_constituency['Postcode'].values[x]
        find_constituency_url = 'https://www.theyworkforyou.com/'
        driver.get(find_constituency_url)
        better_search_id = 'postcode'
        better_click_button = 'body > div.hero > div > div.hero__mp-search > div > div > form > div.medium-3.columns > input'
        pull_name = 'body > div.person-header > div > div > div.person-header__about > p > span.person-header__about__position__constituency'

        try:
            post_code_entry = driver.find_element(By.ID,better_search_id)
            print('By.css_selector, Search_selector worked')
        except NoSuchElementException:
            print("Element not found")
        else:
            post_code_entry.send_keys(input)
            try:
                find_button = driver.find_element(By.CSS_SELECTOR,better_click_button)
                print('by.css_selector,click search button worked')
            except NoSuchElementException:
                print('Element not found')
            else:
                find_button.click()
                try:
                    found_constituency = driver.find_element(By.CSS_SELECTOR,pull_name)
                    print('by.css_selector and pull name worked')
                except NoSuchElementException:
                    not_found_constituency = np.nan
                    constituency_responses.append(not_found_constituency)
                    driver.delete_all_cookies()
                else:
                    constituency_responses.append(found_constituency.text)
                    postcode_input.append(input)
                    driver.delete_all_cookies()

    constituencies_filled = list_to_df(constituency_responses, ['postcode_constituency'])
    constituencies_filled.to_csv('C:\\Users\\natha\\OneDrive\\Desktop\\mising_constituencies_two.csv', index=True)
    print('success', constituencies_filled)
elif ans == 'n':
    print('Done')
    # constituencies_filled_df = pd.read_csv('C:\\Users\\natha\\OneDrive\\Desktop\\mising_constituencies_two.csv')\
    #                                         .add_suffix('_webscrap')
    #
    # constituencies_filled_df[['postcode_webscrap', 'constituency_webscrap']] = constituencies_filled_df[
    #     'postcode_constituency_webscrap'].str.split('/', 1, expand=True)
    #
    # constituencies_filled_df = constituencies_filled_df[['postcode_webscrap','constituency_webscrap']]
    # no_constituency = no_constituency.reset_index()
    #
    # print(no_constituency.columns)
    #
    # add_missing_constituencies = add_dep_metrics.merge(constituencies_filled_df,
    #                                                how='left',
    #                                                left_on='Postcode',
    #                                                right_on='postcode_webscrap')
    #
    #
    # add_missing_constituencies.to_csv('C:\\Users\\natha\\OneDrive\\Desktop\\match_mising_constituencies_two.csv', index=True)



    # new_df = add_dep_metrics.merge(add_missing_constituencies,
    #                                how='left',
    #                                left_on='Postcode',
    #                                right_on='Postcode')
    #
    # print(new_df.columns)



# add constituencies that are not joined.


# no_prem = postcodes.merge(add_dep_metrics,
#                           how ='left',
#                           left_on='Postcode_postcode_data',
#                           right_on = 'Postcode')



# check distinct IDs haven't been bloated / reduced by the joins

check_distinct_ids = add_dep_metrics['Flatten_ID'].unique()
print(len(check_distinct_ids))

#'Unnamed: 0','place_id_google_api_',