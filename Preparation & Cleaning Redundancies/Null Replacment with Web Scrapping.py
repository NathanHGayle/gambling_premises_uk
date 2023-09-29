# Connecting to Git premises-licence-register.csv dataset
import pandas as pd


import requests
import io
url = "https://github.com/NateBI777/gambling_premises_uk/blob/main/premises-licence-register.csv"
download = requests.get(url).content
df_local = pd.read_csv(io.StringIO(download.decode('utf-8')))

# Or use local downloaded file
    # path = 'insert your path here'
    # local_file = path + '\\premises-licence-register.csv'
    # df_local = pd.read_csv(local_file)

# NaNs Matrix
unpivot = df_local.melt(id_vars = 'Account Number', var_name = 'column_name')
distinct = unpivot.groupby('column_name').count()
nulls_df = pd.DataFrame(df_local.isna().sum(),columns = {'total_nulls':'0'}).rename_axis('column_name').reset_index()

# replacing NaNs in 'Premises Activity' with Other category
df_local['Premises Activity'] = df_local['Premises Activity'].fillna('Other')

# Replacing NaNs in 'Local Authority' using GOV.UK's Find You Local Council tool

LA_empty = df_local[df_local['Local Authority'].isnull()]

print('Do you want to authorize the webscrape job, y/n? (approximate run time: 3 minutes...')
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

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    local_council_responses = []

    for x, y in enumerate(LA_empty.values):
        input = LA_empty['Postcode'].values[x]
        find_local_council_url = 'https://www.gov.uk/find-local-council'
        driver.get(find_local_council_url)
        post_code_entry = driver.find_element(By.NAME, 'postcode')
        post_code_entry.send_keys(input)
        find_button = driver.find_element(By.CSS_SELECTOR, '#local-locator-form > fieldset > button')
        find_button.click()
        try:
            two_local_authorities = driver.find_element(By.CSS_SELECTOR,'#content > div > div.govuk-grid-column-two-thirds.article-container > h2')
            if two_local_authorities.text == 'Services in your area are provided by two local authorities':
                first_local_authority = driver.find_element(By.CSS_SELECTOR,'#content > div > div.govuk-grid-column-two-thirds.article-container > div > div.county-result.group.govuk-\!-margin-bottom-8 > h3')
                second_local_authority = driver.find_element(By.CSS_SELECTOR,'#content > div > div.govuk-grid-column-two-thirds.article-container > div > div.district-result.group > h3')
                local_council_responses.append(first_local_authority.text + '/' + second_local_authority.text)
            else:
                try:
                    local_council_answer = driver.find_element(By.CSS_SELECTOR,'#content > div > div.govuk-grid-column-two-thirds.article-container > div > div > p:nth-child(1) > strong')
                    local_council_responses.append(local_council_answer.text)
                except:
                    local_council_responses.append('No Local Council Data Available')
        except:
            try:
                local_council_answer = driver.find_element(By.CSS_SELECTOR, '#content > div > div.govuk-grid-column-two-thirds.article-container > div > div > p:nth-child(1) > strong')
                local_council_responses.append(local_council_answer.text)
            except:
                local_council_responses.append('No Local Council Data Available')
        sleep(2)
    LA_filled = list_to_df(local_council_responses, ['Local Authority Filled'])
    LA_filled.to_csv('insert local path or environment path\\Local Authority Filled.csv', index=True) # change location
    print('success', LA_filled)
elif ans == 'n':
    la_filled_df = pd.read_csv('insert local path or environment path\\Local Authority Filled.csv')

LA_empty = LA_empty.reset_index()
LA_empty = LA_empty.reset_index()
LA_empty = LA_empty[['level_0', 'index']]

# print (la_filled_df)
LA_combined = LA_empty.merge(la_filled_df,how= 'left',left_on ='level_0', right_on = 'Unnamed: 0')
df_local = df_local.reset_index()
df_local = df_local.merge(LA_combined, how= 'left',left_on='index',right_on='index')

df_local['Local Authority'] = df_local['Local Authority'].fillna(df_local['Local Authority Filled'])

# Final Checks
nulls_df = pd.DataFrame(df_local.isna().sum(),columns = {'total_nulls':'0'}).rename_axis('column_name').reset_index()
print('Printing total NaNs by column now that Local Authority has been filled...')
print(nulls_df)
print('Replacing the single NaN in the Postcode field with B74 2XH post a google search...')

df_local['Postcode'] = df_local['Postcode'].fillna('B74 2XH')
nulls_df = pd.DataFrame(df_local.isna().sum(),columns = {'total_nulls':'0'}).rename_axis('column_name').reset_index()
print('Postcode has been filled...')
print(nulls_df)

print('dropping helper columns...')

df_local = df_local.drop(['level_0','Unnamed: 0','Local Authority Filled'], axis= 1)
df_local['Postcode_copy'] = df_local['Postcode']
df_local[['Postcode District','Delete_Me','Extra']] = df_local['Postcode_copy'].str.split(' ',expand=True)
df_local = df_local.drop(['Delete_Me','Postcode_copy','Extra'], axis= 1)

print('Null handling complete!')
df_local.to_csv('insert local path or environment path\\NONULLS_premises-licence-register.csv')






