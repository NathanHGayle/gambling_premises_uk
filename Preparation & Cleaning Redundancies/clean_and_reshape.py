import logging
import pandas as pd
from project_functions import find_from_config
from project_functions import url_to_dataframe as url_to_dataframe
from project_functions import left_join as left_join
from project_functions import df_to_csv
def col_astype(df,col,dtype):
    df[col] = df[col].astype(dtype)
    return df
def cols_underscore(df):
    columns = list(df.columns)
    space_to_underscore = [sub.replace(' ','_') for sub in columns]
    return space_to_underscore
def all_cols_composite_key(df,composite_key_name):
    join_values_as_strings = df.apply(lambda x: ''.join(x.astype(str)), axis=1)
    replace_spaces = join_values_as_strings.replace(' ', '_')
    reset_the_index = replace_spaces.reset_index()
    new_df = df.merge(reset_the_index, how='left', left_on='index', right_on='index')
    return new_df.rename(columns={0:composite_key_name})
def replace_string(df, col, replace_dict):
    return df[col].replace(replace_dict, regex=True)
def clean_and_upper_columns(df, columns):
    replacements = {
        'replace_comma': ',',
        'replace_slash': '/',
        'replace_and': '&',
        'uppercase': lambda x: x.upper()
    }
    for col in columns:
        df[col] = df[col].astype(str).replace(replacements, regex = True)
def select_cols_composite_key(df,cols_list):
    return  df[cols_list].astype(str).apply(lambda x: ''.join(x), axis=1).str.strip()
def pivot_premises_activities(df,column_to_pivot,column_to_group_by):
    pivot_col = pd.get_dummies(df,columns=[column_to_pivot], prefix='', prefix_sep='_').groupby(column_to_group_by).sum()
    # Add suffix to each column name
    pivot_col.columns = [f"{col}_prem_activity" for col in pivot_col.columns]
    return pivot_col
def main():
    df = url_to_dataframe(find_from_config('path','replaced_nulls'))
    # Transformations - includes creating an id field
    df = col_astype(df, 'Account Number', int)
    df.columns = cols_underscore(df)
    df = all_cols_composite_key(df, 'unique_id')
    logging.info(df.columns)
    # Specify columns to replace str
    replacement_dict = {
        'Ladnrokes': 'Ladbrokes',
        '284 – 286 Northolt Road': '284-286 Northolt Road'
    }
    df['Address_Line_1'] = replace_string(df, 'Address_Line_1', replacement_dict)
    # Specify columns to clean and replace_string
    columns_to_clean = ['Address_Line_1', 'Address_Line_2','City']
    clean_and_upper_columns(df, columns_to_clean)
    address_fields = ['Address_Line_1', 'Address_Line_2', 'City', 'Postcode']
    df['Full_Address'] = select_cols_composite_key(df,address_fields)
    distinct_fields = ['Account_Number','Account_Name','Local_Authority','Address_Line_1','Address_Line_2','City','Postcode']
    df['Flatten_ID'] = select_cols_composite_key(df,distinct_fields)
    pivot_cols_data = df[['Flatten_ID','Premises_Activity']]
    pivot = pivot_premises_activities(pivot_cols_data, 'Premises_Activity', 'Flatten_ID')
    # Assert distinct ids
    enforcing_uniqueness = df.groupby('Flatten_ID').agg(lambda x: '/'.join(x.astype(str)))
    enforcing_uniqueness.drop(['Unnamed:_0', 'index','unique_id'], axis=1, inplace=True)
    df_new = left_join(enforcing_uniqueness,
                       pivot,
                       'Flatten_ID',
                       'Flatten_ID'
                       )
    df_new.columns = cols_underscore(df_new)
    df_to_csv(df_new,find_from_config('output_datasets','enforced_uniqueness'))
    logging.info('Successful')
if __name__ == "__main__":
    main()
else:
    pass
