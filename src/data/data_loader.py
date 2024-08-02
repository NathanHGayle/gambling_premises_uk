import pandas as pd
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By
from src.utils import googlemaps_places as gp
from src.data.data_cleaning import url_to_dataframe
from src.data.data_cleaning import left_join


def call_google_api(df_string):
    business_status_str = gp.find_place(client=gp.gmaps,
                                        input=f'{df_string}',
                                        input_type='textquery',
                                        fields=['formatted_address', 'place_id', 'business_status']
                                        )
    return business_status_str


def str_to_dict(df, col):
    return df[col].apply(lambda x: eval(x))


def parse_api_result(df_path, dict_col):
    output = url_to_dataframe(df_path)
    output['google_api_result_dict'] = str_to_dict(output, dict_col)
    # parsing
    first_parse = pd.DataFrame(output['google_api_result_dict'].to_list(), index=output.index)
    first_parse['candidates'] = first_parse['candidates'] \
        .apply(lambda x: x[0] if len(x) > 0 else {'business_status': x,
                                                  'formatted_address': x,
                                                  'place_id': x}
               )
    second_parse = pd.DataFrame(first_parse['candidates'].to_list(), index=first_parse.index).add_suffix('_google_api_')
    output.reset_index(inplace=True)
    second_parse.reset_index(inplace=True)
    final_df = left_join(output,
                         second_parse,
                         'index',
                         'index_google_api_')
    return final_df


def clean_and_format_df(final_df):
    columns_to_drop = ['google_api_result_dict', 'index_google_api_', 'Unnamed: 0', 'index']
    final_df = final_df.drop(columns=columns_to_drop)

    premises_activity_cols = ['Adult_Gaming_Centre', 'Betting_Shop', 'Bingo', 'Casino', 'Casino_2005',
                              'Family_Entertainment_Centre', 'Other', 'Pool_Betting']
    final_df.columns = [sub + '_prem_activity' if sub in premises_activity_cols else sub for sub in final_df.columns]

    date_of_api_call = '2023-11-14'
    final_df['yyyy_mm_dd_google_api'] = pd.to_datetime(date_of_api_call)

    final_df['business_status_google_api_'] = final_df['business_status_google_api_'].fillna('NOT FOUND')
    final_df[['business_status_google_api_', 'formatted_address_google_api_', 'place_id_google_api_']] = \
        final_df[['business_status_google_api_', 'formatted_address_google_api_', 'place_id_google_api_']].applymap(
            lambda x: 'NOT FOUND' if x == [] else x
        )

    return final_df


def extract_postcodes(extract_from):
    uk_pattern = re.compile(r'\b[A-Z]{1,2}\d{1,2}[A-Z]?(\s*\d[A-Z0-9]{0,2})?\b')
    postcodes = [uk_pattern.search(address).group() if uk_pattern.search(address) else None for address in extract_from]
    return postcodes


def split_by_delimiter(df_col, by, stop=0):
    return df_col.str.split(by).str[stop]


def distinct_values(df_col):
    list_unique_values_ = df_col.unique()
    return len(list_unique_values_)
