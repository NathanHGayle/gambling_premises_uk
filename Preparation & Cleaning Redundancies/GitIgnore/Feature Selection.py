import googlemaps_places as gp
import pandas as pd
from project_functions import df_to_csv
from project_functions import find_from_config
from project_functions import url_to_dataframe
from project_functions import left_join


# functions
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


def main():
    df = url_to_dataframe(find_from_config('path', 'distinct_ids'))
    # df['google_api_result'] = df['Full_Address'].apply(lambda x: call_google_api(x))
    # df_to_csv(df, find_from_config('output_datasets','google_api_results'))
    final_df = parse_api_result(find_from_config('output_datasets', 'google_api_results'), 'google_api_results')
    columns_to_drop = ['google_api_result_dict', 'index_google_api_', 'Unnamed: 0', 'index']
    final_df = clean_and_format_df(final_df)
    df_to_csv(final_df, find_from_config('path', 'untested_post_google_api'))
    print('successful')


if __name__ == "__main__":
    main()
else:
    pass
