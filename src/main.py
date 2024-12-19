import pandas as pd
from src.data_ingestion.raw_ws_local_council_data import df_to_csv
from src.utils.utils import find_from_config
from src.utils.custom_logger import logger
from src.data_ingestion.raw_ws_local_council_data import left_join
from src.data_ingestion.raw_ws_local_council_data import list_to_df
from src.data_ingestion.raw_ws_local_council_data import null_filter
from src.data_ingestion.raw_ws_local_council_data import url_to_dataframe
from src.data_ingestion.raw_ws_local_council_data import not_null_filter
from src.data_ingestion.raw_ws_local_council_data import nulls_matrix
from src.data_ingestion.raw_ws_local_council_data import fill_na
from src.data_ingestion.raw_ws_local_council_data import webscrape_local_council
from src.data_ingestion.raw_ws_local_council_data import nan_dataframe
from src.data_ingestion.raw_ws_local_council_data import drop_cols
from src.data_ingestion.raw_ws_local_council_data import col_astype
from src.data_ingestion.raw_ws_local_council_data import pivot_premises_activities
from src.data_ingestion.raw_ws_local_council_data import cols_underscore
from src.data_ingestion.raw_ws_local_council_data import select_cols_composite_key
from src.data_ingestion.raw_ws_local_council_data import all_cols_composite_key
from src.data_ingestion.raw_ws_local_council_data import replace_string
from src.data_ingestion.raw_ws_local_council_data import clean_and_upper_columns
from src.data_ingestion.raw_kagglehub_data import parse_api_result
from src.data_ingestion.raw_kagglehub_data import clean_and_format_df
from src.data_ingestion.raw_ws_local_council_data import unique_vals
from src.data_ingestion.raw_ws_local_council_data import pivot_df
from src.data_ingestion.raw_ws_local_council_data import webscrape_constituencies
from src.data_ingestion.statistics import apply_z_score
from src.data_ingestion.statistics import df_col_std
from src.data_ingestion.statistics import df_col_mean
from src.data_ingestion.raw_ws_local_council_data import distinct_values
from src.data_ingestion.raw_ws_local_council_data import extract_postcodes
from src.data_ingestion.raw_ws_local_council_data import split_by_delimiter
from src.data_ingestion.raw_ws_local_council_data import sum_cols
import splink
from splink.duckdb.duckdb_linker import DuckDBLinker
import splink.duckdb.duckdb_comparison_library as cl
from splink import charts
import altair as alt

alt.renderers.enable('mimetype')


# Replacing NaN values


def main():
    # Replacing NaN values

    df = url_to_dataframe(find_from_config('path', 'original_file'))
    nulls_df = nulls_matrix(df, 'Account Number')
    # replacing NaNs in 'Premises Activity' with Other category
    df['Premises Activity'] = fill_na(df, 'Premises Activity', 'Other')
    # Replacing NaNs in 'Local Authority' using GOV.UK's Find You Local Council tool
    no_local_authority = null_filter(df, 'Local Authority')  # formerly LA_Empty
    filled_local_authority = webscrape_local_council(no_local_authority, 'Postcode')
    # filled_local_authority = url_to_dataframe(find_from_config('output_datasets','local_authority_filled'))
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
    total_nans = pd.DataFrame(df.isna().sum(), columns={'total_nulls': '0'}).rename_axis('column_name').reset_index()
    print(total_nans)
    # Replacing the single NaN in the Postcode field with B74 2XH post a google search...')
    df['Postcode'] = df['Postcode'].fillna('B74 2XH')
    total_nans = nan_dataframe(df)
    columns_to_drop = ['Local Authority Filled', 'Delete_Me', 'Postcode_copy', 'Extra']
    df['Postcode_copy'] = df['Postcode']
    df[['Postcode District', 'Delete_Me', 'Extra']] = df['Postcode_copy'].str.split(' ', expand=True)
    df = drop_cols(df, columns_to_drop)
    df_to_csv(df, find_from_config('output_datasets', 'nan_replacement'))
    print('Null handling complete!')

    # Deduplication Framework:

    print('Running Splink version:', splink.__version__)
    print('Creating new file to include IDs in NONULLs file...')
    # Import dataset -- change method to connect to Git CSV in repository
    df = pd.read_csv('Insert Path - Key Stage Downloads - CSVs\\NONULLS_premises-licence-register.csv')

    df['unique_id'] = df['Flatten_ID']
    print('Dataframe columns:', list(df.columns))

    # Splink Time

    settings = {
        "link_type": "dedupe_only",
        "blocking_rules_to_generate_predictions": [
            # "l.Account_Number = r.Account_Number",
            "l.Postcode = r.Postcode",
            # "l.Local_Authority = r.Local_Authority",
            "l.City = r.City",

        ],
        "comparisons": [
            cl.exact_match("Account_Number"),
            cl.levenshtein_at_thresholds("Account_Name", [1, 2], include_exact_match_level=True),
            cl.levenshtein_at_thresholds("Premises_Activity", [1, 2], include_exact_match_level=True),
            cl.exact_match("Local_Authority"),
            cl.levenshtein_at_thresholds("Address_Line_1", [1, 2], include_exact_match_level=True),
            cl.levenshtein_at_thresholds("Address_Line_2", [1, 2], include_exact_match_level=True),
            cl.exact_match("City"),
            cl.exact_match("Postcode"),
            cl.exact_match("Postcode_District"),
            cl.exact_match("Full_Address"),
            cl.exact_match("Adult_Gaming_Centre"),
            cl.exact_match("Betting_Shop"),
            cl.exact_match("Bingo"),
            cl.exact_match("Casino"),
            cl.exact_match("Casino_2005"),
            cl.exact_match("Family_Entertainment_Centre"),
            cl.exact_match("Other"),
            cl.exact_match("Pool_Betting")

        ],
    }

    linker = DuckDBLinker(df, settings)

    analyse_columns = list(df.columns[2:])

    # EDA
    print('EDA STEP?: Do you want to save the profile chart in your environment, y/n?')
    ans = input()
    if ans == 'y':
        profile_cols = linker.profile_columns(analyse_columns, top_n=10, bottom_n=5)
        charts.save_offline_chart(profile_cols, filename="profilecols.html", overwrite=False)
    else:
        pass

    # Modelling
    linker.load_settings(settings)  # replace with linker.load_settings_from_json("my_settings.json") where appropriate
    print(
        'Please choose a model 1) Estimate_u_using_random_sampling AND  estimate_parameters_using_expectation_maximisation 2) None, 1/2?')
    ans1 = input()
    if ans1 == '1':

        # Train M values
        blocking_rules_for_training = "l.City = r.City and l.Account_Name = r.Account_Name"
        training_session_an = linker.estimate_parameters_using_expectation_maximisation(
            blocking_rules_for_training)  # fix_u_probabilities = False.
        linker.save_settings_to_json("expectation_maximisation.json", overwrite=True)
        m_u_chart = linker.m_u_parameters_chart()
        match_weights = linker.match_weights_chart()
        charts.save_offline_chart(m_u_chart, filename="m_u_chart_from_label.html", overwrite=True)
        charts.save_offline_chart(match_weights, filename="match_weights_from_label.html", overwrite=True)

        # Train M values again
        blocking_rules_for_training_two = "l.Postcode = r.Postcode"
        training_session_two = linker.estimate_parameters_using_expectation_maximisation(
            blocking_rules_for_training_two)  # fix_u_probabilities = False.
        linker.save_settings_to_json("expectation_maximisation_two.json", overwrite=True)
        m_u_chart_two = linker.m_u_parameters_chart()
        match_weights_two = linker.match_weights_chart()
        charts.save_offline_chart(m_u_chart, filename="m_u_chart_two_from_label.html", overwrite=True)
        charts.save_offline_chart(match_weights, filename="match_weights_two_from_label.html", overwrite=True)

        # Train U values
        linker.estimate_u_using_random_sampling(1e8)
        linker.save_settings_to_json("u_from_random_sample.json", overwrite=True)
        m_u_chart = linker.m_u_parameters_chart()
        match_weights = linker.match_weights_chart()
        charts.save_offline_chart(m_u_chart, filename="m_u_chart_u_random.html", overwrite=True)
        charts.save_offline_chart(match_weights, filename="match_weights_u_random.html", overwrite=True)

    else:
        print("You are presuming your model is already trained, continuing on to predictions...")

    print('Now we are going to predict based on a 0.2 threshold match probability...')

    e = linker.predict(threshold_match_probability=0.2)
    df_e = e.as_pandas_dataframe()
    df_e.to_csv('Insert Path - Splink - ML for Deduplication\\Duplication CSV\\Match_Weight_DataFrame.csv', index=True)

    print(df_e)

    records_to_plot = df_e.head().to_dict(orient="read")
    waterfall = charts.waterfall_chart(records_to_plot, linker._settings_obj_, filter_nulls=False)
    charts.save_offline_chart(waterfall, filename="waterfall_chart.html", overwrite=True)

    precision_recalls = linker.precision_recall_chart_from_labels_column("Flatten_ID")
    charts.save_offline_chart(precision_recalls, filename="precision_recalls.html", overwrite=True)

    print('Successful')

    # Cleaning and reshaping the data

    df = url_to_dataframe(find_from_config('path', 'replaced_nulls'))
    # Transformations - includes creating an id field
    df = col_astype(df, 'Account Number', int)
    df.columns = cols_underscore(df)
    df = all_cols_composite_key(df, 'unique_id')
    logger.info(df.columns)
    # Specify columns to replace str
    replacement_dict = {
        'Ladnrokes': 'Ladbrokes',
        '284 – 286 Northolt Road': '284-286 Northolt Road'
    }
    df['Address_Line_1'] = replace_string(df, 'Address_Line_1', replacement_dict)
    # Specify columns to clean and replace_string
    columns_to_clean = ['Address_Line_1', 'Address_Line_2', 'City']
    clean_and_upper_columns(df, columns_to_clean)
    address_fields = ['Address_Line_1', 'Address_Line_2', 'City', 'Postcode']
    df['Full_Address'] = select_cols_composite_key(df, address_fields)
    distinct_fields = ['Account_Number', 'Account_Name', 'Local_Authority', 'Address_Line_1', 'Address_Line_2', 'City',
                       'Postcode']
    df['Flatten_ID'] = select_cols_composite_key(df, distinct_fields)
    pivot_cols_data = df[['Flatten_ID', 'Premises_Activity']]
    pivot = pivot_premises_activities(pivot_cols_data, 'Premises_Activity', 'Flatten_ID')
    # Assert distinct ids
    enforcing_uniqueness = df.groupby('Flatten_ID').agg(lambda x: '/'.join(x.astype(str)))
    enforcing_uniqueness.drop(['Unnamed:_0', 'index', 'unique_id'], axis=1, inplace=True)
    df_new = left_join(enforcing_uniqueness,
                       pivot,
                       'Flatten_ID',
                       'Flatten_ID'
                       )
    df_new.columns = cols_underscore(df_new)
    df_to_csv(df_new, find_from_config('output_datasets', 'enforced_uniqueness'))
    logger.info('Successful')

    # Replace using Google API

    df = url_to_dataframe(find_from_config('path', 'distinct_ids'))
    # df['google_api_result'] = df['Full_Address'].apply(lambda x: call_google_api(x))
    # df_to_csv(df, find_from_config('output_datasets','google_api_results'))
    final_df = parse_api_result(find_from_config('output_datasets', 'google_api_results'), 'google_api_results')
    columns_to_drop = ['google_api_result_dict', 'index_google_api_', 'Unnamed: 0', 'index']
    final_df = clean_and_format_df(final_df)
    df_to_csv(final_df, find_from_config('path', 'untested_post_google_api'))
    print('successful')

    # data transformation

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

    # Statistics

    op_spearman = find_from_config('output_datasets', 'correlation_matrix')
    cols = ['Constituency_postcode_data',
            'business_status_google_api_',
            'Adult_Gaming_Centre_prem_activity',
            'Betting_Shop_prem_activity',
            'Bingo_prem_activity',
            'Casino_prem_activity',
            'Casino_2005_prem_activity',
            'Family_Entertainment_Centre_prem_activity',
            'Other_prem_activity',
            'Pool_Betting_prem_activity',
            'Total_Premises_by_Constituency',
            'Total_Operational_Premises_by_Constituency',
            'Managerial, administrative and professional occupations',
            'Intermediate occupations',
            'Routine and manual occupations',
            'Full-time students',
            'Never worked / long-term unemployed',
            'IMD rank 2019_id_2019_data',
            'IMD rank 2015_id_2019_data',
            'Number of LSOAs in most deprived decile_id_2019_data',
            'Share of LSOAs in most deprived decile_id_2019_data',
            'Income_id_2019_data',
            'Employment_id_2019_data',
            'Education, skills and training_id_2019_data',
            'Health deprivation and disability_id_2019_data',
            'Crime_id_2019_data',
            'Barriers to housing and services_id_2019_data',
            'Living environment_id_2019_data'
            ]
    df = url_to_dataframe('path', 'final_dataset')
    # df.fillna(0,inplace=True)
    df['constituency_name_key'] = df['Constituency_postcode_data'].str.lower().replace(' ', '')
    operational_only = df[df['business_status_google_api_'] == 'OPERATIONAL']
    operationalnot_found = df[df['business_status_google_api_'].isin(['OPERATIONAL', 'NOT FOUND'])]
    rolled_up = operational_only.groupby('constituency_name_key') \
        .agg({
        'Adult_Gaming_Centre_prem_activity': 'sum',
        'Betting_Shop_prem_activity': 'sum',
        'Bingo_prem_activity': 'sum',
        'Casino_prem_activity': 'sum',
        'Casino_2005_prem_activity': 'sum',
        'Family_Entertainment_Centre_prem_activity': 'sum',
        'Other_prem_activity': 'sum',
        'Pool_Betting_prem_activity': 'sum',
        'Total_Premises_by_Constituency': 'max',
        'Total_Operational_Premises_by_Constituency': 'max',
        'Managerial, administrative and professional occupations': 'max',
        'Intermediate occupations': 'max',
        'Routine and manual occupations': 'max',
        'Full-time students': 'max',
        'Never worked / long-term unemployed': 'max',
        'IMD rank 2019_id_2019_data': 'max',
        'IMD rank 2015_id_2019_data': 'max',
        'Number of LSOAs in most deprived decile_id_2019_data': 'max',
        'Income_id_2019_data': 'max',
        'Employment_id_2019_data': 'max',
        'Education, skills and training_id_2019_data': 'max',
        'Health deprivation and disability_id_2019_data': 'max',
        'Crime_id_2019_data': 'max',
        'Barriers to housing and services_id_2019_data': 'max',
        'Living environment_id_2019_data': 'max'
    }
    )
    correlation_dataset = rolled_up.corr(method='spearman')
    # df_to_csv(correlation_dataset,find_from_config('output_datasets','correlation_matrix'))


if __name__ == "__main__":
    main()
else:
    pass
