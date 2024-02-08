import pandas as pd
from project_functions import find_from_config
from project_functions import url_to_dataframe
from project_functions import df_to_csv
def main():
    op_spearman = find_from_config('output_datasets','correlation_matrix')
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
    df = url_to_dataframe('path','final_dataset')
    # df.fillna(0,inplace=True)
    df['constituency_name_key'] = df['Constituency_postcode_data'].str.lower().replace(' ', '')
    operational_only = df[df['business_status_google_api_'] == 'OPERATIONAL']
    operationalnot_found = df[df['business_status_google_api_'].isin(['OPERATIONAL', 'NOT FOUND'])]
    rolled_up = operational_only.groupby('constituency_name_key')\
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
    #df_to_csv(correlation_dataset,find_from_config('output_datasets','correlation_matrix'))

if __name__ == "__main__":
    main()
else:
    pass



