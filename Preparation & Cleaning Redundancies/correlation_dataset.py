import pandas as pd

pearson = 'C:\\Users\\natha\\PycharmProjects\\gambling_premises_in_the_uk\\Preparation & Cleaning Redundancies\\Key Stage Downloads - CSVs\\pearson_matrix.csv'
spearman = 'C:\\Users\\natha\\PycharmProjects\\gambling_premises_in_the_uk\\Preparation & Cleaning Redundancies\\Key Stage Downloads - CSVs\\spearman_matrix.csv'
kendall = 'C:\\Users\\natha\\PycharmProjects\\gambling_premises_in_the_uk\\Preparation & Cleaning Redundancies\\Key Stage Downloads - CSVs\\kendall_matrix.csv'
op_pearson = 'C:\\Users\\natha\\PycharmProjects\\gambling_premises_in_the_uk\\Preparation & Cleaning Redundancies\\Key Stage Downloads - CSVs\\op_pearson_matrix.csv'
op_spearman = 'C:\\Users\\natha\\PycharmProjects\\gambling_premises_in_the_uk\\Preparation & Cleaning Redundancies\\Key Stage Downloads - CSVs\\op_spearman_matrix.csv'
op_nf_spearman = 'C:\\Users\\natha\\PycharmProjects\\gambling_premises_in_the_uk\\Preparation & Cleaning Redundancies\\Key Stage Downloads - CSVs\\op_nf__spearman_matrix.csv'
op_kendall = 'C:\\Users\\natha\\PycharmProjects\\gambling_premises_in_the_uk\\Preparation & Cleaning Redundancies\\Key Stage Downloads - CSVs\\op_kendall_matrix.csv'


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

df = pd.read_csv(
    '/Preparation & Cleaning Redundancies/Key Stage Downloads - CSVs/Final20_01_2024_T2202The_UK_gambling_premises_social_class_and_deprivation_dataset.csv', usecols=cols)

# df.fillna(0,inplace=True)

df['constituency_name_key'] = df['Constituency_postcode_data'].str.lower().replace(' ', '')

operational_only = df[df['business_status_google_api_'] == 'OPERATIONAL']
operationalnot_found = df[df['business_status_google_api_'].isin(['OPERATIONAL', 'NOT FOUND'])]
# now figure out how to get the max or sum of the columns you're interested in. Then group it all.

rolled_up = df.groupby('constituency_name_key')\
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

opp_rolled_up = operational_only.groupby('constituency_name_key')\
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


opp_nf_rolled_up = operationalnot_found.groupby('constituency_name_key')\
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


# rolled_up.corr().to_csv(pearson)
# rolled_up.corr(method='spearman').to_csv(spearman)
# rolled_up.corr(method='kendall').to_csv(kendall)

# opp_rolled_up.corr().to_csv(op_pearson)
# opp_rolled_up.corr(method='spearman').to_csv(op_spearman)
# opp_rolled_up.corr(method='kendall').to_csv(op_kendall)


opp_nf_rolled_up.corr(method='spearman').to_csv(op_nf_spearman)

