import pandas as pd
import numpy as np

# This then changes to be a merge piece

#     #"Sorted Rows" = Table.Sort(#"Changed Type",{{"match_weight", Order.Descending}}),
#     #"FULL ADDRESS LEFT" = Table.AddColumn(#"Sorted Rows", "Full_Address_l", each Text.Combine({[Address_Line_1_l], [Address_Line_2_l], [City_l], [Postcode_l]}, ","), type text),
#     Full_Address_r = Table.AddColumn(#"FULL ADDRESS LEFT", "Full_Address_r", each Text.Combine({[Address_Line_1_r], [Address_Line_2_r], [City_r], [Postcode_r]}, ","), type text),
#     #"Flattened ID" = Table.AddColumn(Full_Address_r, "Flatten_ID", each Text.Combine({Text.From([Account_Number_l], "en-GB"), [Account_Name_l], [Local_Authority_l], [Address_Line_1_l], [Address_Line_2_l], [City_l], [Postcode_l]}, ""), type text),
#     #"Match Weght > 21" = Table.SelectRows(#"Flattened ID", each [match_weight] > 21),
#     #"Sorted Rows1" = Table.Sort(#"Match Weght > 21",{{"match_weight", Order.Descending}}),
#     #"Added Custom" = Table.AddColumn(#"Sorted Rows1", "Custom", each "MW: "),
#     #"Number to Strnig for group" = Table.CombineColumns(Table.TransformColumnTypes(#"Added Custom", {{"match_weight", type text}}, "en-GB"),{"Custom", "match_weight"},Combiner.CombineTextByDelimiter("", QuoteStyle.None),"match_weight.1"),
#     #"Added Custom1" = Table.AddColumn(#"Number to Strnig for group", "Custom", each "MP: "),
#     #"Added Custom2" = Table.AddColumn(#"Added Custom1", "Custom.1", each "AN: "),
#     #"Merged Columns2" = Table.CombineColumns(Table.TransformColumnTypes(#"Added Custom2", {{"Account_Number_r", type text}}, "en-GB"),{"Custom.1", "Account_Number_r"},Combiner.CombineTextByDelimiter("", QuoteStyle.None),"Account_Number_r.1"),
#     #"Merged Columns1" = Table.CombineColumns(Table.TransformColumnTypes(#"Merged Columns2", {{"match_probability", type text}}, "en-GB"),{"Custom", "match_probability"},Combiner.CombineTextByDelimiter("", QuoteStyle.None),"match_probability.1"),
#     #"List Agg Grouping" = Table.Group(#"Merged Columns1", {"Flatten_ID"}, {
#     {"Match_Weight_List_Agg", each Text.Combine([match_weight.1], " / "), type nullable text},
#     {"Match_Probability_List_Agg", each Text.Combine([match_probability.1], " / "), type nullable text},
#     {"Full Address list_agg", each Text.Combine([Full_Address_r], " / "), type nullable text},
#     {"Account_Number_List_Agg", each Text.Combine([Account_Number_r.1], " / "), type nullable text},
#     {"Account_Name_List_Agg", each Text.Combine([Account_Name_r], " / "), type nullable text},
#     {"Premises_Activity_List_Agg", each Text.Combine([Premises_Activity_r], " / "), type nullable text},
#     {"Local_Authority_List_Agg", each Text.Combine([Local_Authority_r], " / "), type nullable text},
#     {"unique_id_r_List_Agg", each Text.Combine([unique_id_r], " / "), type nullable text}
# }),
#     #"Split r ID by Delimetre" = Table.SplitColumn(#"List Agg Grouping", "unique_id_r_List_Agg", Splitter.SplitTextByDelimiter(" / ", QuoteStyle.Csv), {"unique_id_r_List_Agg.1", "unique_id_r_List_Agg.2", "unique_id_r_List_Agg.3", "unique_id_r_List_Agg.4", "unique_id_r_List_Agg.5"}),
#     #"Changed Type1" = Table.TransformColumnTypes(#"Split r ID by Delimetre",{{"unique_id_r_List_Agg.1", type text}, {"unique_id_r_List_Agg.2", type text}, {"unique_id_r_List_Agg.3", type text}, {"unique_id_r_List_Agg.4", type text}, {"unique_id_r_List_Agg.5", type text}}),
#     #"Merge & Remove" = Table.AddColumn(#"Changed Type1", "Custom", each if [unique_id_r_List_Agg.1] <> null or [unique_id_r_List_Agg.2] <> null or [unique_id_r_List_Agg.3] <> null or
# [unique_id_r_List_Agg.4] <> null or [unique_id_r_List_Agg.5] <> null then "merge and remove" else null)
# in
#     #"Merge & Remove"



# OLD CODE:

# Load the CSV file
df = pd.read_csv(
    "/Preparation & Cleaning Redundancies/Splink - ML for Deduplication/Duplication CSV/Match_Weight_DataFrame.csv")

distinct_count_one = df['unique_id_l'].nunique()

print(f"Original Distinct count of 'unique_id_l': {distinct_count_one}")

df.sort_values(by='match_weight', ascending=False, inplace=True)

# Most reliable duplciates
df = df[df['match_weight'] >= 21]

# Create a new column Full Address_r by merging multiple address columns
df['Full Address_r'] = df[['Address_Line_1_r', 'Address_Line_2_r', 'City_r', 'Postcode_r']].apply(lambda x: ', '.join(x), axis=1)

# Create custom columns MW, MP, AN by combining text
df['MW'] = 'MW: ' + df['match_weight'].astype(str)
df['MP'] = 'MP: ' + df['match_probability'].astype(str)
df['AN'] = 'AN: ' + df['Account_Number_r'].astype(str)


print('Creating aggregation fuction...')

# Create lists of aggregated values for certain columns

def listagg_with_separator(series):
    return ' / '.join(series.astype(str))

# Group by 'unique_id_l' and aggregate the columns you want
df_g = df.groupby('unique_id_l').agg({
    'MW': listagg_with_separator,
    'MP': listagg_with_separator,
    'Full Address_r': listagg_with_separator,
    'AN': listagg_with_separator,
    'Account_Name_r': listagg_with_separator,
    'Premises_Activity_r': listagg_with_separator,
    'Local_Authority_r': listagg_with_separator,
    'unique_id_r': listagg_with_separator}).reset_index()

print('Printing distinct count after Grouping By ID...')

distinct_count_two= df_g['unique_id_l'].nunique()

print(f"New Distinct count of 'unique_id_l': {distinct_count_two}")


# Rename the columns

df_g.rename(columns={
    'unique_id_l': 'unique_id_l',
    'MW': 'Match_Weight_List_Agg',
    'MP': 'Match_Probability_List_Agg',
    'Full Address_r': 'Full Address list_agg',
    'AN': 'Account_Number_List_Agg',
    'Account_Name_r': 'Account_Name_List_Agg',
    'Premises_Activity_r': 'Premises_Activity_List_Agg',
    'Local_Authority_r': 'Local_Authority_List_Agg',
    'unique_id_r': 'unique_id_r_List_Agg'
}, inplace=True)

# Split the unique_id_r_List_Agg into separate columns

df_g[['unique_id_r','unique_id_r_1','unique_id_r_2','unique_id_r_3','unique_id_r_4','unique_id_r_5']] = df_g['unique_id_r_List_Agg'].str.split(' / ', expand=True)

# Create a new column Custom for merge and remove
df_g['Custom'] = df_g.apply(lambda x: 'merge and remove' if x[['unique_id_r_1', 'unique_id_r_2', 'unique_id_r_3', 'unique_id_r_4', 'unique_id_r_5']].notna().any() else None, axis=1)


#df_g.to_csv('C:\\Users\\natha\\PycharmProjects\\gambling_premises_in_the_uk\\Preparation & Cleaning Redundancies\\Key Stage Downloads - CSVs\\flattened_add_split.csv')



# Ok so probably solve - pivoit the prem activity on ADDED_ID *before* joinging onto flattened table.
# CHECK THE JOIN BACK TO THE ORIGINAL BECAUSE WE LEFT JOINED ONTO A DUPE DUE TO THE PREMTYPE
# SUGGESTION BEFORE GROUPING ON unique_ID you need to create a NEW unique Id excluding the premises activity so these can both be included , the rest of the logic should be correct.
# Check your joins using unique_id_r instead it may be simpler to use the above id created once identifed for both datasets no?




## REDUCED THE ID COUNT FROM 9103 TO 9020 BY PIVOTTING THE PREM ACTIVTY AND GROUPING BY TO FLATTEN - NEEDS NEW ID TO DO THIS EXCLUDING THE PREM ACTIVITY.

## TO REMOVE THE DUPES NOW -- YOU MAY NEED TO RE-RUN THIS IN THE SPLINK MODEL WITH THE BETTER UNIQUE ID -- BUT THINK.

 ## HOW DO WE


prem_dataset = pd.read_csv(
    '/Preparation & Cleaning Redundancies/Key Stage Downloads - CSVs/Added_ID_NONULLS_premises-licence-register.csv')

print(prem_dataset.columns)

distinct_count_prem = prem_dataset['unique_id'].nunique()

print(f"Prem Distinct count of 'unique_id: {distinct_count_prem}")
















# Join to flattened dupe dataset with the first split to identify which rows are duplicates.

prem_dataset = prem_dataset.merge(df_g[['unique_id_r_1','Custom']], how= 'left', left_on='unique_id', right_on='unique_id_r_1',suffixes= (None,'_1'))
prem_dataset = prem_dataset.merge(df_g[['unique_id_r_2','Custom']], how= 'left', left_on='unique_id', right_on='unique_id_r_2',suffixes= (None,'_2'))
prem_dataset = prem_dataset.merge(df_g[['unique_id_r_3','Custom']], how= 'left', left_on='unique_id', right_on='unique_id_r_3',suffixes= (None,'_3'))
prem_dataset = prem_dataset.merge(df_g[['unique_id_r_4','Custom']], how= 'left', left_on='unique_id', right_on='unique_id_r_4',suffixes= (None,'_4'))
prem_dataset = prem_dataset.merge(df_g[['unique_id_r_5','Custom']], how= 'left', left_on='unique_id', right_on='unique_id_r_5',suffixes= (None,'_5'))


prem_dataset['Remove'] = prem_dataset.apply(lambda x: 'merge and remove' if x[['unique_id_r_1', 'unique_id_r_2', 'unique_id_r_3', 'unique_id_r_4', 'unique_id_r_5']].notna().any() else None, axis=1)

prem_dataset = prem_dataset[prem_dataset['Remove'] != 'merge and remove']

cleaned_official_premises_l_r = prem_dataset.merge(df_g[['unique_id_l', 'Match_Weight_List_Agg','Match_Probability_List_Agg','Full Address list_agg','Account_Number_List_Agg','Account_Name_List_Agg','Premises_Activity_List_Agg',
'Local_Authority_List_Agg', 'unique_id_r_List_Agg']], how= 'left', left_on='unique_id', right_on='unique_id_l',suffixes= (None,'_flattened'))
# explained scatter chart with zones

#  make a full address
# Split full addresss list agg and call it 'alernative full address'
# drop custom columns
cleaned_official_premises_l_r['Full_Address'] = cleaned_official_premises_l_r[['Address_Line_1', 'Address_Line_2', 'City', 'Postcode']].apply(lambda x: ', '.join(x), axis=1)

split_columns = cleaned_official_premises_l_r['Full Address list_agg'].str.split(' / ', expand=True)
split_columns.columns = [f'Full_Address_Alt_{i+1}' for i in range(split_columns.shape[1])]
cleaned_official_premises_l_r = pd.concat([cleaned_official_premises_l_r, split_columns], axis=1)

distinct_count_cleaned = cleaned_official_premises_l_r['unique_id'].nunique()

print(f"Cleaned official Distinct count of 'unique_id: {distinct_count_cleaned}")

columns_to_drop = ['Custom','unique_id_r_2','unique_id_r_3','Custom_3','unique_id_r_4','Custom_4','unique_id_r_5','Custom_5','Remove','unique_id_l']
cleaned_official_premises_l_r = cleaned_official_premises_l_r.drop(columns=columns_to_drop)

# Split Premises_Activity_List_Agg column
premises_activity_split_columns = cleaned_official_premises_l_r['Premises_Activity_List_Agg'].str.split(' / ', expand=True)

# List of activity types
activity_types = [
    'Other', 'Betting Shop', 'Adult Gaming Centre', 'Bingo',
    'Family Entertainment Centre', 'Casino', 'Casino 2005', 'Pool Betting'
]
# Initialize columns for each activity type
for activity_type in activity_types:
    cleaned_official_premises_l_r[f'{activity_type}_la'] = cleaned_official_premises_l_r['Premises_Activity_List_Agg'].str.count(activity_type)
    if f'{activity_type}_og' in cleaned_official_premises_l_r.columns:
        cleaned_official_premises_l_r[f'{activity_type}_og'] = cleaned_official_premises_l_r['Premises_Activity'].str.count(activity_type)
    else:
        cleaned_official_premises_l_r[f'{activity_type}_og'] = 0  # Set count to 0 if the column doesn't exist

# Sum the counts for each activity type
for activity_type in activity_types:
    cleaned_official_premises_l_r[activity_type] = cleaned_official_premises_l_r[f'{activity_type}_la'] + cleaned_official_premises_l_r[f'{activity_type}_og']

# Drop the intermediate columns (_la and _og)
cleaned_official_premises_l_r.drop(columns=[f'{activity_type}_la' for activity_type in activity_types] +
                                       [f'{activity_type}_og' for activity_type in activity_types], inplace=True)

cleaned_official_premises_l_r.drop(columns =['Unnamed: 0','Unnamed:_0','index',)

# Print the DataFrame
cleaned_official_premises_l_r.to_csv('C:\\Users\\natha\\PycharmProjects\\gambling_premises_in_the_uk\\Preparation & Cleaning Redundancies\Key Stage Downloads - CSVs\\cleaned_official_premises_l_r.csv')









# Google API ? Perminantley Closed Premises should be identified.

# GOOGLE API JOIN ADDRESS LINE AND SEARCCH FOR OPEN/CLOSED
#df = pd.read_csv('C:\\Users\\natha\\OneDrive\\Desktop\\Kaggle\\Added_ID_NONULLS_premises-licence-register.csv')