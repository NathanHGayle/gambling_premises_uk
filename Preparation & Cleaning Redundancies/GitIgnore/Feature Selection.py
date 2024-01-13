import googlemaps_places as gp
import pandas as pd
df = pd.read_csv('C:\\Users\\natha\\OneDrive\\Desktop\\Kaggle\\Added_FlatID_NONULLS_premises-licence-register.csv')

def find_easy(df_string):
    business_status_str = gp.find_place(client=gp.gmaps,
                  input = f'{df_string}',
                  input_type= 'textquery',
                  fields= ['formatted_address', 'place_id','business_status']
                  )
    return business_status_str

print('calling google api against all rows...')

# df['google_api_result'] = df['Full_Address'].apply(lambda x: find_easy(x))

print('saving results as field in dataset called gbsnsstus_complete_premises-licence-register.csv...')

#df.to_csv('C:\\Users\\natha\\OneDrive\\Desktop\\Kaggle\\gbsnsstus_complete_premises-licence-register.csv')

output = pd.read_csv('C:\\Users\\natha\\OneDrive\\Desktop\\Kaggle\\gbsnsstus_complete_premises-licence-register.csv')

# convert string to dict
output['google_api_result_dict'] = output['google_api_result'].apply(lambda x: eval(x))

first_parse = pd.DataFrame(output['google_api_result_dict'].to_list(),index=output.index)

first_parse['candidates'] = first_parse['candidates'].apply(lambda x: x[0] if len(x)>0 else {'business_status': x,
                                                                                             'formatted_address': x,
                                                                                             'place_id': x})
second_parse = pd.DataFrame(first_parse['candidates'].to_list(),index=first_parse.index)

output.reset_index(inplace=True)
second_parse.reset_index(inplace=True)
final_df = output.merge(second_parse.add_suffix('_google_api_'),how='left',left_on='index',right_on= 'index_google_api_')

final_df = final_df.drop(columns=['google_api_result_dict','index_google_api_','Unnamed: 0','index'])
prem_cols = ['Adult_Gaming_Centre','Betting_Shop', 'Bingo', 'Casino', 'Casino_2005','Family_Entertainment_Centre',
             'Other', 'Pool_Betting']

columns = list(final_df.columns)
columns = [sub+'_prem_activity'if sub in prem_cols else sub for sub in columns ]
final_df.columns = columns
date_of_api_call = '2023-11-14'
final_df['yyyy_mm_dd_google_api'] = pd.to_datetime(date_of_api_call)

final_df['business_status_google_api_'] = final_df['business_status_google_api_'].fillna('NOT FOUND')
final_df[['business_status_google_api_','formatted_address_google_api_','place_id_google_api_']] = \
    final_df[['business_status_google_api_','formatted_address_google_api_','place_id_google_api_']].applymap(lambda x:
                                                                                                              'NOT FOUND'
                                                                                                              if x == []
                                                                                                              else x
                                                                                                              )

#final_df.to_csv('C:\\Users\\natha\\OneDrive\\Desktop\\Kaggle\\operation_vs_permclose_premises-licence-register.csv')


