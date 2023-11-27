import pandas as pd

df = pd.read_csv('Insert Path - Key Stage Downloads - CSVs\\NONULLS_premises-licence-register.csv')

# Transformations - includes creating an id field
df['Account Number'] = df['Account Number'].astype(int)
columns = list(df.columns)
columns = [sub.replace(' ','_') for sub in columns]
df.columns = columns
analyse_columns = columns[2:]
result = df.apply(lambda x: ''.join(x.astype(str)), axis=1)
result = result.replace(' ','_')
result = result.reset_index()
df = df.merge(result,how='left',left_on='index',right_on='index')
df.rename(columns={0:"unique_id"},inplace=True)

df['Address_Line_1'] = df['Address_Line_1'].str.replace('Ladnrokes','Ladbrokes')
df['Address_Line_1'] = df['Address_Line_1'].str.replace('Ladnrokes','Ladbrokes')
df['Address_Line_1'] = df['Address_Line_1'].str.replace('284 â€“ 286 Northolt Road','284-286 Northolt Road')
df[['Address_Line_1', 'Address_Line_2']] = df[['Address_Line_1', 'Address_Line_2']].astype(str).apply(lambda x: x.str.replace(',', ''))
df[['Address_Line_1', 'Address_Line_2']] = df[['Address_Line_1', 'Address_Line_2']].astype(str).apply(lambda x: x.str.replace("/",'-'))
df[['Address_Line_1', 'Address_Line_2']] = df[['Address_Line_1', 'Address_Line_2']].astype(str).apply(lambda x: x.str.replace("&",'and'))
df[['Address_Line_1', 'Address_Line_2', 'City']] = df[['Address_Line_1', 'Address_Line_2', 'City']].astype(str).apply(lambda x: x.str.upper())

#in PQ I full address after the pivot
df['Full_Address'] = df[['Address_Line_1', 'Address_Line_2', 'City', 'Postcode']].apply(lambda x: ','.join(x), axis=1).str.strip()
df['Flatten_ID'] = df[['Account_Number','Account_Name','Local_Authority','Address_Line_1','Address_Line_2','City','Postcode']].astype(str).apply(lambda x: ''.join(x), axis=1)

#Pivot Premesis Activity
p_cols = df[['Flatten_ID','Premises_Activity']]
p_cols = p_cols.groupby(['Flatten_ID'],group_keys=True).apply(lambda x: x)
p_cols = p_cols.groupby(['Flatten_ID', 'Premises_Activity']).size().unstack(fill_value=0)

# Group BY with LISTAGG for DF then join the two tables

f = df.groupby('Flatten_ID').agg(lambda x: '/'.join(x.astype(str)))
f.drop(['Unnamed:_0', 'index','unique_id'], axis=1, inplace=True)
df_new = f.merge(p_cols,how='left',left_on='Flatten_ID',right_on='Flatten_ID')

new_cols =columns = list(df_new.columns)
added_columns = [sub.replace(' ','_') for sub in columns]
df_new.columns = added_columns

df_new.to_csv('Insert Path - Key Stage Downloads - CSVs\\Added_FlatID_NONULLS_premises-licence-register.csv')

print('Successful')
