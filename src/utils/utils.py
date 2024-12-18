import yaml
import re


def find_from_config(key, name):
    with open('../config/old_config.yaml', 'r') as file:
        config = yaml.safe_load(file)
        return config[key][name]


def str_to_dict(df, col):
    return df[col].apply(lambda x: eval(x))


def extract_postcodes(extract_from):
    uk_pattern = re.compile(r'\b[A-Z]{1,2}\d{1,2}[A-Z]?(\s*\d[A-Z0-9]{0,2})?\b')
    postcodes = [uk_pattern.search(address).group() if uk_pattern.search(address) else None for address in extract_from]
    return postcodes

def split_by_delimiter(df_col, by, stop=0):
    return df_col.str.split(by).str[stop]


def distinct_values(df_col):
    list_unique_values_ = df_col.unique()
    return len(list_unique_values_)


def split_by_delimiter(df_col, by, stop=0):
    return df_col.str.split(by).str[stop]


def df_to_csv(df, path):
    logging.info('saved %s to csv here: %s', f'{df} ,{path}')
    return df.to_csv(path)


def distinct_values(df_col):
    list_unique_values_ = df_col.unique()
    return len(list_unique_values_)


def left_join(df_1, df_2, key_1, key_2):
    return df_1.merge(df_2,
                      how='left',
                      left_on=key_1,
                      right_on=key_2)


def pivot_df(df, col_val, idx, cols_label, agg='sum', fill=0):
    return pd.pivot_table(
        df,
        values=col_val,
        index=idx,
        columns=cols_label,
        aggfunc=agg,
        fill_value=fill
    ).reset_index()


def sum_cols(total_name, df, cols):
    try:
        df[total_name] = df[cols].sum(axis=1)
    except KeyError as e:
        logging.info(f"One or more oclumns don't exist: {e}")


def list_to_df(your_list, your_columns):
    return pd.DataFrame(your_list, index=range(len(your_list)), columns=your_columns)


def null_filter(df, col):
    return df[df[col].isnull()]


def not_null_filter(df, col):
    return df[df[col].notnull()]


def unique_vals(df, col):
    n = len(pd.unique(df[col]))
    return "No.of.unique values :", n


def nulls_matrix(df, col):
    unpivot = df.melt(id_vars=col, var_name='column_name')
    distinct = unpivot.groupby('column_name').count()
    return pd.DataFrame(df.isna().sum(), columns={'total_nulls': '0'}).rename_axis('column_name').reset_index()


def fill_na(df, col, fill):
    return df[col].fillna(fill)


def url_to_dataframe(path, cols=None, data_type=None, low_mem=True, suffix=None):
    if suffix is None:
        return pd.read_csv(path, usecols=cols, dtype=data_type, low_memory=low_mem)
    else:
        return pd.read_csv(path, usecols=cols, dtype=data_type, low_memory=low_mem).add_suffix(suffix)
    

def nan_dataframe(df):
    return pd.DataFrame(df.isna().sum(), columns={'total_nulls': '0'}).rename_axis('column_name').reset_index()


def drop_cols(df, columns_list):
    return df.drop(columns_list, axis=1)


def col_astype(df, col, dtype):
    df[col] = df[col].astype(dtype)
    return df


def cols_underscore(df):
    columns = list(df.columns)
    space_to_underscore = [sub.replace(' ', '_') for sub in columns]
    return space_to_underscore


def all_cols_composite_key(df, composite_key_name):
    join_values_as_strings = df.apply(lambda x: ''.join(x.astype(str)), axis=1)
    replace_spaces = join_values_as_strings.replace(' ', '_')
    reset_the_index = replace_spaces.reset_index()
    new_df = df.merge(reset_the_index, how='left', left_on='index', right_on='index')
    return new_df.rename(columns={0: composite_key_name})


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
        df[col] = df[col].astype(str).replace(replacements, regex=True)


def select_cols_composite_key(df, cols_list):
    return df[cols_list].astype(str).apply(lambda x: ''.join(x), axis=1).str.strip()


def pivot_premises_activities(df, column_to_pivot, column_to_group_by):
    pivot_col = pd.get_dummies(df, columns=[column_to_pivot], prefix='', prefix_sep='_').groupby(
        column_to_group_by).sum()
    # Add suffix to each column name
    pivot_col.columns = [f"{col}_prem_activity" for col in pivot_col.columns]
    return pivot_col


def df_col_mean(df_col):
    return df_col.replace(0, np.nan).dropna().mean()


def df_col_std(df_col):
    return df_col.replace(0, np.nan).dropna().std()


def calculate_z_score(x, mean, stdv):
    if pd.notna(x) and x != 0:
        return (x - mean) / stdv
    else:
        return np.nan


def apply_z_score(df_col, mean, stdv):
    return df_col.apply(lambda x: calculate_z_score(x, mean, stdv))