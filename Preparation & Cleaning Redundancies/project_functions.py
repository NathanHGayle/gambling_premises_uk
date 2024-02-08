import logging
import pandas as pd
import numpy as np
import yaml
import re

def find_from_config(key, name):
    with open('0.config.yaml', 'r') as file:
        config = yaml.safe_load(file)
        return config[key][name]

def url_to_dataframe(path, cols=None, data_type=None, low_mem=True, suffix=None):
    if suffix is None:
        return pd.read_csv(path, usecols=cols, dtype=data_type, low_memory=low_mem)
    else:
        return pd.read_csv(path, usecols=cols, dtype=data_type, low_memory=low_mem).add_suffix(suffix)

def extract_postcodes(extract_from):
    uk_pattern = re.compile(r'\b[A-Z]{1,2}\d{1,2}[A-Z]?(\s*\d[A-Z0-9]{0,2})?\b')
    postcodes = [uk_pattern.search(address).group() if uk_pattern.search(address) else None for address in extract_from]
    return postcodes

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

def calculate_z_score(x, mean, stdv):
    if pd.notna(x) and x != 0:
        return (x - mean) / stdv
    else:
        return np.nan

def apply_z_score(df_col, mean, stdv):
    return df_col.apply(lambda x: calculate_z_score(x, mean, stdv))

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


def df_col_mean(df_col):
    return df_col.replace(0, np.nan).dropna().mean()


def df_col_std(df_col):
    return df_col.replace(0, np.nan).dropna().std()

def null_filter(df, col):
    return df[df[col].isnull()]

def not_null_filter(df,col):
    return df[df[col].notnull()]

def unique_vals(df,col):
    n = len(pd.unique(df[col]))
    return "No.of.unique values :",n

if __name__ == "__main__":
    main()
else:
    pass

