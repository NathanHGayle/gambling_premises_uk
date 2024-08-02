import pandas as pd
import numpy as np


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
