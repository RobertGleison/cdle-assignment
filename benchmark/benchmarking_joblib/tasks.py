import pandas as pd
import numpy as np
from joblib import delayed

def read_file_parquet(df=None, **kwargs):
    return pd.read_parquet(kwargs.get("path"))

@delayed
def count(df=None):
    return len(df)

@delayed
def count_index_length(df=None):
    return len(df.index)

@delayed
def mean(df):
    return df.Fare_Amt.mean().compute()

@delayed
def standard_deviation(df):
    return df.Fare_Amt.std().compute()

@delayed
def mean_of_sum(df):
    return (df.Fare_Amt + df.Tip_Amt).mean().compute()

@delayed
def sum_columns(df):
    return (df.Fare_Amt + df.Tip_Amt).compute()

@delayed
def mean_of_product(df):
    return (df.Fare_Amt * df.Tip_Amt).mean().compute()

@delayed
def product_columns(df):
    return (df.Fare_Amt * df.Tip_Amt).compute()

@delayed
def value_counts(df):
    return df.Fare_Amt.value_counts().compute()

@delayed
def mean_of_complicated_arithmetic_operation(df):
    theta_1 = df.Start_Lon
    phi_1 = df.Start_Lat
    theta_2 = df.End_Lon
    phi_2 = df.End_Lat
    temp = (np.sin((theta_2 - theta_1) / 2 * np.pi / 180) ** 2
           + np.cos(theta_1 * np.pi / 180) * np.cos(theta_2 * np.pi / 180)
           * np.sin((phi_2 - phi_1) / 2 * np.pi / 180) ** 2)
    ret = 2 * np.arctan2(np.sqrt(temp), np.sqrt(1 - temp))
    return ret.mean().compute()

@delayed
def complicated_arithmetic_operation(df):
    theta_1 = df.Start_Lon
    phi_1 = df.Start_Lat
    theta_2 = df.End_Lon
    phi_2 = df.End_Lat
    temp = (np.sin((theta_2 - theta_1) / 2 * np.pi / 180) ** 2
           + np.cos(theta_1 * np.pi / 180) * np.cos(theta_2 * np.pi / 180)
           * np.sin((phi_2 - phi_1) / 2 * np.pi / 180) ** 2)
    ret = 2 * np.arctan2(np.sqrt(temp), np.sqrt(1 - temp))
    return ret.compute()

def groupby_statistics(df):
    return df.groupby(by='Passenger_Count').agg(
        {
            'Fare_Amt': ['mean', 'std'],
            'Tip_Amt': ['mean', 'std']
        }
    )

@delayed
def join_count(df, other):
    return len(pd.merge(df, other, left_index=True, right_index=True))

@delayed
def join_data(df, other):
    return pd.merge(df, other, left_index=True, right_index=True).compute()