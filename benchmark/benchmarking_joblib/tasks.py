import pandas as pd
import numpy as np
from joblib import delayed

def read_file_parquet(df=None, **kwargs):
    file_path = kwargs.get("path")
    return pd.read_parquet(file_path)


def count(df=None):
    return len(df)

def count_index_length(df=None):
    return len(df.index)

def mean(df):
    return df.fare_amount.mean()

def standard_deviation(df):
    return df.fare_amount.std()

def mean_of_sum(df):
    return (df.fare_amount + df.tip_amount).mean()

def sum_columns(df):
    return (df.fare_amount + df.tip_amount)

def mean_of_product(df):
    return (df.fare_amount * df.tip_amount).mean()

def product_columns(df):
    return (df.fare_amount * df.tip_amount)

def value_counts(df):
    return df.fare_amount.value_counts()

def mean_of_complicated_arithmetic_operation(df):
    theta_1 = df.pickup_longitude
    phi_1 = df.pickup_latitude
    theta_2 = df.dropoff_longitude
    phi_2 = df.dropoff_latitude
    temp = (np.sin((theta_2 - theta_1) / 2 * np.pi / 180) ** 2
           + np.cos(theta_1 * np.pi / 180) * np.cos(theta_2 * np.pi / 180)
           * np.sin((phi_2 - phi_1) / 2 * np.pi / 180) ** 2)
    ret = 2 * np.arctan2(np.sqrt(temp), np.sqrt(1 - temp))
    return ret.mean()

def complicated_arithmetic_operation(df):
    theta_1 = df.pickup_longitude
    phi_1 = df.pickup_latitude
    theta_2 = df.dropoff_longitude
    phi_2 = df.dropoff_latitude
    temp = (np.sin((theta_2 - theta_1) / 2 * np.pi / 180) ** 2
           + np.cos(theta_1 * np.pi / 180) * np.cos(theta_2 * np.pi / 180)
           * np.sin((phi_2 - phi_1) / 2 * np.pi / 180) ** 2)
    ret = 2 * np.arctan2(np.sqrt(temp), np.sqrt(1 - temp))
    return ret

def groupby_statistics(df):
    return df.groupby(by='passenger_count').agg(
        {
            'fare_amount': ['mean', 'std'],
            'tip_amount': ['mean', 'std']
        }
    )

def join_count(df, other):
    return len(pd.merge(df, other, left_index=True, right_index=True))

def join_data(df, other):
    return pd.merge(df, other, left_index=True, right_index=True)