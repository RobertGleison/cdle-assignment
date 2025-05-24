import dask.dataframe as dd
import numpy as np

def read_file_parquet(df=None, **kwargs):
    fs = kwargs.get("filesystem")
    file_path = kwargs.get("path")
    if fs:
        with fs.open(file_path, 'rb') as gcp_path:
            return dd.read_parquet(gcp_path)
    else: return dd.read_parquet(file_path)

def count(df=None):
    return len(df)

def count_index_length(df=None):
    return len(df.index)

def mean(df):
    return df.fare_amount.mean().compute()

def standard_deviation(df):
    return df.fare_amount.std().compute()

def mean_of_sum(df):
    return (df.fare_amount + df.tip_amount).mean().compute()

def sum_columns(df):
    return (df.fare_amount + df.tip_amount).compute()

def mean_of_product(df):
    return (df.fare_amount * df.tip_amount).mean().compute()

def product_columns(df):
    return (df.fare_amount * df.tip_amount).compute()

def value_counts(df):
    return df.fare_amount.value_counts().compute()

def complicated_arithmetic_operation(df):
    theta_1 = df.pickup_longitude * np.pi / 180
    phi_1 = df.pickup_latitude * np.pi / 180
    theta_2 = df.dropoff_longitude * np.pi / 180
    phi_2 = df.dropoff_latitude * np.pi / 180
    dtheta = theta_2 - theta_1
    dphi = phi_2 - phi_1
    temp = (np.sin(dphi / 2) ** 2 + np.cos(phi_1) * np.cos(phi_2) * np.sin(dtheta / 2) ** 2)
    ret = 2 * np.arctan2(np.sqrt(temp), np.sqrt(1 - temp))
    return ret.compute()

def mean_of_complicated_arithmetic_operation(df):
    theta_1 = df.pickup_longitude * np.pi / 180
    phi_1 = df.pickup_latitude * np.pi / 180
    theta_2 = df.dropoff_longitude * np.pi / 180
    phi_2 = df.dropoff_latitude * np.pi / 180
    dtheta = theta_2 - theta_1
    dphi = phi_2 - phi_1
    temp = (np.sin(dphi / 2) ** 2 + np.cos(phi_1) * np.cos(phi_2) * np.sin(dtheta / 2) ** 2)
    ret = 2 * np.arctan2(np.sqrt(temp), np.sqrt(1 - temp))
    return ret.mean().compute()

def groupby_statistics(df):
    return df.groupby(by='passenger_count').agg(
      {
        'fare_amount': ['mean', 'std'],
        'tip_amount': ['mean', 'std']
      }
    ).compute()

def join_count(df, other):
    return len(dd.merge(df, other, left_index=True, right_index=True))

def join_data(df, other):
    return dd.merge(df, other, left_index=True, right_index=True).compute()