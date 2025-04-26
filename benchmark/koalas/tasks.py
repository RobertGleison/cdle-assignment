import pandas as pd
import numpy as np
import pyspark.pandas as ks


def read_file_parquet(df=None):
    return ks.read_parquet('/FileStore/ks_taxi_parquet', index_col='index')

def count(df=None):
    return len(df)

def count_index_length(df=None):
    return len(df.index)

def mean(df):
    return df.fare_amt.mean()

def standard_deviation(df):
    return df.fare_amt.std()

def mean_of_sum(df):
    return (df.fare_amt + df.tip_amt).mean()

def sum_columns(df):
    x = df.fare_amt + df.tip_amt
    x.to_pandas()
    return x

def mean_of_product(df):
    return (df.fare_amt * df.tip_amt).mean()

def product_columns(df):
    x = df.fare_amt * df.tip_amt
    x.to_pandas()
    return x

def value_counts(df):
    val_counts = df.fare_amt.value_counts()
    val_counts.to_pandas()
    return val_counts

def complicated_arithmetic_operation(df):
    theta_1 = df.start_lon
    phi_1 = df.start_lat
    theta_2 = df.end_lon
    phi_2 = df.end_lat
    temp = (np.sin((theta_2 - theta_1) / 2 * np.pi / 180) ** 2
           + np.cos(theta_1 * np.pi / 180) * np.cos(theta_2 * np.pi / 180) * np.sin((phi_2 - phi_1) / 2 * np.pi / 180) ** 2)
    ret = np.multiply(np.arctan2(np.sqrt(temp), np.sqrt(1-temp)),2)
    ret.to_pandas()
    return ret

def mean_of_complicated_arithmetic_operation(df):
    theta_1 = df.start_lon
    phi_1 = df.start_lat
    theta_2 = df.end_lon
    phi_2 = df.end_lat
    temp = (np.sin((theta_2 - theta_1) / 2 * np.pi / 180) ** 2
           + np.cos(theta_1 * np.pi / 180) * np.cos(theta_2 * np.pi / 180) * np.sin((phi_2 - phi_1) / 2 * np.pi / 180) ** 2)
    ret = np.multiply(np.arctan2(np.sqrt(temp), np.sqrt(1-temp)),2)
    return ret.mean()

def groupby_statistics(df):
    gb = df.groupby(by='passenger_count').agg(
      {
        'fare_amt': ['mean', 'std'],
        'tip_amt': ['mean', 'std']
      }
    )
    gb.to_pandas()
    return gb

def join_count(df, other):
    return len(df.merge(other.spark.hint("broadcast"), left_index=True, right_index=True))

def join_data(df, other):
    ret = df.merge(other.spark.hint("broadcast"), left_index=True, right_index=True)
    ret.to_pandas()
    return ret