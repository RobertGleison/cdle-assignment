import pandas as pd
import numpy as np
import pyspark.pandas as ks


def read_file_parquet(df=None):
    return ks.read_parquet("/home/robert/Desktop/cdle-assignment/datasets/taxis_2009-01.parquet")

def count(df=None):
    return len(df)

def count_index_length(df=None):
    return len(df.index)

def mean(df):
    return df.Fare_Amt.mean()

def standard_deviation(df):
    return df.Fare_Amt.std()

def mean_of_sum(df):
    return (df.Fare_Amt + df.Tip_Amt).mean()

def sum_columns(df):
    x = df.Fare_Amt + df.Tip_Amt
    x.to_pandas()
    return x

def mean_of_product(df):
    return (df.Fare_Amt * df.Tip_Amt).mean()

def product_columns(df):
    x = df.Fare_Amt * df.Tip_Amt
    x.to_pandas()
    return x

def value_counts(df):
    val_counts = df.Fare_Amt.value_counts()
    val_counts.to_pandas()
    return val_counts

def complicated_arithmetic_operation(df):
    theta_1 = df.Start_Lon
    phi_1 = df.Start_Lat
    theta_2 = df.End_Lon
    phi_2 = df.End_Lat
    temp = (np.sin((theta_2 - theta_1) / 2 * np.pi / 180) ** 2
           + np.cos(theta_1 * np.pi / 180) * np.cos(theta_2 * np.pi / 180) * np.sin((phi_2 - phi_1) / 2 * np.pi / 180) ** 2)
    ret = np.multiply(np.arctan2(np.sqrt(temp), np.sqrt(1-temp)),2)
    ret.to_pandas()
    return ret

def mean_of_complicated_arithmetic_operation(df):
    theta_1 = df.Start_Lon
    phi_1 = df.Start_Lat
    theta_2 = df.End_Lon
    phi_2 = df.End_Lat
    temp = (np.sin((theta_2 - theta_1) / 2 * np.pi / 180) ** 2
           + np.cos(theta_1 * np.pi / 180) * np.cos(theta_2 * np.pi / 180) * np.sin((phi_2 - phi_1) / 2 * np.pi / 180) ** 2)
    ret = np.multiply(np.arctan2(np.sqrt(temp), np.sqrt(1-temp)),2)
    return ret.mean()

def groupby_statistics(df):
    gb = df.groupby(by='passenger_count').agg(
      {
        'Fare_Amt': ['mean', 'std'],
        'Tip_Amt': ['mean', 'std']
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