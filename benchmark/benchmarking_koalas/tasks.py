import pyspark.pandas as ps
from benchmarking_spark.spark_session import get_spark
from pyspark.sql import functions as F
import math
import numpy as np
from pyspark.sql.functions import (
    col, mean, stddev,sin, cos, sqrt, atan2, lit, avg, pi
)

def read_file_parquet(df=None, **kwargs):
    spark = get_spark()
    file_path = kwargs.get("path")
    spark_df = spark.read.parquet(file_path)
    return spark_df.pandas_api()

def count(df):
    return len(df)

def count_index_length(df):
    return len(df)

def mean(df):
    return df["fare_amount"].mean()

def standard_deviation(df):
    return df["fare_amount"].std()

def mean_of_sum(df):
    return (df["fare_amount"] + df["tip_amount"]).mean()

def sum_columns(df):
    return df["fare_amount"] + df["tip_amount"]

def mean_of_product(df):
    return (df["fare_amount"] * df["tip_amount"]).mean()

def product_columns(df):
    return df["fare_amount"] * df["tip_amount"]

def value_counts(df):
    return df["fare_amount"].value_counts()

def complicated_arithmetic_operation(df):
    theta_1 = df.pickup_longitude
    phi_1 = df.pickup_latitude
    theta_2 = df.dropoff_longitude
    phi_2 = df.dropoff_latitude
    temp = (np.sin((theta_2 - theta_1) / 2 * np.pi / 180) ** 2
           + np.cos(theta_1 * np.pi / 180) * np.cos(theta_2 * np.pi / 180) * np.sin((phi_2 - phi_1) / 2 * np.pi / 180) ** 2)
    ret = np.multiply(np.arctan2(np.sqrt(temp), np.sqrt(1-temp)),2)
    return ret

def mean_of_complicated_arithmetic_operation(df):
    theta_1 = df.pickup_longitude
    phi_1 = df.pickup_latitude
    theta_2 = df.dropoff_longitude
    phi_2 = df.dropoff_latitude
    temp = (np.sin((theta_2 - theta_1) / 2 * np.pi / 180) ** 2
           + np.cos(theta_1 * np.pi / 180) * np.cos(theta_2 * np.pi / 180) * np.sin((phi_2 - phi_1) / 2 * np.pi / 180) ** 2)
    ret = np.multiply(np.arctan2(np.sqrt(temp), np.sqrt(1-temp)),2)
    return ret.mean()

def groupby_statistics(df):
    gb = df.groupby(by='passenger_count').agg(
      {
        'fare_amount': ['mean', 'std'],
        'tip_amount': ['mean', 'std']
      }
    )
    return gb

def join_count(df, other):
    return len(df.merge(other.spark.hint("broadcast"), left_index=True, right_index=True))

def join_data(df, other):
    ret = df.merge(other.spark.hint("broadcast"), left_index=True, right_index=True)
    return ret
