import pyspark.pandas as ps
from math import pi
import numpy as np

def read_file_parquet_pandas(**kwargs):
    """Read parquet file using Spark pandas"""
    return ps.read_parquet(kwargs.get("path"))

def count_pandas(df):
    """Count rows using Spark pandas"""
    return len(df)

def count_index_length_pandas(df):
    """Count index length (same as count for DataFrame)"""
    return len(df)

def mean_pandas(df):
    """Calculate mean of Fare_Amt column"""
    return df["Fare_Amt"].mean()

def standard_deviation_pandas(df):
    """Calculate standard deviation of Fare_Amt column"""
    return df["Fare_Amt"].std()

def mean_of_sum_pandas(df):
    """Calculate mean of sum of Fare_Amt and Tip_Amt"""
    return (df["Fare_Amt"] + df["Tip_Amt"]).mean()

def sum_columns_pandas(df):
    """Return sum of Fare_Amt and Tip_Amt as a new Series"""
    return df["Fare_Amt"] + df["Tip_Amt"]

def mean_of_product_pandas(df):
    """Calculate mean of product of Fare_Amt and Tip_Amt"""
    return (df["Fare_Amt"] * df["Tip_Amt"]).mean()

def product_columns_pandas(df):
    """Return product of Fare_Amt and Tip_Amt as a new Series"""
    return df["Fare_Amt"] * df["Tip_Amt"]

def value_counts_pandas(df):
    """Count occurrences of each value in Fare_Amt column"""
    return df["Fare_Amt"].value_counts()

def optimized_complicated_arithmetic_operation_pandas(df):
    """Calculate haversine distance between start and end coordinates"""
    theta_1 = df["Start_Lon"] * pi / 180
    phi_1 = df["Start_Lat"] * pi / 180
    theta_2 = df["End_Lon"] * pi / 180
    phi_2 = df["End_Lat"] * pi / 180

    dtheta = theta_2 - theta_1
    dphi = phi_2 - phi_1

    temp = (np.sin(dphi / 2) ** 2) + (np.cos(phi_1) * np.cos(phi_2) * (np.sin(dtheta / 2) ** 2))
    distance = 2 * np.arctan2(np.sqrt(temp), np.sqrt(1 - temp))
    return distance

def mean_of_complicated_arithmetic_operation_pandas(df):
    """Calculate mean haversine distance"""
    distance = optimized_complicated_arithmetic_operation_pandas(df)
    return distance.mean()

def groupby_statistics_pandas(df):
    """Calculate statistics grouped by Passenger_Count"""
    grouped = df.groupby("Passenger_Count")
    return grouped.agg({
        "Fare_Amt": ["mean", "std"],
        "Tip_Amt": ["mean", "std"]
    })

def join_count_pandas(df, other):
    """Join and count rows using Spark pandas"""
    joined = df.merge(other, on="Passenger_Count")
    return len(joined)

def join_data_pandas(df, other):
    """Join dataframes on Passenger_Count"""
    return df.merge(other, on="Passenger_Count")