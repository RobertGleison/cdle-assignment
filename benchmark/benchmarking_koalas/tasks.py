import pyspark.pandas as ps
from math import pi
import numpy as np
from pyspark.sql.functions import (
    col, mean, stddev,sin, cos, sqrt, atan2, lit, avg, pi
)

def read_file_parquet(df=None, **kwargs):
    return ps.read_parquet(kwargs.get("path"))

def count(df):
    return len(df)

def count_index_length(df):
    return len(df)

def mean(df):
    return df["Fare_Amt"].mean()

def standard_deviation(df):
    return df["Fare_Amt"].std()

def mean_of_sum(df):
    return (df["Fare_Amt"] + df["Tip_Amt"]).mean()

def sum_columns(df):
    return df["Fare_Amt"] + df["Tip_Amt"]

def mean_of_product(df):
    return (df["Fare_Amt"] * df["Tip_Amt"]).mean()

def product_columns(df):
    return df["Fare_Amt"] * df["Tip_Amt"]

def value_counts(df):
    return df["Fare_Amt"].value_counts()

def complicated_arithmetic_operation(df):
    # Process in smaller chunks if DataFrame is large
    if len(df) > 1000000:  # Adjust threshold as needed
        return ps.concat([complicated_arithmetic_operation(df.iloc[i:i+100000])
                      for i in range(0, len(df), 100000)])

    lat1 = df["Start_Lat"].to_numpy() * np.pi/180
    lon1 = df["Start_Lon"].to_numpy() * np.pi/180
    lat2 = df["End_Lat"].to_numpy() * np.pi/180
    lon2 = df["End_Lon"].to_numpy() * np.pi/180
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    return 2 * np.arctan2(np.sqrt(a), np.sqrt(1-a))

def mean_of_complicated_arithmetic_operation(df):
    lat1 = df["Start_Lat"] * np.pi/180
    lon1 = df["Start_Lon"] * np.pi/180
    lat2 = df["End_Lat"] * np.pi/180
    lon2 = df["End_Lon"] * np.pi/180

    temp = (
        np.sin((lat2 - lat1)/2)**2 +
        np.cos(lat1) * np.cos(lat2) * np.sin((lon2 - lon1)/2)**2
    )
    return (2 * np.arctan2(np.sqrt(temp), np.sqrt(1 - temp))).mean()


def groupby_statistics(df):
    grouped = df.groupby("Passenger_Count")
    return grouped.agg({
        "Fare_Amt": ["mean", "std"],
        "Tip_Amt": ["mean", "std"]
    })

def join_count(df, other):
    joined = df.merge(other, on="Passenger_Count")
    return len(joined)

def join_data(df, other):
    return df.merge(other, on="Passenger_Count")