import pyspark.pandas as ps
from pyspark.sql import functions as F
import math
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
    theta_1 = col("Start_Lon") * math.pi / 180
    phi_1 = col("Start_Lat") * math.pi / 180
    theta_2 = col("End_Lon") * math.pi / 180
    phi_2 = col("End_Lat") * math.pi / 180
    dtheta = theta_2 - theta_1
    dphi = phi_2 - phi_1
    temp = (sin(dphi / 2) ** 2) + (cos(phi_1) * cos(phi_2) * (sin(dtheta / 2) ** 2))
    distance = lit(2) * atan2(sqrt(temp), sqrt(lit(1) - temp))
    return distance

def mean_of_complicated_arithmetic_operation(df):
    # Convert spark.pandas DataFrame to PySpark DataFrame
    spark_df = df.to_spark()
    theta_1 = F.col("Start_Lon") * math.pi / 180
    phi_1 = F.col("Start_Lat") * math.pi / 180
    theta_2 = F.col("End_Lon") * math.pi / 180
    phi_2 = F.col("End_Lat") * math.pi / 180
    dtheta = theta_2 - theta_1
    dphi = phi_2 - phi_1
    temp = (F.sin(dphi/2)**2 + F.cos(phi_1) * F.cos(phi_2) * F.sin(dtheta/2)**2)
    distance = F.lit(2) * F.atan2(F.sqrt(temp), F.sqrt(F.lit(1) - temp))
    spark_df_with_distance = spark_df.withColumn("distance", distance)
    mean_distance = spark_df_with_distance.agg(F.avg("distance")).collect()[0][0]
    return mean_distance

def groupby_statistics(df):
    grouped = df.groupby("Passenger_Count").agg({
        "Fare_Amt": ["mean", "std"],
        "Tip_Amt": ["mean", "std"]
    })
    # Reset index while staying in Koalas
    return grouped.reset_index()

def join_count(df, other):
    return len(df.merge(other.spark.hint("broadcast"), on="Passenger_Count"))

def join_data(df, other):
    return df.merge(other.spark.hint("broadcast"), on="Passenger_Count")
