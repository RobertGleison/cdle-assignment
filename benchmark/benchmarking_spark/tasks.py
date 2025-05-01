from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, mean as _mean, stddev as _stddev,
    sin, cos, sqrt, atan2, lit, avg as _avg
)
import math

spark = SparkSession.builder.getOrCreate()

def read_file_parquet(df=None, **kwargs):
    return spark.read.parquet(kwargs.get("path"))

def count(df=None):
    return df.count()

def count_index_length(df=None):
    return df.count()

def mean(df):
    return df.select(_mean("Fare_Amt")).first()[0]

def standard_deviation(df):
    return df.select(_stddev("Fare_Amt")).first()[0]

def mean_of_sum(df):
    return df.select((col("Fare_Amt") + col("Tip_Amt")).alias("sum_amt")) \
             .agg(_mean("sum_amt")).first()[0]

def sum_columns(df):
    return df.withColumn("sum_amt", col("Fare_Amt") + col("Tip_Amt")).select("sum_amt")

def mean_of_product(df):
    return df.select((col("Fare_Amt") * col("Tip_Amt")).alias("product_amt")) \
             .agg(_mean("product_amt")).first()[0]

def product_columns(df):
    return df.withColumn("product_amt", col("Fare_Amt") * col("Tip_Amt")).select("product_amt")

def value_counts(df):
    return df.groupBy("Fare_Amt").count()

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
    theta_1 = col("Start_Lon") * math.pi / 180
    phi_1 = col("Start_Lat") * math.pi / 180
    theta_2 = col("End_Lon") * math.pi / 180
    phi_2 = col("End_Lat") * math.pi / 180
    dtheta = theta_2 - theta_1
    dphi = phi_2 - phi_1
    temp = (sin(dphi / 2) ** 2) + (cos(phi_1) * cos(phi_2) * (sin(dtheta / 2) ** 2))
    distance = lit(2) * atan2(sqrt(temp), sqrt(lit(1) - temp))
    distance_mean = df.agg(_avg(distance).alias("mean_distance")).collect()[0]["mean_distance"]
    return distance_mean

def groupby_statistics(df):
    return df.groupBy("Passenger_Count").agg(
        _mean("Fare_Amt"),
        _stddev("Fare_Amt"),
        _mean("Tip_Amt"),
        _stddev("Tip_Amt"),
    )

def join_count(df, other):
    joined = df.join(other, on="Passenger_Count")
    return joined.count()

def join_data(df, other):
    return df.join(other, on="Passenger_Count")