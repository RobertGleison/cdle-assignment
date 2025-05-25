from benchmark.benchmarking_spark.spark_session import get_spark
from pyspark.sql.functions import (
    col, mean as _mean, stddev as _stddev,
    sin, cos, sqrt, atan2, lit, avg as _avg
)
import math

def read_file_parquet(df=None, **kwargs):
    file_path = kwargs.get("path")
    # fs = kwargs.get("filesystem")
    # if fs:
    #     with fs.open(file_path, 'rb') as gcp_path:
    #         return spark.read_parquet(gcp_path)
    spark = get_spark()
    return spark.read.parquet(file_path)

def count(df=None):
    return df.count()

def count_index_length(df=None):
    return df.count()

def mean(df):
    return df.select(_mean("fare_amount")).first()[0]

def standard_deviation(df):
    return df.select(_stddev("fare_amount")).first()[0]

def mean_of_sum(df):
    return df.select((col("fare_amount") + col("tip_amount")).alias("sum_amt")) \
             .agg(_mean("sum_amt")).first()[0]

def sum_columns(df):
    return df.withColumn("sum_amt", col("fare_amount") + col("tip_amount")).select("sum_amt")

def mean_of_product(df):
    return df.select((col("fare_amount") * col("tip_amount")).alias("product_amt")) \
             .agg(_mean("product_amt")).first()[0]

def product_columns(df):
    return df.withColumn("product_amt", col("fare_amount") * col("tip_amount")).select("product_amt")

def value_counts(df):
    return df.groupBy("fare_amount").count()

def complicated_arithmetic_operation(df):
    theta_1 = col("pickup_longitude") * math.pi / 180
    phi_1 = col("pickup_latitude") * math.pi / 180
    theta_2 = col("dropoff_longitude") * math.pi / 180
    phi_2 = col("dropoff_latitude") * math.pi / 180
    dtheta = theta_2 - theta_1
    dphi = phi_2 - phi_1
    temp = (sin(dphi / 2) ** 2) + (cos(phi_1) * cos(phi_2) * (sin(dtheta / 2) ** 2))
    distance = lit(2) * atan2(sqrt(temp), sqrt(lit(1) - temp))
    return distance

def mean_of_complicated_arithmetic_operation(df):
    theta_1 = col("pickup_longitude") * math.pi / 180
    phi_1 = col("pickup_latitude") * math.pi / 180
    theta_2 = col("dropoff_longitude") * math.pi / 180
    phi_2 = col("dropoff_latitude") * math.pi / 180
    dtheta = theta_2 - theta_1
    dphi = phi_2 - phi_1
    temp = (sin(dphi / 2) ** 2) + (cos(phi_1) * cos(phi_2) * (sin(dtheta / 2) ** 2))
    distance = lit(2) * atan2(sqrt(temp), sqrt(lit(1) - temp))
    distance_mean = df.agg(_avg(distance).alias("mean_distance")).collect()[0]["mean_distance"]
    return distance_mean

def groupby_statistics(df):
    return df.groupBy("passenger_count").agg(
        _mean("fare_amount"),
        _stddev("fare_amount"),
        _mean("tip_amount"),
        _stddev("tip_amount"),
    )

def join_count(df, other):
    joined = df.join(other, on="passenger_count")
    return joined.count()

def join_data(df, other):
    return df.join(other, on="passenger_count")