from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean as _mean, stddev as _stddev, count as _count, expr
from pyspark.sql.types import DoubleType
import numpy as np

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
    df = df.withColumn("theta_1", col("Start_Lon").cast(DoubleType()))
    df = df.withColumn("phi_1", col("Start_Lat").cast(DoubleType()))
    df = df.withColumn("theta_2", col("End_Lon").cast(DoubleType()))
    df = df.withColumn("phi_2", col("End_Lat").cast(DoubleType()))

    expr_str = """
        2 * atan2(
            sqrt(
                sin(radians((theta_2 - theta_1) / 2)) * sin(radians((theta_2 - theta_1) / 2)) +
                cos(radians(theta_1)) * cos(radians(theta_2)) *
                sin(radians((phi_2 - phi_1) / 2)) * sin(radians((phi_2 - phi_1) / 2))
            ),
            sqrt(
                1 - (
                    sin(radians((theta_2 - theta_1) / 2)) * sin(radians((theta_2 - theta_1) / 2)) +
                    cos(radians(theta_1)) * cos(radians(theta_2)) *
                    sin(radians((phi_2 - phi_1) / 2)) * sin(radians((phi_2 - phi_1) / 2))
                )
            )
        )
    """

    return df.withColumn("distance", expr(expr_str)).select("distance")

def mean_of_complicated_arithmetic_operation(df):
    distance_df = complicated_arithmetic_operation(df)
    return distance_df.agg(_mean("distance")).first()[0]

def groupby_statistics(df):
    return df.groupBy("passenger_count").agg(
        _mean("Fare_Amt").alias("Fare_Amt_mean"),
        _stddev("Fare_Amt").alias("Fare_Amt_std"),
        _mean("Tip_Amt").alias("Tip_Amt_mean"),
        _stddev("Tip_Amt").alias("Tip_Amt_std"),
    )

def join_count(df, other):
    joined = df.join(other.hint("broadcast"), on="id")  # Replace "id" with appropriate join key
    return joined.count()

def join_data(df, other):
    return df.join(other.hint("broadcast"), on="id")  # Replace "id" with appropriate join key
