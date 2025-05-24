from benchmark_setup import benchmark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from benchmark.benchmarking_spark.tasks import (  # assuming you renamed your task file from `koalas` to `spark`
    mean_of_complicated_arithmetic_operation,
    complicated_arithmetic_operation,
    groupby_statistics,
    standard_deviation,
    count_index_length,
    read_file_parquet,
    mean_of_product,
    product_columns,
    value_counts,
    mean_of_sum,
    sum_columns,
    join_count,
    join_data,
    count,
    mean,
)

#test
class DistributedSparkBenchmark:
    def __init__(self, file_path, filesystem=None):
        self.filesystem = filesystem
        self.client = (
            SparkSession.builder
            .appName("DistributedSparkBenchmark")
            .master("spark://localhost:7077") # ou .master("yarn")
            .config("spark.executor.memory", "4g")
            .config("spark.executor.instances", "4")
            .getOrCreate()
        )
        self.benchmarks_results = self.run_benchmark(file_path)

    def run_benchmark(self, file_path: str) -> None:
        if self.fs:
            with self.fs.open(file_path, 'rb') as gcp_path:
                spark_data = self.client.read.parquet(gcp_path)
        else: spark_data = self.client.read.parquet(file_path)

        if "2009" in file_path:
            rename_map = {
                'Start_Lon': 'pickup_longitude',
                'Start_Lat': 'pickup_latitude',
                'End_Lon': 'dropoff_longitude',
                'End_Lat': 'dropoff_latitude',
                'Passenger_Count': 'passenger_count',
                'Tip_Amt': 'tip_amount',
                'Fare_Amt': 'fare_amount',
            }

            for old_col, new_col in rename_map.items():
                spark_data = spark_data.withColumnRenamed(old_col, new_col)

        client = self.client

        spark_benchmarks = {
            'duration': [],
            'task': [],
        }

        # Normal local running
        spark_benchmarks = self.run_common_benchmarks(spark_data, 'spark local', spark_benchmarks, file_path)

        # Filtered local running
        filtered_data = spark_data.filter((col("tip_amount") >= 1) & (col("tip_amount") <= 5))
        spark_benchmarks = self.run_common_benchmarks(filtered_data, 'spark local filtered', spark_benchmarks, file_path)

        # Filtered with cache running
        filtered_data.cache()
        print(f'Enforce caching: {filtered_data.count()} rows of filtered data')
        spark_benchmarks = self.run_common_benchmarks(filtered_data, 'spark local filtered cache', spark_benchmarks, file_path)
        self.client.stop()
        return spark_benchmarks


    def run_common_benchmarks(self, data, name_prefix: str, spark_benchmarks: dict, file_path: str) -> dict:
        benchmark(read_file_parquet, df=None, benchmarks=spark_benchmarks, name=f'{name_prefix} read file', path=file_path)
        benchmark(count, df=data, benchmarks=spark_benchmarks, name=f'{name_prefix} count')
        benchmark(count_index_length, df=data, benchmarks=spark_benchmarks, name=f'{name_prefix} count index length')
        benchmark(mean, df=data, benchmarks=spark_benchmarks, name=f'{name_prefix} mean')
        benchmark(standard_deviation, df=data, benchmarks=spark_benchmarks, name=f'{name_prefix} standard deviation')
        benchmark(mean_of_sum, df=data, benchmarks=spark_benchmarks, name=f'{name_prefix} mean of columns addition')
        benchmark(sum_columns, df=data, benchmarks=spark_benchmarks, name=f'{name_prefix} addition of columns')
        benchmark(mean_of_product, df=data, benchmarks=spark_benchmarks, name=f'{name_prefix} mean of columns multiplication')
        benchmark(product_columns, df=data, benchmarks=spark_benchmarks, name=f'{name_prefix} multiplication of columns')
        benchmark(value_counts, df=data, benchmarks=spark_benchmarks, name=f'{name_prefix} value counts')
        benchmark(mean_of_complicated_arithmetic_operation, df=data, benchmarks=spark_benchmarks, name=f'{name_prefix} mean of complex arithmetic ops')
        benchmark(complicated_arithmetic_operation, df=data, benchmarks=spark_benchmarks, name=f'{name_prefix} complex arithmetic ops')
        benchmark(groupby_statistics, df=data, benchmarks=spark_benchmarks, name=f'{name_prefix} groupby statistics')

        # For join, convert groupby result to Spark DataFrame
        other_df = groupby_statistics(data)
        other_df = other_df.toDF(*[f"{c}" for c in other_df.columns])  # ensure flat column names
        benchmark(join_count, data, benchmarks=spark_benchmarks, name=f'{name_prefix} join count', other=other_df)
        benchmark(join_data, data, benchmarks=spark_benchmarks, name=f'{name_prefix} join', other=other_df)

        return spark_benchmarks
