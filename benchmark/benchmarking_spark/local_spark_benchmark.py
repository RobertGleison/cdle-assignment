from benchmark.benchmark_setup import benchmark
from benchmark.benchmarking_spark.spark_session import get_spark
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import col
from benchmark.benchmarking_spark.tasks import (
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

class LocalSparkBenchmark:
    def __init__(self, filesystem=None, profile = False):
        self.filesystem = filesystem
        self.client = get_spark()
        self.profile = profile

    def run_benchmark(self, file_path: str) -> None:
        gcs_path = f"gs://{file_path}" if self.filesystem else file_path
        spark_data = self.client.read.parquet(gcs_path)
        spark_data = spark_data.withColumn("index", monotonically_increasing_id())

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


        spark_benchmarks = {
            'duration': [],
            'task': [],
        }

        # Normal local running
        spark_benchmarks = self.run_common_benchmarks(spark_data, 'local', spark_benchmarks, gcs_path)

        # Filtered local running
        filtered_data = spark_data.filter((col("tip_amount") >= 1) & (col("tip_amount") <= 5))
        spark_benchmarks = self.run_common_benchmarks(filtered_data, 'local filtered', spark_benchmarks, gcs_path)

        # Filtered with cache running
        filtered_data.cache()
        print(f'Enforce caching: {filtered_data.count()} rows of filtered data')
        spark_benchmarks = self.run_common_benchmarks(filtered_data, 'local filtered cache', spark_benchmarks, gcs_path)
        return spark_benchmarks


    def run_common_benchmarks(self, data, name_prefix: str, spark_benchmarks: dict, file_path: str) -> dict:
        benchmark(read_file_parquet, df=None, benchmarks=spark_benchmarks, name=f'{name_prefix} read file', path=file_path, filesystem=self.filesystem, profile=self.profile, tool="spark")
        benchmark(count, df=data, benchmarks=spark_benchmarks, name=f'{name_prefix} count', profile=self.profile, tool="spark")
        benchmark(count_index_length, df=data, benchmarks=spark_benchmarks, name=f'{name_prefix} count index length', profile=self.profile, tool="spark")
        benchmark(mean, df=data, benchmarks=spark_benchmarks, name=f'{name_prefix} mean', profile=self.profile, tool="spark")
        benchmark(standard_deviation, df=data, benchmarks=spark_benchmarks, name=f'{name_prefix} standard deviation', profile=self.profile, tool="spark")
        benchmark(mean_of_sum, df=data, benchmarks=spark_benchmarks, name=f'{name_prefix} mean of columns addition', profile=self.profile, tool="spark")
        benchmark(sum_columns, df=data, benchmarks=spark_benchmarks, name=f'{name_prefix} addition of columns', profile=self.profile, tool="spark")
        benchmark(mean_of_product, df=data, benchmarks=spark_benchmarks, name=f'{name_prefix} mean of columns multiplication', profile=self.profile, tool="spark")
        benchmark(product_columns, df=data, benchmarks=spark_benchmarks, name=f'{name_prefix} multiplication of columns', profile=self.profile, tool="spark")
        benchmark(value_counts, df=data, benchmarks=spark_benchmarks, name=f'{name_prefix} value counts', profile=self.profile, tool="spark")
        benchmark(complicated_arithmetic_operation, df=data, benchmarks=spark_benchmarks, name=f'{name_prefix} complex arithmetic ops', profile=self.profile, tool="spark")
        benchmark(mean_of_complicated_arithmetic_operation, df=data, benchmarks=spark_benchmarks, name=f'{name_prefix} mean of complex arithmetic ops', profile=self.profile, tool="spark")
        benchmark(groupby_statistics, df=data, benchmarks=spark_benchmarks, name=f'{name_prefix} groupby statistics', profile=self.profile, tool="spark")

        # For join, convert groupby result to Spark DataFrame
        other_df = groupby_statistics(data)
        # other_df = other_df.toDF(*[f"{c}" for c in other_df.columns])  # ensure flat column names
        flattened_cols = [f"{c[0]}_{c[1]}" if isinstance(c, tuple) else c for c in other_df.columns]
        other_df = other_df.toDF(*flattened_cols)
        other_df = other_df.withColumn("index", monotonically_increasing_id())
        benchmark(join_count, data, benchmarks=spark_benchmarks, name=f'{name_prefix} join count', other=other_df, profile=self.profile, tool="spark")
        benchmark(join_data, data, benchmarks=spark_benchmarks, name=f'{name_prefix} join', other=other_df, profile=self.profile, tool="spark")

        return spark_benchmarks
