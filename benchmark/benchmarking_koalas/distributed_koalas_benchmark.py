from benchmark_setup import benchmark
from pyspark.sql import SparkSession
import pyspark.pandas as ks
import pandas as pd
import numpy as np
from benchmark.koalas.tasks import (
    mean_of_complicated_arithmetic_operation,
    complicated_arithmetic_operation,
    count_index_length,
    groupby_statistics,
    standard_deviation,
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


class DistributedKoalasBenchmark:
    def __init__(self, file_path, filesystem=None):
        self.filesystem = filesystem
        self.benchmarks_results = self.run_benchmark(file_path)
        self.client = SparkSession.builder.getOrCreate()


    def run_benchmark(self, file_path: str) -> None:
        if self.filesystem:
            with self.filesystem.open(file_path, 'rb') as gcp_path:
                koalas_data = ks.read_parquet(gcp_path, index_col=None)
        else: koalas_data = ks.read_parquet(file_path, index_col=None)


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
                koalas_data = koalas_data.withColumnRenamed(old_col, new_col)

        client = self.client

        koalas_benchmarks = {
            'duration': [],
            'task': [],
        }

        # Normal distributed running
        koalas_benchmarks = self.run_common_benchmarks(koalas_data, 'koalas distributed', koalas_benchmarks, file_path)

        # Filtered distributed running
        expr_filter = (koalas_data.tip_amount >= 1) & (koalas_data.tip_amount <= 5)
        filtered_koalas_data = koalas_data[expr_filter]
        koalas_benchmarks = self.run_common_benchmarks(filtered_koalas_data, 'koalas distributed filtered', koalas_benchmarks, file_path)

        # Filtered with cache runnning
        filtered_koalas_data = filtered_koalas_data.spark.cache()
        print(f'Enforce caching: {len(filtered_koalas_data)} rows of filtered data')
        koalas_benchmarks = self.run_common_benchmarks(filtered_koalas_data, 'koalas distributed filtered cache', koalas_benchmarks, file_path)

        self.benchmarks_results = koalas_benchmarks


    def run_common_benchmarks(self, data: ks.DataFrame, name_prefix: str, koalas_benchmarks: dict, file_path: str) -> dict:
        benchmark(read_file_parquet, df=None, benchmarks=koalas_benchmarks, name=f'{name_prefix} read file', path=file_path)
        benchmark(count, df=data, benchmarks=koalas_benchmarks, name=f'{name_prefix} count')
        benchmark(count_index_length, df=data, benchmarks=koalas_benchmarks, name=f'{name_prefix} count index length')
        benchmark(mean, df=data, benchmarks=koalas_benchmarks, name=f'{name_prefix} mean')
        benchmark(standard_deviation, df=data, benchmarks=koalas_benchmarks, name=f'{name_prefix} standard deviation')
        benchmark(mean_of_sum, df=data, benchmarks=koalas_benchmarks, name=f'{name_prefix} mean of columns addition')
        benchmark(sum_columns, df=data, benchmarks=koalas_benchmarks, name=f'{name_prefix} addition of columns')
        benchmark(mean_of_product, df=data, benchmarks=koalas_benchmarks, name=f'{name_prefix} mean of columns multiplication')
        benchmark(product_columns, df=data, benchmarks=koalas_benchmarks, name=f'{name_prefix} multiplication of columns')
        benchmark(value_counts, df=data, benchmarks=koalas_benchmarks, name=f'{name_prefix} value counts')
        benchmark(mean_of_complicated_arithmetic_operation, df=data, benchmarks=koalas_benchmarks, name=f'{name_prefix} mean of complex arithmetic ops')
        benchmark(complicated_arithmetic_operation, df=data, benchmarks=koalas_benchmarks, name=f'{name_prefix} complex arithmetic ops')
        benchmark(groupby_statistics, df=data, benchmarks=koalas_benchmarks, name=f'{name_prefix} groupby statistics')

        other = groupby_statistics(data)
        other.columns = pd.Index([e[0]+'_' + e[1] for e in other.columns.tolist()])
        benchmark(join_count, data, benchmarks=koalas_benchmarks, name=f'{name_prefix} join count', other=other)
        benchmark(join_data, data, benchmarks=koalas_benchmarks, name=f'{name_prefix} join', other=other)

        return koalas_benchmarks
