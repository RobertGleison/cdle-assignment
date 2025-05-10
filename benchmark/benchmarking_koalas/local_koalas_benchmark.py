from benchmark.benchmark_setup import benchmark
from pyspark.sql import SparkSession
import pyspark.pandas as ks
import pandas as pd
import numpy as np
from benchmark.benchmarking_koalas.tasks import (
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


class LocalKoalasBenchmark:
    def __init__(self, file_path):
        self.client = SparkSession.builder.master("local[1]").config("spark.executor.memory", "10g").config("spark.driver.memory", "10g").config("spark.executor.instances", "1").config("spark.task.cpus", "1").getOrCreate()
        self.benchmarks_results = self.run_benchmark(file_path)


    def run_benchmark(self, file_path: str) -> None:
        koalas_data = ks.read_parquet(file_path, index_col=None)

        koalas_benchmarks = {
            'duration': [],
            'task': [],
        }

        # Normal local running
        koalas_benchmarks = self.run_common_benchmarks(koalas_data, 'koalas local', koalas_benchmarks, file_path)

        # Filtered local running
        expr_filter = (koalas_data.Tip_Amt >= 1) & (koalas_data.Tip_Amt <= 5)
        filtered_koalas_data = koalas_data[expr_filter]
        koalas_benchmarks = self.run_common_benchmarks(filtered_koalas_data, 'koalas local filtered', koalas_benchmarks, file_path)

        # Filtered with cache runnning
        filtered_koalas_data = filtered_koalas_data.spark.cache()
        print(f'Enforce caching: {len(filtered_koalas_data)} rows of filtered data')
        koalas_benchmarks = self.run_common_benchmarks(filtered_koalas_data, 'koalas local filtered cache', koalas_benchmarks, file_path)
        return koalas_benchmarks



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
        benchmark(complicated_arithmetic_operation, df=data, benchmarks=koalas_benchmarks, name=f'{name_prefix} complex arithmetic ops')
        benchmark(mean_of_complicated_arithmetic_operation, df=data, benchmarks=koalas_benchmarks, name=f'{name_prefix} mean of complex arithmetic ops')
        benchmark(groupby_statistics, df=data, benchmarks=koalas_benchmarks, name=f'{name_prefix} groupby statistics')

        other = groupby_statistics(data)
        other.columns = [
            col[0] if col[1] == '' else f"{col[0]}_{col[1]}"
            for col in other.columns
        ]
        other = other.rename(columns={"Passenger_Count_": "Passenger_Count"})

        benchmark(join_count, data, benchmarks=koalas_benchmarks, name=f'{name_prefix} join count', other=other)
        benchmark(join_data, data, benchmarks=koalas_benchmarks, name=f'{name_prefix} join', other=other)

        return koalas_benchmarks
