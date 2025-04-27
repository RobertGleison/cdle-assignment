from benchmark_setup import benchmark
from pyspark.sql import SparkSession
import pyspark.pandas as ks
import pandas as pd
import numpy as np
from benchmark.koalas.tasks import (
    read_file_parquet,
    count,
    count_index_length,
    mean,
    standard_deviation,
    mean_of_sum,
    sum_columns,
    mean_of_product,
    product_columns,
    value_counts,
    mean_of_complicated_arithmetic_operation,
    complicated_arithmetic_operation,
    groupby_statistics,
    join_count,
    join_data,
)


class Benchmark:
    def __init__(self):
        self.benchmarks_results = None

    def run_benchmark(self, file_path):
        session = SparkSession.builder.getOrCreate()
        koalas_data = ks.read_parquet(file_path)
        koalas_benchmarks = {
            'duration': [],
            'task': [],
        }

        benchmark(read_file_parquet, df=None, benchmarks=koalas_benchmarks, name='koalas local read file', path = file_path)
        benchmark(count, df=koalas_data, benchmarks=koalas_benchmarks, name='koalas local count')
        benchmark(count_index_length, df=koalas_data, benchmarks=koalas_benchmarks, name='koalas local count index length')
        benchmark(mean, df=koalas_data, benchmarks=koalas_benchmarks, name='koalas local mean')
        benchmark(standard_deviation, df=koalas_data, benchmarks=koalas_benchmarks, name='koalas local standard deviation')
        benchmark(mean_of_sum, df=koalas_data, benchmarks=koalas_benchmarks, name='koalas local mean of columns addition')
        benchmark(sum_columns, df=koalas_data, benchmarks=koalas_benchmarks, name='koalas local addition of columns')
        benchmark(mean_of_product, df=koalas_data, benchmarks=koalas_benchmarks, name='koalas local mean of columns multiplication')
        benchmark(product_columns, df=koalas_data, benchmarks=koalas_benchmarks, name='koalas local multiplication of columns')
        benchmark(value_counts, df=koalas_data, benchmarks=koalas_benchmarks, name='koalas local value counts')
        benchmark(mean_of_complicated_arithmetic_operation, df=koalas_data, benchmarks=koalas_benchmarks, name='koalas local mean of complex arithmetic oks')
        benchmark(complicated_arithmetic_operation, df=koalas_data, benchmarks=koalas_benchmarks, name='koalas local complex arithmetic oks')
        benchmark(groupby_statistics, df=koalas_data, benchmarks=koalas_benchmarks, name='koalas local groupby statistics')

        other = groupby_statistics(koalas_data)
        other.columns = pd.Index([e[0]+'_' + e[1] for e in other.columns.tolist()])
        benchmark(join_count, koalas_data, benchmarks=koalas_benchmarks, name='koalas local join count', other=other)
        benchmark(join_data, koalas_data, benchmarks=koalas_benchmarks, name='koalas local join', other=other)

        self.benchmarks_results = koalas_benchmarks
        session.stop()

