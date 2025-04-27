from benchmark_setup import benchmark
import pandas as pd
import numpy as np
from benchmark.benchmarking_joblib.tasks import (
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
        joblib_data = read_file_parquet("/home/robert/Desktop/cdle-assignment/datasets/taxis_2009-01.parquet")
        joblib_data = joblib_data.compute()

        joblib_benchmarks = {
            'duration': [],  # in seconds
            'task': [],
        }

        benchmark(read_file_parquet, df=None, benchmarks=joblib_benchmarks, name='read file')
        benchmark(count, df=joblib_data, benchmarks=joblib_benchmarks, name='count')
        benchmark(count_index_length, df=joblib_data, benchmarks=joblib_benchmarks, name='count index length')
        benchmark(mean, df=joblib_data, benchmarks=joblib_benchmarks, name='mean')
        benchmark(standard_deviation, df=joblib_data, benchmarks=joblib_benchmarks, name='standard deviation')
        benchmark(mean_of_sum, df=joblib_data, benchmarks=joblib_benchmarks, name='mean of columns addition')
        benchmark(sum_columns, df=joblib_data, benchmarks=joblib_benchmarks, name='addition of columns')
        benchmark(mean_of_product, df=joblib_data, benchmarks=joblib_benchmarks, name='mean of columns multiplication')
        benchmark(product_columns, df=joblib_data, benchmarks=joblib_benchmarks, name='multiplication of columns')
        benchmark(value_counts, df=joblib_data, benchmarks=joblib_benchmarks, name='value counts')
        benchmark(mean_of_complicated_arithmetic_operation, df=joblib_data, benchmarks=joblib_benchmarks, name='mean of complex arithmetic ops')
        benchmark(complicated_arithmetic_operation, df=joblib_data, benchmarks=joblib_benchmarks, name='complex arithmetic ops')
        benchmark(groupby_statistics, df=joblib_data, benchmarks=joblib_benchmarks, name='groupby statistics')

        other = groupby_statistics(joblib_data)
        other.columns = pd.Index([e[0]+'_' + e[1] for e in other.columns.tolist()])
        benchmark(join_count, joblib_data, benchmarks=joblib_benchmarks, name='join count', other=other)
        benchmark(join_data, joblib_data, benchmarks=joblib_benchmarks, name='join', other=other)
