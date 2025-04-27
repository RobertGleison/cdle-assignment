from benchmark import benchmark
import pandas as pd
import numpy as np
import modin.pandas as mpd
from bencharking_modin.tasks import (
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
        modin_data = mpd.read_parquet('/dbfs/FileStore/ks_taxi_parquet', index='index')

        modin_benchmarks = {
            'duration': [],  # in seconds
            'task': [],
        }

        benchmark(read_file_parquet, df=None, benchmarks=modin_benchmarks, name='read file')
        benchmark(count, df=modin_data, benchmarks=modin_benchmarks, name='count')
        benchmark(count_index_length, df=modin_data, benchmarks=modin_benchmarks, name='count index length')
        benchmark(mean, df=modin_data, benchmarks=modin_benchmarks, name='mean')
        benchmark(standard_deviation, df=modin_data, benchmarks=modin_benchmarks, name='standard deviation')
        benchmark(mean_of_sum, df=modin_data, benchmarks=modin_benchmarks, name='mean of columns addition')
        benchmark(sum_columns, df=modin_data, benchmarks=modin_benchmarks, name='addition of columns')
        benchmark(mean_of_product, df=modin_data, benchmarks=modin_benchmarks, name='mean of columns multiplication')
        benchmark(product_columns, df=modin_data, benchmarks=modin_benchmarks, name='multiplication of columns')
        benchmark(value_counts, df=modin_data, benchmarks=modin_benchmarks, name='value counts')
        benchmark(mean_of_complicated_arithmetic_operation, df=modin_data, benchmarks=modin_benchmarks, name='mean of complex arithmetic ops')
        benchmark(complicated_arithmetic_operation, df=modin_data, benchmarks=modin_benchmarks, name='complex arithmetic ops')
        benchmark(groupby_statistics, df=modin_data, benchmarks=modin_benchmarks, name='groupby statistics')

        other = groupby_statistics(modin_data)
        other.columns = pd.Index([e[0]+'_' + e[1] for e in other.columns.tolist()])
        benchmark(join_count, df=modin_data, benchmarks=modin_benchmarks, name='join count', other=other)
        benchmark(join_data, df=modin_data, benchmarks=modin_benchmarks, name='join', other=other)