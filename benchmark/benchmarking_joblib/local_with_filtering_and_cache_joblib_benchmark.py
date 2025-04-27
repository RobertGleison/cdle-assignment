from benchmark import benchmark
from distributed import wait
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
        joblib_data = read_file_parquet().compute()

        joblib_benchmarks = {
            'duration': [],  # in seconds
            'task': [],
        }

        expr_filter = (joblib_data.tip_amt >= 1) & (joblib_data.tip_amt <= 5)
        joblib_filtered = joblib_data[expr_filter]

        # Add caching
        joblib_filtered = read_file_parquet().compute()
        print('Waiting until all futures are finished')
        wait(joblib_filtered)
        print('All futures are finished')

        benchmark(count, joblib_filtered, benchmarks=joblib_benchmarks, name='filtered count')
        benchmark(count_index_length, joblib_filtered, benchmarks=joblib_benchmarks, name='filtered count index length')
        benchmark(mean, joblib_filtered, benchmarks=joblib_benchmarks, name='filtered mean')
        benchmark(standard_deviation, joblib_filtered, benchmarks=joblib_benchmarks, name='filtered standard deviation')
        benchmark(mean_of_sum, joblib_filtered, benchmarks=joblib_benchmarks, name ='filtered mean of columns addition')
        benchmark(sum_columns, df=joblib_filtered, benchmarks=joblib_benchmarks, name='filtered addition of columns')
        benchmark(mean_of_product, joblib_filtered, benchmarks=joblib_benchmarks, name ='filtered mean of columns multiplication')
        benchmark(product_columns, df=joblib_filtered, benchmarks=joblib_benchmarks, name='filtered multiplication of columns')
        benchmark(mean_of_complicated_arithmetic_operation, joblib_filtered, benchmarks=joblib_benchmarks, name='filtered mean of complex arithmetic ops')
        benchmark(complicated_arithmetic_operation, joblib_filtered, benchmarks=joblib_benchmarks, name='filtered complex arithmetic ops')
        benchmark(value_counts, joblib_filtered, benchmarks=joblib_benchmarks, name ='filtered value counts')
        benchmark(groupby_statistics, joblib_filtered, benchmarks=joblib_benchmarks, name='filtered groupby statistics')

        other = groupby_statistics(joblib_filtered)
        other.columns = pd.Index([e[0]+'_' + e[1] for e in other.columns.tolist()])
        benchmark(join_count, joblib_filtered, benchmarks=joblib_benchmarks, name='filtered join count', other=other)
        benchmark(join_data, joblib_filtered, benchmarks=joblib_benchmarks, name='filtered join', other=other)
