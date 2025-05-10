from benchmark.benchmark_setup import benchmark
import pandas as pd
import time
from joblib import Parallel, delayed, Memory
from benchmark.benchmarking_joblib.tasks import (
    read_file_parquet,
    mean_of_complicated_arithmetic_operation,
    complicated_arithmetic_operation,
    groupby_statistics,
    count_index_length,
    standard_deviation,
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

memory = Memory('joblib_cache', verbose=0)

class LocalJoblibBenchmark:
    def __init__(self, input_path):
        self.benchmarks_results = self.run_benchmark(input_path)

    def run_benchmark(self, file_path: str) -> dict:
        joblib_data = pd.read_parquet(file_path)
        joblib_benchmarks = {
            'duration': [],
            'task': [],
        }

        # Normal local running
        joblib_benchmarks = self.run_common_benchmarks(joblib_data, 'local', joblib_benchmarks, file_path)

        # Filtered local running
        expr_filter = (joblib_data.Tip_Amt >= 1) & (joblib_data.Tip_Amt <= 5)
        filtered_joblib_data = joblib_data[expr_filter]
        joblib_benchmarks = self.run_common_benchmarks(filtered_joblib_data, 'local filtered', joblib_benchmarks, file_path)

        # Filtered with cache running
        joblib_benchmarks = self.run_common_benchmarks(filtered_joblib_data, 'local filtered cache', joblib_benchmarks, file_path)

        return joblib_benchmarks

    def run_common_benchmarks(self, data: pd.DataFrame, name_prefix: str, joblib_benchmarks: dict, file_path: str) -> dict:
        # This function adapts the benchmark function to work with joblib's delayed
        def wrapped_benchmark(func, name, **kwargs):
            def inner_func():
                # Notice we're calling the imported benchmark function here
                return benchmark(func, data, joblib_benchmarks, name, **kwargs)
            return inner_func

        other = groupby_statistics(data)
        other.columns = pd.Index([e[0]+'_' + e[1] for e in other.columns.tolist()])

        # Create a list of tasks, but don't use delayed yet
        tasks = [
            wrapped_benchmark(read_file_parquet, f'{name_prefix} read file', path=file_path),
            wrapped_benchmark(count, f'{name_prefix} count'),
            wrapped_benchmark(count_index_length, f'{name_prefix} count index length'),
            wrapped_benchmark(mean, f'{name_prefix} mean'),
            wrapped_benchmark(standard_deviation, f'{name_prefix} standard deviation'),
            wrapped_benchmark(mean_of_sum, f'{name_prefix} mean of columns addition'),
            wrapped_benchmark(sum_columns, f'{name_prefix} addition of columns'),
            wrapped_benchmark(mean_of_product, f'{name_prefix} mean of columns multiplication'),
            wrapped_benchmark(product_columns, f'{name_prefix} multiplication of columns'),
            wrapped_benchmark(value_counts, f'{name_prefix} value counts'),
            wrapped_benchmark(mean_of_complicated_arithmetic_operation, f'{name_prefix} mean of complex arithmetic ops'),
            wrapped_benchmark(complicated_arithmetic_operation, f'{name_prefix} complex arithmetic ops'),
            wrapped_benchmark(groupby_statistics, f'{name_prefix} groupby statistics'),
            wrapped_benchmark(join_count, f'{name_prefix} join count', other=other),
            wrapped_benchmark(join_data, f'{name_prefix} join', other=other),
        ]

        # Run tasks sequentially - a simpler approach to avoid synchronization issues with the benchmark dict
        for task in tasks:
            task()

        return joblib_benchmarks