from benchmark_setup import benchmark
import pandas as pd
from joblib import Parallel, delayed, Memory

from benchmarking_modin.tasks import (
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
    def __init__(self, file_path):
        self.benchmarks_results = self.run_benchmark(file_path)

    def run_benchmark(self, file_path: str) -> None:
        joblib_data = pd.read_parquet(file_path)
        joblib_benchmarks = {
            'duration': [],
            'task': [],
        }

        # Normal local running
        joblib_benchmarks = self.run_common_benchmarks(joblib_data, 'Joblib local', joblib_benchmarks, file_path)

        # Filtered local running
        expr_filter = (joblib_data.Tip_Amt >= 1) & (joblib_data.Tip_Amt <= 5)
        filtered_joblib_data = joblib_data[expr_filter]
        joblib_benchmarks = self.run_common_benchmarks(filtered_joblib_data, 'Joblib local filtered', joblib_benchmarks, file_path)

        # Filtered with cache running
        joblib_benchmarks = self.run_common_benchmarks(filtered_joblib_data, 'Joblib local filtered cache', joblib_benchmarks, file_path)
        self.benchmarks_results = joblib_benchmarks


    def run_common_benchmarks(self, data: pd.DataFrame, name_prefix: str, joblib_benchmarks: dict, file_path: str) -> dict:
        def task(func, name, **kwargs):
            # Wrap the benchmark function in a delayed call
            return delayed(self.cache_benchmark)(func, df=data, benchmarks=joblib_benchmarks, name=name, **kwargs)

        other = groupby_statistics(data)
        other.columns = pd.Index([e[0]+'_' + e[1] for e in other.columns.tolist()])

        tasks = [
            task(read_file_parquet, f'{name_prefix} read file', path=file_path),
            task(count, f'{name_prefix} count'),
            task(count_index_length, f'{name_prefix} count index length'),
            task(mean, f'{name_prefix} mean'),
            task(standard_deviation, f'{name_prefix} standard deviation'),
            task(mean_of_sum, f'{name_prefix} mean of columns addition'),
            task(sum_columns, f'{name_prefix} addition of columns'),
            task(mean_of_product, f'{name_prefix} mean of columns multiplication'),
            task(product_columns, f'{name_prefix} multiplication of columns'),
            task(value_counts, f'{name_prefix} value counts'),
            task(mean_of_complicated_arithmetic_operation, f'{name_prefix} mean of complex arithmetic ops'),
            task(complicated_arithmetic_operation, f'{name_prefix} complex arithmetic ops'),
            task(groupby_statistics, f'{name_prefix} groupby statistics'),
            task(join_count, f'{name_prefix} join count', other=other),
            task(join_data, f'{name_prefix} join', other=other),
        ]
        Parallel(n_jobs=4)(tasks)

        return joblib_benchmarks

    def cache_benchmark(self, func, df, benchmarks, name, **kwargs):
        cached_func = memory.cache(func)
        return cached_func(df=df, benchmarks=benchmarks, name=name, **kwargs)
