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
        joblib_data = read_file_parquet(file_path).compute()

        joblib_benchmarks = {
            'duration': [],  # in seconds
            'task': [],
        }

        expr_filter = (joblib_data.Tip_Amt >= 1) & (joblib_data.Tip_Amt <= 5)
        dask_filtered = joblib_data[expr_filter]

        benchmark(count, dask_filtered, benchmarks=joblib_benchmarks, name='filtered cache count')
        benchmark(count_index_length, dask_filtered, benchmarks=joblib_benchmarks, name='filtered count index length')
        benchmark(mean, dask_filtered, benchmarks=joblib_benchmarks, name='filtered mean')
        benchmark(standard_deviation, dask_filtered, benchmarks=joblib_benchmarks, name='filtere standard deviation')
        benchmark(mean_of_sum, dask_filtered, benchmarks=joblib_benchmarks, name ='filtered mean of columns addition')
        benchmark(sum_columns, df=dask_filtered, benchmarks=joblib_benchmarks, name='filtered addition of columns')
        benchmark(mean_of_product, dask_filtered, benchmarks=joblib_benchmarks, name ='filtered mean of columns multiplication')
        benchmark(product_columns, df=dask_filtered, benchmarks=joblib_benchmarks, name='filtered multiplication of columns')
        benchmark(mean_of_complicated_arithmetic_operation, dask_filtered, benchmarks=joblib_benchmarks, name='filtered mean of complex arithmetic ops')
        benchmark(complicated_arithmetic_operation, dask_filtered, benchmarks=joblib_benchmarks, name='filtered complex arithmetic ops')
        benchmark(value_counts, dask_filtered, benchmarks=joblib_benchmarks, name ='filtered value counts')
        benchmark(groupby_statistics, dask_filtered, benchmarks=joblib_benchmarks, name='filtered groupby statistics')

        other = groupby_statistics(dask_filtered)
        other.columns = pd.Index([e[0]+'_' + e[1] for e in other.columns.tolist()])
        benchmark(join_count, dask_filtered, benchmarks=joblib_benchmarks, name='filtered join count', other=other)
        benchmark(join_data, dask_filtered, benchmarks=joblib_benchmarks, name='filtered join', other=other)
