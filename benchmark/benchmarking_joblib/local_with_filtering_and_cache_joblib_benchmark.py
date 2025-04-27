from benchmark_setup import benchmark
from dask.distributed import Client
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
        joblib_data = read_file_parquet(file_path).compute()
        client = Client(memory_limit='10GB')

        joblib_benchmarks = {
            'duration': [],  # in seconds
            'task': [],
        }

        expr_filter = (joblib_data.Tip_Amt >= 1) & (joblib_data.Tip_Amt <= 5)
        joblib_filtered = joblib_data[expr_filter]

        # Add caching
        print('Persisting filtered data')
        try:
            joblib_filtered = client.persist(joblib_filtered)
            print('Waiting until all futures are finished')
            wait(joblib_filtered)
            print('All futures are finished')
        except AssertionError:
            print('Note: Cannot persist data - it might already be a pandas DataFrame, not a Dask collection')
        
        benchmark(count, joblib_filtered, benchmarks=joblib_benchmarks, name='filtered count')
        benchmark(count_index_length, joblib_filtered, benchmarks=joblib_benchmarks, name='filtered cache count index length')
        benchmark(mean, joblib_filtered, benchmarks=joblib_benchmarks, name='filtered cache mean')
        benchmark(standard_deviation, joblib_filtered, benchmarks=joblib_benchmarks, name='filtered cache standard deviation')
        benchmark(mean_of_sum, joblib_filtered, benchmarks=joblib_benchmarks, name ='filtered cache mean of columns addition')
        benchmark(sum_columns, df=joblib_filtered, benchmarks=joblib_benchmarks, name='filtered cache addition of columns')
        benchmark(mean_of_product, joblib_filtered, benchmarks=joblib_benchmarks, name ='filtered cache mean of columns multiplication')
        benchmark(product_columns, df=joblib_filtered, benchmarks=joblib_benchmarks, name='filtered cache multiplication of columns')
        benchmark(mean_of_complicated_arithmetic_operation, joblib_filtered, benchmarks=joblib_benchmarks, name='filtered cache mean of complex arithmetic ops')
        benchmark(complicated_arithmetic_operation, joblib_filtered, benchmarks=joblib_benchmarks, name='filtered cache complex arithmetic ops')
        benchmark(value_counts, joblib_filtered, benchmarks=joblib_benchmarks, name ='filtered cache value counts')
        benchmark(groupby_statistics, joblib_filtered, benchmarks=joblib_benchmarks, name='filtered cache groupby statistics')

        other = groupby_statistics(joblib_filtered)
        other.columns = pd.Index([e[0]+'_' + e[1] for e in other.columns.tolist()])
        benchmark(join_count, joblib_filtered, benchmarks=joblib_benchmarks, name='filtered cache join count', other=other)
        benchmark(join_data, joblib_filtered, benchmarks=joblib_benchmarks, name='filtered cache join', other=other)
        client.close()