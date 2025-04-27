from benchmarking_dask.tasks import (
    mean_of_complicated_arithmetic_operation,
    complicated_arithmetic_operation,
    count_index_length,
    groupby_statistics,
    standard_deviation,
    read_file_parquet,
    mean_of_product,
    product_columns,
    value_counts,
    sum_columns,
    mean_of_sum,
    join_count,
    join_data,
    count,
    mean,
)
from benchmark_setup import benchmark
from dask.distributed import Client
from distributed import wait
import dask.dataframe as dd
import pandas as pd
import numpy as np
import dask


class Benchmark:
    def __init__(self):
        self.benchmarks_results = None

    def run_benchmark(self, file_path):
        dask_data = dd.read_parquet(file_path, index_col = 'index')
        client = Client(memory_limit='10GB')

        dask_benchmarks = {
            'duration': [],
            'task': [],
        }

        expr_filter = (dask_data.Tip_Amt >= 1) & (dask_data.Tip_Amt <= 5)
        dask_data = dask_data[expr_filter]

        # cache of plan execution DAG
        dask_data = client.persist(dask_data)
        wait(dask_data)

        benchmark(read_file_parquet, df=None, benchmarks=dask_benchmarks, name='filtered cache read file', path = file_path)
        benchmark(count, df=dask_data, benchmarks=dask_benchmarks, name='filtered cache count')
        benchmark(count_index_length, df=dask_data, benchmarks=dask_benchmarks, name='filtered cache count index length')
        benchmark(mean, df=dask_data, benchmarks=dask_benchmarks, name='filtered cache mean')
        benchmark(standard_deviation, df=dask_data, benchmarks=dask_benchmarks, name='filtered cache standard deviation')
        benchmark(mean_of_sum, df=dask_data, benchmarks=dask_benchmarks, name='filtered cache mean of columns addition')
        benchmark(sum_columns, df=dask_data, benchmarks=dask_benchmarks, name='filtered cache addition of columns')
        benchmark(mean_of_product, df=dask_data, benchmarks=dask_benchmarks, name='filtered cache mean of columns multiplication')
        benchmark(product_columns, df=dask_data, benchmarks=dask_benchmarks, name='filtered cache multiplication of columns')
        benchmark(value_counts, df=dask_data, benchmarks=dask_benchmarks, name='filtered cache value counts')
        benchmark(mean_of_complicated_arithmetic_operation, df=dask_data, benchmarks=dask_benchmarks, name='filtered cache mean of complex arithmetic ops')
        benchmark(complicated_arithmetic_operation, df=dask_data, benchmarks=dask_benchmarks, name='filtered cache complex arithmetic ops')
        benchmark(groupby_statistics, df=dask_data, benchmarks=dask_benchmarks, name='filtered cache groupby statistics')

        other = groupby_statistics(dask_data)
        other.columns = pd.Index([e[0]+'_' + e[1] for e in other.columns.tolist()])
        benchmark(join_count, dask_data, benchmarks=dask_benchmarks, name='filtered cache join count', other=other)
        benchmark(join_data, dask_data, benchmarks=dask_benchmarks, name='filtered cache join', other=other)

        self.benchmarks_results = dask_benchmarks
        client.close()

