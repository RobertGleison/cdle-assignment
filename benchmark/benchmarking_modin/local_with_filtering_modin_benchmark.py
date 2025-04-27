from benchmarking_modin.tasks import (
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
import modin.pandas as pd
import numpy as np


class Benchmark:
    def __init__(self):
        self.benchmarks_results = None

    def run_benchmark(self, file_path):
        modin_data = pd.read_parquet(file_path)

        modin_benchmarks = {
            'duration': [],
            'task': [],
        }

        expr_filter = (modin_data.Tip_Amt >= 1) & (modin_data.Tip_Amt <= 5)
        modin_data = modin_data[expr_filter]

        benchmark(read_file_parquet, df=None, benchmarks=modin_benchmarks, name='filtered read file', path=file_path)
        benchmark(count, df=modin_data, benchmarks=modin_benchmarks, name='filtered count')
        benchmark(count_index_length, df=modin_data, benchmarks=modin_benchmarks, name='filtered count index length')
        benchmark(mean, df=modin_data, benchmarks=modin_benchmarks, name='filtered mean')
        benchmark(standard_deviation, df=modin_data, benchmarks=modin_benchmarks, name='filtered standard deviation')
        benchmark(mean_of_sum, df=modin_data, benchmarks=modin_benchmarks, name='filtered mean of columns addition')
        benchmark(sum_columns, df=modin_data, benchmarks=modin_benchmarks, name='filtered addition of columns')
        benchmark(mean_of_product, df=modin_data, benchmarks=modin_benchmarks, name='filtered mean of columns multiplication')
        benchmark(product_columns, df=modin_data, benchmarks=modin_benchmarks, name='filtered multiplication of columns')
        benchmark(value_counts, df=modin_data, benchmarks=modin_benchmarks, name='filtered value counts')
        benchmark(mean_of_complicated_arithmetic_operation, df=modin_data, benchmarks=modin_benchmarks, name='filtered mean of complex arithmetic ops')
        benchmark(complicated_arithmetic_operation, df=modin_data, benchmarks=modin_benchmarks, name='filtered complex arithmetic ops')
        benchmark(groupby_statistics, df=modin_data, benchmarks=modin_benchmarks, name='filtered groupby statistics')

        other = groupby_statistics(modin_data)
        other.columns = pd.Index([e[0]+'_' + e[1] for e in other.columns.tolist()])
        benchmark(join_count, modin_data, benchmarks=modin_benchmarks, name='filtered join count', other=other)
        benchmark(join_data, modin_data, benchmarks=modin_benchmarks, name='filtered join', other=other)

        self.benchmarks_results = modin_benchmarks