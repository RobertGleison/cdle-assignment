from benchmark_setup import benchmark
import cudf
import pandas as pd
from benchmark.benchmarking_rapids.tasks import (
    mean_of_complicated_arithmetic_operation,
    complicated_arithmetic_operation,
    count_index_length,
    groupby_statistics,
    standard_deviation,
    read_file_parquet,
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


class DistributedRapidsBenchmark:
    def __init__(self, file_path):
        self.benchmarks_results = self.run_benchmark(file_path)

    def run_benchmark(self, file_path: str) -> None:
        rapids_data = cudf.read_parquet(file_path)

        rapids_benchmarks = {
            'duration': [],
            'task': [],
        }

        # Normal distributed running
        rapids_benchmarks = self.un_common_benchmarks(rapids_data, 'rapids distributed', rapids_benchmarks, file_path)

        # Filtered distributed running
        filtered_data = rapids_data[(rapids_data.Tip_Amt >= 1) & (rapids_data.Tip_Amt <= 5)]
        rapids_benchmarks = self.run_common_benchmarks(filtered_data, 'rapids distributed filtered', rapids_benchmarks, file_path)

        # Filtered with "cache" running (cuDF loads all into GPU already)
        cached_data = filtered_data.copy(deep=False)
        print(f'Enforce caching: {len(cached_data)} rows of filtered data')
        rapids_benchmarks = self.run_common_benchmarks(cached_data, 'rapids distributed filtered cache', rapids_benchmarks, file_path)

        self.benchmarks_results = rapids_benchmarks

    def run_common_benchmarks(self, data: cudf.DataFrame, name_prefix: str, rapids_benchmarks: dict, file_path: str) -> dict:
        benchmark(read_file_parquet, df=None, benchmarks=rapids_benchmarks, name=f'{name_prefix} read file', path=file_path)
        benchmark(count, df=data, benchmarks=rapids_benchmarks, name=f'{name_prefix} count')
        benchmark(count_index_length, df=data, benchmarks=rapids_benchmarks, name=f'{name_prefix} count index length')
        benchmark(mean, df=data, benchmarks=rapids_benchmarks, name=f'{name_prefix} mean')
        benchmark(standard_deviation, df=data, benchmarks=rapids_benchmarks, name=f'{name_prefix} standard deviation')
        benchmark(mean_of_sum, df=data, benchmarks=rapids_benchmarks, name=f'{name_prefix} mean of columns addition')
        benchmark(sum_columns, df=data, benchmarks=rapids_benchmarks, name=f'{name_prefix} addition of columns')
        benchmark(mean_of_product, df=data, benchmarks=rapids_benchmarks, name=f'{name_prefix} mean of columns multiplication')
        benchmark(product_columns, df=data, benchmarks=rapids_benchmarks, name=f'{name_prefix} multiplication of columns')
        benchmark(value_counts, df=data, benchmarks=rapids_benchmarks, name=f'{name_prefix} value counts')
        benchmark(mean_of_complicated_arithmetic_operation, df=data, benchmarks=rapids_benchmarks, name=f'{name_prefix} mean of complex arithmetic ops')
        benchmark(complicated_arithmetic_operation, df=data, benchmarks=rapids_benchmarks, name=f'{name_prefix} complex arithmetic ops')
        benchmark(groupby_statistics, df=data, benchmarks=rapids_benchmarks, name=f'{name_prefix} groupby statistics')

        other = groupby_statistics(data)
        other.columns = pd.Index([f"{e[0]}_{e[1]}" for e in other.columns.to_pandas().tolist()])
        benchmark(join_count, data, benchmarks=rapids_benchmarks, name=f'{name_prefix} join count', other=other)
        benchmark(join_data, data, benchmarks=rapids_benchmarks, name=f'{name_prefix} join', other=other)

        return rapids_benchmarks
