from benchmark_setup import benchmark
from dask.distributed import Client
import modin.config as modin_cfg
import modin.pandas as mpd
import pandas as pd
from benchmark.benchmarking_modin.tasks import (
    mean_of_complicated_arithmetic_operation,
    complicated_arithmetic_operation,
    groupby_statistics,
    count_index_length,
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

modin_cfg.Engine.put("dask")

class LocalModinBenchmark:
    def __init__(self, file_path):
        self.client = Client(
            n_workers=1,
            memory_limit='10GB',
            processes=True
            )
        self.benchmarks_results = self.run_benchmark(file_path)


    def run_benchmark(self, file_path: str) -> None:
        modin_data = mpd.read_parquet(file_path)
        client = self.client

        modin_benchmarks = {
            'duration': [],
            'task': [],
        }

        # Normal local running
        modin_benchmarks = self.run_common_benchmarks(modin_data, 'modin local', modin_benchmarks, file_path)

        # Filtered local running
        expr_filter = (modin_data.Tip_Amt >= 1) & (modin_data.Tip_Amt <= 5)
        filtered_modin_data = modin_data[expr_filter]
        modin_benchmarks = self.run_common_benchmarks(filtered_modin_data, 'modin local filtered', modin_benchmarks, file_path)

        # Filtered with cache runnning
        filtered_modin_data = filtered_modin_data.copy() # Uses copy instead of persist cause modin is not a dask object
        modin_benchmarks = self.run_common_benchmarks(filtered_modin_data, 'modin local filtered cache', modin_benchmarks, file_path)

        client.close()

        return modin_benchmarks


    def run_common_benchmarks(self, data: mpd.DataFrame, name_prefix: str, modin_benchmarks: dict, file_path: str) -> dict:
        benchmark(read_file_parquet, df=None, benchmarks=modin_benchmarks, name=f'{name_prefix} read file', path=file_path)
        benchmark(count, df=data, benchmarks=modin_benchmarks, name=f'{name_prefix} count')
        benchmark(count_index_length, df=data, benchmarks=modin_benchmarks, name=f'{name_prefix} count index length')
        benchmark(mean, df=data, benchmarks=modin_benchmarks, name=f'{name_prefix} mean')
        benchmark(standard_deviation, df=data, benchmarks=modin_benchmarks, name=f'{name_prefix} standard deviation')
        benchmark(mean_of_sum, df=data, benchmarks=modin_benchmarks, name=f'{name_prefix} mean of columns addition')
        benchmark(sum_columns, df=data, benchmarks=modin_benchmarks, name=f'{name_prefix} addition of columns')
        benchmark(mean_of_product, df=data, benchmarks=modin_benchmarks, name=f'{name_prefix} mean of columns multiplication')
        benchmark(product_columns, df=data, benchmarks=modin_benchmarks, name=f'{name_prefix} multiplication of columns')
        benchmark(value_counts, df=data, benchmarks=modin_benchmarks, name=f'{name_prefix} value counts')
        benchmark(mean_of_complicated_arithmetic_operation, df=data, benchmarks=modin_benchmarks, name=f'{name_prefix} mean of complex arithmetic ops')
        benchmark(complicated_arithmetic_operation, df=data, benchmarks=modin_benchmarks, name=f'{name_prefix} complex arithmetic ops')
        benchmark(groupby_statistics, df=data, benchmarks=modin_benchmarks, name=f'{name_prefix} groupby statistics')

        other = groupby_statistics(data)
        other.columns = pd.Index([e[0]+'_' + e[1] for e in other.columns.tolist()])
        benchmark(join_count, data, benchmarks=modin_benchmarks, name=f'{name_prefix} join count', other=other)
        benchmark(join_data, data, benchmarks=modin_benchmarks, name=f'{name_prefix} join', other=other)

        return modin_benchmarks
