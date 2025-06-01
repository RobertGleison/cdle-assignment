from benchmark_setup import benchmark
from dask.distributed import Client
import modin.config as modin_cfg
import modin.pandas as mpd
import pandas as pd
import os
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
    def __init__(self, filesystem=None, profile = False):
        self.filesystem = filesystem
        self.client = Client(
            n_workers=1,
            memory_limit='40GB',
            processes=True
            )
        self.profile = profile

    def run_benchmark(self, file_path: str) -> None:
        if self.filesystem is None:
            gcs_path = file_path
        else:
            gcs_path = f"gs://{file_path}" if not file_path.startswith('gs://') else file_path
        modin_data = mpd.read_parquet(gcs_path)
        modin_data["index"] = modin_data.index

        if "2009" in file_path:
            modin_data.rename(
                columns={
                    'Start_Lon': 'pickup_longitude',
                    'Start_Lat': 'pickup_latitude',
                    'End_Lon': 'dropoff_longitude',
                    'End_Lat': 'dropoff_latitude',
                    'Passenger_Count': 'passenger_count',
                    'Tip_Amt': 'tip_amount',
                    'Fare_Amt': 'fare_amount',
                },
                inplace=True
            )


        modin_benchmarks = {
            'duration': [],
            'task': [],
        }

        # Normal local running
        modin_benchmarks = self.run_common_benchmarks(modin_data, 'local', modin_benchmarks, gcs_path)

        # Filtered local running
        expr_filter = (modin_data.tip_amount >= 1) & (modin_data.tip_amount <= 5)
        filtered_modin_data = modin_data[expr_filter]
        modin_benchmarks = self.run_common_benchmarks(filtered_modin_data, 'local filtered', modin_benchmarks, gcs_path)

        # Filtered with cache runnning
        filtered_modin_data = filtered_modin_data.copy() # Uses copy instead of persist cause modin is not a dask object
        modin_benchmarks = self.run_common_benchmarks(filtered_modin_data, 'local filtered cache', modin_benchmarks, gcs_path)

        return modin_benchmarks


    def run_common_benchmarks(self, data: mpd.DataFrame, name_prefix: str, modin_benchmarks: dict, file_path: str) -> dict:
        benchmark(read_file_parquet, df=None, benchmarks=modin_benchmarks, name=f'{name_prefix} read file', path=file_path, filesystem=self.filesystem, profile=self.profile, tool="modin")
        benchmark(count, df=data, benchmarks=modin_benchmarks, name=f'{name_prefix} count', profile=self.profile, tool="modin")
        benchmark(count_index_length, df=data, benchmarks=modin_benchmarks, name=f'{name_prefix} count index length', profile=self.profile, tool="modin")
        benchmark(mean, df=data, benchmarks=modin_benchmarks, name=f'{name_prefix} mean', profile=self.profile, tool="modin")
        benchmark(standard_deviation, df=data, benchmarks=modin_benchmarks, name=f'{name_prefix} standard deviation', profile=self.profile, tool="modin")
        benchmark(mean_of_sum, df=data, benchmarks=modin_benchmarks, name=f'{name_prefix} mean of columns addition', profile=self.profile, tool="modin")
        benchmark(sum_columns, df=data, benchmarks=modin_benchmarks, name=f'{name_prefix} addition of columns', profile=self.profile, tool="modin")
        benchmark(mean_of_product, df=data, benchmarks=modin_benchmarks, name=f'{name_prefix} mean of columns multiplication', profile=self.profile, tool="modin")
        benchmark(product_columns, df=data, benchmarks=modin_benchmarks, name=f'{name_prefix} multiplication of columns', profile=self.profile, tool="modin")
        benchmark(value_counts, df=data, benchmarks=modin_benchmarks, name=f'{name_prefix} value counts', profile=self.profile, tool="modin")
        benchmark(mean_of_complicated_arithmetic_operation, df=data, benchmarks=modin_benchmarks, name=f'{name_prefix} mean of complex arithmetic ops', profile=self.profile, tool="modin")
        benchmark(complicated_arithmetic_operation, df=data, benchmarks=modin_benchmarks, name=f'{name_prefix} complex arithmetic ops', profile=self.profile, tool="modin")
        benchmark(groupby_statistics, df=data, benchmarks=modin_benchmarks, name=f'{name_prefix} groupby statistics', profile=self.profile, tool="modin")

        other = groupby_statistics(data)
        other.columns = pd.Index([e[0]+'_' + e[1] for e in other.columns.tolist()])
        benchmark(join_count, data, benchmarks=modin_benchmarks, name=f'{name_prefix} join count', other=other, profile=self.profile, tool="modin")
        benchmark(join_data, data, benchmarks=modin_benchmarks, name=f'{name_prefix} join', other=other, profile=self.profile, tool="modin")

        return modin_benchmarks
