from dask.distributed import Client, wait
from benchmark_setup import benchmark
import dask.dataframe as dd
import pandas as pd
import numpy as np
import os
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
    mean
)

# dask.config.set({
#     'distributed.worker.memory.target': 0.6,
#     'distributed.worker.memory.spill': 0.7,
#     'distributed.worker.memory.pause': 0.8,
# })

class LocalDaskBenchmark:
    def __init__(self, filesystem=None):
        self.filesystem = filesystem
        self.client = Client(
            n_workers=os.cpu_count(),
            memory_limit='40GB',
            processes=True
            )

    def run_benchmark(self, file_path: str) -> None:
        if self.filesystem:
            # Convert to proper GCS URI format for Dask
            gcs_path = f"gs://{file_path}" if not file_path.startswith('gs://') else file_path
            dask_data = dd.read_parquet(gcs_path)
        else:
            gcs_path = file_path
            dask_data = dd.read_parquet(file_path)

        if "2009" in file_path:
            dask_data = dask_data.rename(
                columns={'Start_Lon': 'pickup_longitude',
                         'Start_Lat': 'pickup_latitude',
                         'End_Lon': 'dropoff_longitude',
                         'End_Lat': 'dropoff_latitude',
                         'Passenger_Count': 'passenger_count',
                         'Tip_Amt': 'tip_amount',
                         'Fare_Amt': 'fare_amount',
                         }
                )

        client = self.client

        dask_benchmarks = {
            'duration': [],
            'task': [],
        }

        # Normal local running
        dask_benchmarks = self.run_common_benchmarks(dask_data, 'local', dask_benchmarks, gcs_path)

        # Filtered local running
        expr_filter = (dask_data.tip_amount >= 1) & (dask_data.tip_amount <= 5)
        filtered_dask_data = dask_data[expr_filter]
        dask_benchmarks = self.run_common_benchmarks(filtered_dask_data, 'local filtered', dask_benchmarks, gcs_path)

        # Filtered with cache runnning
        filtered_dask_data = client.persist(filtered_dask_data)
        wait(filtered_dask_data)
        dask_benchmarks = self.run_common_benchmarks(filtered_dask_data, 'local filtered cache', dask_benchmarks, gcs_path)
        return dask_benchmarks


    def run_common_benchmarks(self, data: dd.DataFrame, name_prefix: str, dask_benchmarks: dict, file_path: str) -> dict:
        benchmark(read_file_parquet, df=None, benchmarks=dask_benchmarks, name=f'{name_prefix} read file', path=file_path, filesystem=self.filesystem)
        benchmark(count, df=data, benchmarks=dask_benchmarks, name=f'{name_prefix} count')
        benchmark(count_index_length, df=data, benchmarks=dask_benchmarks, name=f'{name_prefix} count index length')
        benchmark(mean, df=data, benchmarks=dask_benchmarks, name=f'{name_prefix} mean')
        benchmark(standard_deviation, df=data, benchmarks=dask_benchmarks, name=f'{name_prefix} standard deviation')
        benchmark(mean_of_sum, df=data, benchmarks=dask_benchmarks, name=f'{name_prefix} mean of columns addition')
        benchmark(sum_columns, df=data, benchmarks=dask_benchmarks, name=f'{name_prefix} addition of columns')
        benchmark(mean_of_product, df=data, benchmarks=dask_benchmarks, name=f'{name_prefix} mean of columns multiplication')
        benchmark(product_columns, df=data, benchmarks=dask_benchmarks, name=f'{name_prefix} multiplication of columns')
        benchmark(value_counts, df=data, benchmarks=dask_benchmarks, name=f'{name_prefix} value counts')
        benchmark(mean_of_complicated_arithmetic_operation, df=data, benchmarks=dask_benchmarks, name=f'{name_prefix} mean of complex arithmetic ops')
        benchmark(complicated_arithmetic_operation, df=data, benchmarks=dask_benchmarks, name=f'{name_prefix} complex arithmetic ops')
        benchmark(groupby_statistics, df=data, benchmarks=dask_benchmarks, name=f'{name_prefix} groupby statistics')

        other = groupby_statistics(data)
        other.columns = pd.Index([e[0]+'_' + e[1] for e in other.columns.tolist()])
        benchmark(join_count, data, benchmarks=dask_benchmarks, name=f'{name_prefix} join count', other=other)
        benchmark(join_data, data, benchmarks=dask_benchmarks, name=f'{name_prefix} join', other=other)

        return dask_benchmarks
