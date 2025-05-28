from benchmark.benchmark_setup import benchmark
from benchmark.benchmarking_spark.spark_session import get_spark
from pyspark.sql.functions import monotonically_increasing_id
import pyspark.pandas as ks
import pandas as pd
import numpy as np
from benchmark.benchmarking_koalas.tasks import (
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

class LocalKoalasBenchmark:
    def __init__(self, filesystem=None):
        self.filesystem = filesystem
        self.client = get_spark()


    def run_benchmark(self, file_path: str) -> None:
        gcs_path = f"gs://{file_path}" if self.filesystem else file_path
        spark_df = self.client.read.parquet(gcs_path)
        spark_df = spark_df.withColumn("index", monotonically_increasing_id())
        koalas_data = spark_df.pandas_api()

        if "2009" in file_path:
            rename_map = {
                'Start_Lon': 'pickup_longitude',
                'Start_Lat': 'pickup_latitude',
                'End_Lon': 'dropoff_longitude',
                'End_Lat': 'dropoff_latitude',
                'Passenger_Count': 'passenger_count',
                'Tip_Amt': 'tip_amount',
                'Fare_Amt': 'fare_amount'
            }
            koalas_data = koalas_data.rename(columns=rename_map)

        koalas_benchmarks = {
            'duration': [],
            'task': [],
        }

        # Normal local running
        koalas_benchmarks = self.run_common_benchmarks(koalas_data, 'local', koalas_benchmarks, gcs_path)

        # Filtered local running
        expr_filter = (koalas_data.tip_amount >= 1) & (koalas_data.tip_amount <= 5)
        filtered_koalas_data = koalas_data[expr_filter]
        koalas_benchmarks = self.run_common_benchmarks(filtered_koalas_data, 'local filtered', koalas_benchmarks, gcs_path)

        # Filtered with cache runnning
        filtered_koalas_data = filtered_koalas_data.spark.cache()
        print(f'Enforce caching: {len(filtered_koalas_data)} rows of filtered data')
        koalas_benchmarks = self.run_common_benchmarks(filtered_koalas_data, 'local filtered cache', koalas_benchmarks, gcs_path)
        return koalas_benchmarks


    def run_common_benchmarks(self, data: ks.DataFrame, name_prefix: str, koalas_benchmarks: dict, file_path: str) -> dict:
        benchmark(read_file_parquet, df=None, benchmarks=koalas_benchmarks, name=f'{name_prefix} read file', path=file_path, filesystem=self.filesystem)
        benchmark(count, df=data, benchmarks=koalas_benchmarks, name=f'{name_prefix} count')
        benchmark(count_index_length, df=data, benchmarks=koalas_benchmarks, name=f'{name_prefix} count index length')
        benchmark(mean, df=data, benchmarks=koalas_benchmarks, name=f'{name_prefix} mean')
        benchmark(standard_deviation, df=data, benchmarks=koalas_benchmarks, name=f'{name_prefix} standard deviation')
        benchmark(mean_of_sum, df=data, benchmarks=koalas_benchmarks, name=f'{name_prefix} mean of columns addition')
        benchmark(sum_columns, df=data, benchmarks=koalas_benchmarks, name=f'{name_prefix} addition of columns')
        benchmark(mean_of_product, df=data, benchmarks=koalas_benchmarks, name=f'{name_prefix} mean of columns multiplication')
        benchmark(product_columns, df=data, benchmarks=koalas_benchmarks, name=f'{name_prefix} multiplication of columns')
        benchmark(value_counts, df=data, benchmarks=koalas_benchmarks, name=f'{name_prefix} value counts')
        benchmark(complicated_arithmetic_operation, df=data, benchmarks=koalas_benchmarks, name=f'{name_prefix} complex arithmetic ops')
        benchmark(mean_of_complicated_arithmetic_operation, df=data, benchmarks=koalas_benchmarks, name=f'{name_prefix} mean of complex arithmetic ops')
        benchmark(groupby_statistics, df=data, benchmarks=koalas_benchmarks, name=f'{name_prefix} groupby statistics')

        other = ks.DataFrame(groupby_statistics(data).to_pandas())
        other.columns = pd.Index([e[0]+'_' + e[1] for e in other.columns.tolist()])

        benchmark(join_count, data, benchmarks=koalas_benchmarks, name=f'{name_prefix} join count', other=other)
        benchmark(join_data, data, benchmarks=koalas_benchmarks, name=f'{name_prefix} join', other=other)

        return koalas_benchmarks
