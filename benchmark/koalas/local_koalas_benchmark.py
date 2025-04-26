from benchmark import benchmark
from pyspark.sql import SparkSession
import pyspark.pandas as ks
import pandas as pd
import numpy as np
from benchmark.koalas.tasks import (
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

session = SparkSession.builder.getOrCreate()
koalas_data = ks.read_parquet('/FileStore/ks_taxi_parquet', index_col='index')
koalas_benchmarks = {
    'duration': [],  # in seconds
    'task': [],
}

benchmark(read_file_parquet, df=None, benchmarks=koalas_benchmarks, name='read file')
benchmark(count, df=koalas_data, benchmarks=koalas_benchmarks, name='count')
benchmark(count_index_length, df=koalas_data, benchmarks=koalas_benchmarks, name='count index length')
benchmark(mean, df=koalas_data, benchmarks=koalas_benchmarks, name='mean')
benchmark(standard_deviation, df=koalas_data, benchmarks=koalas_benchmarks, name='standard deviation')
benchmark(mean_of_sum, df=koalas_data, benchmarks=koalas_benchmarks, name='mean of columns addition')
benchmark(sum_columns, df=koalas_data, benchmarks=koalas_benchmarks, name='addition of columns')
benchmark(mean_of_product, df=koalas_data, benchmarks=koalas_benchmarks, name='mean of columns multiplication')
benchmark(product_columns, df=koalas_data, benchmarks=koalas_benchmarks, name='multiplication of columns')
benchmark(value_counts, df=koalas_data, benchmarks=koalas_benchmarks, name='value counts')
benchmark(mean_of_complicated_arithmetic_operation, df=koalas_data, benchmarks=koalas_benchmarks, name='mean of complex arithmetic oks')
benchmark(complicated_arithmetic_operation, df=koalas_data, benchmarks=koalas_benchmarks, name='complex arithmetic oks')
benchmark(groupby_statistics, df=koalas_data, benchmarks=koalas_benchmarks, name='groupby statistics')

other = groupby_statistics(koalas_data)
other.columns = pd.Index([e[0]+'_' + e[1] for e in other.columns.tolist()])
benchmark(join_count, koalas_data, benchmarks=koalas_benchmarks, name='join count', other=other)
benchmark(join_data, koalas_data, benchmarks=koalas_benchmarks, name='join', other=other)

session.stop()