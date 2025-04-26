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

expr_filter = (koalas_data.tip_amt >= 1) & (koalas_data.tip_amt <= 5)
koalas_filtered = koalas_data[expr_filter]

# Add caching
koalas_filtered = koalas_filtered.spark.cache()
print(f'Enforce caching: {len(koalas_filtered)} rows of filtered data')

benchmark(count, koalas_filtered, benchmarks=koalas_benchmarks, name='filtered count')
benchmark(count_index_length, koalas_filtered, benchmarks=koalas_benchmarks, name='filtered count index length')
benchmark(mean, koalas_filtered, benchmarks=koalas_benchmarks, name='filtered mean')
benchmark(standard_deviation, koalas_filtered, benchmarks=koalas_benchmarks, name='filtered standard deviation')
benchmark(mean_of_sum, koalas_filtered, benchmarks=koalas_benchmarks, name ='filtered mean of columns addition')
benchmark(sum_columns, df=koalas_filtered, benchmarks=koalas_benchmarks, name='filtered addition of columns')
benchmark(mean_of_product, koalas_filtered, benchmarks=koalas_benchmarks, name ='filtered mean of columns multiplication')
benchmark(product_columns, df=koalas_filtered, benchmarks=koalas_benchmarks, name='filtered multiplication of columns')
benchmark(mean_of_complicated_arithmetic_operation, koalas_filtered, benchmarks=koalas_benchmarks, name='filtered mean of complex arithmetic oks')
benchmark(complicated_arithmetic_operation, koalas_filtered, benchmarks=koalas_benchmarks, name='filtered complex arithmetic oks')
benchmark(value_counts, koalas_filtered, benchmarks=koalas_benchmarks, name ='filtered value counts')
benchmark(groupby_statistics, koalas_filtered, benchmarks=koalas_benchmarks, name='filtered groupby statistics')

other = groupby_statistics(koalas_filtered)
other.columns = pd.Index([e[0]+'_' + e[1] for e in other.columns.tolist()])
benchmark(join_count, koalas_filtered, benchmarks=koalas_benchmarks, name='filtered join count', other=other)
benchmark(join_data, koalas_filtered, benchmarks=koalas_benchmarks, name='filtered join', other=other)

session.stop()