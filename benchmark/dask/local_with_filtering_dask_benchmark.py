from dask.distributed import Client
from benchmark import benchmark
import dask.dataframe as dd
import pandas as pd
import dask
import numpy as np
from benchmark.dask.tasks import (
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

client = Client()
dask_data = dd.read_parquet('/dbfs/FileStore/ks_taxi_parquet', index='index')
dask_benchmarks = {
    'duration': [],  # in seconds
    'task': [],
}

expr_filter = (dask_data.tip_amt >= 1) & (dask_data.tip_amt <= 5)
dask_filtered = dask_data[expr_filter]

benchmark(count, dask_filtered, benchmarks=dask_benchmarks, name='filtered count')
benchmark(count_index_length, dask_filtered, benchmarks=dask_benchmarks, name='filtered count index length')
benchmark(mean, dask_filtered, benchmarks=dask_benchmarks, name='filtered mean')
benchmark(standard_deviation, dask_filtered, benchmarks=dask_benchmarks, name='filtered standard deviation')
benchmark(mean_of_sum, dask_filtered, benchmarks=dask_benchmarks, name ='filtered mean of columns addition')
benchmark(sum_columns, df=dask_filtered, benchmarks=dask_benchmarks, name='filtered addition of columns')
benchmark(mean_of_product, dask_filtered, benchmarks=dask_benchmarks, name ='filtered mean of columns multiplication')
benchmark(product_columns, df=dask_filtered, benchmarks=dask_benchmarks, name='filtered multiplication of columns')
benchmark(mean_of_complicated_arithmetic_operation, dask_filtered, benchmarks=dask_benchmarks, name='filtered mean of complex arithmetic ops')
benchmark(complicated_arithmetic_operation, dask_filtered, benchmarks=dask_benchmarks, name='filtered complex arithmetic ops')
benchmark(value_counts, dask_filtered, benchmarks=dask_benchmarks, name ='filtered value counts')
benchmark(groupby_statistics, dask_filtered, benchmarks=dask_benchmarks, name='filtered groupby statistics')

other = groupby_statistics(dask_filtered)
other.columns = pd.Index([e[0]+'_' + e[1] for e in other.columns.tolist()])
benchmark(join_count, dask_filtered, benchmarks=dask_benchmarks, name='filtered join count', other=other)
benchmark(join_data, dask_filtered, benchmarks=dask_benchmarks, name='filtered join', other=other)

client.close()