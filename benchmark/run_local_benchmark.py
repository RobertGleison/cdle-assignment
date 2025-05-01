from benchmarking_joblib.local_joblib_benchmark import LocalJoblibBenchmark
from benchmarking_koalas.local_koalas_benchmark import LocalKoalasBenchmark
# from benchmarking_rapids.local_rapids_benchmark import LocalRapidsBenchmark
from benchmarking_modin.local_modin_benchmark import LocalModinBenchmark
from benchmarking_spark.local_spark_benchmark import LocalSparkBenchmark
from benchmarking_dask.local_dask_benchmark import LocalDaskBenchmark
from benchmark_setup import get_results
from datetime import datetime
import pandas as pd

if __name__ == "__main__":

    # https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page to donwload the datasets
    input_path = "datasets/taxis_2009-01_reduced_with_id.parquet"

    local_joblib_benchmarks = get_results(LocalJoblibBenchmark(input_path).benchmarks_results)
    local_koalas_benchmarks = get_results(LocalKoalasBenchmark(input_path).benchmarks_results)
    # local_rapids_benchmarks = get_results(LocalRapidsBenchmark(input_path).benchmarks_results)
    local_modin_benchmarks = get_results(LocalModinBenchmark(input_path).benchmarks_results)
    local_spark_benchmarks = get_results(LocalSparkBenchmark(input_path).benchmarks_results)
    local_dask_benchmarks = get_results(LocalDaskBenchmark(input_path).benchmarks_results)

    df = pd.concat(
        [
        local_joblib_benchmarks.duration,
        local_koalas_benchmarks.duration,
        # local_rapids_benchmarks.duration,
        local_modin_benchmarks.duration,
        local_spark_benchmarks.duration,
        local_dask_benchmarks.duration,
        ],
        axis=1,
        keys=['joblib',
              'koalas',
            #   'rapids',
              'modin',
              'spark',
              'dask'
            ]
        )

    filename = 'distributed_benchmark_' + datetime.now().strftime("%Y%m%d_%H%M%S") + ".csv"
    df.to_csv(filename)
