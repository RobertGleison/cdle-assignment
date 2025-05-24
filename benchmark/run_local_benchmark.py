from benchmarking_joblib.local_joblib_benchmark import LocalJoblibBenchmark
from benchmarking_koalas.local_koalas_benchmark import LocalKoalasBenchmark
from benchmarking_modin.local_modin_benchmark import LocalModinBenchmark
from benchmarking_spark.local_spark_benchmark import LocalSparkBenchmark
from benchmarking_dask.local_dask_benchmark import LocalDaskBenchmark
from benchmark_setup import get_results
from datetime import datetime
import pandas as pd
import os

if __name__ == "__main__":

    folder_path = "datasets/"
    # https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page to download the datasets

    # Loop over all files in the folder
    for filename in os.listdir(folder_path):
        file_path = os.path.join(folder_path, filename)
        if os.path.isfile(file_path):
            print(f"Processing file: {file_path}")

        local_joblib_benchmarks = get_results(LocalJoblibBenchmark(file_path).benchmarks_results).set_index('task')
        local_koalas_benchmarks = get_results(LocalKoalasBenchmark(file_path).benchmarks_results).set_index('task')
        local_modin_benchmarks = get_results(LocalModinBenchmark(file_path).benchmarks_results).set_index('task')
        local_spark_benchmarks = get_results(LocalSparkBenchmark(file_path).benchmarks_results).set_index('task')
        local_dask_benchmarks = get_results(LocalDaskBenchmark(file_path).benchmarks_results).set_index('task')

        df = pd.concat(
            [
            local_joblib_benchmarks.duration,
            local_koalas_benchmarks.duration,
            local_modin_benchmarks.duration,
            local_spark_benchmarks.duration,
            local_dask_benchmarks.duration,
            ],
            axis=1,
            keys=['joblib',
                'koalas',
                'modin',
                'spark',
                'dask'
                ]
            )

        os.makedirs('logs', exist_ok=True)
        filename = 'logs/local_benchmark_' + datetime.now().strftime("%Y%m%d_%H%M%S") + ".csv"
        df.to_csv(filename)
