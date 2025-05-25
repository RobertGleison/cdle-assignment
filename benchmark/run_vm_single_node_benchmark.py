from benchmarking_joblib.local_joblib_benchmark import LocalJoblibBenchmark
from benchmarking_koalas.local_koalas_benchmark import LocalKoalasBenchmark
from benchmarking_modin.local_modin_benchmark import LocalModinBenchmark
from benchmarking_spark.local_spark_benchmark import LocalSparkBenchmark
from benchmarking_dask.local_dask_benchmark import LocalDaskBenchmark
from benchmark_setup import get_results
from datetime import datetime
import pandas as pd
import gcsfs


import os
from io import StringIO

if __name__ == "__main__":

    bucket_path = "cdle-datasets"
    output_bucket_path = "cdle-benchmark-results"

    filesystem = gcsfs.GCSFileSystem()
    parquet_files = filesystem.glob(f"{bucket_path}/*.parquet")

    for file in parquet_files:
        print(f"Processing: {file}")
        # https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page to donwload the datasets
        #input_path = "../datasets/taxis_2009-01_reduced.parquet"

        print("Running joblib")
        local_joblib_benchmarks = get_results(LocalJoblibBenchmark(file, filesystem).benchmarks_results).set_index('task')
        print("Running modin")
        local_modin_benchmarks = get_results(LocalModinBenchmark(file, filesystem).benchmarks_results).set_index('task')
        print("Running spark")
        local_spark_benchmarks = get_results(LocalSparkBenchmark(file, filesystem).benchmarks_results).set_index('task')
        print("Running dask")
        local_dask_benchmarks = get_results(LocalDaskBenchmark(file, filesystem).benchmarks_results).set_index('task')
        print("Running koalas")
        local_koalas_benchmarks = get_results(LocalKoalasBenchmark(file, filesystem).benchmarks_results).set_index('task')

        df = pd.concat(
            [
            local_joblib_benchmarks.duration,
            local_modin_benchmarks.duration,
            local_spark_benchmarks.duration,
            local_dask_benchmarks.duration,
            local_koalas_benchmarks.duration,
            ],
            axis=1,
            keys=['joblib',
                'modin',
                'spark',
                'dask',
                'koalas'
                ]
            )

        # Save CSV to memory
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        csv_buffer.seek(0)

        # Write to GCS output bucket
        output_path = f"{output_bucket_path}/local_benchmark_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        with filesystem.open(output_path, 'w') as f:
            f.write(csv_buffer.getvalue())

        print(f"Saved benchmark result to: gs://{output_path}")
