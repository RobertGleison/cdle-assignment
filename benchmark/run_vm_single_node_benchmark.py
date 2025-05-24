from benchmarking_joblib.local_joblib_benchmark import LocalJoblibBenchmark
from benchmarking_koalas.local_koalas_benchmark import LocalKoalasBenchmark
from benchmarking_modin.local_modin_benchmark import LocalModinBenchmark
from benchmarking_spark.local_spark_benchmark import LocalSparkBenchmark
from benchmarking_dask.local_dask_benchmark import LocalDaskBenchmark
from benchmark_setup import get_results
from dotenv import load_dotenv
from datetime import datetime
import pandas as pd
import gcsfs
import os
from io import StringIO

load_dotenv("../.env")

if __name__ == "__main__":

    bucket_path = os.getenv("GCP_BUCKET_PATH")
    output_bucket_path = os.getenv("GCP_OUTPUT_BUCKET_PATH")

    fs = gcsfs.GCSFileSystem()
    parquet_files = fs.glob(f"{bucket_path}/*.parquet")

    for file in parquet_files:
        print(f"Processing: {file}")
        # https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page to donwload the datasets
        #input_path = "../datasets/taxis_2009-01_reduced.parquet"

        local_joblib_benchmarks = get_results(LocalJoblibBenchmark(file, fs).benchmarks_results).set_index('task')
        local_koalas_benchmarks = get_results(LocalKoalasBenchmark(file, fs).benchmarks_results).set_index('task')
        local_modin_benchmarks = get_results(LocalModinBenchmark(file, fs).benchmarks_results).set_index('task')
        local_spark_benchmarks = get_results(LocalSparkBenchmark(file, fs).benchmarks_results).set_index('task')
        local_dask_benchmarks = get_results(LocalDaskBenchmark(file, fs).benchmarks_results).set_index('task')

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

        # Save CSV to memory
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        csv_buffer.seek(0)

        # Write to GCS output bucket
        output_path = f"{output_bucket_path}/local_benchmark_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        with fs.open(output_path, 'w') as f:
            f.write(csv_buffer.getvalue())

        print(f"Saved benchmark result to: gs://{output_path}")
