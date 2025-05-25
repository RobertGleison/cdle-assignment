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

    # Instantiate each benchmark class once
    koalas_runner = LocalKoalasBenchmark(filesystem=filesystem)
    joblib_runner = LocalJoblibBenchmark(filesystem=filesystem)
    modin_runner = LocalModinBenchmark(filesystem=filesystem)
    spark_runner = LocalSparkBenchmark(filesystem=filesystem)
    dask_runner = LocalDaskBenchmark(filesystem=filesystem)

    for file in parquet_files:
        print(f"\n=== Processing: {file} ===")

        print("Running Koalas...")
        koalas_results = koalas_runner.run_benchmark(file)
        local_koalas_benchmarks = get_results(koalas_results).set_index("task")

        print("Running Joblib...")
        joblib_results = joblib_runner.run_benchmark(file)
        local_joblib_benchmarks = get_results(joblib_results).set_index("task")

        print("Running Modin...")
        modin_results = modin_runner.run_benchmark(file)
        local_modin_benchmarks = get_results(modin_results).set_index("task")

        print("Running Spark...")
        spark_results = spark_runner.run_benchmark(file)
        local_spark_benchmarks = get_results(spark_results).set_index("task")

        print("Running Dask...")
        dask_results = dask_runner.run_benchmark(file)
        local_dask_benchmarks = get_results(dask_results).set_index("task")

        # Combine all benchmark results
        df = pd.concat(
            [
                local_koalas_benchmarks.duration,
                local_joblib_benchmarks.duration,
                local_modin_benchmarks.duration,
                local_spark_benchmarks.duration,
                local_dask_benchmarks.duration,
            ],
            axis=1,
            keys=["koalas", "joblib", "modin", "spark", "dask"],
        )

        # Save to GCS
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        csv_buffer.seek(0)

        base_name = os.path.basename(file).replace(".parquet", "")
        output_path = f"{output_bucket_path}/local_benchmark_{base_name}.csv"

        with filesystem.open(output_path, "w") as f:
            f.write(csv_buffer.getvalue())

        print(f"✅ Saved benchmark result to: gs://{output_path}")
