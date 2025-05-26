from benchmarking_koalas.distributed_koalas_benchmark import DistributedKoalasBenchmark
from benchmarking_modin.distributed_modin_benchmark import DistributedModinBenchmark
from benchmarking_spark.distributed_spark_benchmark import DistributedSparkBenchmark
from benchmarking_dask.distributed_dask_benchmark import DistributedDaskBenchmark
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
    koalas_runner = DistributedKoalasBenchmark(filesystem=filesystem)
    modin_runner = DistributedModinBenchmark(filesystem=filesystem)
    spark_runner = DistributedSparkBenchmark(filesystem=filesystem)
    dask_runner = DistributedDaskBenchmark(filesystem=filesystem)

    for file in parquet_files:
        print(f"\n=== Processing: {file} ===")

        print("Running Koalas...")
        koalas_results = koalas_runner.run_benchmark(file)
        distributed_koalas_benchmarks = get_results(koalas_results).set_index("task")

        print("Running Modin...")
        modin_results = modin_runner.run_benchmark(file)
        distributed_modin_benchmarks = get_results(modin_results).set_index("task")

        print("Running Spark...")
        spark_results = spark_runner.run_benchmark(file)
        distributed_spark_benchmarks = get_results(spark_results).set_index("task")

        print("Running Dask...")
        dask_results = dask_runner.run_benchmark(file)
        distributed_dask_benchmarks = get_results(dask_results).set_index("task")

        # Combine all benchmark results
        df = pd.concat(
            [
                distributed_koalas_benchmarks.duration,
                distributed_modin_benchmarks.duration,
                distributed_spark_benchmarks.duration,
                distributed_dask_benchmarks.duration,
            ],
            axis=1,
            keys=["koalas", "modin", "spark", "dask"],
        )

        # Save to GCS
        csv_buffer = StringIO()
        df.to_csv(csv_buffer)
        csv_buffer.seek(0)

        base_name = os.path.basename(file).replace(".parquet", "")
        output_path = f"{output_bucket_path}/distributed_benchmark_{base_name}.csv"

        with filesystem.open(output_path, "w") as f:
            f.write(csv_buffer.getvalue())

        print(f"✅ Saved benchmark result to: gs://{output_path}")
