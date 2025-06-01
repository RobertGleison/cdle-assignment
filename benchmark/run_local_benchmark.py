# from benchmarking_joblib.local_joblib_benchmark import LocalJoblibBenchmark
from benchmarking_koalas.local_koalas_benchmark import LocalKoalasBenchmark
# from benchmarking_modin.local_modin_benchmark import LocalModinBenchmark
# from benchmarking_spark.local_spark_benchmark import LocalSparkBenchmark
# from benchmarking_dask.local_dask_benchmark import LocalDaskBenchmark
from benchmark_setup import get_results
from datetime import datetime
import pandas as pd
import os

if __name__ == "__main__":
    profile_flag = False
    folder_path = "../datasets/"
    # https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page to download the datasets

    # Loop over all files in the folder
    for file in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file)

        # Instantiate each benchmark class once
        koalas_runner = LocalKoalasBenchmark(profile=profile_flag)
        # joblib_runner = LocalJoblibBenchmark(profile=profile_flag)
        # modin_runner = LocalModinBenchmark(profile=profile_flag)
        # spark_runner = LocalSparkBenchmark(profile=profile_flag)
        # dask_runner = LocalDaskBenchmark(profile=profile_flag)

        print(f"\n=== Processing: {file_path} ===")

        print("Running Koalas...")
        koalas_results = koalas_runner.run_benchmark(file_path)
        local_koalas_benchmarks = get_results(koalas_results).set_index("task")

        # print("Running Joblib...")
        # joblib_results = joblib_runner.run_benchmark(file_path)
        # local_joblib_benchmarks = get_results(joblib_results).set_index("task")

        # print("Running Modin...")
        # modin_results = modin_runner.run_benchmark(file_path)
        # local_modin_benchmarks = get_results(modin_results).set_index("task")

        # print("Running Spark...")
        # spark_results = spark_runner.run_benchmark(file_path)
        # local_spark_benchmarks = get_results(spark_results).set_index("task")

        # print("Running Dask...")
        # dask_results = dask_runner.run_benchmark(file_path)
        # local_dask_benchmarks = get_results(dask_results)#.set_index("task")

        # # Combine all benchmark results
        # df = pd.concat(
        #     [
        #         #local_koalas_benchmarks.duration,
        #         #local_joblib_benchmarks.duration,
        #         #local_modin_benchmarks.duration,
        #         # local_spark_benchmarks.duration,
        #         local_dask_benchmarks.duration,
        #     ],
        #     axis=1,
        #     keys=[
        #         #"koalas",
        #           #"joblib",
        #           #"modin",
        #         #   "spark",
        #             "dask"
        #             ],
        # )

    os.makedirs('logs', exist_ok=True)
    filename = 'logs/local_benchmark_' + datetime.now().strftime("%Y%m%d_%H%M%S") + ".csv"
    local_koalas_benchmarks.to_csv(filename)
