from benchmark_setup import get_results
from benchmarking_dask.local_dask_benchmark import Benchmark as L_Banchmark
from benchmarking_dask.local_with_filtering_dask_benchmark import Benchmark as LF_Banchmark
from benchmarking_dask.local_with_filtering_and_cache_dask_benchmark import Benchmark as LFC_Banchmark
from datetime import datetime
import pandas as pd


if __name__ == '__main__':
    file_path = "/home/robert/Desktop/cdle-assignment/datasets/taxis_2009-01.parquet"

    # Benchmark 1: Dask Local
    dask_l = L_Banchmark()
    dask_l.run_benchmark(file_path)
    results_dask_l = dask_l.benchmarks_results

    # Benchmark 2: Dask Local with Filtering
    dask_lf = LF_Banchmark()
    dask_lf.run_benchmark(file_path)
    results_dask_lf = dask_lf.benchmarks_results

    # Benchmark 3: Dask Local with Filtering and Cache
    dask_lfc = LFC_Banchmark()
    dask_lfc.run_benchmark(file_path)
    results_dask_lfc = dask_lfc.benchmarks_results

    # Combine the results
    list_of_results = [results_dask_l, results_dask_lf, results_dask_lfc]
    dask_benchmarks = {}

    for result in list_of_results:
        for k, v in result.items():
            dask_benchmarks.setdefault(k, []).extend(v)

    # Process the results
    dask_res_temp = get_results(dask_benchmarks).set_index('task')

    # Save results to CSV
    filename = 'dask_local' + datetime.now().strftime("%H%M%S")
    dask_res_temp.to_csv(f"logs/{filename}.csv")
