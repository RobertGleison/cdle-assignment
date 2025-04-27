from benchmark_setup import get_results
from benchmarking_dask.local_dask_benchmark import Benchmark as L_Benchmark_dask
from benchmarking_dask.local_with_filtering_dask_benchmark import Benchmark as LF_Benchmark_dask
from benchmarking_dask.local_with_filtering_and_cache_dask_benchmark import Benchmark as LFC_Benchmark_dask

from benchmarking_joblib.local_joblib_benchmark import Benchmark as L_Benchmark_joblib
from benchmarking_joblib.local_with_filtering_joblib_benchmark import Benchmark as LF_Benchmark_joblib
from benchmarking_joblib.local_with_filtering_and_cache_joblib_benchmark import Benchmark as LFC_Benchmark_joblib

from benchmarking_modin.local_modin_benchmark import Benchmark as L_Benchmark_modin
from benchmarking_modin.local_with_filtering_modin_benchmark import Benchmark as LF_Benchmark_modin
from benchmarking_modin.local_with_filtering_and_cache_modin_benchmark import Benchmark as LFC_Benchmark_modin

from datetime import datetime
import pandas as pd


if __name__ == '__main__':
    file_path = "/home/robert/Desktop/cdle-assignment/datasets/taxis_2009-01.parquet"

    # Benchmark 1: Dask Local
    dask_l = L_Benchmark_dask()
    dask_l.run_benchmark(file_path)
    results_dask_l = dask_l.benchmarks_results

    # Benchmark 2: Dask Local with Filtering
    dask_lf = LF_Benchmark_dask()
    dask_lf.run_benchmark(file_path)
    results_dask_lf = dask_lf.benchmarks_results

    # Benchmark 3: Dask Local with Filtering and Cache
    dask_lfc = LFC_Benchmark_dask()
    dask_lfc.run_benchmark(file_path)
    results_dask_lfc = dask_lfc.benchmarks_results

    # Benchmark 1: Joblib Local
    joblib_l = L_Benchmark_joblib()
    joblib_l.run_benchmark(file_path)
    results_joblib_l = joblib_l.benchmarks_results

    # Benchmark 2: Joblib Local with Filtering
    joblib_lf = LF_Benchmark_joblib()
    joblib_lf.run_benchmark(file_path)
    results_joblib_lf = joblib_lf.benchmarks_results

    # Benchmark 3: Joblib Local with Filtering and Cache
    joblib_lfc = LFC_Benchmark_joblib()
    joblib_lfc.run_benchmark(file_path)
    results_joblib_lfc = joblib_lfc.benchmarks_results

    # Benchmark 1: Modin Local
    modin_l = L_Benchmark_modin()
    modin_l.run_benchmark(file_path)
    results_modin_l = modin_l.benchmarks_results

    # Benchmark 2: Modin Local with Filtering
    modin_lf = LF_Benchmark_modin()
    modin_lf.run_benchmark(file_path)
    results_modin_lf = modin_lf.benchmarks_results

    # Benchmark 3: Modin Local with Filtering and Cache
    modin_lfc = LFC_Benchmark_modin()
    modin_lfc.run_benchmark(file_path)
    results_modin_lfc = modin_lfc.benchmarks_results


    # Combine the results
    list_of_results = [results_dask_l, results_dask_lf, results_dask_lfc, results_joblib_l, results_joblib_lf, results_joblib_lfc, results_modin_l, results_modin_lf, results_modin_lfc]
    dask_benchmarks = {}

    for result in list_of_results:
        for k, v in result.items():
            dask_benchmarks.setdefault(k, []).extend(v)

    # Process the results
    dask_res_temp = get_results(dask_benchmarks).set_index('task')

    # Save results to CSV
    filename = 'dask_local' + datetime.now().strftime("%H%M%S")
    dask_res_temp.to_csv(f"logs/{filename}.csv")
