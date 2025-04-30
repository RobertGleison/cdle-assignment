from benchmarking_joblib.distributed_joblib_benchmark import DistributedJoblibBenchmark
from benchmarking_koalas.distributed_koalas_benchmark import DistributedKoalasBenchmark
from benchmarking_rapids.distributed_rapids_benchmark import DistributedRapidsBenchmark
from benchmarking_modin.distributed_modin_benchmark import DistributedModinBenchmark
from benchmarking_spark.distributed_spark_benchmark import DistributedSparkBenchmark
from benchmarking_dask.distributed_dask_benchmark import DistributedDaskBenchmark
from benchmark_setup import get_results
from datetime import datetime
import pandas as pd

if __name__ == "__main__":
    
    distributed_joblib_benchmarks = get_results(DistributedJoblibBenchmark().benchmarks_results)
    distributed_rapids_benchmarks = get_results(DistributedRapidsBenchmark().benchmarks_results)
    distributed_koalas_benchmarks = get_results(DistributedKoalasBenchmark().benchmarks_results)
    distributed_modin_benchmarks = get_results(DistributedModinBenchmark().benchmarks_results)
    distributed_spark_benchmarks = get_results(DistributedSparkBenchmark().benchmarks_results)
    distributed_dask_benchmarks = get_results(DistributedDaskBenchmark().benchmarks_results)

    df = pd.concat(
        [
        distributed_joblib_benchmarks.duration,
        distributed_rapids_benchmarks.duration,
        distributed_koalas_benchmarks.duration,
        distributed_modin_benchmarks.duration,
        distributed_spark_benchmarks.duration,
        distributed_dask_benchmarks.duration
        ],
        axis=1,
        keys=['joblib',
              'rapids',
              'koalas',
              'modin',
              'spark',
              'dask'
            ]
        )

    filename = 'distributed_benchmark_' + datetime.now().strftime("%Y%m%d_%H%M%S") + ".csv"
    df.to_csv(f"logs/{filename}")
