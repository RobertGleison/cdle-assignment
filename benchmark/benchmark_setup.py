import pandas as pd
import time
import os
import cProfile
import pstats
import io

def benchmark(f, df, benchmarks, name, tool, profile=False, **kwargs):
    """Benchmark the given function against the given DataFrame.

    Parameters:
    - f: function to benchmark
    - df: data frame
    - benchmarks: container for benchmark results
    - name: task name
    - profile: whether to enable cProfile for this function

    Returns:
    - Duration (in seconds) of the given operation
    """
    start_time = time.time()

    if profile:
        pr = cProfile.Profile()
        pr.enable()
        result = f(df, **kwargs)
        pr.disable()

        # Save profiling result to file
        os.makedirs("profiles", exist_ok=True)
        profile_output = io.StringIO()
        stats = pstats.Stats(pr, stream=profile_output).sort_stats(pstats.SortKey.CUMULATIVE)
        stats.print_stats(20)  # Top 20 slowest calls

        safe_name = f"{tool}_{name}".replace(" ", "_").replace("/", "_")
        with open(f"profiles/{safe_name}.prof.txt", "w") as f:
            f.write(profile_output.getvalue())
    else:
        result = f(df, **kwargs)

    duration = time.time() - start_time
    benchmarks['duration'].append(duration)
    benchmarks['task'].append(name)
    print(f"{name} took: {duration:.4f} seconds")
    return duration


def get_results(benchmarks):
    """Return a pandas DataFrame containing benchmark results."""
    return pd.DataFrame.from_dict(benchmarks)