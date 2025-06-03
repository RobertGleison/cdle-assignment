# Data Processing Benchmark

This project provides a benchmarking suite for comparing the performance of various distributed data processing frameworks (Koalas, Modin, Spark, Dask, Joblib) on large datasets. It includes scripts for running benchmarks locally, on a single VM, or in a distributed cluster, with results exportable to local files or Google Cloud Storage.

---

## Prerequisites

- **Python**: Version specified in `benchmark/.python-version` (e.g., 3.10.x)
- **pyenv**: For managing Python versions ([pyenv installation guide](https://github.com/pyenv/pyenv))
- **Java JDK**: OpenJDK 17+ (automatically installed by the Makefile if missing)
- **Poetry**: For dependency management (installed by the Makefile)
- **Google Cloud SDK**: If using GCS features

---

## Setup Instructions

1. **Clone the repository** and enter the `benchmark` directory:
   ```sh
   cd benchmark
   ```

2. **Run the setup using Makefile** (installs JDK, pyenv, creates virtualenv, installs Poetry and dependencies):
   ```sh
   make setup
   ```

   - If you don't have `pyenv` or `jdk` installed, the Makefile will prompt or install them.
   - Poetry dependencies are defined in `pyproject.toml`.

3. **Activate virtual env**:
    In the `benchmark` folder, activate the virtual environment using the following command:
    ```
    source .venv/bin/activate
    ```

4. **(Optional) Clean the environment**:
   ```sh
   make clean
   ```

---

## Running Benchmarks

All commands below should be run from the `benchmark` directory (ensure your virtual env is activated).

- **Local Benchmark**:
  ```sh
  PYTHONPATH=.. poetry run python run_local_benchmark.py
  ```

- **Single VM Benchmark**:
  ```sh
  PYTHONPATH=.. poetry run python run_vm_single_node_benchmark.py
  ```

- **Distributed Cluster Benchmark**:
  ```sh
  PYTHONPATH=.. poetry run python run_cluster_distributed_benchmark.py
  ```

- **Note**: For GCS integration, set up your `.env` file with `GCP_BUCKET_PATH` and `GCP_OUTPUT_BUCKET_PATH`. If you want to run the benchmarking localy in your computed, you can write the files path inside run_local_benchmark and run it.

---

## Project Structure

- `benchmark/`
  - `run_local_benchmark.py`: Runs benchmarks on local datasets.
  - `run_vm_single_node_benchmark.py`: Runs benchmarks on a single VM, reading/writing from GCS.
  - `run_cluster_distributed_benchmark.py`: Runs distributed benchmarks, reading/writing from GCS.
  - `benchmark_setup.py`: Common setup and result utilities.
  - `utils/reduce_parquet_file.py`: Utility to reduce large Parquet files by sampling rows.
  - `jars/gcs-connector-hadoop3-2.2.11-shaded.jar`: GCS connector for Spark/Hadoop.
  - `benchmarking_*`: Framework-specific benchmarking modules.
  - `profiles/`: Results of cProfiler execution (profiling).
  - `pyproject.toml`, `poetry.lock`: Dependency management.
  - `.python-version`: Python version for pyenv.

---

## Profiling with cProfile

This project supports performance profiling using Python's built-in `cProfile` module. Profiling allows you to analyze the time spent in different parts of your code, helping you identify bottlenecks.

### How to Enable Profiling

1. **Edit the desired benchmark script** (`run_local_benchmark.py`, `run_vm_single_node_benchmark.py`, or `run_cluster_distributed_benchmark.py`).
2. Locate the line where the benchmark class is instantiated, e.g.:
   ```python
   profile_flag = False
   ```
3. **Set the `profile` flag to `True`:**
   ```python
   profile_flag = True
   ```
    This will trigger cProfiler and after run the benchmarks,it will create all profiles csv files.

4. **Run the benchmark as usual.**
   Profiling results will be saved in the `profiles/` directory.

---

## VSCode Integration for Debug

- If you are using vscode or forks of vscode, add these configs to `.vscode/launch.json` for easy debugging from python run*.py files:
  - **Run/Debug Python scripts** with the correct `PYTHONPATH` and virtualenv.
  - Example configuration:
    ```json
    {
      "name": "Python: Current File",
      "type": "python",
      "request": "launch",
      "program": "${file}",
      "console": "integratedTerminal",
      "env": {
        "PYTHONPATH": "${workspaceFolder}"
      },
      "python": "${workspaceFolder}/benchmark/.venv/bin/python"
    }
    ```






