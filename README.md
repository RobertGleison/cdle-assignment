
# Benchmarking - Python Tools for Data Processing

This repository includes a benchmarking setup for calculating the processing time of some datasets with identical tasks, both on a single-node machine and in a cluster.

## Running the Benchmark for Single Node on a Local Machine

To run the benchmarking on a local machine, follow these steps:

### 1. Set Python Version
Ensure that your local Python environment matches the version specified in the `.python-version` file. This ensures that the correct Python version is used to mount the virtual environment.

### 2. Setup Virtual Environment
Navigate to the `benchmark` folder and run the following command to set up the virtual environment:
```
make setup
```
This will create a virtual environment with the correct Python version and install all the necessary dependencies required to run the benchmark.

To remove the virtual environment and its dependencies, run:
```
make clean
```

### 3. Activate Virtual Environment
In the `benchmark` folder, activate the virtual environment using the following command:
```
source .venv/bin/activate
```

### 4. Debugging with VSCode
To enable debugging in VSCode, add the following configuration to your `launch.json` file:
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

### 5. Running the Benchmark
Once your environment is set up and activated, execute the benchmark file using the following command (or any other way to do it):
```
PYTHONPATH=.. poetry run python run_local_benchmark.py
```
This python_path is a reference from the root workfolder.

## Datasets Used for Benchmarking
The datasets used in the benchmark can be found on the official New York City TLC website:
[NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

# Infrastructure with Terraform

The `terraform_infra` folder contains the Infrastructure-as-Code (IaC) configuration to create a cluster and a virtual machine in Google Cloud Platform (GCP) using a student voucher. This allows you to run the benchmarking in a cloud environment.

