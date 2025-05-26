from pyspark.sql import SparkSession

_spark = None

def get_spark():
    global _spark
    if _spark is None:
        _spark = (
            SparkSession.builder
            .master("k8s://https://34.133.96.19:6443")  # Replace <k8s-api-server> with your API endpoint
            .appName("BenchmarkApp")
            .config("spark.kubernetes.container.image", "gcr.io/bigdata-452109/cdle-benchmarking:latest")
            .config("spark.executor.instances", "3")  # Use 3 executors
            .config("spark.executor.cores", "1")
            .config("spark.executor.memory", "40g")
            .config("spark.driver.memory", "10g")
            .config("spark.task.cpus", "1")
            .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
            .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
            .config("spark.jars", "/opt/spark/jars/gcs-connector-hadoop3-2.2.11-shaded.jar")
            .getOrCreate()
        )
    return _spark
