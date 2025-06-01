from pyspark.sql import SparkSession

_spark = None

def get_spark():
    global _spark
    if _spark is None:
        _spark = SparkSession.builder \
            .master("local[*]") \
            .appName("BenchmarkApp") \
            .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
            .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
            .config("spark.jars", "/home/robertgleison12/cdle-assignment/benchmark/jars/gcs-connector-hadoop3-2.2.11-shaded.jar") \
            .config("spark.executor.memory", "8g") \
            .config("spark.driver.memory", "40g") \
            .config("spark.executor.instances", "1") \
            .config("spark.task.cpus", "1") \
            .getOrCreate()
    return _spark
