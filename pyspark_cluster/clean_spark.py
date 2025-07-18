import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark import SparkContext
import os
import logging
import sys

# Suppress all Spark and Py4J logging
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("pyspark").setLevel(logging.ERROR)
logging.getLogger("org.apache.spark").setLevel(logging.ERROR)
logging.getLogger("org.apache.hadoop").setLevel(logging.ERROR)
logging.getLogger("org.apache.hadoop.util.NativeCodeLoader").setLevel(logging.ERROR)
logging.getLogger("org.apache.spark.util.GarbageCollectionMetrics").setLevel(logging.ERROR)

# Suppress stdout for Spark initialization messages
class SuppressOutput:
    def __enter__(self):
        self._original_stdout = sys.stdout
        self._original_stderr = sys.stderr
        sys.stdout = open(os.devnull, 'w')
        sys.stderr = open(os.devnull, 'w')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stderr.close()
        sys.stdout = self._original_stdout
        sys.stderr = self._original_stderr

def get_clean_spark_session(app_name: str = "CleanPySparkApp", master_url: str = "local[*]") -> SparkSession:
    """
    Create and return a SparkSession with all warnings suppressed.
    """
    # Stop any existing SparkContext
    try:
        if SparkContext._active_spark_context is not None:
            SparkContext._active_spark_context.stop()
            SparkContext._active_spark_context = None
    except:
        pass
    
    # Set environment variables to suppress warnings
    os.environ['SPARK_LOCAL_IP'] = 'localhost'
    os.environ['SPARK_DRIVER_HOST'] = 'localhost'
    os.environ['SPARK_LOCAL_DIRS'] = '/tmp'
    os.environ['HADOOP_HOME'] = '/tmp'  # Use a writable temp dir
    os.environ['SPARK_LOG_LEVEL'] = 'ERROR'
    
    # Disable security manager for Java 17+ compatibility
    os.environ['SPARK_DRIVER_OPTS'] = '-Djava.security.manager=allow'
    
    with SuppressOutput():
        spark = (
            SparkSession.builder
            .appName(app_name)
            .master(master_url)
            .config("spark.driver.host", "localhost")
            .config("spark.driver.bindAddress", "localhost")
            .config("spark.driver.allowMultipleContexts", "true")
            .config("spark.ui.port", "4040")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.dynamicAllocation.enabled", "false")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.hadoop.fs.defaultFS", "file:///")
            .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
            .config("spark.hadoop.fs.local.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
            .config("spark.eventLog.enabled", "false")
            .getOrCreate()
        )
    return spark

def get_clean_cluster_session(app_name: str = "CleanClusterApp") -> SparkSession:
    return get_clean_spark_session(app_name, "spark://spark-master:7077")

def get_clean_local_session(app_name: str = "LocalPySparkApp") -> SparkSession:
    return get_clean_spark_session(app_name, "local[*]")

# Example usage
if __name__ == "__main__":
    print("Creating clean Spark session...")
    spark = get_clean_local_session("TestApp")
    print(f"Spark session created successfully: {spark}")
    data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
    df = spark.createDataFrame(data, ["name", "age"])
    print("DataFrame created successfully:")
    df.show()
    spark.stop()
    print("Spark session stopped.") 