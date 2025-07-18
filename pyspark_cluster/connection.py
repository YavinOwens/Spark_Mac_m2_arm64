import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark import SparkContext
import os
import logging

# Suppress Spark logging
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("pyspark").setLevel(logging.ERROR)
logging.getLogger("org.apache.spark").setLevel(logging.ERROR)
logging.getLogger("org.apache.hadoop").setLevel(logging.ERROR)
logging.getLogger("org.apache.hadoop.util.NativeCodeLoader").setLevel(logging.ERROR)
logging.getLogger("org.apache.spark.util.GarbageCollectionMetrics").setLevel(logging.ERROR)

def get_spark_session(app_name: str = "PySparkApp") -> SparkSession:
    """
    Create and return a SparkSession connected to the cluster defined in docker-compose.yml.
    """
    # First, try to stop any existing SparkContext
    try:
        if SparkContext._active_spark_context is not None:
            SparkContext._active_spark_context.stop()
            SparkContext._active_spark_context = None
            print("Stopped existing SparkContext")
    except:
        pass
    
    master_url = os.getenv("SPARK_MASTER_URL", "spark://localhost:7077")
    
    # Get host IP that Docker containers can reach
    import subprocess
    try:
        # Get the host IP address that Docker containers can reach
        result = subprocess.run(['ifconfig', 'en0'], capture_output=True, text=True)
        if result.returncode == 0:
            for line in result.stdout.split('\n'):
                if 'inet ' in line and '127.0.0.1' not in line:
                    host_ip = line.split()[1]
                    break
            else:
                host_ip = '192.168.65.1'  # Default Docker Desktop host IP
        else:
            host_ip = '192.168.65.1'  # Default fallback
    except:
        host_ip = '192.168.65.1'  # Default fallback
    
    # Set environment variables for Java security manager
    os.environ['SPARK_LOCAL_IP'] = host_ip
    os.environ['SPARK_DRIVER_HOST'] = host_ip
    os.environ['HADOOP_HOME'] = '/dev/null'  # Disable Hadoop native libraries
    os.environ['SPARK_LOG_LEVEL'] = 'ERROR'
    os.environ['HADOOP_USER_NAME'] = 'spark'  # Set Hadoop user to avoid security issues
    
    # Configure Python for executors (use Docker container Python)
    os.environ['PYSPARK_PYTHON'] = '/opt/bitnami/python/bin/python3'
    os.environ['PYSPARK_DRIVER_PYTHON'] = 'python3'  # Driver runs on host
    
    # Java options (only non-Spark options)
    java_options = [
        "-Dhadoop.native.lib=false",
        "-Dhadoop.home.dir=/dev/null"
    ]
    
    java_options_str = " ".join(java_options)
    
    return (
        SparkSession.builder
        .appName(app_name)
        .master(master_url)
        # Suppress warnings and configure logging
        .config("spark.driver.extraJavaOptions", java_options_str)
        .config("spark.executor.extraJavaOptions", java_options_str)
        # Spark-specific configurations
        .config("spark.driver.host", host_ip)
        .config("spark.driver.bindAddress", host_ip)
        .config("spark.ui.port", "4040")
        .config("spark.driver.allowMultipleContexts", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.dynamicAllocation.enabled", "false")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # Configure Python for executors (Docker containers)
        .config("spark.pyspark.python", "/opt/bitnami/python/bin/python3")
        .config("spark.pyspark.driver.python", "python3")
        .config("spark.executorEnv.PYSPARK_PYTHON", "/opt/bitnami/python/bin/python3")
        # Configure file system to avoid Hadoop native library issues
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.fs.local.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        # Additional warning suppression
        .config("spark.eventLog.enabled", "false")
        .config("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors", "")
        .config("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors", "")
        .getOrCreate()
    )

def get_local_spark_session(app_name: str = "LocalPySparkApp") -> SparkSession:
    """
    Create and return a SparkSession in local mode for testing.
    """
    # First, try to stop any existing SparkContext
    try:
        if SparkContext._active_spark_context is not None:
            SparkContext._active_spark_context.stop()
            SparkContext._active_spark_context = None
            print("Stopped existing SparkContext")
    except:
        pass
    
    # Set environment variables for Hadoop authentication
    os.environ['HADOOP_HOME'] = '/dev/null'
    os.environ['HADOOP_USER_NAME'] = 'spark'
    
    # Java options (only non-Spark options)
    java_options = [
        "-Dhadoop.native.lib=false",
        "-Dhadoop.home.dir=/dev/null"
    ]
    
    java_options_str = " ".join(java_options)
    
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        # Suppress warnings and configure logging
        .config("spark.driver.extraJavaOptions", java_options_str)
        .config("spark.executor.extraJavaOptions", java_options_str)
        # Spark-specific configurations (use localhost for local mode)
        .config("spark.driver.host", "localhost")
        .config("spark.driver.bindAddress", "localhost")
        .config("spark.ui.port", "4040")
        .config("spark.driver.allowMultipleContexts", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.dynamicAllocation.enabled", "false")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        # Configure file system to avoid Hadoop native library issues
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.hadoop.fs.local.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        # Additional warning suppression
        .config("spark.eventLog.enabled", "false")
        .config("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors", "")
        .config("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors", "")
        .getOrCreate()
    ) 