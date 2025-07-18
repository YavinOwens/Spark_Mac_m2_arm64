# Spark Cluster Setup and Imports
# Use this code in your Jupyter notebooks to connect to the Spark cluster

# ============================================================================
# ESSENTIAL IMPORTS FOR SPARK CLUSTER
# ============================================================================

# Core PySpark imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Spark ML imports
from pyspark.ml.feature import *
from pyspark.ml.classification import *
from pyspark.ml.regression import *
from pyspark.ml.evaluation import *
from pyspark.ml.tuning import *

# Spark Streaming (if needed)
from pyspark.streaming import StreamingContext

# ============================================================================
# SPARK SESSION SETUP
# ============================================================================

def create_spark_session(app_name="MySparkApp"):
    """
    Create and configure SparkSession connected to the cluster
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("spark://spark-master:7077") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewJoin.enabled", "true") \
        .config("spark.sql.adaptive.localShuffleReader.enabled", "true") \
        .config("spark.sql.adaptive.optimizeSkewedJoin.enabled", "true") \
        .getOrCreate()
    
    return spark

# ============================================================================
# QUICK START FUNCTION
# ============================================================================

def quick_start():
    """
    Quick setup - creates Spark session and returns it
    """
    spark = create_spark_session()
    print("✅ Spark session created successfully!")
    print(f"🔗 Spark Master: {spark.conf.get('spark.master')}")
    print(f"📊 Spark Version: {spark.version}")
    return spark

# ============================================================================
# USAGE EXAMPLES
# ============================================================================

def example_usage():
    """
    Example code showing how to use the Spark cluster
    """
    # Create Spark session
    spark = create_spark_session("ExampleApp")
    
    # Example 1: Create sample data
    data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    df = spark.createDataFrame(data, ["name", "age"])
    
    # Example 2: Read from mounted data directory
    # df = spark.read.csv("/home/jovyan/data/your_file.csv", header=True)
    
    # Example 3: Basic transformations
    result = df.filter(col("age") > 25).groupBy("name").count()
    
    # Example 4: Show results
    result.show()
    
    return spark, df, result

# ============================================================================
# COMMON DATA SOURCES
# ============================================================================

def read_csv_file(file_path, header=True, infer_schema=True):
    """
    Read CSV file from the mounted data directory
    """
    spark = create_spark_session()
    return spark.read.csv(file_path, header=header, inferSchema=infer_schema)

def read_json_file(file_path):
    """
    Read JSON file from the mounted data directory
    """
    spark = create_spark_session()
    return spark.read.json(file_path)

def read_parquet_file(file_path):
    """
    Read Parquet file from the mounted data directory
    """
    spark = create_spark_session()
    return spark.read.parquet(file_path)

# ============================================================================
# CLUSTER MONITORING
# ============================================================================

def get_cluster_info():
    """
    Get information about the Spark cluster
    """
    spark = create_spark_session()
    
    print("🔍 Spark Cluster Information:")
    print(f"   Master URL: {spark.conf.get('spark.master')}")
    print(f"   Spark Version: {spark.version}")
    print(f"   Application ID: {spark.sparkContext.applicationId}")
    print(f"   Driver Host: {spark.sparkContext.driverHost}")
    print(f"   Driver Port: {spark.sparkContext.driverPort}")
    
    return spark

# ============================================================================
# JUPYTER NOTEBOOK USAGE
# ============================================================================

"""
# Copy and paste this code into your Jupyter notebook:

# 1. Import the setup
from spark_cluster_setup import *

# 2. Create Spark session
spark = quick_start()

# 3. Start working with data
# Example: Read a CSV file
df = read_csv_file("/home/jovyan/data/your_data.csv")

# Example: Create sample data
data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
df = spark.createDataFrame(data, ["name", "age"])

# Example: Basic operations
df.show()
df.printSchema()
df.count()

# Example: Transformations
result = df.filter(col("age") > 25).groupBy("name").count()
result.show()
""" 