from .clean_spark import get_clean_local_session, get_clean_cluster_session
import time
import psutil
import os

def get_executor_info(spark):
    """
    Safely get executor information from Spark session
    """
    try:
        # Get basic executor info from Spark context
        sc = spark.sparkContext
        if hasattr(sc, 'defaultParallelism'):
            return f"Default parallelism: {sc.defaultParallelism}"
        else:
            return "N/A (Context not available)"
    except Exception as e:
        return f"N/A (Error: {str(e)[:50]}...)"

def compare_sessions():
    """
    Compare local vs cluster Spark sessions
    """
    print("=" * 60)
    print("SPARK SESSION COMPARISON")
    print("=" * 60)
    
    # Test 1: Local Session (SKIPPED)
    # print("\n1. TESTING LOCAL SESSION")
    # print("-" * 30)
    # start_time = time.time()
    # local_spark = get_clean_local_session("LocalComparison")
    # print(f"Master URL: {local_spark.conf.get('spark.master')}")
    # print(f"Driver Host: {local_spark.conf.get('spark.driver.host')}")
    # print(f"Available Executors: {get_executor_info(local_spark)}")
    # data = [(i, f"item_{i}") for i in range(1000)]
    # df = local_spark.createDataFrame(data, ["id", "name"])
    # count = df.count()
    # print(f"Records processed: {count}")
    # local_spark.stop()
    # local_time = time.time() - start_time
    # print(f"Local session time: {local_time:.2f} seconds")
    
    # Test 2: Cluster Session
    print("\n1. TESTING CLUSTER SESSION")
    print("-" * 30)
    start_time = time.time()
    try:
        from pyspark.sql import SparkSession
        
        # Java and Python environment already set above
        cluster_spark = SparkSession.builder \
            .appName("ClusterComparison") \
            .master("spark://localhost:7077") \
            .config("spark.submit.deployMode", "client") \
            .config("spark.pyspark.python", "python3.10") \
            .config("spark.pyspark.driver.python", "python3.10") \
            .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
            .getOrCreate()
        
        # Set log level to ERROR to reduce log noise
        cluster_spark.sparkContext.setLogLevel("ERROR")
        
        print(f"Master URL: {cluster_spark.conf.get('spark.master')}")
        print(f"Available Executors: {get_executor_info(cluster_spark)}")
        print(f"Spark UI: {cluster_spark.sparkContext.uiWebUrl}")
        data = [(i, f"item_{i}") for i in range(1000)]
        df = cluster_spark.createDataFrame(data, ["id", "name"])
        count = df.count()
        print(f"Records processed: {count}")
        cluster_spark.stop()
        cluster_time = time.time() - start_time
        print(f"Cluster session time: {cluster_time:.2f} seconds")
    except Exception as e:
        print(f"Cluster session failed: {str(e)}")
        print("This is expected if Docker containers are not running.")
        print("To test cluster mode, start the containers with: docker-compose up -d")

def test_resource_usage():
    """
    Test resource usage differences
    """
    print("\n" + "=" * 60)
    print("RESOURCE USAGE COMPARISON")
    print("=" * 60)
    
    # Get initial memory usage
    initial_memory = psutil.virtual_memory().used / (1024 * 1024)  # MB
    
    print(f"\nInitial memory usage: {initial_memory:.2f} MB")
    
    # Test local session memory
    print("\nTesting local session memory usage...")
    local_spark = get_clean_local_session("MemoryTest")
    
    # Create some data to use memory
    data = [(i, f"data_{i}" * 100) for i in range(10000)]
    df = local_spark.createDataFrame(data, ["id", "data"])
    df.cache()  # Cache to use memory
    df.count()
    
    local_memory = psutil.virtual_memory().used / (1024 * 1024)
    print(f"Local session memory usage: {local_memory:.2f} MB")
    print(f"Memory increase: {local_memory - initial_memory:.2f} MB")
    
    local_spark.stop()
    
    # Test cluster session memory
    print("\nTesting cluster session memory usage...")
    try:
        from pyspark.sql import SparkSession
        
        # Set Java 11 environment
        os.environ['JAVA_HOME'] = '/opt/homebrew/Cellar/openjdk@11/11.0.27'
        os.environ['PATH'] = f"/opt/homebrew/Cellar/openjdk@11/11.0.27/bin:{os.environ.get('PATH', '')}"
        
        cluster_spark = SparkSession.builder \
            .appName("MemoryTest") \
            .master("spark://localhost:7077") \
            .config("spark.submit.deployMode", "client") \
            .config("spark.pyspark.python", "python3.10") \
            .config("spark.pyspark.driver.python", "python3.10") \
            .config("spark.driver.host", "192.168.0.224") \
            .config("spark.driver.bindAddress", "192.168.0.224") \
            .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow") \
            .getOrCreate()
        
        # Same data
        data = [(i, f"data_{i}" * 100) for i in range(10000)]
        df = cluster_spark.createDataFrame(data, ["id", "data"])
        df.cache()  # Cache to use memory
        df.count()
        
        cluster_memory = psutil.virtual_memory().used / (1024 * 1024)
        print(f"Cluster session memory usage: {cluster_memory:.2f} MB")
        print(f"Memory increase: {cluster_memory - initial_memory:.2f} MB")
        
        cluster_spark.stop()
        
        print(f"\nMemory comparison:")
        print(f"Local session used: {local_memory - initial_memory:.2f} MB")
        print(f"Cluster session used: {cluster_memory - initial_memory:.2f} MB")
        print("Note: Cluster session memory is distributed across Docker containers")
        
    except Exception as e:
        print(f"Cluster session failed: {str(e)}")
        print("Memory comparison not available for cluster session.")

def show_docker_status():
    """
    Show Docker container status
    """
    print("\n" + "=" * 60)
    print("DOCKER CONTAINER STATUS")
    print("=" * 60)
    
    import subprocess
    
    try:
        # Check if containers are running
        result = subprocess.run(['docker', 'ps', '--filter', 'name=spark'], 
                              capture_output=True, text=True)
        
        if result.returncode == 0 and result.stdout.strip():
            print("Running Spark containers:")
            print(result.stdout)
        else:
            print("No Spark containers running.")
            print("To start containers, run: docker-compose up -d")
            
    except FileNotFoundError:
        print("Docker not found. Make sure Docker is installed and running.")

if __name__ == "__main__":
    show_docker_status()
    compare_sessions()
    test_resource_usage() 