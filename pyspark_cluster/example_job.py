from .clean_spark import get_clean_local_session, get_clean_cluster_session

def run_example_job(use_cluster: bool = False):
    """
    Run an example Spark job with warnings suppressed.
    
    Args:
        use_cluster: If True, use cluster mode; if False, use local mode
    """
    if use_cluster:
        spark = get_clean_cluster_session("ExampleClusterJob")
        print("Running example job on Spark cluster...")
    else:
        spark = get_clean_local_session("ExampleLocalJob")
        print("Running example job in local mode...")
    
    # Create sample data
    data = [(1, "Alice", 25), (2, "Bob", 30), (3, "Charlie", 35)]
    df = spark.createDataFrame(data, ["id", "name", "age"])
    
    print("Sample DataFrame:")
    df.show()
    
    # Perform some operations
    print("DataFrame schema:")
    df.printSchema()
    
    print("Count of records:")
    print(df.count())
    
    print("Average age:")
    df.select("age").agg({"age": "avg"}).show()
    
    spark.stop()
    print("Job completed successfully!")

if __name__ == "__main__":
    # Run in local mode by default
    run_example_job(use_cluster=False)
    
    # Uncomment to run on cluster
    # run_example_job(use_cluster=True) 