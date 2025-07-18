#!/usr/bin/env python3
"""
Working Docker Cluster Mode Demo - No Python Version Issues
"""

print("🔧 Testing Spark Docker Cluster Mode (No Version Conflicts)...")
print("=" * 60)

try:
    from pyspark_cluster.connection import get_spark_session
    
    print("📡 Connecting to Spark in Docker cluster mode...")
    spark = get_spark_session("DockerClusterDemo")
    print("✅ Connected successfully!")
    print(f"   Spark version: {spark.version}")
    print(f"   Master URL: {spark.conf.get('spark.master')}")
    print(f"   App Name: {spark.conf.get('spark.app.name')}")
    
    print("\n📊 Testing DataFrame creation...")
    data = [
        ("Alice", 25, "Engineer", 75000),
        ("Bob", 30, "Manager", 85000),
        ("Charlie", 35, "Engineer", 80000),
        ("Diana", 28, "Analyst", 65000),
    ]
    
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("role", StringType(), True),
        StructField("salary", IntegerType(), True)
    ])
    
    df = spark.createDataFrame(data, schema)
    print("✅ Sample DataFrame created!")
    
    print("\n📋 Data Preview:")
    df.show()
    
    print("\n📈 Testing aggregation...")
    from pyspark.sql.functions import avg, count
    result = df.groupBy("role").agg(
        count("*").alias("count"),
        avg("salary").alias("avg_salary")
    )
    result.show()
    
    print("\n📊 Testing SQL...")
    df.createOrReplaceTempView("employees")
    sql_result = spark.sql("SELECT role, AVG(salary) as avg_sal FROM employees GROUP BY role ORDER BY avg_sal DESC")
    sql_result.show()
    
    spark.stop()
    print("\n✅ Docker cluster mode test completed successfully!")
    print("🎉 Perfect for distributed development and testing!")
    
except Exception as e:
    print(f"❌ Error: {str(e)}")
    import traceback
    traceback.print_exc() 