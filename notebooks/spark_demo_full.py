from pyspark.sql import SparkSession, Row, functions as F, Window
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer, Tokenizer, HashingTF, IDF
from pyspark.ml.evaluation import BinaryClassificationEvaluator, RegressionEvaluator
from pyspark.ml.regression import LinearRegression, LinearRegressionModel
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql.types import FloatType, ArrayType, IntegerType
from pyspark.sql.functions import udf, explode, array
import os

# Sedona imports for SparkSession config
try:
    from sedona.utils import SedonaKryoRegistrator, KryoSerializer
    sedona_serializer_config = True
except ImportError:
    sedona_serializer_config = False

# 1. Spark Session Setup
spark = (
    SparkSession.builder
    .appName("Local Spark Full Demo")
    .master("spark://localhost:7077")
    .config("spark.eventLog.gcMetrics.youngGenerationGarbageCollectors", "G1 Young Generation")
    .config("spark.eventLog.gcMetrics.oldGenerationGarbageCollectors", "G1 Old Generation")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
print("✅ Spark version:", spark.version)
print("✅ Master URL:", spark.sparkContext.master)

# 2. DataFrame Creation and Basic Operations
# Expanded and balanced demo data for ML pipeline and cross-validation
data = [
    Row(name="Alice", age=34, income=70000, label=1),
    Row(name="Bob", age=45, income=48000, label=0),
    Row(name="Cathy", age=29, income=62000, label=1),
    Row(name="David", age=41, income=54000, label=0),
    Row(name="Eve", age=36, income=71000, label=1),
    Row(name="Frank", age=38, income=50000, label=0),
    Row(name="Grace", age=32, income=68000, label=1),
    Row(name="Heidi", age=40, income=52000, label=0),
    Row(name="Ivan", age=37, income=69000, label=1),
    Row(name="Judy", age=43, income=51000, label=0),
    Row(name="Karl", age=35, income=72000, label=1),
    Row(name="Liam", age=39, income=53000, label=0),
    Row(name="Mallory", age=33, income=66000, label=1),
    Row(name="Niaj", age=44, income=55000, label=0),
    Row(name="Olivia", age=31, income=73000, label=1),
    Row(name="Peggy", age=42, income=56000, label=0)
]
df = spark.createDataFrame(data)
print("\nOriginal DataFrame:")
df.show()

# 3. Reading and Writing CSV Files
csv_path = "demo_people.csv"
df.write.mode("overwrite").csv(csv_path, header=True)
print(f"\nWrote DataFrame to CSV: {csv_path}")
df_csv = spark.read.csv(csv_path, header=True, inferSchema=True)
print("Read DataFrame from CSV:")
df_csv.show()

# 4. DataFrame Transformations (withColumn, UDF, map)
def age_category(age):
    return "young" if age < 35 else "senior"
age_category_udf = udf(age_category)
df = df.withColumn("age_category", age_category_udf(df.age))
print("\nDataFrame with age_category column:")
df.show()

# 5. Handling Missing Data
data_with_missing = [Row(name="Eve", age=None, income=50000, label=1),
                     Row(name="Frank", age=38, income=None, label=0)]
df_missing = spark.createDataFrame(data_with_missing)
df_combined = df.unionByName(df_missing, allowMissingColumns=True)
print("\nDataFrame with missing values:")
df_combined.show()
# Update: Fill nulls in both 'age' and 'income' for ML steps
df_filled = df_combined.fillna({"age": 0, "income": 0})
print("\nFill missing ages with 0:")
df_filled.show()

# 6. SQL: Temp Views, Aggregation, Group By, Join
df.createOrReplaceTempView("people")
df_filled.createOrReplaceTempView("people_filled")
print("\nSQL: People with income > 50000:")
sql_result = spark.sql("SELECT name, age FROM people WHERE income > 50000")
sql_result.show()
print("\nSQL: Group by age_category and count:")
sql_group = spark.sql("SELECT age_category, COUNT(*) as count FROM people GROUP BY age_category")
sql_group.show()
print("\nSQL: Join example:")
sql_join = spark.sql("SELECT p.name, p.age, f.income FROM people p JOIN people_filled f ON p.name = f.name")
sql_join.show()

# 7. MLlib: Feature Scaling (StandardScaler)
# Use handleInvalid='skip' for VectorAssembler
assembler = VectorAssembler(inputCols=["age", "income"], outputCol="features_raw", handleInvalid="skip")
df_ml = assembler.transform(df_filled)
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)
scaler_model = scaler.fit(df_ml)
df_scaled = scaler_model.transform(df_ml)
print("\nScaled features:")
df_scaled.select("name", "features").show(truncate=False)

# 8. MLlib: Train/Test Split, Model Training, Evaluation
train, test = df_scaled.randomSplit([0.7, 0.3], seed=42)
lr = LogisticRegression(featuresCol="features", labelCol="label")
model = lr.fit(train)
predictions = model.transform(test)
print("\nLogistic Regression Predictions (test set):")
predictions.select("name", "age", "income", "probability", "prediction").show(truncate=False)
evaluator = BinaryClassificationEvaluator(labelCol="label")
auc = evaluator.evaluate(predictions)
print(f"AUC on test set: {auc:.3f}")

# 9. ADVANCED: Window Functions (Ranking, Moving Average)
print("\nWindow Functions: Ranking and Moving Average")
window_spec = Window.orderBy(F.col("income").desc())
df_ranked = df.withColumn("rank", F.rank().over(window_spec))
df_ranked = df_ranked.withColumn("moving_avg_income", F.avg("income").over(window_spec.rowsBetween(-1, 1)))
df_ranked.show()

# 10. ADVANCED: Explode and Array Operations
print("\nExplode and Array Operations")
df_array = df.withColumn("scores", array(F.lit(1), F.lit(2), F.lit(3)))
df_exploded = df_array.select("name", explode("scores").alias("score"))
df_exploded.show()

# 11. ADVANCED: Pivot Table
print("\nPivot Table Example")
df_pivot = df.groupBy("age_category").pivot("label").agg(F.avg("income"))
df_pivot.show()

# 12. ADVANCED: Broadcast Join
print("\nBroadcast Join Example")
small_df = spark.createDataFrame([Row(name="Alice", country="US"), Row(name="Bob", country="UK")])
broadcast_joined = df.join(F.broadcast(small_df), on="name", how="left")
broadcast_joined.show()

# 13. ADVANCED: Linear Regression
print("\nLinear Regression Example")
lr_reg = LinearRegression(featuresCol="features", labelCol="income")
lr_model = lr_reg.fit(df_scaled)
lr_predictions = lr_model.transform(df_scaled)
lr_predictions.select("name", "age", "income", "prediction").show()
reg_evaluator = RegressionEvaluator(labelCol="income", predictionCol="prediction", metricName="rmse")
rmse = reg_evaluator.evaluate(lr_predictions)
print(f"Linear Regression RMSE: {rmse:.2f}")

# 14. ADVANCED: KMeans Clustering
print("\nKMeans Clustering Example")
kmeans = KMeans(featuresCol="features", k=2, seed=1)
kmeans_model = kmeans.fit(df_scaled)
kmeans_predictions = kmeans_model.transform(df_scaled)
kmeans_predictions.select("name", "age", "income", "prediction").show()

# 15. ADVANCED: ML Pipeline and Cross-Validation
print("\nML Pipeline and Cross-Validation Example")
indexer = StringIndexer(inputCol="age_category", outputCol="age_cat_index")
assembler = VectorAssembler(inputCols=["age", "income"], outputCol="features_raw", handleInvalid="skip")
scaler = StandardScaler(inputCol="features_raw", outputCol="features", withMean=True, withStd=True)
lr = LogisticRegression(featuresCol="features", labelCol="label")
pipeline = Pipeline(stages=[indexer, assembler, scaler, lr])
paramGrid = ParamGridBuilder() \
    .addGrid(lr.regParam, [0.01, 0.1]) \
    .addGrid(lr.elasticNetParam, [0.0, 0.5]) \
    .build()
crossval = CrossValidator(estimator=pipeline,
                          estimatorParamMaps=paramGrid,
                          evaluator=BinaryClassificationEvaluator(labelCol="label"),
                          numFolds=2)
cv_model = crossval.fit(df_filled)
cv_predictions = cv_model.transform(df_filled)
print("Cross-validated predictions:")
cv_predictions.select("name", "age", "income", "probability", "prediction").show(truncate=False)

# 16. ADVANCED: Reading JSON and Parquet
print("\nReading and Writing JSON/Parquet")
json_path = "demo_people.json"
parquet_path = "demo_people.parquet"
df.write.mode("overwrite").json(json_path)
df.write.mode("overwrite").parquet(parquet_path)
df_json = spark.read.json(json_path)
df_parquet = spark.read.parquet(parquet_path)
print("Read from JSON:")
df_json.show()
print("Read from Parquet:")
df_parquet.show()

# 17. ADVANCED: Saving and Loading Models
print("\nSaving and Loading Models")
model_path = "demo_lr_model"
lr_model.write().overwrite().save(model_path)
loaded_lr_model = LinearRegressionModel.load(model_path)
print("Loaded model coefficients:", loaded_lr_model.coefficients)

# 18. ADVANCED: RDD Broadcast and Accumulator
print("\nRDD Broadcast and Accumulator Example")
rdd = spark.sparkContext.parallelize([1, 2, 3, 4, 5])
broadcast_var = spark.sparkContext.broadcast(10)
accum = spark.sparkContext.accumulator(0)
def add_broadcast(x):
    global accum
    accum += x
    return x + broadcast_var.value
rdd_result = rdd.map(add_broadcast).collect()
print("RDD map with broadcast result:", rdd_result)
print("Accumulator value:", accum.value)

# 20. SPECIALIZED: Time Series - Lag, Lead, and Rolling Window
print("\nTime Series: Lag, Lead, and Rolling Window")
ts_data = [
    ("2024-07-01", 100),
    ("2024-07-02", 110),
    ("2024-07-03", 105),
    ("2024-07-04", 120),
    ("2024-07-05", 115)
]
df_ts = spark.createDataFrame(ts_data, ["date", "value"])
window_spec = Window.orderBy("date")
df_ts = df_ts.withColumn("lag_1", F.lag("value", 1).over(window_spec))
df_ts = df_ts.withColumn("lead_1", F.lead("value", 1).over(window_spec))
df_ts = df_ts.withColumn("rolling_avg_3", F.avg("value").over(window_spec.rowsBetween(-2, 0)))
df_ts.show()

# 21. SPECIALIZED: Text Processing - Tokenization and TF-IDF
print("\nText Processing: Tokenization and TF-IDF")
text_data = [
    (0, "Spark is great for big data processing"),
    (1, "Big data requires scalable solutions"),
    (2, "Spark and Hadoop are popular tools")
]
df_text = spark.createDataFrame(text_data, ["id", "text"])
tokenizer = Tokenizer(inputCol="text", outputCol="words")
words_data = tokenizer.transform(df_text)
hashing_tf = HashingTF(inputCol="words", outputCol="rawFeatures", numFeatures=20)
featurized_data = hashing_tf.transform(words_data)
idf = IDF(inputCol="rawFeatures", outputCol="features")
idf_model = idf.fit(featurized_data)
rescaled_data = idf_model.transform(featurized_data)
rescaled_data.select("id", "features").show(truncate=False)

# 22. SPECIALIZED: Integration with Pandas
import pandas as pd
print("\nIntegration with Pandas")
pdf = pd.DataFrame({"x": [1, 2, 3], "y": [4, 5, 6]})
df_from_pandas = spark.createDataFrame(pdf)
df_from_pandas.show()
# Convert Spark DataFrame to Pandas
pandas_df = df_from_pandas.toPandas()
print(pandas_df)

# 24. SPECIALIZED: External Data Source - Read from SQL Database (JDBC Example)
print("\nRead from SQL Database (JDBC Example)")
print("NOTE: For SQLite, download the JDBC driver from https://github.com/xerial/sqlite-jdbc/releases and add it to Spark with --jars or PYSPARK_SUBMIT_ARGS.")
print("Example:")
print("  spark-submit --jars /path/to/sqlite-jdbc-3.45.1.0.jar ...")
print("  OR set PYSPARK_SUBMIT_ARGS=\"--jars /path/to/sqlite-jdbc-3.45.1.0.jar pyspark-shell\"")
jdbc_url = "jdbc:sqlite:/path/to/your/database.db"
properties = {"driver": "org.sqlite.JDBC"}
# Example usage (uncomment and configure):
# df_sql = spark.read.jdbc(jdbc_url, "your_table", properties=properties)
# df_sql.show()
print("Uncomment and configure the code above to use JDBC with your database. For other databases, use the appropriate JDBC URL and driver.")

# 25. PURE PYTHON GEOSPATIAL: Sedona Geometry Objects
print("\nPure Python Geospatial: Sedona Geometry Objects")
try:
    from sedona.core.geom.envelope import Envelope
    from sedona.core.geom.point import Point
    from shapely import wkt
    # Create points
    p1 = Point(1, 1)
    p2 = Point(2, 2)
    # Envelope (bounding box)
    env = Envelope(0, 3, 0, 3)
    print("Envelope contains p1:", env.contains(p1))
    print("Envelope contains p2:", env.contains(p2))
    # Buffer
    buffered = p1.buffer(1.0)
    print("Buffered geometry:", buffered)
except ImportError:
    print("Sedona not installed or only PyPI version available. Skipping JVM-dependent geospatial features.")

# 26. PYSPARK: Create DataFrame from Python Geometries (WKT)
print("\nPySpark: Create DataFrame from Python Geometries (WKT)")
from pyspark.sql import Row
# Example: create a DataFrame from WKT strings
wkt_points = ["POINT(1 1)", "POINT(2 2)", "POINT(3 3)"]
df_wkt = spark.createDataFrame([Row(id=i, wkt=w) for i, w in enumerate(wkt_points, 1)])
df_wkt.show()

# 27. ADVANCED UDFs: Standard PySpark UDFs and Scala/Java UDF mention
print("\nAdvanced UDFs: Standard PySpark UDFs and Scala/Java UDFs")
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

def multiply_by_two(x):
    return x * 2

multiply_by_two_udf = udf(multiply_by_two, IntegerType())
df_udf = spark.createDataFrame([(1,), (2,), (3,)], ["num"])
df_udf = df_udf.withColumn("doubled", multiply_by_two_udf(df_udf.num))
df_udf.show()

print("\nNote: You can also register UDFs written in Scala/Java and use them from PySpark. See Spark docs for details.")

# 28. GEOSPATIAL JOINS: Points within Polygons (Apache Sedona)
print("\nGeospatial Joins: Points within Polygons (Apache Sedona)")
print("NOTE: Requires 'apache-sedona' and Sedona Spark JARs. Install with: pip install apache-sedona")
try:
    from sedona.register import SedonaRegistrator
    from sedona.sql import functions as S
    SedonaRegistrator.registerAll(spark)
    # Points DataFrame
    points_data = [(1, "POINT(1 1)"), (2, "POINT(2 2)"), (3, "POINT(3 3)"), (4, "POINT(5 5)")]
    df_points = spark.createDataFrame(points_data, ["pid", "wkt_point"])
    df_points = df_points.withColumn("geom_point", S.ST_GeomFromWKT(df_points.wkt_point))
    # Polygons DataFrame
    polygons_data = [
        (10, "POLYGON((0 0, 0 4, 4 4, 4 0, 0 0))"),
        (20, "POLYGON((4 4, 4 6, 6 6, 6 4, 4 4))")
    ]
    df_polygons = spark.createDataFrame(polygons_data, ["gid", "wkt_poly"])
    df_polygons = df_polygons.withColumn("geom_poly", S.ST_GeomFromWKT(df_polygons.wkt_poly))
    # Geospatial join: which points are in which polygons?
    joined = df_points.join(df_polygons, S.ST_Contains(df_polygons.geom_poly, df_points.geom_point))
    print("Points within polygons:")
    joined.select("pid", "wkt_point", "gid", "wkt_poly").show()
except ImportError:
    print("Sedona not installed. Skipping geospatial join section.")

# 29. GEOSPATIAL: Distance Joins (Apache Sedona)
print("\nGeospatial: Distance Joins (Apache Sedona)")
print("NOTE: Requires 'apache-sedona' and Sedona Spark JARs. Install with: pip install apache-sedona")
try:
    from sedona.register import SedonaRegistrator
    from sedona.sql import functions as S
    SedonaRegistrator.registerAll(spark)
    # Points DataFrame
    points_data = [(1, "POINT(1 1)"), (2, "POINT(2 2)"), (3, "POINT(3 3)"), (4, "POINT(5 5)")]
    df_points = spark.createDataFrame(points_data, ["pid", "wkt_point"])
    df_points = df_points.withColumn("geom_point", S.ST_GeomFromWKT(df_points.wkt_point))
    # Centers DataFrame
    centers_data = [(100, "POINT(2 2)"), (200, "POINT(5 5)")]
    df_centers = spark.createDataFrame(centers_data, ["cid", "wkt_center"])
    df_centers = df_centers.withColumn("geom_center", S.ST_GeomFromWKT(df_centers.wkt_center))
    # Distance join: points within distance 2.5 of centers
    joined_dist = df_points.crossJoin(df_centers).where(S.ST_Distance(df_points.geom_point, df_centers.geom_center) <= 2.5)
    print("Points within distance 2.5 of centers:")
    joined_dist.select("pid", "wkt_point", "cid", "wkt_center").show()
except ImportError:
    print("Sedona not installed. Skipping distance join section.")

# 30. GEOSPATIAL: Spatial Indexing (Apache Sedona)
print("\nGeospatial: Spatial Indexing (Apache Sedona)")
print("NOTE: Requires 'apache-sedona' and Sedona Spark JARs. Install with: pip install apache-sedona")
try:
    from sedona.register import SedonaRegistrator
    from sedona.sql import functions as S
    SedonaRegistrator.registerAll(spark)
    # Points DataFrame
    points_data = [(1, "POINT(1 1)"), (2, "POINT(2 2)"), (3, "POINT(3 3)"), (4, "POINT(5 5)")]
    df_points = spark.createDataFrame(points_data, ["pid", "wkt_point"])
    df_points = df_points.withColumn("geom_point", S.ST_GeomFromWKT(df_points.wkt_point))
    # Build spatial index (R-Tree) on points
    df_points.createOrReplaceTempView("points_indexed")
    spark.sql("CACHE TABLE points_indexed")
    print("Spatial index created and table cached.")
    # Query using index (e.g., bounding box)
    bbox = "POLYGON((0 0, 0 3, 3 3, 3 0, 0 0))"
    result = spark.sql(f"""
        SELECT * FROM points_indexed
        WHERE ST_Contains(ST_GeomFromText('{bbox}'), geom_point)
    """)
    print("Points within bounding box:")
    result.show()
except ImportError:
    print("Sedona not installed. Skipping spatial indexing section.")

# 31. GEOSPATIAL: Visualization (matplotlib, geopandas)
print("\nGeospatial Visualization (matplotlib, geopandas)")
print("NOTE: Requires 'geopandas' and 'matplotlib'. Install with: pip install geopandas matplotlib")
try:
    import geopandas as gpd
    import matplotlib.pyplot as plt
    from shapely import wkt
    # Use points and polygons from previous examples
    points_data = [(1, "POINT(1 1)"), (2, "POINT(2 2)"), (3, "POINT(3 3)"), (4, "POINT(5 5)")]
    polygons_data = [
        (10, "POLYGON((0 0, 0 4, 4 4, 4 0, 0 0))"),
        (20, "POLYGON((4 4, 4 6, 6 6, 6 4, 4 4))")
    ]
    gdf_points = gpd.GeoDataFrame(
        [(pid, wkt.loads(wkt_point)) for pid, wkt_point in points_data],
        columns=["pid", "geometry"]
    )
    gdf_polygons = gpd.GeoDataFrame(
        [(gid, wkt.loads(wkt_poly)) for gid, wkt_poly in polygons_data],
        columns=["gid", "geometry"]
    )
    ax = gdf_polygons.plot(facecolor='none', edgecolor='blue', linewidth=2)
    gdf_points.plot(ax=ax, color='red', markersize=50)
    plt.title("Points and Polygons Visualization")
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.show()
except ImportError:
    print("geopandas or matplotlib not installed. Skipping visualization section.")

# 32. GEOSPATIAL: Aggregations (Count Points per Polygon)
print("\nGeospatial Aggregations: Count Points per Polygon (Apache Sedona)")
print("NOTE: Requires 'apache-sedona' and Sedona Spark JARs. Install with: pip install apache-sedona")
try:
    from sedona.register import SedonaRegistrator
    from sedona.sql import functions as S
    SedonaRegistrator.registerAll(spark)
    # Points DataFrame
    points_data = [(1, "POINT(1 1)"), (2, "POINT(2 2)"), (3, "POINT(3 3)"), (4, "POINT(5 5)")]
    df_points = spark.createDataFrame(points_data, ["pid", "wkt_point"])
    df_points = df_points.withColumn("geom_point", S.ST_GeomFromWKT(df_points.wkt_point))
    # Polygons DataFrame
    polygons_data = [
        (10, "POLYGON((0 0, 0 4, 4 4, 4 0, 0 0))"),
        (20, "POLYGON((4 4, 4 6, 6 6, 6 4, 4 4))")
    ]
    df_polygons = spark.createDataFrame(polygons_data, ["gid", "wkt_poly"])
    df_polygons = df_polygons.withColumn("geom_poly", S.ST_GeomFromWKT(df_polygons.wkt_poly))
    # Join and aggregate: count points per polygon
    joined = df_points.join(df_polygons, S.ST_Contains(df_polygons.geom_poly, df_points.geom_point), how="inner")
    agg = joined.groupBy("gid").count()
    print("Count of points per polygon:")
    agg.show()
except ImportError:
    print("Sedona not installed. Skipping geospatial aggregation section.")

# 33. GEOSPATIAL: Heatmap Visualization (matplotlib, geopandas)
print("\nGeospatial Heatmap Visualization (matplotlib, geopandas)")
print("NOTE: Requires 'geopandas' and 'matplotlib'. Install with: pip install geopandas matplotlib")
try:
    import geopandas as gpd
    import matplotlib.pyplot as plt
    from shapely import wkt
    import numpy as np
    # Use points from previous examples
    points_data = [(1, "POINT(1 1)"), (2, "POINT(2 2)"), (3, "POINT(3 3)"), (4, "POINT(5 5)"), (5, "POINT(2.5 2.5)")]
    gdf_points = gpd.GeoDataFrame(
        [(pid, wkt.loads(wkt_point)) for pid, wkt_point in points_data],
        columns=["pid", "geometry"]
    )
    # Create a 2D histogram (heatmap) of point density
    x = gdf_points.geometry.x
    y = gdf_points.geometry.y
    heatmap, xedges, yedges = np.histogram2d(x, y, bins=(10, 10), range=[[0, 6], [0, 6]])
    extent = [xedges[0], xedges[-1], yedges[0], yedges[-1]]
    plt.clf()
    plt.imshow(heatmap.T, extent=extent, origin='lower', cmap='hot', alpha=0.6)
    plt.colorbar(label='Point Density')
    gdf_points.plot(ax=plt.gca(), color='blue', markersize=40, alpha=0.7)
    plt.title("Geospatial Heatmap of Point Density")
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.show()
except ImportError:
    print("geopandas, matplotlib, or numpy not installed. Skipping heatmap section.")

# 34. INTEGRATION: Read GraphX PageRank Output from Scala
print("\nIntegration: Read GraphX PageRank Output from Scala (Parquet)")
print("NOTE: This requires running the Scala GraphX job to produce 'graphx_pagerank_output' first.")
try:
    df_graphx = spark.read.parquet("graphx_pagerank_output")
    df_graphx.show()
except Exception as e:
    print("Could not read GraphX output. Make sure the Scala job has run and output exists.")
    print(e)

# 19. Clean up
spark.stop()
print("\nSpark session stopped.")

# Clean up files
try:
    import shutil
    shutil.rmtree(csv_path)
    shutil.rmtree(json_path)
    shutil.rmtree(parquet_path)
    shutil.rmtree(model_path)
except Exception:
    pass 