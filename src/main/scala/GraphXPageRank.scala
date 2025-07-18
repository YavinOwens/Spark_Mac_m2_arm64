import org.apache.spark.sql.SparkSession
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

object GraphXPageRank {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("GraphXPageRank").getOrCreate()
    val sc = spark.sparkContext

    // Example graph: vertices and edges
    val vertices: RDD[(VertexId, String)] = sc.parallelize(Seq(
      (1L, "Alice"), (2L, "Bob"), (3L, "Charlie")
    ))
    val edges: RDD[Edge[Int]] = sc.parallelize(Seq(
      Edge(1L, 2L, 1), Edge(2L, 3L, 1), Edge(3L, 1L, 1)
    ))

    val graph = Graph(vertices, edges)
    val ranks = graph.pageRank(0.0001).vertices

    // Join with names
    val ranksWithNames = ranks.join(vertices).map {
      case (id, (rank, name)) => (id, name, rank)
    }

    import spark.implicits._
    val df = ranksWithNames.toDF("id", "name", "pagerank")
    df.write.mode("overwrite").parquet("graphx_pagerank_output")
    spark.stop()
  }
} 