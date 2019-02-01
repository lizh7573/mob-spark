import org.apache.spark.graphx._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

import SparkSessionHolder.spark.implicits._

case class Swap(time: Long, ids: Array[Int])

object Swapmob {
  val sc = SparkSessionHolder.spark.sparkContext

  implicit class Swaps(swaps: Dataset[Swap]) {

    /* Returns a graph representing the swap. The vertices of the graph
     * are given by one starting and one ending vertex for every id
     * (trajectory) and one vertex for every swap performed. The edges
     * are given by paths from the starting vertex of an id to its end
     * vertex visiting every swap it takes part it along the way in
     * time order.
     *
     * The input ids should have one column named "id" and contain all
     * the ids of the co-trajectory. It is required since it could be
     * that some ids do not take part in any swaps but still need to
     * be part of the graph. */
    def graph(ids: Dataset[Int]): Graph[Swap, Int] = {

      val startVertices = ids.map(id => Swap(Long.MinValue, Array(id)))
      val endVertices = ids.map(id => Swap(Long.MaxValue, Array(id)))

      /* Compute all the vertices for the graph. The vertex ID is computed
       * so that it is increasing in time. */
      val vertices: Dataset[(Long, Swap)] = startVertices
        .union(swaps.sort("time"))
        .union(endVertices)
        .withColumn("vertexID", monotonically_increasing_id())
        .as[(Long, Array[Int], Long)]
        .map{case (time, ids, vertexID) => (vertexID, Swap(time, ids))}


      val edges: Dataset[Edge[Int]] = ids
        .joinWith(vertices, array_contains(vertices.col("_2").getField("ids"), ids.col("id")))
        .groupBy("_1")
        .agg(collect_list("_2._1"))
        .as[(Int, Array[Long])]
        .flatMap{case (id, vertexIDs) => vertexIDs
          .sorted
          .sliding(2)
          .map(vIDs => Edge(vIDs(0), vIDs(1), id))}

      Graph(vertices.rdd, edges.rdd)

    }

  }
}
