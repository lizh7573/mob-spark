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
        .cache

      vertices.count

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

  /* Returns a graph holding data about the number of possible paths.
   * The input consists of the graph to consider together with a list
   * of vertices that are possible starting vertices for the paths.
   * The returned graph holds the same data as the original graph
   * together with information about how many paths starting from any
   * of the starting vertices and ending at the current vertex there
   * are. If reverse is set to true the paths are considered in the
   * opposite direction. The argument maxIter is used to set the
   * maximum number of iteration in the Pregel program used for the
   * computations. */
  def numPaths(graph: Graph[Swap, Int],
    startVertices: Array[Long],
    reverse: Boolean = false,
    maxIter: Int = Int.MaxValue):
      Graph[(Swap, Map[(Long, Int), Long]), Int] = {

    /* The number of paths to a vertex is represented by a map which maps
     * incoming edges (represented by the vertex Id for other end of
     * the edge and the edge's id) to number of paths coming from that
     * edge. */

    /* Initialize all vertices with the empty map, except the starting
     * verices for which it maps an artifical edge to 1. */
    /* TODO: Optimize this by doing storing startVertices in an RDD and
     * doing a join. */
    val g: Graph[(Swap, Map[(Long, Int), Long]), Int] =
      graph.mapVertices{case (vId, swap) =>
        (swap,
          if (startVertices.contains(vId))
            Map((vId, -1) -> 1L).withDefaultValue(0L)
          else
            Map().withDefaultValue(0L)
        )
      }

    val g2: Graph[(Swap, Map[(Long, Int), Long]), Int] =
      g.pregel(Map(): Map[(Long, Int), Long], maxIter)(
        // Update vertices by updating the map with the new
        // information.
        (id, value, message) => (value._1, value._2 ++ message),
        // Send the number of paths to this vertex to neighbours which
        // do not have the updated information. If reverese is set to
        // true it sends the information backwards in the graph.
        triplet => {
          if (!reverse) {
            if (triplet.srcAttr._2.values.sum
              > triplet.dstAttr._2((triplet.srcId, triplet.attr)))
              Iterator((triplet.dstId,
                Map((triplet.srcId, triplet.attr) -> triplet.srcAttr._2.values.sum)))
            else
              Iterator.empty
          } else {
            if (triplet.dstAttr._2.values.sum
              > triplet.srcAttr._2((triplet.dstId, triplet.attr)))
              Iterator((triplet.srcId,
                Map((triplet.dstId, triplet.attr) -> triplet.dstAttr._2.values.sum)))
            else
              Iterator.empty
          }

        },
        _ ++ _
      )

    g2
  }
}
