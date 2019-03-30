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
      Graph[(Swap, Map[(Long, Int), scala.math.BigInt]), Int] = {

    /* The number of paths to a vertex is represented by a map which maps
     * incoming edges (represented by the vertex Id for other end of
     * the edge and the edge's id) to number of paths coming from that
     * edge. */

    /* Initialize all vertices with the empty map, except the starting
     * verices for which it maps an artifical edge to 1. */
    /* TODO: Optimize this by doing storing startVertices in an RDD and
     * doing a join. */
    val g: Graph[(Swap, Map[(Long, Int), scala.math.BigInt]), Int] =
      graph.mapVertices{case (vId, swap) =>
        (swap,
          if (startVertices.contains(vId))
            Map((vId, -1) -> BigInt(1)).withDefaultValue(BigInt(0))
          else
            Map().withDefaultValue(0L)
        )
      }

    val g2: Graph[(Swap, Map[(Long, Int), scala.math.BigInt]), Int] =
      g.pregel(Map(): Map[(Long, Int), scala.math.BigInt], maxIter)(
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

  /* Returns a graph holding data about the total number of possible
   * paths. This works similarly to numPaths above but for computing
   * the total number of paths in the graph. It is equivalent to
   * numPaths called with all starting vertices of the graph but
   * better optimized for that. */
  def numPathsTotal(graph: Graph[Swap, Int],
    maxIter: Int = Int.MaxValue):
      Graph[(Int, BigInt), Int] = {

    /* Vertices have are given by (status: Int, paths: BigInt), paths is
     * the number of paths to this vertex and the status of the vertex
     * is given by:
     * -n: Waiting for n parents
     * 0: Ready
     * 1: Done */

    val inDegrees: VertexRDD[Int] = graph.inDegrees

    val startGraph: Graph[(Int, BigInt), Int] = graph
      .outerJoinVertices(inDegrees){(vId, swap, inDegOpt) =>
        val state = inDegOpt match {
          case Some(inDeg) => -inDeg
          case None => 0
        }

        val paths = if (swap.time == Long.MinValue)
          BigInt(1)
        else
          BigInt(0)

        (state, paths)
      }.cache

    startGraph.vertices.count
    startGraph.edges.count

    println("Created start graph")

    val resultGraph: Graph[(Int, BigInt), Int] = startGraph
      .pregel((0, BigInt(0)), maxIter)(
        // Update vertices by updating the map with the new
        // information.
        (id, value, message) => (value._1 + message._1, value._2 + message._2),
        // If this vertex is ready (status == 0) then send the number
        // of paths to this vertex and set the status to done to not
        // send again.
        triplet => {
          if (triplet.srcAttr._1 == 0)
            Iterator((triplet.dstId, (1, triplet.srcAttr._2)))
          else
            Iterator.empty
        },
        (m1, m2) => (m1._1 + m2._1, m1._2 + m2._2)
      )

    resultGraph
  }

  def numPathsArray(graph: Graph[Swap, Int], startVertices: Set[Long],
    verbose: Boolean = false): Array[(Long, BigInt)] = {

    /* We map the vertex IDs to a linear id 0, 1, ..., N. Also create a
     * reverse map. */
    val m = graph
      .vertices
      .map(_._1)
      .zipWithIndex
      .collectAsMap
      .mapValues(_.toInt)

    val mReversed = for ((k, v) <- m) yield (v, k)

    /* An array containing the children of all vertices. Some vertices have
     * several edges to one of their childs and to represent this the
     * children are represented by their ID together with an indexing
     * representing the edge. */
    val childrenDataset: Dataset[(Long, Array[Long])] = graph
      .triplets
      .toDS
      .map(triplet => (triplet.srcId, triplet.dstId))
      .groupBy($"_1".alias("vertex"))
      .agg(collect_list("_2").alias("children"))
      .as[(Long, Array[Long])]
      .union(graph
        .vertices
        .toDS
        .filter(_._2.time == Long.MaxValue)
        .map(x => (x._1, Array(): Array[Long])))

    val children: Array[Array[(Int, Int)]] = childrenDataset
      .collect
      .map{case (vertex, children) => (m(vertex), children.map(m(_)))}
      .sortBy(_._1)
      .map(_._2
        .groupBy(c => c)
        .values
        .toArray
        .map(_.zipWithIndex)
        .flatten)

    /* Compute an array containing the in degrees of all vertices. */
    val inDegreesMap: Map[Int, Int] = graph
      .inDegrees
      .collect
      .map{case (vertex, degree) => (m(vertex), degree)}
      .toMap
      .withDefaultValue(0)

    val inDegrees: Array[Int] = children
      .indices
      .toArray
      .map(inDegreesMap(_))

    /* Set up an array containing the information about the number of
     * paths to each vertex. This is represented by a map containging
     * the number of paths from each of its ingoing edges. This is
     * filled with data in the loop below. Vertices at which a path
     * can start are represented by mapping the artificial vertex
     * (-1, -1) to 1, for vertices where a path cannot start it is
     * mapped to zero. */
    val paths: Array[collection.mutable.Map[(Int, Int), BigInt]] =
      children
        .indices
        .toArray
        .map(v =>
          if (startVertices.contains(mReversed(v)))
            collection.mutable.Map((-1, -1) -> BigInt(1))
          else
            collection.mutable.Map((-1, -1) -> BigInt(0)))

    /* Set the active vertices to the root vertices for the graph, those
     * with no in going edges. */
    var activeVertices: collection.mutable.Set[Int] =
      collection.mutable.Set() ++ graph
        .vertices
        .filter(_._2.time == Long.MinValue)
        .map(_._1)
        .collect
        .map(m(_))
        .toSet

    var i = 0

    while(!activeVertices.isEmpty){
      if(verbose){println(i.toString ++ ": " ++ activeVertices.size.toString)}
      i = i + 1

      val newVertices: collection.mutable.Set[Int] = collection.mutable.Set()

      activeVertices.foreach{v =>
        val sum = paths(v).values.sum

        children(v).foreach{case (c, i) =>
          paths(c) += (v, i) -> sum

          /* We active a vertex once all of its parents have been activated. The
           * number of parents is given by the in degree and the
           * number of activated parents is given by the current
           * number of elements in the map storing the number of paths
           * minus 1 since we do not count the artificial vertex
           * (-1, -1). */
          if (paths(c).size - 1 == inDegrees(c)){
            newVertices += c
          }
        }
      }

      activeVertices = newVertices
    }

    paths.zipWithIndex.map{case (paths, v) =>
      (mReversed(v), paths.values.sum)
    }
  }
}
