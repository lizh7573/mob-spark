import org.apache.spark.graphx._
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import java.io._

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

  def numPaths(graph: Graph[Swap, Int], startVertices: Set[Long],
    reverse: Boolean = false, verbose: Boolean = false):
      Map[Long, BigInt] = {
    /* We map the vertices to a linear index starting from 0. */
    val indices: Map[Long, Int] = graph
      .vertices
      .map(_._1)
      .zipWithIndex
      .collect
      .toMap
      .mapValues(_.toInt)

    val indicesInverse: Map[Int, Long] = for ((v, i) <- indices) yield (i, v)

    /* Compute maps for the children and the in degrees of all
     * vertices. */
    val (children, inDegrees): (Array[Array[(Int, Int)]], Array[Int]) =
      numPathsPreCompute(graph, indices, reverse)

    /* Set up an array containing the information about the number of
     * paths to each vertex. This is represented by a map containging
     * the number of paths from each of its ingoing edges. This is
     * populated with data in the loop below. Vertices at which a path
     * can start are represented by mapping the artificial vertex
     * (-1, -1) to 1, for vertices where a path cannot start it is
     * mapped to zero. */
    val paths: Array[collection.mutable.Map[(Int, Int), BigInt]] =
      (0 to indices.size - 1)
        .map{i =>
          if (startVertices.contains(indicesInverse(i)))
            collection.mutable.Map((-1, -1) -> BigInt(1))
          else
            collection.mutable.Map((-1, -1) -> BigInt(0))
        }
        .toArray

    /* Set the active vertices to the root vertices for the graph, those
     * with no in going edges. */
    var activeVertices: Set[Int] =
      graph
        .vertices
        .map(_._1)
        .collect
        .map(v => indices(v))
        .filter(inDegrees(_) == 0)
        .toSet

    val res = numPathsIteration(children, inDegrees, paths, activeVertices,
      Set(): Set[Int], verbose)

    (0 to indices.size - 1)
      .map(i => (indicesInverse(i), res(i)))
      .toMap
  }

  /* Compute the data in numPaths that only depend on the graph and not
   * on the start vertices. */
  def numPathsPreCompute(g: Graph[Swap, Int], indices: Map[Long, Int],
    reverse: Boolean = false):
      (Array[Array[(Int, Int)]], Array[Int]) = {
    /* If reverse is true we instead of the original graph consider the
     * reversed graph. */
    val graph: Graph[Swap, Int] = if(reverse){
      g.reverse
    }else{
      g
    }.cache

    val indicesInverse: Map[Int, Long] = for ((v, i) <- indices) yield (i, v)

    /* Compute an array containing the children of all vertices. Some
     * vertices have several edges to one of their childs and to
     * represent this the children are represented by their ID
     * together with an indexing representing the edge. */
    val childrenMap: Map[Long, Array[(Long, Int)]] = graph
      .triplets
      .toDS
      .map(triplet => (triplet.srcId, triplet.dstId))
      .groupBy($"_1".alias("vertex"))
      .agg(collect_list("_2").alias("children"))
      .as[(Long, Array[Long])]
      .collect
      .toMap
      .mapValues(_
        .groupBy(c => c)
        .values
        .toArray
        .map(_.zipWithIndex)
        .flatten)
      .withDefaultValue(Array())

    val children: Array[Array[(Int, Int)]] = (0 to indicesInverse.size - 1)
      .map(i => childrenMap(indicesInverse(i)).map(x => (indices(x._1), x._2)))
      .toArray

    /* Compute a map containing the in degrees of all vertices. */
    val inDegreesMap: Map[Long, Int] = graph
      .inDegrees
      .collect
      .toMap
      .withDefaultValue(0)

    val inDegrees: Array[Int] = (0 to indicesInverse.size - 1)
      .map(i => inDegreesMap(indicesInverse(i)))
      .toArray

    (children, inDegrees)
  }

  def numPathsIteration(children: Array[Array[(Int, Int)]],
    inDegrees: Array[Int],
    paths: Array[collection.mutable.Map[(Int, Int), BigInt]],
    startVertices: Set[Int],
    endVertices: Set[Int] = Set(),
    verbose: Boolean = false):
      Array[BigInt] = {
    val checkEndVertices: Boolean = !endVertices.isEmpty

    var activeVertices = collection.mutable.Set() ++ startVertices
    var remainingVertices = collection.mutable.Set() ++ endVertices
    var i = 0

    while(!activeVertices.isEmpty && !(checkEndVertices && remainingVertices.isEmpty)){
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

        /* Remove the just activated vertex from the set of end vertices to be
         * reached. */
        remainingVertices -= v
      }

      activeVertices = newVertices
    }

    paths.map(_.values.sum)
  }

  /* Compute the total number of paths through the graph. */
  def numPathsTotal(graph: Graph[Swap, Int]): BigInt = {
    /* Start vertices using original vertex ids */
    val startVertices: Set[Long] = graph
      .vertices
      .filter(_._2.time == Long.MinValue)
      .map(_._1)
      .collect
      .toSet

    val paths: Map[Long, BigInt] = numPaths(graph, startVertices)

    graph
      .vertices
      .filter(_._2.time == Long.MaxValue)
      .map(_._1)
      .collect
      .map(paths(_))
      .sum
  }

  /* For every given measurement compute the number of paths passing
   * through it. Optionally give a filename to write output to this
   * file. */
  def numPathsMeasurements(graph: Graph[Swap, Int],
    ids: Dataset[Int],
    measurements: Dataset[MeasurementID]): Dataset[(MeasurementID, BigInt)] = {
    println("Precomputing data")
    /* Map the vertices to a linear index starting from 0 */
    val indices: Map[Long, Int] = graph
      .vertices
      .map(_._1)
      .zipWithIndex
      .collect
      .toMap
      .mapValues(_.toInt)

    /* Find the set of start and end vertices in the graph */
    val startVertices: Set[Int] = graph
      .vertices
      .filter(_._2.time == Long.MinValue)
      .map(_._1)
      .collect
      .map(indices(_))
      .toSet

    val endVertices: Set[Int] = graph
      .vertices
      .filter(_._2.time == Long.MaxValue)
      .map(_._1)
      .collect
      .map(indices(_))
      .toSet

    /* Precompute data for computing number of paths */
    val (children, inDegrees): (Array[Array[(Int, Int)]], Array[Int]) =
      numPathsPreCompute(graph, indices, false)
    val (childrenReverse, inDegreesReverse): (Array[Array[(Int, Int)]], Array[Int]) =
      numPathsPreCompute(graph, indices, true)

    println("Computing number of paths for the whole graph")
    /* Compute the number of paths to the vertices going forward in the graph */
    val numPaths: Array[BigInt] = {
      val pathsInit: Array[collection.mutable.Map[(Int, Int), BigInt]] =
        (0 to indices.size - 1)
          .map{i =>
            if (startVertices.contains(i))
              collection.mutable.Map((-1, -1) -> BigInt(1))
            else
              collection.mutable.Map((-1, -1) -> BigInt(0))
          }
          .toArray

      Swapmob.numPathsIteration(children, inDegrees, pathsInit,
        startVertices)
    }

    /* Compute the number of paths to the vertices going backwards in the
     * graph */
    val numPathsReverse: Array[BigInt] = {
      val pathsInit: Array[collection.mutable.Map[(Int, Int), BigInt]] =
        (0 to indices.size - 1)
          .map{i =>
            if (endVertices.contains(i))
              collection.mutable.Map((-1, -1) -> BigInt(1))
            else
              collection.mutable.Map((-1, -1) -> BigInt(0))
          }
          .toArray

      Swapmob.numPathsIteration(childrenReverse, inDegreesReverse, pathsInit,
        endVertices)
    }

    println("Computing chain of vertices")
    /* For every trajectory find the chain of vertices for it in the
     * graph */
    val vertices: Dataset[(VertexId, Swap)] = graph.vertices.toDS
    val verticesTrajectories: Map[Int, Array[(Int, Long)]] = ids
      .join(vertices, array_contains(vertices.col("_2.ids"), ids.col("id")))
      .select($"id", $"_1".alias("vertexID"), $"_2.time".alias("time"))
      .groupBy($"id")
      .agg(collect_list(struct($"vertexID", $"time")).alias("vertices"))
      .as[(Int, Array[(Long, Long)])]
      .map{case (id, vertices) => (id, vertices.sortBy(_._2))}
      .collect
      .map{case (id, vertices) =>
        (id, vertices.map{case (vertexID, time) => (indices(vertexID), time)})
      }
      .toMap

    val bcNumPaths = sc.broadcast(numPaths)
    val bcNumPathsReverse = sc.broadcast(numPathsReverse)
    val bcVerticesTrajectories = sc.broadcast(verticesTrajectories)

    println("Computing number of paths trough measurements")
    measurements
      .map{case MeasurementID(id, m) =>
        val i: Int = bcVerticesTrajectories.value(id).indexWhere(_._2 > m.time)

        val vertexBefore: Int = bcVerticesTrajectories.value(id)(i - 1)._1
        val vertexAfter: Int = bcVerticesTrajectories.value(id)(i)._1

        val paths: BigInt = bcNumPaths.value(vertexBefore)*bcNumPathsReverse.value(vertexAfter)

        (MeasurementID(id, m), paths)
      }
  }

  /* For every original trajectory in the graph compute the number of
   * paths between its start and end vertex. Gives a mapping from
   * trajectory id to the number of such paths. Optionally give a
   * filename to write output to this file. */
  def numPathsStartEnd(graph: Graph[Swap, Int]):
      Array[(Int, BigInt)] = {
    println("Precomputing data")
    /* Map the vertices to a linear index starting from 0 */
    val indices: Map[Long, Int] = graph
      .vertices
      .map(_._1)
      .zipWithIndex
      .collect
      .toMap
      .mapValues(_.toInt)

    /* Find start and end vertices for all trajectories as given by the
     * linear index. Gives a mapping from trajectory ids to linear
     * vertex ids */
    val trajectoriesStartVertices: Map[Int, Int] = graph
      .vertices
      .filter(v => v._2.time == Long.MinValue)
      .map(v => (v._2.ids.head, v._1))
      .collect
      .map{case (id, i) => (id, indices(i))}
      .toMap

    val trajectoriesEndVertices: Map[Int, Int] = graph
      .vertices
      .filter(v => v._2.time == Long.MaxValue)
      .map(v => (v._2.ids.head, v._1))
      .collect
      .map{case (id, i) => (id, indices(i))}
      .toMap

    val trajectoriesStartEndVertices: Dataset[(Int, Int, Int)] = SparkSessionHolder
      .spark
      .createDataset(trajectoriesStartVertices
        .keys
        .map{id =>
          (id, trajectoriesStartVertices(id), trajectoriesEndVertices(id))
        }
        .toArray
      )
      .sort($"_1")

    /* Precompute data for computing number of paths */
    val (children, inDegrees): (Array[Array[(Int, Int)]], Array[Int]) =
      numPathsPreCompute(graph, indices, false)

    /* Find the set of start and end vertices in the graph */
    val startVertices: Set[Int] = trajectoriesStartVertices
      .map(_._2)
      .toSet

    val numIndices: Int = indices.size

    val stepSize: Int = 20
    val perLine: Int = 50
    var i: Int = 0
    println("Computing number of paths between start and end vertex")
    println("Number of steps to compute: "
      + trajectoriesStartEndVertices.count.toString)
    println("Every dot is " + stepSize.toString + " steps, " + perLine.toString + " per line")

    trajectoriesStartEndVertices
      .collect
      .map{case (id, startVertex, endVertex) =>
        val pathsInit: Array[collection.mutable.Map[(Int, Int), BigInt]] =
          (0 to numIndices - 1)
            .map{i =>
              if (i == startVertex)
                collection.mutable.Map((-1, -1) -> BigInt(1))
              else
                collection.mutable.Map((-1, -1) -> BigInt(0))
            }
            .toArray

        val numPaths: BigInt = numPathsIteration(children,
          inDegrees,
          pathsInit,
          startVertices,
          Set(endVertex))(endVertex)

        i = i + 1
        if(i % stepSize == 0){
          print(".")
        }
        if(i % (perLine*stepSize) == 0){
          print("\n")
        }

        (id, numPaths)
      }
  }

  /* For every given pair of id and array of measurements compute the
   * number of paths going through all those measurements. */
  def numPathsNMeasurements(graph: Graph[Swap, Int],
    ids: Dataset[Int],
    measurements: Array[(Int, Array[Measurement])]):
      Array[BigInt] = {
    println("Precomputing data")
    /* Map the vertices to a linear index starting from 0 */
    val indices: Map[Long, Int] = graph
      .vertices
      .map(_._1)
      .zipWithIndex
      .collect
      .toMap
      .mapValues(_.toInt)

    /* Find the set of start and end vertices in the graph */
    val startVertices: Set[Int] = graph
      .vertices
      .filter(_._2.time == Long.MinValue)
      .map(_._1)
      .collect
      .map(indices(_))
      .toSet

    val endVertices: Set[Int] = graph
      .vertices
      .filter(_._2.time == Long.MaxValue)
      .map(_._1)
      .collect
      .map(indices(_))
      .toSet

    /* Precompute data for computing number of paths */
    val (children, inDegrees): (Array[Array[(Int, Int)]], Array[Int]) =
      numPathsPreCompute(graph, indices, false)
    val (childrenReverse, inDegreesReverse): (Array[Array[(Int, Int)]], Array[Int]) =
      numPathsPreCompute(graph, indices, true)

    println("Computing number of paths for the whole graph")
    /* Compute the number of paths to the vertices going forward in the graph */
    val numPaths: Array[BigInt] = {
      val pathsInit: Array[collection.mutable.Map[(Int, Int), BigInt]] =
        (0 to indices.size - 1)
          .map{i =>
            if (startVertices.contains(i))
              collection.mutable.Map((-1, -1) -> BigInt(1))
            else
              collection.mutable.Map((-1, -1) -> BigInt(0))
          }
          .toArray

      Swapmob.numPathsIteration(children, inDegrees, pathsInit,
        startVertices)
    }

    /* Compute the number of paths to the vertices going backwards in the
     * graph */
    val numPathsReverse: Array[BigInt] = {
      val pathsInit: Array[collection.mutable.Map[(Int, Int), BigInt]] =
        (0 to indices.size - 1)
          .map{i =>
            if (endVertices.contains(i))
              collection.mutable.Map((-1, -1) -> BigInt(1))
            else
              collection.mutable.Map((-1, -1) -> BigInt(0))
          }
          .toArray

      Swapmob.numPathsIteration(childrenReverse, inDegreesReverse, pathsInit,
        endVertices)
    }

    println("Computing chain of vertices")
    /* For every trajectory find the chain of vertices for it in the
     * graph */
    val vertices: Dataset[(VertexId, Swap)] = graph.vertices.toDS
    val verticesTrajectories: Map[Int, Array[(Int, Long)]] = ids
      .join(vertices, array_contains(vertices.col("_2.ids"), ids.col("id")))
      .select($"id", $"_1".alias("vertexID"), $"_2.time".alias("time"))
      .groupBy($"id")
      .agg(collect_list(struct($"vertexID", $"time")).alias("vertices"))
      .as[(Int, Array[(Long, Long)])]
      .map{case (id, vertices) => (id, vertices.sortBy(_._2))}
      .collect
      .map{case (id, vertices) =>
        (id, vertices.map{case (vertexID, time) => (indices(vertexID), time)})
      }
      .toMap

    val stepSize: Int = 20
    val perLine: Int = 50
    var i: Int = 0
    println("Computing number of paths trough measurements")
    println("Number of steps to compute: " + measurements.length)
    println("Every dot is " + stepSize.toString + " steps, " + perLine.toString + " per line")
    measurements
      .map{case (id, ms) =>
        /* Find the chain of vertices. That is, for every measurement find the
         * vertex occurring right before and right after it. In case
         * several measurement fall in between the same vertices only
         * keep them once. */
        val verticesChain: Array[Int] = ms
          .map(m => verticesTrajectories(id).indexWhere(_._2 > m.time))
          .distinct
          .flatMap(i => verticesTrajectories(id).slice(i - 1, i + 1).map(_._1))

        /* Number of paths before the first measurement */
        val numPathsBefore: BigInt = numPaths(verticesChain.head)

        /* Number of paths between measurements */
        val numPathsBetween: BigInt = verticesChain
          .drop(1)
          .dropRight(1)
          .sliding(2, 2)
          .map{vertices =>
            val start: Int = vertices(0)
            val end: Int = vertices(1)

            if (start == end){
              /* If start and end vertices are the same there is only ever one path
               * between them */
              BigInt(1)
            }else{
              val pathsInit: Array[collection.mutable.Map[(Int, Int), BigInt]] =
                (0 to indices.size - 1)
                  .map{i =>
                    if (i == start)
                      collection.mutable.Map((-1, -1) -> BigInt(1))
                    else
                      collection.mutable.Map((-1, -1) -> BigInt(0))
                  }
                  .toArray

              numPathsIteration(children, inDegrees, pathsInit, startVertices, Set(end))(end)
            }
          }
          .product

        /* Number of paths after the last measurement */
        val numPathsAfter: BigInt = numPathsReverse(verticesChain.last)

        i = i + 1
        if(i % stepSize == 0){
          print(".")
        }
        if(i % (perLine*stepSize) == 0){
          print("\n")
        }

        numPathsBefore*numPathsBetween*numPathsAfter
      }
  }
}
