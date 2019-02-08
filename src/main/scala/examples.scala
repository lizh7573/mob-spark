import CoTrajectoryUtils._
import Swapmob._
import org.apache.spark.graphx._

object Examples {
  val spark = SparkSessionHolder.spark
  import spark.implicits._

  /* An example with an artifical co-trajectory, i.e. not using real
   * data. It parses the co-trajectory and gives some basic stats
   * about it. It then computes the possible swaps and again give some
   * basic stats. Finally it computes the DAG representation of
   * SwapMob and gives some more involed data about this. */
  def example1() = {
    /* Parse the co-trajectory and give basic stats */
    val cotraj = Parse
      .testCoTrajectory("data/examples/cotraj-graph-swap-big-example.txt")
      .cache

    println("Number of trajectories: " + cotraj.count.toString)

    println("Number of measurements: " + cotraj
      .map(_.measurements.length)
      .reduce(_+_)
      .toString)

    /* Compute all possible swaps */
    val partitioning = (10L, 1.0/3)
    val swaps = cotraj
      .map(_.partitionDistinct(partitioning))
      .swaps(partitioning._1)
      .cache

    println("Number of possible swaps: " + swaps.count.toString)

    /* Compute the DAG representation of SwapMob */
    val ids = cotraj.select($"id").as[Int].cache
    val graph = swaps
      .graph(ids)
      .partitionBy(PartitionStrategy.EdgePartition1D, 8)
      .cache

    /* Compute the total number of paths in the graph */
    val startVertices: Array[Long] = ids.collect.map{i =>
      graph
        .vertices
        .filter{case (vID, swap) =>
          swap.time == Long.MinValue && swap.ids.contains(i)}
        .first
        ._1
    }

    val g = Swapmob.numPaths(graph, startVertices)

    println("Number of possible paths in the DAG: " + g
      .vertices
      .filter(_._2._1.time == Long.MaxValue)
      .map(_._2._2.values.head)
      .reduce(_+_)
      .toString)

    /* Assuming we know that a trajectory had the following measurement.
     * Give the number of paths passing through that measurement,
     * given by the product of paths goint from it and paths going to
     * it. */

    val m = Measurement(65L, Location(Array(2.5)))

    // Find the id of the trajectory that m belongs to
    val id = cotraj.filter(_.measurements.contains(m)).first.id

    println("m = " + m.toString)

    // Find the vertex for the swaps occurring right before and after
    // the measurement
    val vertexBefore: Long = graph
      .vertices
      .filter(_._2.ids.contains(id))
      .filter(_._2.time <= m.time)
      .sortBy(_._2.time, false)
      .first
      ._1

    val g2 = Swapmob.numPaths(graph, Array(vertexBefore), true)
    val pathsBefore = g2
      .vertices
      .filter(_._2._1.time == Long.MinValue)
      .map(_._2._2.values.headOption.getOrElse(0L))
      .reduce(_+_)

    println("Number of possible paths before m: " + pathsBefore.toString)

    val vertexAfter: Long = graph
      .vertices
      .filter(_._2.ids.contains(id))
      .filter(_._2.time > m.time)
      .sortBy(_._2.time)
      .first
      ._1

    val g3 = Swapmob.numPaths(graph, Array(vertexAfter))
    val pathsAfter = g3
      .vertices
      .filter(_._2._1.time == Long.MaxValue)
      .map(_._2._2.values.headOption.getOrElse(0L))
      .reduce(_+_)

    println("Number of possible paths after m: " + pathsAfter.toString)
    println("Total number of paths passing through m: " + (pathsBefore*pathsAfter).toString)

    println("Number of paths starting and ending at the same vertices as trajectory i:")

    ids.collect.sorted.foreach{ id =>
      val startVertex: Long = graph.
        vertices
        .filter(_._2.time == Long.MinValue)
        .filter(_._2.ids.contains(id))
        .first
        ._1

      val endVertex: Long = graph.
        vertices
        .filter(_._2.time == Long.MaxValue)
        .filter(_._2.ids.contains(id))
        .first
        ._1

      val g4 = Swapmob.numPaths(graph, Array(startVertex))
      println("i = " + id.toString + ": " + g4.
        vertices
        .filter(_._1 == endVertex)
        .map(_._2._2.values.headOption.getOrElse(0L))
        .reduce(_+_)
        .toString
      )


    }
  }
}
