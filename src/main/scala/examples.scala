import CoTrajectoryUtils._
import Swapmob._
import org.apache.spark.sql.Dataset
import org.apache.spark.graphx._
import scalax.chart.api._
import java.io._

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

    /* Compute the DAG representation of SwapMob. */
    val ids = cotraj.select($"id").as[Int].cache
    val graph = swaps
      .graph(ids)
      .cache

    /* Find the start and end vertices in the graph. */
    val startVertices: Set[Long] = graph
      .vertices
      .filter(_._2.time == Long.MinValue)
      .map(_._1)
      .collect
      .toSet

    val endVertices: Set[Long] = graph
      .vertices
      .filter(_._2.time == Long.MaxValue)
      .map(_._1)
      .collect
      .toSet

    /* Compute the total number of paths in the graph. */
    val paths: Array[(Long, BigInt)] = Swapmob.numPathsArray(graph, startVertices)

    println("Number of possible paths in the DAG: " + paths
      .filter(v => endVertices.contains(v._1))
      .map(_._2)
      .sum
      .toString)

    /* Assuming we know that a trajectory had the following measurement.
     * Give the number of paths passing through that measurement,
     * given by the product of paths goint from it and paths going to
     * it. */
    val m = Measurement(65L, Location(Array(2.5)))

    /* Find the id of the trajectory that m belongs to. */
    val id = cotraj.filter(_.measurements.contains(m)).first.id

    println("m = " + m.toString)

    /* Find the vertex for the swaps occurring right before and after
     the measurement. */
    val vertexBefore: Long = graph
      .vertices
      .filter(v => v._2.time <= m.time && v._2.ids.contains(id))
      .sortBy(_._2.time, false)
      .first
      ._1

    val vertexAfter: Long = graph
      .vertices
      .filter(v => v._2.time > m.time && v._2.ids.contains(id))
      .sortBy(_._2.time)
      .first
      ._1

    val pathsBefore: BigInt = Swapmob
      .numPathsArray(graph, Set(vertexBefore), true)
      .filter(v => startVertices.contains(v._1))
      .map(_._2)
      .sum

    println("Number of possible paths before m: " + pathsBefore.toString)

    val pathsAfter: BigInt = Swapmob
      .numPathsArray(graph, Set(vertexAfter))
      .filter(v => endVertices.contains(v._1))
      .map(_._2)
      .sum

    println("Number of possible paths after m: " + pathsAfter.toString)
    println("Total number of paths passing through m: " + (pathsBefore*pathsAfter).toString)

    /* Look at the family of predicates given by knowing exactly one
     * measurement. This is the same as done above but done for all
     * measurements. */

    val measurements: Array[MeasurementID] = cotraj.measurements.collect

    val pathsThroughMeasurements = measurements
      .map{case MeasurementID(id, m) =>
        val vertexBefore: Long = graph
          .vertices
          .filter(v => v._2.time <= m.time && v._2.ids.contains(id))
          .sortBy(_._2.time, false)
          .first
          ._1

        val vertexAfter: Long = graph
          .vertices
          .filter(v => v._2.time > m.time && v._2.ids.contains(id))
          .sortBy(_._2.time)
          .first
          ._1

        val pathsBefore: BigInt = Swapmob
          .numPathsArray(graph, Set(vertexBefore), true)
          .filter(v => startVertices.contains(v._1))
          .map(_._2)
          .sum

        val pathsAfter: BigInt = Swapmob
          .numPathsArray(graph, Set(vertexAfter))
          .filter(v => endVertices.contains(v._1))
          .map(_._2)
          .sum

        println("m = " + m.toString + ": " + (pathsBefore*pathsAfter).toString)
        pathsBefore*pathsAfter
      }

    val pathsDistribution = pathsThroughMeasurements
      .groupBy(i => i)
      .map(g => (g._1 , g._2.length))
      .toVector

    val pathsDistributionChar = XYBarChart(pathsDistribution)
    val pathsDistributionCharFileName = "paths-dist.pdf"

    pathsDistributionChar.saveAsPDF(pathsDistributionCharFileName)
    println("Saved distribution of paths for knowing one measurement to "
      + pathsDistributionCharFileName)

    /* Given that we know the first and last measurement of a trajectory,
     * give the number of possible paths between them. Do this for all
     * original trajectories. */
    println("Number of paths starting and ending at the same vertices as trajectory i:")

    ids.collect.sorted.foreach{ id =>
      val startVertex: Long = graph
        .vertices
        .filter(v => v._2.time == Long.MinValue && v._2.ids.contains(id))
        .first
        ._1

      val endVertex: Long = graph
        .vertices
        .filter(v => v._2.time == Long.MaxValue && v._2.ids.contains(id))
        .first
        ._1

      val paths = Swapmob
        .numPathsArray(graph, Set(startVertex))
        .filter(_._1 == endVertex)
        .map(_._2)
        .sum

      println("i = " + id.toString + ": " + paths.toString)
    }
  }

  /* An example with the co-trajectory from the T-drive dataset. It
   * parses the co-trajectory and gives some basic stats about it. It
   * then computes the possible swaps and again give some basic
   * stats. */
  def example2() = {
    val cotraj = CoTrajectoryUtils.getCoTrajectory(
      Preprocess.dropShort(
        Preprocess.keepBox(
          Parse.beijing(Parse.beijingFile),
          Preprocess.boxBeijing),
        10))
      .cache

    val ids = cotraj.select($"id").as[Int].cache

    println("Number of trajectories: " + ids.count.toString)

    println("Number of measurements: " + cotraj
      .map(_.measurements.length)
      .reduce(_+_)
      .toString)

    /* Compute all possible swaps */
    val partitioning = (60L, 0.001)
    val swaps = cotraj
      .map(_.partitionDistinct(partitioning))
      .swaps(partitioning._1)
      .cache

    println("Total number of swaps: " + swaps.count.toString)

    /* Compute the number of swaps per trajectory */
    val numSwaps = swaps
      .flatMap(_.ids)
      .withColumnRenamed("value", "id")
      .groupBy("id")
      .count
      .as[(Int, Long)]
      .union(ids.map((_, 0)))
      .groupBy("id")
      .sum("count")
      .select($"id", $"sum(count)".alias("swaps"))
      .as[(Int, Long)]
      .cache

    println("Average number of swaps per trajectory "
      + (numSwaps.map(_._2).reduce(_ + _)/ids.count).toString)

    /* Trajectories with less than 20 swaps */
    val numSwaps20 = numSwaps.filter(_._2 < 20).cache

    println("Trajectories with less than 20 swaps: "
      + numSwaps20.count.toString
      + " (" + (100.0*numSwaps20.count/ids.count).toString + "%)")

    /* Plot the distribution of swaps */
    val dist20: Vector[(Long, Long)] = numSwaps20
      .groupBy("swaps")
      .count
      .as[(Long, Long)]
      .collect
      .toVector

    val chart20 = XYBarChart(dist20)

    val chart20FileName = "swaps-distribution-20.pdf"

    chart20.saveAsPDF(chart20FileName)
    println("Saved distribution of swaps for trajectories participating in less than 20 swaps to "
      + chart20FileName)

    val numSwapsOther = numSwaps.filter(_._2 >= 20).cache

    println("Trajectories with at least 20 swaps: "
      + numSwapsOther.count.toString
      + " (" + (100.0*numSwapsOther.count/ids.count).toString + "%)")

    /* Plot the distribution of swaps */
    val distOther: Vector[(Long, Long)] = numSwapsOther
      .groupBy("swaps")
      .count
      .as[(Long, Long)]
      .collect
      .toVector

    val chartOther = XYBarChart(distOther)

    val chartOtherFileName = "swaps-distribution-other.pdf"

    chartOther.saveAsPDF(chartOtherFileName)
    println("Saved distribution of swaps for trajectories participating in at least 20 swaps to "
      + chartOtherFileName)


    /* Compute the DAG representation of SwapMob */
    val graph = swaps
      .graph(ids)
      .cache

    /*
    /* Compute the total number of paths in the graph */
    val startVertices: Array[Long] = graph
      .vertices
      .filter(_._2.time == Long.MinValue)
      .map(_._1)
      .collect

    println("Number of start vertices: " + startVertices.length.toString)

    val g = Swapmob.numPaths(graph, startVertices)

    println("Computed graph with number of paths")
     */

    /* Randomly choose a trajectory and a measurement for that trajectory.
     * Compute the number of paths in the graph that passes through
     * this measurement. */
    val rng = scala.util.Random
    rng.setSeed(42)

    val randTraj: Trajectory = cotraj.rdd.takeSample(false, 1, 42).head
    val m: Measurement = randTraj
      .measurements(rng.nextInt(randTraj.measurements.length))

    /* Find the vertices in the graph occurring right before and after
     * m */
    val vertexBefore: Long = graph
      .vertices
      .filter(v => v._2.ids.contains(randTraj.id) && v._2.time <= m.time)
      .sortBy(_._2.time, false)
      .first
      ._1

    val vertexAfter: Long = graph
      .vertices
      .filter(v => v._2.ids.contains(randTraj.id) && v._2.time > m.time)
      .sortBy(_._2.time)
      .first
      ._1

    println("m = " + m.toString)

    val g2 = Swapmob.numPaths(graph, Array(vertexBefore), true)
    val pathsBefore = g2
      .vertices
      .filter(_._2._1.time == Long.MinValue)
      .map(_._2._2.values.headOption.getOrElse(BigInt(0)))
      .reduce(_+_)

    println("Number of possible paths before m: " + pathsBefore.toString)


    g2
  }

  /* Generates data for visualizing some trajectories from taxis in
   * Beijing. */
  def figure1(n: Int = 10, seed: Int = 0) = {
    val cotraj = CoTrajectoryUtils.getCoTrajectory(
      Preprocess.dropShort(
        Preprocess.keepBox(
          Parse.sanFransisco(Parse.sanFransiscoFile),
          Preprocess.boxSanFransisco),
        10))

    val sample: Array[Trajectory] = cotraj.rdd.takeSample(false, n, seed)

    val html: String = Visualize.genLeafletHTML(sample.map(_.toJson))

    val fileName: String = "co-trajectory-example.html"

    val pw = new PrintWriter(new File(fileName))
    pw.write(html)
    pw.close

  }
}
