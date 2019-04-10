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

    /* We want to compute the number of paths in the graph for several
     * different start vertices. It is then much more efficient to
     * pre-compute the required data and use numPathsIteration instead
     * of calling numPaths several time. We precompute the data
     * here. We do it for both the graph and the reversed graph. */

    /* We map the vertices to a linear index starting from 0. */
    val indices: Map[Long, Int] = graph
      .vertices
      .map(_._1)
      .zipWithIndex
      .collect
      .toMap
      .mapValues(_.toInt)

    val indicesInverse: Map[Int, Long] = for ((v, i) <- indices) yield (i, v)

    /* Find the start and end vertices in the graph. */
    val startVertices: Set[Long] = graph
      .vertices
      .filter(_._2.time == Long.MinValue)
      .map(_._1)
      .collect
      .toSet

    val startVerticesLinear: Set[Int] = startVertices.map(indices(_))

    val endVertices: Set[Long] = graph
      .vertices
      .filter(_._2.time == Long.MaxValue)
      .map(_._1)
      .collect
      .toSet

    val endVerticesLinear: Set[Int] = endVertices.map(indices(_))

    val (children, inDegrees) = numPathsPreCompute(graph, indices, false)
    val (childrenReverse, inDegreesReverse) = numPathsPreCompute(graph, indices, true)

    ////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////
    /* Compute the total number of paths in the graph. */
    val pathsCount: BigInt = {
      val pathsInit: Array[collection.mutable.Map[(Int, Int), BigInt]] =
      (0 to indices.size - 1)
        .map{i =>
          if (startVertices.contains(indicesInverse(i)))
            collection.mutable.Map((-1, -1) -> BigInt(1))
          else
            collection.mutable.Map((-1, -1) -> BigInt(0))
        }
        .toArray

      val paths: Array[BigInt] = Swapmob.numPathsIteration(children,
        inDegrees, pathsInit, startVerticesLinear)

      endVerticesLinear
        .toIterator
        .map(i => paths(i))
        .sum
    }

    println("Number of possible paths in the DAG: " + pathsCount.toString)

    ////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////
    /* Assuming we know that a trajectory had the following measurement.
     * Give the number of paths passing through that measurement,
     * given by the product of paths goint from it and paths going to
     * it. */
    val m = Measurement(65L, Location(Array(2.5)))

    /* Find the id of the trajectory that m belongs to. */
    val id = cotraj.filter(_.measurements.contains(m)).first.id

    println("m = " + m.toString)

    /* Find the number of paths before m. */
    val pathsBeforeCount: BigInt = {
      val vertexBefore: Long = graph
        .vertices
        .filter(v => v._2.time <= m.time && v._2.ids.contains(id))
        .sortBy(_._2.time, false)
        .first
        ._1

      val pathsInit: Array[collection.mutable.Map[(Int, Int), BigInt]] =
        (0 to indices.size - 1)
          .map{i =>
            if (indicesInverse(i) == vertexBefore)
              collection.mutable.Map((-1, -1) -> BigInt(1))
            else
              collection.mutable.Map((-1, -1) -> BigInt(0))
          }
          .toArray

      val paths: Array[BigInt] = numPathsIteration(childrenReverse,
        inDegreesReverse, pathsInit, endVerticesLinear)

      startVerticesLinear
        .toIterator
        .map(i => paths(i))
        .sum
    }

    println("Number of possible paths before m: " + pathsBeforeCount.toString)

    /* Find the number of paths after m. */
    val pathsAfterCount: BigInt = {
      val vertexAfter: Long = graph
        .vertices
        .filter(v => v._2.time > m.time && v._2.ids.contains(id))
        .sortBy(_._2.time)
        .first
        ._1

      val pathsInit: Array[collection.mutable.Map[(Int, Int), BigInt]] =
        (0 to indices.size - 1)
          .map{i =>
            if (indicesInverse(i) == vertexAfter)
              collection.mutable.Map((-1, -1) -> BigInt(1))
            else
              collection.mutable.Map((-1, -1) -> BigInt(0))
          }
          .toArray

      val paths: Array[BigInt] = numPathsIteration(children,
        inDegrees, pathsInit, startVerticesLinear)

      endVerticesLinear
        .toIterator
        .map(i => paths(i))
        .sum
    }

    println("Number of possible paths after m: " + pathsAfterCount.toString)
    println("Total number of paths passing through m: "
      + (pathsBeforeCount*pathsAfterCount).toString)

    /* Look at the family of predicates given by knowing exactly one
     * measurement. This is the same as done above but done for all
     * measurements. */

    val measurements: Array[MeasurementID] = cotraj.measurements.collect

    val pathsThroughMeasurements = measurements
      .map{case MeasurementID(id, m) =>

        /* Find the number of paths before m. */
        val pathsBeforeCount: BigInt = {
          val vertexBefore: Long = graph
            .vertices
            .filter(v => v._2.time <= m.time && v._2.ids.contains(id))
            .sortBy(_._2.time, false)
            .first
            ._1

          val pathsInit: Array[collection.mutable.Map[(Int, Int), BigInt]] =
            (0 to indices.size - 1)
              .map{i =>
                if (indicesInverse(i) == vertexBefore)
                  collection.mutable.Map((-1, -1) -> BigInt(1))
                else
                  collection.mutable.Map((-1, -1) -> BigInt(0))
              }
              .toArray

          val paths: Array[BigInt] = numPathsIteration(childrenReverse,
            inDegreesReverse, pathsInit, endVerticesLinear)

          startVerticesLinear
            .toIterator
            .map(i => paths(i))
            .sum
        }

        /* Find the number of paths after m. */
        val pathsAfterCount: BigInt = {
          val vertexAfter: Long = graph
            .vertices
            .filter(v => v._2.time > m.time && v._2.ids.contains(id))
            .sortBy(_._2.time)
            .first
            ._1

          val pathsInit: Array[collection.mutable.Map[(Int, Int), BigInt]] =
            (0 to indices.size - 1)
              .map{i =>
                if (indicesInverse(i) == vertexAfter)
                  collection.mutable.Map((-1, -1) -> BigInt(1))
                else
                  collection.mutable.Map((-1, -1) -> BigInt(0))
              }
              .toArray

          val paths: Array[BigInt] = numPathsIteration(children,
            inDegrees, pathsInit, startVerticesLinear)

          endVerticesLinear
            .toIterator
            .map(i => paths(i))
            .sum
        }

        println("m = " + m.toString + ": " + (pathsBeforeCount*pathsAfterCount).toString)
        pathsBeforeCount*pathsAfterCount
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

    ids.collect.sorted.foreach{id =>
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

      val pathsInit: Array[collection.mutable.Map[(Int, Int), BigInt]] =
        (0 to indices.size - 1)
          .map{i =>
            if (indicesInverse(i) == startVertex)
              collection.mutable.Map((-1, -1) -> BigInt(1))
            else
              collection.mutable.Map((-1, -1) -> BigInt(0))
          }
          .toArray

      val paths: Array[BigInt] = numPathsIteration(children,
        inDegrees, pathsInit, startVerticesLinear)

      println("i = " + id.toString + ": " + paths(indices(endVertex)))
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

    val noSwaps: Long = numSwaps.filter(_._2 == 0).count

    println("Trajectories with 0 swaps: "
      + noSwaps.toString
      + " (" + (100.0*noSwaps/ids.count).toString + "%)")

    /* Trajectories with less than 20 swaps */
    val numSwaps20: Dataset[(Int, Long)] = numSwaps.filter(_._2 < 20).cache

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

    /* We want to compute the number of paths in the graph for several
     * different start vertices. It is then much more efficient to
     * pre-compute the required data and use numPathsIteration instead
     * of calling numPaths several time. We precompute the data
     * here. */

    /* We map the vertices to a linear index starting from 0. */
    val indices: Map[Long, Int] = graph
      .vertices
      .map(_._1)
      .zipWithIndex
      .collect
      .toMap
      .mapValues(_.toInt)

    val indicesInverse: Map[Int, Long] = for ((v, i) <- indices) yield (i, v)

    /* Find the start and end vertices in the graph. */
    val startVertices: Set[Long] = graph
      .vertices
      .filter(_._2.time == Long.MinValue)
      .map(_._1)
      .collect
      .toSet

    val startVerticesLinear: Set[Int] = startVertices.map(indices(_))

    val endVertices: Set[Long] = graph
      .vertices
      .filter(_._2.time == Long.MaxValue)
      .map(_._1)
      .collect
      .toSet

    val endVerticesLinear: Set[Int] = endVertices.map(indices(_))

    val (children, inDegrees) = numPathsPreCompute(graph, indices, false)
    val (childrenReverse, inDegreesReverse) = numPathsPreCompute(graph, indices, true)

    ////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////
    /* Compute the total number of paths in the graph. */

    val pathsTotal: Array[BigInt] = {
      val pathsInit: Array[collection.mutable.Map[(Int, Int), BigInt]] =
        (0 to indices.size - 1)
          .map{i =>
            if (startVerticesLinear.contains(i))
              collection.mutable.Map((-1, -1) -> BigInt(1))
            else
              collection.mutable.Map((-1, -1) -> BigInt(0))
          }
          .toArray

      Swapmob.numPathsIteration(children, inDegrees, pathsInit,
        startVerticesLinear)
    }

    val pathsTotalReverse: Array[BigInt] = {
      val pathsInit: Array[collection.mutable.Map[(Int, Int), BigInt]] =
        (0 to indices.size - 1)
          .map{i =>
            if (endVerticesLinear.contains(i))
              collection.mutable.Map((-1, -1) -> BigInt(1))
            else
              collection.mutable.Map((-1, -1) -> BigInt(0))
          }
          .toArray

      Swapmob.numPathsIteration(childrenReverse, inDegreesReverse, pathsInit,
        endVerticesLinear)
    }

    val pathsTotalCount: BigInt = endVerticesLinear
        .toIterator
        .map(i => pathsTotal(i))
        .sum

    println("Number of possible paths in the DAG: " + pathsTotalCount.bitLength.toString)

    ////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////
    def pathsGivenMeasurements(id: Int, measurements: Array[Measurement]):
        BigInt = {
      val t0 = System.nanoTime()

      val vertices: Array[Long] = {
        val verticesTrajectory: Array[(Long, Swap)] = graph
          .vertices
          .filter(_._2.ids.contains(id))
          .sortBy(_._2.time)
          .collect

        measurements
          .flatMap{m =>
            val i = verticesTrajectory.indexWhere(_._2.time > m.time)

            Array(verticesTrajectory(i - 1)._1, verticesTrajectory(i)._1)
          }
      }

      /* Paths before the first known measurement */
      val pathsBeforeCount: BigInt = pathsTotal(indices(vertices.head))

      /* Paths between the known measurements */
      val pathsBetweenCount: Array[BigInt] = (1 to measurements.size - 1)
        .map{i =>
          if (vertices(2*i - 1) == vertices(2*i + 1)){
            /* The two measurements are between the same vertices */
            BigInt(1)
          } else {
            /* First vertex after measurement i - 1. */
            val vertexStart: Long = vertices(2*i - 1)
            /* First vertex before measurement i. */
            val vertexEnd: Long = vertices(2*i)

            val pathsInit: Array[collection.mutable.Map[(Int, Int), BigInt]] =
              (0 to indices.size - 1)
                .map{i =>
                  if (i == indices(vertexStart))
                    collection.mutable.Map((-1, -1) -> BigInt(1))
                  else
                    collection.mutable.Map((-1, -1) -> BigInt(0))
                }
                .toArray

            val paths: Array[BigInt] = numPathsIteration(children,
              inDegrees, pathsInit, startVerticesLinear)

            paths(indices(vertexEnd))
          }
        }
        .toArray

      /* Paths after the last known measurement */
      val pathsAfterCount: BigInt = pathsTotalReverse(indices(vertices.last))

      val t1 = System.nanoTime()
      println("Time: " + (t1 - t0).toDouble/1000000000.0 + " seconds")

      pathsBeforeCount*pathsBetweenCount.product*pathsAfterCount
    }

    ////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////
    /* Randomly choose N1 measurements. For each measurement compute the
     * number of paths in the graph that passes through it. */
    val rng = scala.util.Random
    rng.setSeed(42)

    val N1 = 10
    println("Randomly choose " + N1.toString + " measurements and compute "
      +"the number of trajectories passing through each of these.")

    val measurementsSample: Array[MeasurementID] = cotraj
      .measurements
      .rdd
      .takeSample(false, N1, rng.nextInt())

    val pathsThroughMeasurements: Array[BigInt] =
      measurementsSample
        .map{case MeasurementID(id, m) =>

          val paths: BigInt = pathsGivenMeasurements(id, Array(m))

          println("m = " + m.toString + ": " + paths.bitLength.toString)

          paths
        }

    ////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////
    /* Randomly choose N2 trajectories, with replacement. For each
     * trajectory choose 4 random measurements and compute the number
     * of paths passing through all of these measurements. */
    val N2 = 10
    println("Randomly choose " + N2.toString
      + " trajectories and for each one randomly choose 4 measurements and "
      + "compute the number of paths passing through all of these.")

    val trajectoriesSample1: Array[Trajectory] = cotraj
      .rdd
      .takeSample(true, N2, rng.nextInt())

    val pathsThroughTrajectories: Array[BigInt] =
      trajectoriesSample1
        .map{case Trajectory(id, measurements) =>

          val sample: Array[Measurement] = rng
            .shuffle(0 to measurements.size - 1)
            .take(4)
            .sorted
            .map(measurements(_))
            .toArray

          val paths: BigInt = pathsGivenMeasurements(id, sample)

          println("id = " + id.toString + ": " + paths.bitLength.toString)

          paths
        }

    ////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////
    /* Randomly choose N3 trajectories, without replacement. For each
     * trajectory compute the number of paths between the first and
     * final measurement. */
    val N3 = 10
    println("Randomly choose " + N3.toString
      + " trajectories and compute the number of paths going through"
      + " its first and final measurement.")

    val trajectoriesSample2: Array[Trajectory] = cotraj
      .rdd
      .takeSample(false, N3, rng.nextInt())

    val pathsFirstFinal: Array[BigInt] =
      trajectoriesSample2
        .map{case Trajectory(id, measurements) =>

          val paths: BigInt = pathsGivenMeasurements(id,
            Array(measurements.head, measurements.last))

          println("id = " + id.toString + ": " + paths.bitLength.toString)

          paths
        }
  }

  def example2predicate2() = {
    val cotraj = CoTrajectoryUtils.getCoTrajectory(
      Preprocess.dropShort(
        Preprocess.keepBox(
          Parse.beijing(Parse.beijingFile),
          Preprocess.boxBeijing),
        10))
      .cache

    val ids = cotraj.select($"id").as[Int].cache

    /* Compute all possible swaps */
    val partitioning = (60L, 0.001)
    val swaps = cotraj
      .map(_.partitionDistinct(partitioning))
      .swaps(partitioning._1)
      .cache

    /* Compute the DAG representation of SwapMob */
    val graph = swaps
      .graph(ids)
      .cache

    /* We want to compute the number of paths in the graph for several
     * different start vertices. It is then much more efficient to
     * pre-compute the required data and use numPathsIteration instead
     * of calling numPaths several time. We precompute the data
     * here. */

    /* We map the vertices to a linear index starting from 0. */
    val indices: Map[Long, Int] = graph
      .vertices
      .map(_._1)
      .zipWithIndex
      .collect
      .toMap
      .mapValues(_.toInt)

    val indicesInverse: Map[Int, Long] = for ((v, i) <- indices) yield (i, v)

    /* Find the start and end vertices in the graph. */
    val startVertices: Set[Long] = graph
      .vertices
      .filter(_._2.time == Long.MinValue)
      .map(_._1)
      .collect
      .toSet

    val startVerticesLinear: Set[Int] = startVertices.map(indices(_))

    val endVertices: Set[Long] = graph
      .vertices
      .filter(_._2.time == Long.MaxValue)
      .map(_._1)
      .collect
      .toSet

    val endVerticesLinear: Set[Int] = endVertices.map(indices(_))

    val (children, inDegrees) = numPathsPreCompute(graph, indices, false)

    val trajectoriesVertexStart: Map[Int, Long] = graph
      .vertices
      .filter(v => v._2.time == Long.MinValue)
      .map(v => (v._2.ids.head, v._1))
      .collect
      .toMap

    val trajectoriesVertexEnd: Map[Int, Long] = graph
      .vertices
      .filter(v => v._2.time == Long.MaxValue)
      .map(v => (v._2.ids.head, v._1))
      .collect
      .toMap

    val pw = new PrintWriter(new File("trajectoriesPredicate2.txt" ))

    pw.write("Id,Paths\n")

    val trajectoriesPaths: Map[Int, BigInt] = trajectoriesVertexStart
      .keys
      .toArray
      .sorted
      .map{id =>
        val vertexStartLinear: Int = indices(trajectoriesVertexStart(id))
        val vertexEndLinear: Int = indices(trajectoriesVertexEnd(id))

        val pathsInit: Array[collection.mutable.Map[(Int, Int), BigInt]] =
          (0 to indices.size - 1)
            .map{i =>
              if (i == vertexStartLinear)
                collection.mutable.Map((-1, -1) -> BigInt(1))
              else
                collection.mutable.Map((-1, -1) -> BigInt(0))
            }
            .toArray

        val paths: Array[BigInt] = numPathsIteration(children,
          inDegrees, pathsInit, startVerticesLinear)

        println(id.toString + " " + paths(vertexEndLinear).toString)
        pw.write(id.toString + "," + paths(vertexEndLinear).toString + "\n")

        (id, paths(vertexEndLinear))
      }
      .toMap

    pw.close
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
