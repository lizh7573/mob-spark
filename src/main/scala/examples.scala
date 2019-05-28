import CoTrajectoryUtils._
import Swapmob._
import org.apache.spark.sql.Dataset
import org.apache.spark.graphx._
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
    /* Open file for normal output */
    val output = new PrintWriter(new File("output/example1.txt"))

    /* Parse the co-trajectory */
    val cotraj: Dataset[Trajectory] = Parse
      .testCoTrajectory("data/examples/cotrajectory_example.csv")
      .cache

    /* Compute number of trajectories and number of measurements */
    val numTrajectories: Long = cotraj.count
    val numMeasurements: Long = cotraj.map(_.measurements.length).reduce(_ + _)

    output.println("Number of trajectories: " + numTrajectories.toString)
    println("Number of trajectories: " + numTrajectories.toString)

    output.println("Number of measurements: " + numMeasurements.toString)
    println("Number of measurements: " + numMeasurements.toString)

    /* Compute possible swaps */
    val partitioning: (Long, Double) = (2L, 1.0)
    val swaps: Dataset[Swap] = cotraj
      .map(_.partitionDistinct(partitioning))
      .swaps(partitioning._1)
      .cache

    val numSwaps: Long = swaps.count

    output.println("Number of possible swaps: " + numSwaps.toString)
    println("Number of possible swaps: " + numSwaps.toString)

    /* Compute the DAG representation of SwapMob */
    val ids: Dataset[Int] = cotraj.select($"id").as[Int].cache
    val graph: Graph[Swap, Int] = swaps.graph(ids).cache

    /* We want to compute the number of paths in the graph for several
     * different start vertices. It is then much more efficient to
     * pre-compute the required data and use numPathsIteration instead
     * of calling numPaths several time. We precompute the data
     * here. We do it for both the graph and the reversed graph. */

    /* Map the vertices to a linear index starting from 0 */
    val indices: Map[Long, Int] = graph
      .vertices
      .map(_._1)
      .zipWithIndex
      .collect
      .toMap
      .mapValues(_.toInt)

    val indicesInverse: Map[Int, Long] = for ((v, i) <- indices) yield (i, v)

    /* Find start and end vertices in the graph */
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

    /* Precompute data for computing number of paths */
    val (children, inDegrees): (Array[Array[(Int, Int)]], Array[Int]) =
      numPathsPreCompute(graph, indices, false)
    val (childrenReverse, inDegreesReverse): (Array[Array[(Int, Int)]], Array[Int]) =
      numPathsPreCompute(graph, indices, true)

    /* Compute total number of paths in the graph */
    val numPathsTotal: BigInt = {
      val pathsInit: Array[collection.mutable.Map[(Int, Int), BigInt]] =
      (0 to indices.size - 1)
        .map{i =>
          if (startVertices.contains(indicesInverse(i)))
            collection.mutable.Map((-1, -1) -> BigInt(1))
          else
            collection.mutable.Map((-1, -1) -> BigInt(0))
        }
        .toArray

      val numPaths: Array[BigInt] = Swapmob.numPathsIteration(children,
        inDegrees, pathsInit, startVerticesLinear)

      endVerticesLinear
        .toIterator
        .map(numPaths(_))
        .sum
    }

    output.println("Number of possible paths in the DAG: " + numPathsTotal.toString)
    println("Number of possible paths in the DAG: " + numPathsTotal.toString)

    /* Assuming we know that a trajectory had the following measurement.
     * Give the number of paths passing through that measurement,
     * given by the product of paths goint from it and paths going to
     * it. */
    val m: Measurement = Measurement(13L, Location(Array(2.5)))

    output.println("m = " + m.toString)
    println("m = " + m.toString)

    /* Find the id of the trajectory that m belongs to. */
    val id: Long = cotraj.filter(_.measurements.contains(m)).first.id

    /* Find the number of paths before m. */
    val numPathsBeforeM: BigInt = {
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

      val numPaths: Array[BigInt] = numPathsIteration(childrenReverse,
        inDegreesReverse, pathsInit, endVerticesLinear)

      startVerticesLinear
        .toIterator
        .map(numPaths(_))
        .sum
    }

    output.println("Number of possible paths before m: " + numPathsBeforeM.toString)
    println("Number of possible paths before m: " + numPathsBeforeM.toString)

    /* Find the number of paths after m. */
    val numPathsAfterM: BigInt = {
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

      val numPaths: Array[BigInt] = numPathsIteration(children,
        inDegrees, pathsInit, startVerticesLinear)

      endVerticesLinear
        .toIterator
        .map(numPaths(_))
        .sum
    }

    output.println("Number of possible paths after m: " + numPathsAfterM.toString)
    println("Number of possible paths after m: " + numPathsAfterM.toString)

    val numPathsM: BigInt = numPathsBeforeM*numPathsAfterM

    output.println("Total number of paths passing through m: " + numPathsM.toString)
    println("Total number of paths passing through m: " + numPathsM.toString)

    /* Look at the family of predicates given by knowing exactly one
     * measurement. This is the same as done above but done for all
     * measurements. */
    val measurements: Array[MeasurementID] = cotraj.measurements.collect

    /* Write data to csv file */
    val outputNumPathsMeasurementsName: String = "output/example1-1.csv"
    val outputNumPathsMeasurements =
      new PrintWriter(new File(outputNumPathsMeasurementsName))

    output.println("Output data about number of paths through measurements to " +
      outputNumPathsMeasurementsName)
    println("Output data about number of paths through measurements to " +
      outputNumPathsMeasurementsName)

    outputNumPathsMeasurements.println("numPaths")

    val numPathsMeasurements: Array[BigInt] = measurements
      .map{case MeasurementID(id, m) =>

        /* Find number of paths before */
        val numPathsBefore: BigInt = {
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

          val numPaths: Array[BigInt] = numPathsIteration(childrenReverse,
            inDegreesReverse, pathsInit, endVerticesLinear)

          startVerticesLinear
            .toIterator
            .map(numPaths(_))
            .sum
        }

        /* Find number of paths after */
        val numPathsAfter: BigInt = {
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

          val numPaths: Array[BigInt] = numPathsIteration(children,
            inDegrees, pathsInit, startVerticesLinear)

          endVerticesLinear
            .toIterator
            .map(numPaths(_))
            .sum
        }

        val numPaths: BigInt = numPathsBefore*numPathsAfter

        output.println("m = " + m.toString + ": " + numPaths.toString)
        println("m = " + m.toString + ": " + numPaths.toString)

        outputNumPathsMeasurements.println(numPaths.toString)

        numPaths
      }

    outputNumPathsMeasurements.close()

    /* Given that we know the first and last measurement of a trajectory,
     * give the number of possible paths between them. Do this for all
     * original trajectories. */
    val outputNumPathsStartEndName: String = "output/example1-2.csv"
    val outputNumPathsStartEnd =
      new PrintWriter(new File(outputNumPathsStartEndName))

    output.println("Output data about number of paths starting and ending at a" +
      " the same vertices as trajectory i to " + outputNumPathsStartEndName)
    println("Output data about number of paths starting and ending at a" +
      " the same vertices as trajectory i to " + outputNumPathsStartEndName)

    outputNumPathsStartEnd.println("id,numPaths")

    val numPathsStartEnd: Map[Int, BigInt] = Swapmob.numPathsStartEnd(graph)

    ids.collect.sorted.foreach{id =>
      val numPaths: BigInt = numPathsStartEnd(id)

      output.println("id = " + id.toString + ": " + numPaths)
      println("id = " + id.toString + ": " + numPaths)

      outputNumPathsStartEnd.println(id.toString + "," + numPaths.toString)
    }

    outputNumPathsStartEnd.close()

    output.close()
  }

  /* An example with the co-trajectory from the T-drive dataset. It
   * parses the co-trajectory and gives some basic stats about it. It
   * then computes the possible swaps and again give some basic
   * stats. */
  def example2() = {
    /* Open file for normal output */
    val output = new PrintWriter(new File("output/example2.txt"))

    /* Parse the co-trajectory */
    val cotraj: Dataset[Trajectory] = CoTrajectoryUtils.getCoTrajectory(
      Preprocess.dropShort(
        Preprocess.keepBox(
          Parse.beijing(Parse.beijingFile),
          Preprocess.boxBeijing),
        10))
      .cache

    /* Compute number of trajectories and number of measurements */
    val numTrajectories: Long = cotraj.count
    val numMeasurements: Long = cotraj.map(_.measurements.length).reduce(_ + _)

    output.println("Number of trajectories: " + numTrajectories.toString)
    println("Number of trajectories: " + numTrajectories.toString)

    output.println("Number of measurements: " + numMeasurements.toString)
    println("Number of measurements: " + numMeasurements.toString)

    /* Compute possible swaps */
    val partitioning: (Long, Double) = (60L, 0.001)
    val swaps: Dataset[Swap] = cotraj
      .map(_.partitionDistinct(partitioning))
      .swaps(partitioning._1)
      .cache

    val numSwapsTotal: Long = swaps.count

    output.println("Number of possible swaps: " + numSwapsTotal.toString)
    println("Number of possible swaps: " + numSwapsTotal.toString)

    /* Compute the number of swaps per trajectory */
    val ids: Dataset[Int] = cotraj.select($"id").as[Int].cache
    val numSwaps: Dataset[(Int, Long)] = swaps
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

    val avgSwaps: Double = numSwaps.map(_._2).reduce(_ + _)/numTrajectories

    output.println("Average number of swaps per trajectory "
      + avgSwaps.toString)
    println("Average number of swaps per trajectory "
      + avgSwaps.toString)

    val noSwaps: Long = numSwaps.filter(_._2 == 0).count

    output.println("Trajectories with 0 swaps: "
      + noSwaps.toString
      + " (" + (100.0*noSwaps/numTrajectories).toString + "%)")
    println("Trajectories with 0 swaps: "
      + noSwaps.toString
      + " (" + (100.0*noSwaps/numTrajectories).toString + "%)")

    /* Number of trajectories with less than 20 swaps */
    val less20Swaps: Long = numSwaps.filter(_._2 < 20).count

    output.println("Trajectories with less than 20 swaps: "
      + less20Swaps.toString
      + " (" + (100.0*less20Swaps/numTrajectories).toString + "%)")
    println("Trajectories with less than 20 swaps: "
      + less20Swaps.toString
      + " (" + (100.0*less20Swaps/numTrajectories).toString + "%)")

    /* Number of trajectories at least 20 swaps */
    val atLeast20Swaps: Long = numSwaps.filter(_._2 >= 20).count

    output.println("Trajectories with at least 20 swaps: "
      + atLeast20Swaps.toString
      + " (" + (100.0*atLeast20Swaps/numTrajectories).toString + "%)")
    println("Trajectories with at least 20 swaps: "
      + atLeast20Swaps.toString
      + " (" + (100.0*atLeast20Swaps/numTrajectories).toString + "%)")

    /* Write data about number of swaps to a csv file */
    val outputNumSwapsName: String = "output/example2-1.csv"
    val outputNumSwaps = new PrintWriter(new File(outputNumSwapsName))

    output.println("Output data about number of swaps to "
      + outputNumSwapsName)
    println("Output data about number of swaps to "
      + outputNumSwapsName)

    outputNumSwaps.println("id,numSwaps")

    numSwaps
      .collect
      .foreach{case (id, swaps) =>
        outputNumSwaps.println(id.toString + "," + swaps.toString)
      }

    /* Compute the DAG representation of SwapMob */
    val graph: Graph[Swap, Int] = swaps
      .graph(ids)
      .cache

    /* Find start and end vertices in the graph. */
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

    val numPaths: Map[Long, BigInt] = Swapmob.numPaths(graph, startVertices)

    /* Compute total number of paths in the graph */
    val numPathsTotal: BigInt = endVertices
      .toIterator
      .map(numPaths(_))
      .sum

    output.println("Number of possible paths in the DAG: " + numPathsTotal.toString)
    println("Number of possible paths in the DAG: " + numPathsTotal.toString)

    output.close()
  }

  /* Compute the number of paths going through all of the different
   * measurements. This corresponds to the first family of predicates
   * in the thesis. The fraction parameter indicates the fraction of
   * measurements that should be sampled. */
  def example2NumPathsMeasurements(fraction: Double = 0.05):
      Map[MeasurementID, BigInt] = {
    /* Open file for normal output */
    val filename = "output/example2NumPathsMeasurements.csv"
    val output = new PrintWriter(new File(filename))
    println("Output data to " + filename)

    /* Parse the co-trajectory */
    val cotraj = CoTrajectoryUtils.getCoTrajectory(
      Preprocess.dropShort(
        Preprocess.keepBox(
          Parse.beijing(Parse.beijingFile),
          Preprocess.boxBeijing),
        10))
      .cache

    val ids = cotraj.select($"id").as[Int].cache

    /* Compute possible swaps */
    val partitioning = (60L, 0.001)
    val swaps = cotraj
      .map(_.partitionDistinct(partitioning))
      .swaps(partitioning._1)
      .cache

    /* Compute the DAG representation of SwapMob */
    val graph = swaps
      .graph(ids)
      .cache

    println("Computed graph")

    /* We want to compute the number of paths in the graph for several
     * different start vertices. It is then much more efficient to
     * pre-compute the required data and use numPathsIteration instead
     * of calling numPaths several time. We precompute the data
     * here. We do it for both the graph and the reversed graph. */

    /* Map the vertices to a linear index starting from 0 */
    val indices: Map[Long, Int] = graph
      .vertices
      .map(_._1)
      .zipWithIndex
      .collect
      .toMap
      .mapValues(_.toInt)

    val indicesInverse: Map[Int, Long] = for ((v, i) <- indices) yield (i, v)

    /* Find start and end vertices in the graph */
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

    /* Precompute data for computing number of paths */
    val (children, inDegrees): (Array[Array[(Int, Int)]], Array[Int]) =
      numPathsPreCompute(graph, indices, false)
    val (childrenReverse, inDegreesReverse): (Array[Array[(Int, Int)]], Array[Int]) =
      numPathsPreCompute(graph, indices, true)

    /* Compute the number of paths to the vertices going forward in the graph */
    val numPaths: Array[BigInt] = {
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

    /* Compute the number of paths to the vertices going backwards in the
     * graph */
    val numPathsReverse: Array[BigInt] = {
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

    println("Computed paths")

    /* For every trajectory find the chain of vertices for it in the
     * graph */
    var i = 0
    val verticesTrajectories: Map[Int, Array[(Int, Long)]] = ids
      .collect
      .map{id =>

        if (i % 100 == 0){println(i)}

        i = i + 1

        (id, graph
          .vertices
          .filter(_._2.ids.contains(id))
          .collect
          .sortBy(_._2.time)
          .map{case (v, swap) => (indices(v), swap.time)})
      }
      .toMap

    println("Computed chain of vertices for trajectories")

    output.println("id,numPaths")

    val numPathsMeasurements: Map[MeasurementID, BigInt] = cotraj
      .measurements
      .sample(false, fraction)
      .collect
      .map{m =>
        val (vertexBeforeLinear: Int, vertexAfterLinear: Int) = {
          val i = verticesTrajectories(m.id).indexWhere(_._2 > m.measurement.time)

          (i - 1, i)
        }

        val paths: BigInt =
          numPathsReverse(vertexBeforeLinear)*numPaths(vertexAfterLinear)

        output.println(m.id.toString + "," + paths.toString)

        (m, paths)
      }
      .toMap

    output.close()

    numPathsMeasurements
  }

  /* Given that we know the first and last measurement of a trajectory,
   * compute number of possible paths between them. Do this for all
   * original trajectories. This corresponds to the second family of
   * predicates in the thesis. */
  def example2NumPathsStartEnd() = {
    /* File to write data to */
    val filename = "output/example2NumPathsStartEnd.csv"
    println("Output data to " + filename)

    println("Computing graph")
    /* Parse the co-trajectory */
    val cotraj = CoTrajectoryUtils.getCoTrajectory(
      Preprocess.dropShort(
        Preprocess.keepBox(
          Parse.beijing(Parse.beijingFile),
          Preprocess.boxBeijing),
        10))
      .cache

    /* Compute possible swaps */
    val partitioning = (60L, 0.001)
    val swaps = cotraj
      .map(_.partitionDistinct(partitioning))
      .swaps(partitioning._1)
      .cache

    /* Compute the graph representation of SwapMob */
    val ids = cotraj.select($"id").as[Int].cache
    val graph = swaps
      .graph(ids)
      .cache

    println("Computing number of paths")
    Swapmob.numPathsStartEnd(graph, filename)
  }

  /* Given that we know the N measurements of a trajectory, give the
   * number of possible paths going through all of them. This
   * corresponds to the third family of predicates in the thesis. The
   * sample size determines the number of samples to consider, this is
   * not an exact number but an approximate one. */
  def example2NumPathsNMeasurements(N: Int = 4, sampleSize: Int = 20000) = {
    /* Open file for normal output */
    val filename = "output/example2NumPathsNMeasurements.csv"
    val output = new PrintWriter(new File(filename))
    println("Output data to " + filename)

    /* Parse the co-trajectory */
    val cotraj = CoTrajectoryUtils.getCoTrajectory(
      Preprocess.dropShort(
        Preprocess.keepBox(
          Parse.beijing(Parse.beijingFile),
          Preprocess.boxBeijing),
        10))
      .cache

    val ids = cotraj.select($"id").as[Int].cache

    /* Compute possible swaps */
    val partitioning = (60L, 0.001)
    val swaps = cotraj
      .map(_.partitionDistinct(partitioning))
      .swaps(partitioning._1)
      .cache

    /* Compute the DAG representation of SwapMob */
    val graph = swaps
      .graph(ids)
      .cache

    println("Computed graph")

    /* We want to compute the number of paths in the graph for several
     * different start vertices. It is then much more efficient to
     * pre-compute the required data and use numPathsIteration instead
     * of calling numPaths several time. We precompute the data
     * here. We do it for both the graph and the reversed graph. */

    /* Map the vertices to a linear index starting from 0 */
    val indices: Map[Long, Int] = graph
      .vertices
      .map(_._1)
      .zipWithIndex
      .collect
      .toMap
      .mapValues(_.toInt)

    val indicesInverse: Map[Int, Long] = for ((v, i) <- indices) yield (i, v)

    /* Find start and end vertices in the graph */
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

    /* Precompute data for computing number of paths */
    val (children, inDegrees): (Array[Array[(Int, Int)]], Array[Int]) =
      numPathsPreCompute(graph, indices, false)
    val (childrenReverse, inDegreesReverse): (Array[Array[(Int, Int)]], Array[Int]) =
      numPathsPreCompute(graph, indices, true)

    /* Compute the number of paths to the vertices going forward in the graph */
    val numPaths: Array[BigInt] = {
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

    /* Compute the number of paths to the vertices going backwards in the
     * graph */
    val numPathsReverse: Array[BigInt] = {
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

    println("Computed paths")

    /* For every trajectory find the chain of vertices for it in the
     * graph */
    var i = 0
    val verticesTrajectories: Map[Int, Array[(Int, Long)]] = ids
      .collect
      .map{id =>

        if (i % 100 == 0){println(i)}
        i = i + 1

        (id, graph
          .vertices
          .filter(_._2.ids.contains(id))
          .collect
          .sortBy(_._2.time)
          .map{case (v, swap) => (indices(v), swap.time)})
      }
      .toMap

    println("Computed chain of vertices for trajectories")

    /* Sample the data, sample trajectories with replacement and for
     * each trajectory sample measurements without replacement. */
    val sampleTrajectories: java.util.Iterator[Trajectory] = cotraj
      .sample(true, sampleSize/cotraj.count)
      .toLocalIterator

    /* For every sample trajectory find N random measurements and compute
     * the number of paths

     - Find the vertices before and after the measurements
     - Compute the number of paths before the first vertex
     - Compute the number of paths between the vertices
     - Compute the number of paths after the last vertex */
    val rng = scala.util.Random

    output.println("id,numPaths")
    i = 0

    while(sampleTrajectories.hasNext){
      /* Progress */
      if (i % 100 == 0){println(i)}
      i = i + 1

      /* Current trajectory */
      val trajectory: Trajectory = sampleTrajectories
        .next

      var paths: BigInt = BigInt(1)
      /* If the number of vertices for the trajectory is only two it means
       * it has not participated in any swaps and the result will
       * always be 1. */
      if (verticesTrajectories(trajectory.id).length != 2){
        /* Indices of the measurements to sample */
        val sampleIndices: Array[Int] = rng
          .shuffle(0 to (trajectory.measurements.length - 1))
          .toArray
          .take(N)

        /* Sampled measurements */
        val sampleMeasurements: Array[Measurement] = sampleIndices
          .map(trajectory.measurements(_))

        /* Find the chain of vertices. This includes the very first vertex of
         * the trajectory but not the very last one, unless it happens
         * to be the included as as a vertex right after a
         * measurement. */
        val verticesChainLinear: Array[Int] = sampleMeasurements
          .foldLeft(Array(0)){
            case (chain, m) =>
              val i = verticesTrajectories(trajectory.id)
                .indexWhere(_._2 > m.time, chain.last)

              if(i == chain.last)
                chain
              else
                chain ++ Array(i - 1, i)
          }
          .map(verticesTrajectories(trajectory.id)(_)._1)

        /* Number of paths before the first measurement */
        val numPathsBefore: BigInt = numPaths(verticesChainLinear(1))

        /* Number of paths between measurements */
        val numPathsBetween: Array[BigInt] = verticesChainLinear
          .drop(2)
          .dropRight(1)
          .sliding(2, 2)
          .map{vertices =>
            val start: Int = vertices(0)
            val end: Int = vertices(1)

            val pathsInit: Array[collection.mutable.Map[(Int, Int), BigInt]] =
              (0 to indices.size - 1)
                .map{i =>
                  if (i == start)
                    collection.mutable.Map((-1, -1) -> BigInt(1))
                  else
                    collection.mutable.Map((-1, -1) -> BigInt(0))
                }
                .toArray

            numPathsIteration(children,
              inDegrees, pathsInit, startVerticesLinear)(end)
          }
          .toArray

        /* Number of paths after the last measurement */
        val numPathsAfter: BigInt = numPathsReverse(verticesChainLinear.last)

        paths = numPathsBefore*numPathsBetween.product*numPathsAfter
      }

      output.println(trajectory.id.toString + "," + paths.toString)
    }

    output.close()
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
    pw.println(html)
    pw.close

  }
}
