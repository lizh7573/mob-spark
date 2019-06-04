import CoTrajectoryUtils._
import Swapmob._
import org.apache.spark.sql.Dataset
import org.apache.spark.graphx._
import java.io._
import scala.collection.JavaConversions._

object Examples {
  val spark = SparkSessionHolder.spark
  import spark.implicits._

  /* Helper function for sampling N distinct elements from an array
   * keeping the original ordering. */
  def sample(from: Array[Measurement], N: Int, rng: scala.util.Random) = {
    /* Sample indices to take */
    val indices: scala.collection.immutable.IndexedSeq[Int] = rng
      .shuffle(0 to (from.length - 1))
      .take(N)
      .sorted

    indices.map(from(_)).toArray
  }

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

    /* Find the total number of paths in the graph. */
    val numPathsTotal: BigInt = Swapmob.numPathsTotal(graph)

    output.println("Number of possible paths in the DAG: " + numPathsTotal.toString)
    println("Number of possible paths in the DAG: " + numPathsTotal.toString)

    /* Look at the family of predicates given by knowing exactly one
     * measurement. */


    val numPathsMeasurements: Dataset[(MeasurementID, BigInt)] =
      Swapmob.numPathsMeasurements(graph, ids, cotraj.measurements)

    val outputNumPathsMeasurementsName: String = "output/example1-1.csv"
    val outputNumPathsMeasurements = new PrintWriter(new File(outputNumPathsMeasurementsName))
    output.println("Output data about number of paths through measurements to " +
      outputNumPathsMeasurementsName)
    println("Output data about number of paths through measurements to " +
      outputNumPathsMeasurementsName)

    outputNumPathsMeasurements.println("id,numPaths")
    numPathsMeasurements
      .map(x => x._1.id.toString + "," + x._2.toString)
      .toLocalIterator
      .foreach(outputNumPathsMeasurements.println(_))

    outputNumPathsMeasurements.close()

    val m: MeasurementID = MeasurementID(2, Measurement(13L, Location(Array(2.5))))
    val numPathsM: BigInt = numPathsMeasurements.filter(_._1 == m).head._2

    output.println("Number of paths trough the measurement m = " + m.toString
        + ": " + numPathsM.toString)
    println("Number of paths trough the measurement m = " + m.toString
      + ": " + numPathsM.toString)

    /* Look at family of predicates given by knowing first and last
     * measurement. */
    val numPathsStartEnd: Array[(Int, BigInt)] = Swapmob.numPathsStartEnd(graph)

    val outputNumPathsStartEndName: String = "output/example1-2.csv"
    val outputNumPathsStartEnd = new PrintWriter(new File(outputNumPathsStartEndName))
    output.println("Output data about number of paths starting and ending at a" +
      " the same vertices as trajectory i to " + outputNumPathsStartEndName)
    println("Output data about number of paths starting and ending at a" +
      " the same vertices as trajectory i to " + outputNumPathsStartEndName)

    outputNumPathsStartEnd.println("id,numPaths")
    numPathsStartEnd
      .map(x => x._1.toString + "," + x._2.toString)
      .foreach(outputNumPathsStartEnd.println(_))

    outputNumPathsStartEnd.close()

    numPathsStartEnd.foreach{case (id, numPaths) =>
      output.println("id = " + id.toString + ": " + numPaths)
      println("id = " + id.toString + ": " + numPaths)
    }

    output.close()
  }

  /* An example with the co-trajectory from the T-drive dataset. It
   * parses the co-trajectory and gives some basic stats about it. It
   * then computes the possible swaps and again give some basic
   * stats. */
  def example2() = {
    /* Open file for normal output */
    val filename = "output/example2.txt"
    val output = new PrintWriter(new File(filename))
    println("Output data to " + filename)

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

    val less20Swaps: Long = numSwaps.filter(_._2 < 20).count

    output.println("Trajectories with less than 20 swaps: "
      + less20Swaps.toString
      + " (" + (100.0*less20Swaps/numTrajectories).toString + "%)")
    println("Trajectories with less than 20 swaps: "
      + less20Swaps.toString
      + " (" + (100.0*less20Swaps/numTrajectories).toString + "%)")

    val atLeast20Swaps: Long = numSwaps.filter(_._2 >= 20).count

    output.println("Trajectories with at least 20 swaps: "
      + atLeast20Swaps.toString
      + " (" + (100.0*atLeast20Swaps/numTrajectories).toString + "%)")
    println("Trajectories with at least 20 swaps: "
      + atLeast20Swaps.toString
      + " (" + (100.0*atLeast20Swaps/numTrajectories).toString + "%)")

    val outputNumSwapsName: String = "output/example2-1.csv"
    val outputNumSwaps = new PrintWriter(new File(outputNumSwapsName))
    output.println("Output data about number of swaps to " + outputNumSwapsName)
    println("Output data about number of swaps to " + outputNumSwapsName)

    outputNumSwaps.println("id,numSwaps")

    numSwaps
      .collect
      .foreach{case (id, swaps) =>
        outputNumSwaps.println(id.toString + "," + swaps.toString)
      }

    outputNumSwaps.close()

    /* Compute the DAG representation of SwapMob */
    val graph: Graph[Swap, Int] = swaps
      .graph(ids)
      .cache

    /* Compute total number of paths in the graph */
    val numPathsTotal: BigInt = Swapmob.numPathsTotal(graph)

    output.println("Number of possible paths in the DAG: " + numPathsTotal.toString)
    println("Number of possible paths in the DAG: " + numPathsTotal.toString)

    output.close()
  }

  /* Compute the number of paths going through all of the different
   * measurements. This corresponds to the first family of predicates
   * in the thesis. The fraction parameter indicates the fraction of
   * measurements that should be sampled. */
  def example2NumPathsMeasurements(fraction: Double = 0.05):
      Dataset[(MeasurementID, BigInt)] = {
    println("Computing graph")
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

    val seed = 42
    val numPathsMeasurements: Dataset[(MeasurementID, BigInt)] =
      Swapmob.numPathsMeasurements(graph, ids,
        cotraj.measurements.sample(false, fraction, seed))

    val filename = "output/example2NumPathsMeasurements.csv"
    val output = new PrintWriter(new File(filename))
    println("Outputting data to " + filename)

    output.println("id,numPaths")
    numPathsMeasurements
      .map(x => x._1.id.toString + "," + x._2.toString)
      .toLocalIterator
      .foreach(output.println(_))

    output.close()

    numPathsMeasurements
  }

  /* Given that we know the first and last measurement of a trajectory,
   * compute number of possible paths between them. Do this for all
   * original trajectories. This corresponds to the second family of
   * predicates in the thesis. */
  def example2NumPathsStartEnd() = {
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

    val numPathsStartEnd: Array[(Int, BigInt)] = Swapmob.numPathsStartEnd(graph)

    val filename = "output/example2NumPathsStartEnd.csv"
    val output = new PrintWriter(new File(filename))
    println("Outputting data to " + filename)

    output.println("id,numPaths")
    numPathsStartEnd
      .map(x => x._1.toString + "," + x._2.toString)
      .foreach(output.println(_))

    output.close()
  }

  /* Given that we know the N measurements of a trajectory, give the
   * number of possible paths going through all of them. This
   * corresponds to the third family of predicates in the thesis. The
   * sample size determines the number of samples to consider, this is
   * not an exact number but an approximate one. */
  def example2NumPathsNMeasurements(N: Int = 4, sampleSize: Int = 20000) = {
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

    /* Sample the data, sample trajectories with replacement and for
     * each trajectory sample measurements without replacement. */
    val seed = 42
    val rng = new scala.util.Random(seed)
    val sampleMeasurements: Array[(Int, Array[Measurement])] = cotraj
      .sample(true, sampleSize/cotraj.count, seed)
      .collect
      .map(r => (r.id, sample(r.measurements, N, rng)))

    val numPathsNMeasurements: Array[BigInt] =
      Swapmob.numPathsNMeasurements(graph, ids, sampleMeasurements)

    val filename = "output/example2NumPathsNMeasurements.csv"
    val output = new PrintWriter(new File(filename))
    println("Outputting data to " + filename)

    output.println("id,numPaths")
    numPathsNMeasurements
      .map(_.toString)
      .foreach(output.println(_))

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
