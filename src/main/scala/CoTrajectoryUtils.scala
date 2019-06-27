import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset

import SparkSessionHolder.spark.implicits._

object CoTrajectoryUtils {

  /* Takes a dataset of measurements with IDs and returns the
   * corresponding co-trajectory. */
  def getCoTrajectory(data: Dataset[MeasurementID]): Dataset[Trajectory] =
    data.groupBy("id")
      .agg(collect_set($"measurement").alias("measurements"))
      .as[Trajectory]
      .map(_.normalize())

  /* Takes a dataset of partitioned measurements with IDs and returns
   * the corresponding co-trajectory. */
  def getCoTrajectoryPartition(data: Dataset[MeasurementPartitionID]):
      Dataset[TrajectoryPartition] =
    data.groupBy("id")
      .agg(collect_set($"partition").alias("partitions"))
      .as[TrajectoryPartition]
      .map(_.normalize)

  /* An implicit class for a co-trajectory consisting of measurements.
   * Most of the co-trajectory specific methods are implemented
   * here.*/
  implicit class CoTrajectory(cotraj: Dataset[Trajectory]) {

    /* Return a Dataset of all measurements in the co-trajectory. */
    def measurements(): Dataset[MeasurementID] = cotraj
      .flatMap((r => r.measurements.map(m => MeasurementID(r.id, m))))

    /* Split a co-trajectory into several co-trajectories where each one
     * only contains measurements for a single date. Returns an array
     * of tuples where the first element corresponds to the Unix-time
     * of the beginning of the date and the second element to the
     * corresponding co-trajectory. */
    def splitByDate(): Array[(Long, Dataset[Trajectory])] = {
      val grouped = cotraj.flatMap(_.splitByDate)
      val dates = grouped.map(_._1).distinct.collect

      dates.map(date => (date, grouped.filter(_._1 == date).map(_._2)))
    }

    /* Return a Dataset with the jumpchains of the co-trajectory's
     * trajectories. The jumpchain of a trajectory is the chain of
     * locations for the trajectory, removing any succesive
     * duplicates. */
    def jumpchain(partitioning: Double):
        Dataset[(Int, Array[LocationPartition])] =
      cotraj.map(_.jumpchain(partitioning))
        .withColumnRenamed("_1", "id").withColumnRenamed("_2", "jumpchain")
        .as[(Int, Array[LocationPartition])]

    /* Return a Dataset with the jumpchain times of the co-trajectory's
     * trajectories. The jumpchain times of a trajectory is a list of
     * times where each time represents the time the trajectory stays
     * in one particular location. */
    def jumpchainTimes(partitioning: Double): Dataset[(Int, Array[Int])] =
      cotraj.map(_.jumpchainTimes(partitioning))
        .withColumnRenamed("_1", "id").withColumnRenamed("_2", "jumpchainTimes")
        .as[(Int, Array[Int])]

    /* Return a Dataset with the transitions of the co-trajectory's
     * trajectories. The list of transition of a trajectory consists
     * of pairs of locations together with the time spent in the first
     * location before going to the second. */
    def transitions(partitioning: Double):
        Dataset[(LocationPartition, LocationPartition, Long, Double)] =
      cotraj.flatMap(_.transitions(partitioning)._2)
        .groupBy("_1", "_2")
        .agg(count("_3").alias("count"), avg("_3").alias("time"))
        .withColumnRenamed("_1", "from")
        .withColumnRenamed("_2", "to")
        .as[(LocationPartition, LocationPartition, Long, Double)]

    /* Returns an enumeration of all the partitions occurring in the
     * co-trajectory. */
    def enumeratePartitions(partitioning: Double):
        Dataset[(LocationPartition, BigInt)] =
      cotraj.flatMap(_.measurements.map(_.location.partition(partitioning)))
        .distinct
        .rdd
        .zipWithIndex
        .toDS.withColumnRenamed("_1", "location").withColumnRenamed("_2", "id")
        .as[(LocationPartition, BigInt)]

    /* Return a map matched version of the co-trajectory. Every trajectory
     * is replaced with its map matched version. */
    def mapMatch(): Dataset[Trajectory] = cotraj.mapPartitions{partition =>
      val mm = GraphHopperHelper.getMapMatcher

      partition.map(_.mapMatch(mm))
      }
  }

  /* An implicit class for a co-trajectory consisting of partitioned
   * measurements. Most of the co-trajectory specific methods are
   * implemented here.*/
  implicit class CoTrajectoryPartition(cotraj: Dataset[TrajectoryPartition]) {

    /* Return a Dataset of all partitioned measurements in the
     * co-trajectory. */
    def measurements(): Dataset[MeasurementPartitionID] = cotraj
      .flatMap((r => r.partitions.map(m =>
        MeasurementPartitionID(r.id, m))))

    /* Return a Dataset of with the jumpchains of the co-trajectorys
     * trajectories. The jumpchain of a trajectory is the chain of
     * locations for the trajectory, removing any succesive
     * duplicates. */
    def jumpchain(): Dataset[(Int, Array[LocationPartition])] =
      cotraj.map(_.jumpchain)
        .withColumnRenamed("_1", "id").withColumnRenamed("_2", "jumpchain")
        .as[(Int, Array[LocationPartition])]

    /* Return a Dataset with the jumpchain times of the co-trajectory's
     * trajectories. The jumpchain times of a trajectory is a list of
     * times where each time represents the time the trajectory stays
     * in one particular location. */
    def jumpchainTimes(): Dataset[(Int, Array[Int])] = cotraj
      .map(_.jumpchainTimes)
      .withColumnRenamed("_1", "id").withColumnRenamed("_2", "jumpchainTimes")
      .as[(Int, Array[Int])]

    /* Return a Dataset with the transitions of the co-trajectory's
     * trajectories. The list of transition of a trajectory consists
     * of pairs of locations together with the time spent in the first
     * location before going to the second. */
    def transitions():
        Dataset[(LocationPartition, LocationPartition, Long, Double)] =
      cotraj.flatMap(_.transitions()._2)
        .groupBy("_1", "_2")
        .agg(count("_3").alias("count"), avg("_3").alias("time"))
        .withColumnRenamed("_1", "from")
        .withColumnRenamed("_2", "to")
        .as[(LocationPartition, LocationPartition, Long, Double)]

    /* Returns an enumeration of all the partitions occurring in the
     * co-trajectory. */
    def enumeratePartitions(): Dataset[(LocationPartition, BigInt)] =
      cotraj.flatMap(_.partitions)
        .distinct
        .rdd
        .zipWithIndex
        .toDS.withColumnRenamed("_1", "location").withColumnRenamed("_2", "id")
        .as[(LocationPartition, BigInt)]

    /* Return the list of all swaps to apply when performing Swapmob. This
     * methods assumes that no trajectory has several measurements in
     * the same time interval. This can be assured by partitioning the
     * trajectories with partitionDistinct instead of just partition. */
    def swaps(partitioning: Long): Dataset[Swap] =
      cotraj
        .measurements
        .groupBy("partition")
        .agg(collect_set($"id").alias("ids"))
        .filter(size($"ids") > 1)
        .as[(MeasurementPartition, Array[Int])]
        .map{case (m, ids) => Swap((m.time + 1L)*partitioning, ids)}
  }
}
