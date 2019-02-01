import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import SparkSessionHolder.spark.implicits._

object CoTrajectoryUtils {

  /* Takes a set of measurements with IDs and returns the
   * corresponding co-trajectory. */
  def getCoTrajectory(data: org.apache.spark.sql.Dataset[MeasurementID]):
      org.apache.spark.sql.Dataset[Trajectory] = data.groupBy("id")
    .agg(collect_set($"measurement").alias("measurements"))
    .as[Trajectory]
    .map(r => Trajectory(r.id, r.measurements.sortBy(_.time)))

  /* Takes a set of grid measurements with IDs and returns the
   * corresponding co-trajectory. */
  def getCoTrajectoryGrid(data: org.apache.spark.sql.Dataset[GridID]):
      org.apache.spark.sql.Dataset[TrajectoryGrid] = data.groupBy("id")
    .agg(collect_set($"grid").alias("grids"))
    .as[TrajectoryGrid]
    .map(r => TrajectoryGrid(r.id, r.grids.sortBy(_.time)))

  /* An implicit class for a co-trajectory consisting of measurements.
   * Most of the co-trajectory specific methods are implemented
   * here.*/
  implicit class CoTrajectory(cotraj:
      org.apache.spark.sql.Dataset[Trajectory]) {

    /* Return a Dataset of all measurements in the co-trajectory. */
    def measurements(): org.apache.spark.sql.Dataset[MeasurementID] =
      cotraj.flatMap((r => r.measurements.map(m => MeasurementID(r.id, m))))

    /* Split a co-trajectory into several co-trajectories where each one
     * only contains measurements for a single date. Returns an array
     * of tuples where the first element corresponds to the Unix-time
     * of the beginning of the date and the second element to the
     * corresponding co-trajectory. */
    def splitByDate(): Array[(Long, org.apache.spark.sql.Dataset[Trajectory])] = {
      val grouped = cotraj.flatMap(_.splitByDate)
      val dates = grouped.map(_._1).distinct.collect

      dates.map(date => (date, grouped.filter(_._1 == date).map(_._2)))
    }

    /* Return a Dataset with the jumpchains of the co-trajectory's
     * trajectories. The jumpchain of a trajectory is the chain of
     * locations for the trajectory, removing any succesive
     * duplicates. */
    def jumpchain(partitioning: Double):
        org.apache.spark.sql.Dataset[(Int, Array[LocationPartition])] =
      cotraj.map(_.jumpchain(partitioning))
        .withColumnRenamed("_1", "id").withColumnRenamed("_2", "jumpchain")
        .as[(Int, Array[LocationPartition])]

    /* Return a Dataset with the jumpchain times of the co-trajectory's
     * trajectories. The jumpchain times of a trajectory is a list of
     * times where each time represents the time the trajectory stays
     * in one particular location. */
    def jumpchainTimes(partitioning: Double):
        org.apache.spark.sql.Dataset[(Int, Array[Int])] =
      cotraj.map(_.jumpchainTimes(partitioning))
        .withColumnRenamed("_1", "id").withColumnRenamed("_2", "jumpchainTimes")
        .as[(Int, Array[Int])]

    /* Return a Dataset with the transitions of the co-trajectory's
     * trajectories. The list of transition of a trajectory consists
     * of pairs of locations together with the time spent in the first
     * location before going to the second. */
    def transitions(partitioning: Double):
        org.apache.spark.sql.Dataset[(LocationPartition, LocationPartition,
          Long, Double)] =
      cotraj.flatMap(_.transitions(partitioning)._2)
        .groupBy("_1", "_2")
        .agg(count("_3").alias("count"), avg("_3").alias("time"))
        .withColumnRenamed("_1", "from")
        .withColumnRenamed("_2", "to")
        .as[(LocationPartition, LocationPartition, Long, Double)]

    /* Returns an enumeration of all the partitions occurring in the
     * co-trajectory. */
    def enumeratePartitions(partitioning: Double):
        org.apache.spark.sql.Dataset[(LocationPartition, BigInt)] =
      cotraj.flatMap(_.measurements.map(_.location.partition(partitioning)))
        .distinct
        .rdd
        .zipWithIndex
        .toDS.withColumnRenamed("_1", "location").withColumnRenamed("_2", "id")
        .as[(LocationPartition, BigInt)]

    /* Return a map matched version of the co-trajectory. Every trajectory
     * is replaced with its map matched version. */
    def mapMatch(): org.apache.spark.sql.Dataset[Trajectory] =
      cotraj.mapPartitions{partition =>
        val mm = GraphHopperHelper.getMapMatcher

        partition.map(_.mapMatch(mm))
      }
  }

  /* An implicit class for a co-trajectory consisting of grid
   * measurements. Most of the co-trajectory specific methods are
   * implemented here.*/
  implicit class CoTrajectoryGrid(cotraj:
      org.apache.spark.sql.Dataset[TrajectoryGrid]) {

    /* Return a Dataset of all grids in the co-trajectory. */
    def measurements(): org.apache.spark.sql.Dataset[GridID] =
      cotraj.flatMap((r => r.grids.map(m => GridID(r.id, m))))

    /* Return a Dataset of with the jumpchains of the co-trajectorys
     * trajectories. The jumpchain of a trajectory is the chain of
     * locations for the trajectory, removing any succesive
     * duplicates. */
    def jumpchain():
        org.apache.spark.sql.Dataset[(Int, Array[LocationPartition])] =
      cotraj.map(_.jumpchain)
        .withColumnRenamed("_1", "id").withColumnRenamed("_2", "jumpchain")
        .as[(Int, Array[LocationPartition])]

    /* Return a Dataset with the jumpchain times of the co-trajectory's
     * trajectories. The jumpchain times of a trajectory is a list of
     * times where each time represents the time the trajectory stays
     * in one particular location. */
    def jumpchainTimes():
        org.apache.spark.sql.Dataset[(Int, Array[Int])] =
      cotraj.map(_.jumpchainTimes)
        .withColumnRenamed("_1", "id").withColumnRenamed("_2", "jumpchainTimes")
        .as[(Int, Array[Int])]

    /* Return a Dataset with the transitions of the co-trajectory's
     * trajectories. The list of transition of a trajectory consists
     * of pairs of locations together with the time spent in the first
     * location before going to the second. */
    def transitions():
      org.apache.spark.sql.Dataset[(LocationPartition, LocationPartition,
        Long, Double)] =
    cotraj.flatMap(_.transitions()._2)
      .groupBy("_1", "_2")
      .agg(count("_3").alias("count"), avg("_3").alias("time"))
      .withColumnRenamed("_1", "from")
      .withColumnRenamed("_2", "to")
      .as[(LocationPartition, LocationPartition, Long, Double)]

    /* Returns an enumeration of all the partitions occurring in the
     * co-trajectory. */
    def enumeratePartitions():
        org.apache.spark.sql.Dataset[(LocationPartition, BigInt)] =
      cotraj.flatMap(_.grids)
        .distinct
        .rdd
        .zipWithIndex
        .toDS.withColumnRenamed("_1", "location").withColumnRenamed("_2", "id")
        .as[(LocationPartition, BigInt)]

    /* Return the list of all swaps to apply when performing Swapmob. This
     * methods assumes that no trajectory has several measurements in
     * the same time interval. This can be assured by partitioning the
     * trajectories with partitionDistinct instead of just partition. */
    def swaps(partitioning: Long):
        org.apache.spark.sql.Dataset[Swap] =
      cotraj
        .measurements
        .groupBy("grid")
        .agg(collect_set($"id").alias("ids"))
        .filter(size($"ids") > 1)
        .as[(Grid, Array[Int])]
        .map{case (grid, ids) => Swap((grid.time + 1L)*partitioning, ids)}
  }
}
