import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import SparkSessionHolder.spark.implicits._

object CoTrajectoryUtils {

  def getCoTrajectory(data: org.apache.spark.sql.Dataset[MeasurementID]):
      org.apache.spark.sql.Dataset[Trajectory] = data.groupBy("id")
    .agg(collect_set($"measurement").alias("measurements"))
    .as[Trajectory]
    .map(r => Trajectory(r.id, r.measurements.sortBy(_.time)))

  def getCoTrajectoryGrid(data: org.apache.spark.sql.Dataset[GridID]):
      org.apache.spark.sql.Dataset[TrajectoryGrid] = data.groupBy("id")
    .agg(collect_set($"grid").alias("grids"))
    .as[TrajectoryGrid]
    .map(r => TrajectoryGrid(r.id, r.grids.sortBy(_.time)))

  implicit class CoTrajectory(cotraj:
      org.apache.spark.sql.Dataset[Trajectory]) {

    def jumpchain(partitioning: Double):
        org.apache.spark.sql.Dataset[(Int, Array[LocationPartition])] =
      cotraj.map(_.jumpchain(partitioning))
        .withColumnRenamed("_1", "id").withColumnRenamed("_2", "jumpchain")
        .as[(Int, Array[LocationPartition])]

    def jumpchainTimes(partitioning: Double):
        org.apache.spark.sql.Dataset[(Int, Array[Int])] =
      cotraj.map(_.jumpchainTimes(partitioning))
        .withColumnRenamed("_1", "id").withColumnRenamed("_2", "jumpchainTimes")
        .as[(Int, Array[Int])]

    def transitions(partitioning: Double):
        org.apache.spark.sql.Dataset[(LocationPartition, LocationPartition,
          Long, Double)] =
      cotraj.flatMap(_.transitions(partitioning)._2)
        .groupBy("_1", "_2")
        .agg(count("_3").alias("count"), avg("_3").alias("time"))
        .withColumnRenamed("_1", "from")
        .withColumnRenamed("_2", "to")
        .as[(LocationPartition, LocationPartition, Long, Double)]

    def enumeratePartitions(partitioning: Double):
        org.apache.spark.sql.Dataset[(LocationPartition, BigInt)] =
      cotraj.flatMap(_.measurements.map(_.location.partition(partitioning)))
        .distinct
        .rdd
        .zipWithIndex
        .toDS.withColumnRenamed("_1", "location").withColumnRenamed("_2", "id")
        .as[(LocationPartition, BigInt)]
  }

  implicit class CoTrajectoryGrid(cotraj:
      org.apache.spark.sql.Dataset[TrajectoryGrid]) {

    def jumpchain():
        org.apache.spark.sql.Dataset[(Int, Array[LocationPartition])] =
      cotraj.map(_.jumpchain)
        .withColumnRenamed("_1", "id").withColumnRenamed("_2", "jumpchain")
        .as[(Int, Array[LocationPartition])]

    def jumpchainTimes():
        org.apache.spark.sql.Dataset[(Int, Array[Int])] =
      cotraj.map(_.jumpchainTimes)
        .withColumnRenamed("_1", "id").withColumnRenamed("_2", "jumpchainTimes")
        .as[(Int, Array[Int])]

    def transitions():
      org.apache.spark.sql.Dataset[(LocationPartition, LocationPartition,
        Long, Double)] =
    cotraj.flatMap(_.transitions()._2)
      .groupBy("_1", "_2")
      .agg(count("_3").alias("count"), avg("_3").alias("time"))
      .withColumnRenamed("_1", "from")
      .withColumnRenamed("_2", "to")
      .as[(LocationPartition, LocationPartition, Long, Double)]

    def enumeratePartitions():
        org.apache.spark.sql.Dataset[(LocationPartition, BigInt)] =
      cotraj.flatMap(_.grids)
        .distinct
        .rdd
        .zipWithIndex
        .toDS.withColumnRenamed("_1", "location").withColumnRenamed("_2", "id")
        .as[(LocationPartition, BigInt)]
  }
}
