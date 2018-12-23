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
  }
}
