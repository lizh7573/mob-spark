import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class Swap(grid: Grid, ids: Array[Int])

object Swapmob {

  import SparkSessionHolder.spark.implicits._

  type Measurements = org.apache.spark.sql.Dataset[MeasurementID]
  type Grids = org.apache.spark.sql.Dataset[GridID]
  type Swaps = org.apache.spark.sql.Dataset[Swap]

  val partitioning: (Long, Double) = (60, 0.001)

  def partition(data: Measurements, partitioning: (Long, Double)): Grids = {
    return data.map(_.partition(partitioning))
  }

  def swaps(data: Grids): Swaps = {
    return data
      .groupBy("grid")
      .agg(collect_set($"id").alias("ids"))
      .as[Swap]
      .filter(_.ids.length > 1)
  }


}
