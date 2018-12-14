import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

case class Swap(grid: Grid, ids: Array[Int])

object Swapmob {

  import SparkSessionHolder.spark.implicits._

  type Measurements = org.apache.spark.sql.Dataset[MeasurementID]
  type Grids = org.apache.spark.sql.Dataset[GridID]
  type Swaps = org.apache.spark.sql.Dataset[Swap]

  val partitioning: (Double, Double) = (60, 0.001)

  def partition(data: Measurements, partitioning: (Double, Double)): Grids = {
    val (timePartitioning, xPartitioning) = partitioning

    return data.map{
      case MeasurementID(id, Measurement(time, x)) =>
        val timePartition = (time/timePartitioning).toInt
        val xPartition = Array((x(0)/xPartitioning).toInt,
          (x(1)/xPartitioning).toInt)

        GridID(id, Grid(timePartition, xPartition))
    }
  }

  def swaps(data: Grids): Swaps = {
    return data
      .groupBy("grid")
      .agg(collect_set($"id").alias("ids"))
      .as[Swap]
      .filter(_.ids.length > 1)
  }


}
