/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession

object Preprocess {

  import SparkSessionHolder.spark.implicits._

  val box = Array((115.0, 117.0), (39.0, 41.0))
  /**
    * Keep only measurements whose location is inside the given box.
   */
  def keepBox(data: org.apache.spark.sql.Dataset[MeasurementID],
    box: Array[(Double, Double)]): org.apache.spark.sql.Dataset[MeasurementID] = {
    return data.filter(_.measurement.location.isInBox(box))
  }

  /**
    *  Drop all measurements whose id occurs less than the given
    *  number of type in the whole dataset.
    */
  def dropShort(data: org.apache.spark.sql.Dataset[MeasurementID],
    limit: Int): org.apache.spark.sql.Dataset[MeasurementID] = {

    val counts = data.groupBy("id").count

    val joined = data.join(counts, "id")

    val filtered = joined.filter($"count" >= limit)

    return filtered.select($"id", $"measurement").as[MeasurementID]
  }
}
