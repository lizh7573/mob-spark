import org.apache.spark.sql.Dataset

object Preprocess {

  import SparkSessionHolder.spark.implicits._

  val boxBeijing = Array((115.0, 117.0), (39.0, 41.0))
  val boxSanFransisco = Array((-122.449, -122.397), (37.747, 37.772))

  /*
   * Keep only measurements whose location is inside the given box.
   */
  def keepBox(data: Dataset[MeasurementID], box: Array[(Double, Double)]):
      Dataset[MeasurementID] = data.filter(_.measurement.location.isInBox(box))

  /*
   *  Drop all measurements whose id occurs less than the given
   *  number of type in the whole dataset.
   */
  def dropShort(data: Dataset[MeasurementID], limit: Int):
      Dataset[MeasurementID] = {

    val counts = data.groupBy("id").count

    val joined = data.join(counts, "id")

    val filtered = joined.filter($"count" >= limit)

    return filtered.select($"id", $"measurement").as[MeasurementID]
  }
}
