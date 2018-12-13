/* SimpleApp.scala */
import org.apache.spark.sql.SparkSession

object Preprocess {

  import SparkSessionHolder.spark.implicits._

  val box = ((115, 117), (39, 41))
  /**
    * Keep only measurements whose location is inside the given box.
   */
  def keepBox(data: org.apache.spark.sql.Dataset[Measurement],
    box: ((Int, Int), (Int, Int))): org.apache.spark.sql.Dataset[Measurement] = {
    // Keep only measurements inside the given box
    val ((lowx1, uppx1), (lowx2, uppx2)) = box

    return data.filter{m => m.x(0) >= lowx1 && m.x(0) <= uppx1 &&
      m.x(1) >= lowx2 && m.x(1) <= uppx2}
  }

  /**
    *  Drop all measurements whose id occurs less than the given
    *  number of type in the whole dataset.
    */
  def dropShort(data: org.apache.spark.sql.Dataset[Measurement],
    limit: Int): org.apache.spark.sql.Dataset[Measurement] = {

    val counts = data.groupBy("id").count

    val joined = data.join(counts, "id")

    val filtered = joined.filter($"count" >= limit)

    return filtered.select($"id", $"time", $"x").as[Measurement]
  }
}
