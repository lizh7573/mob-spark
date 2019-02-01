import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._

object SwapmobStatistics
{
  import SparkSessionHolder.spark.implicits._

  /* Return a dataset containing the number of swaps that each id
   * participate in. */
  def numSwaps(swaps: Dataset[Swap]): Dataset[(Int, BigInt)] =
    swaps
      .flatMap(_.ids)
      .withColumnRenamed("value", "id")
      .groupBy("id")
      .count
      .as[(Int, BigInt)]

  /* Return a dataset containing an array of the times for the swaps for
   * each id. */
  def swapTimes(swaps: Dataset[Swap]): Dataset[(Int, Array[Int])] =
    swaps
      .flatMap(s => s.ids.map(id => (id, s.time)))
      .withColumnRenamed("_1", "id")
      .groupBy("id")
      .agg(collect_list("_2").alias("times"))
      .as[(Int, Array[Int])]
}
