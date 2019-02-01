import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SwapmobStatistics
{
  import SparkSessionHolder.spark.implicits._

  type Swaps = org.apache.spark.sql.Dataset[Swap]

  def numSwaps(swaps: Swaps):
      org.apache.spark.sql.Dataset[(Int, BigInt)] = {
    return swaps
      .flatMap(s => s.ids).withColumnRenamed("value", "id")
      .groupBy("id")
      .count.as[(Int, BigInt)]
  }

  def swapTimes(swaps: Swaps):
      org.apache.spark.sql.Dataset[(Int, Array[Int])] = {
    return swaps
      .flatMap(s => s.ids.map(id => (id, s.time)))
      .withColumnRenamed("_1", "id")
      .groupBy("id")
      .agg(collect_list("_2").alias("times"))
      .as[(Int, Array[Int])]
  }
}
