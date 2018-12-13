// Holds id, time and location for one measurement
case class Measurement(id: Int, time: Int, x: Array[Double])
import org.apache.spark.sql.SparkSession

object SparkSessionHolder {
  val spark = SparkSession.builder.appName("Swapmob").getOrCreate()
}
