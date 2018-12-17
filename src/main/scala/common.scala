import org.apache.spark.sql.SparkSession

object SparkSessionHolder {
  val spark = SparkSession.builder.appName("Swapmob").getOrCreate()
}
